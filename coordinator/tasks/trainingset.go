// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package tasks

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/featureform/logging"

	"github.com/featureform/fferr"
	"github.com/featureform/metadata"
	"github.com/featureform/provider"
	pl "github.com/featureform/provider/location"
	pc "github.com/featureform/provider/provider_config"
	ps "github.com/featureform/provider/provider_schema"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/runner"
	"github.com/featureform/scheduling"
)

type TrainingSetTask struct {
	BaseTask
}

func (t *TrainingSetTask) Run() error {
	logger := t.logger.With("%#v\n", t.taskDef.Target)
	ctx := logger.AttachToContext(context.Background())
	nv, ok := t.taskDef.Target.(scheduling.NameVariant)
	if !ok {
		logger.Errorw("cannot create a training set from target type", "type", t.taskDef.TargetType)
		return fferr.NewInternalErrorf("cannot create a source from target type: %s", t.taskDef.TargetType)
	}
	tsId := metadata.ResourceID{Name: nv.Name, Variant: nv.Variant, Type: metadata.TRAINING_SET_VARIANT}
	logger = t.logger.WithResource(logging.TrainingSetVariant, tsId.Name, tsId.Variant).
		With("task_id", t.taskDef.TaskId, "task_run_id", t.taskDef.ID)
	logger.Info("Running training set job on resource")
	if err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Starting Training Set Creation..."); err != nil {
		return err
	}

	if t.isDelete {
		logger.Debugw("Handling deletion")
		return t.handleDeletion(ctx, tsId, logger)
	}

	ts, err := t.metadata.GetTrainingSetVariant(ctx, metadata.NameVariant{Name: nv.Name, Variant: nv.Variant})
	if err != nil {
		logger.Errorw("Failed to get training set variant", "error", err)
		return err
	}

	store, getStoreErr := getOfflineStore(ctx, t.BaseTask, t.metadata, ts, logger)
	if getStoreErr != nil {
		return getStoreErr
	}
	logger.Debugw("Training set offline store", "type", fmt.Sprintf("%T", store))
	defer func(store provider.OfflineStore) {
		err := store.Close()
		if err != nil {
			logger.Errorf("could not close offline store: %v", err)
		}
	}(store)

	providerResID := provider.ResourceID{Name: nv.Name, Variant: nv.Variant, Type: provider.TrainingSet}

	_, getTsError := store.GetTrainingSet(providerResID)
	var datasetNotFoundError *fferr.DatasetNotFoundError
	tsNotFound := errors.As(getTsError, &datasetNotFoundError)
	if tsNotFound {
		logger.Debugw("Training set not found in store, creating new training set", "resource_id", providerResID)
	} else {
		if getTsError != nil {
			logger.Errorw("Failed to get training set", "error", getTsError)
			return getTsError
		}
		logger.Debugw("Training set already exists")
		return nil
	}

	if err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Waiting for dependencies to complete..."); err != nil {
		return err
	}
	featureSourceMappings := make([]provider.SourceMapping, len(ts.Features()))
	features := ts.Features()
	featureList := make([]provider.ResourceID, len(features))
	for i, feature := range features {
		featureList[i] = provider.ResourceID{Name: feature.Name, Variant: feature.Variant, Type: provider.Feature}
		featureResource, err := t.metadata.GetFeatureVariant(ctx, feature)
		if err != nil {
			logger.Errorw("Failed to get feature variant", "error", err)
			return err
		}
		sourceNameVariant := featureResource.Source()
		sourceVariant, err := t.awaitPendingSource(sourceNameVariant)
		if err != nil {
			logger.Errorw("Failed to wait on pending feature source", "error", err)
			return err
		}
		_, err = t.AwaitPendingFeature(ctx, metadata.NameVariant{Name: feature.Name, Variant: feature.Variant})
		if err != nil {
			logger.Errorw("Failed to wait on pending feature variant", "error", err)
			return err
		}
		featureSourceMappings[i], err = t.getFeatureSourceMapping(ctx, featureResource, sourceVariant)
		if err != nil {
			logger.Errorw("Failed to get feature source mapping", "error", err)
			return err
		}
	}

	lagFeatures := ts.LagFeatures()
	lagFeaturesList := make([]provider.LagFeatureDef, len(lagFeatures))
	for i, lagFeature := range lagFeatures {
		lagFeaturesList[i] = provider.LagFeatureDef{
			FeatureName:    lagFeature.GetFeature(),
			FeatureVariant: lagFeature.GetVariant(),
			LagName:        lagFeature.GetName(),
			LagDelta:       lagFeature.GetLag().AsDuration(), // see if need to convert it to time.Duration
		}
	}

	label, err := ts.FetchLabel(t.metadata, ctx)
	if err != nil {
		logger.Errorw("Failed to fetch label", "error", err)
		return err
	}
	labelSourceNameVariant := label.Source()
	_, err = t.awaitPendingSource(labelSourceNameVariant)
	if err != nil {
		logger.Errorw("Failed to wait on pending label source", "error", err)
		return err
	}
	label, err = t.AwaitPendingLabel(ctx, metadata.NameVariant{Name: label.Name(), Variant: label.Variant()})
	if err != nil {
		logger.Errorw("Failed to wait on pending label variant", "error", err)
		return err
	}
	labelSourceMapping, err := t.getLabelSourceMapping(ctx, label)
	if err != nil {
		logger.Errorw("Failed to get label source mapping", "error", err)
		return err
	}
	resourceSnowflakeConfig := &metadata.ResourceSnowflakeConfig{}
	if store.Type() == pt.SnowflakeOffline {
		tempConfig, err := ts.ResourceSnowflakeConfig()
		if err != nil {
			logger.Errorw("Failed to get resource snowflake config", "error", err)
			return err
		}
		resourceSnowflakeConfig = tempConfig
	}

	trainingSetDef := provider.TrainingSetDef{
		ID:                      providerResID,
		Label:                   provider.ResourceID{Name: label.Name(), Variant: label.Variant(), Type: provider.Label},
		LabelSourceMapping:      labelSourceMapping,
		Features:                featureList,
		FeatureSourceMappings:   featureSourceMappings,
		LagFeatures:             lagFeaturesList,
		ResourceSnowflakeConfig: resourceSnowflakeConfig,
		Type:                    ts.TrainingSetType(),
	}
	logger.Debugw("Successfully created training set def", "def", trainingSetDef)
	return t.runTrainingSetJob(trainingSetDef, store)
}

func (t *TrainingSetTask) handleDeletion(ctx context.Context, tsId metadata.ResourceID, logger logging.Logger) error {
	logger.Info("Deleting training set", "resource_id", tsId)
	tsToDelete, err := t.metadata.GetStagedForDeletionTrainingSetVariant(ctx,
		metadata.NameVariant{
			Name:    tsId.Name,
			Variant: tsId.Variant,
		}, logger)
	if err != nil {
		logger.Errorw("Failed to get training set to delete", "error", err)
		return err
	}

	trainingSetTable, err := ps.ResourceToTableName(provider.TrainingSet.String(), tsId.Name, tsId.Variant)
	if err != nil {
		logger.Errorw("Failed to get table name for training set", "error", err)
		return err
	}
	logger.Debugw("Deleting training set at location", "location", trainingSetTable)
	store, getStoreErr := getOfflineStore(ctx, t.BaseTask, t.metadata, tsToDelete, logger)
	if getStoreErr != nil {
		logger.Errorw("Failed to get store", "error", getStoreErr)
		return getStoreErr
	}

	trainingSetLocation := pl.NewSQLLocation(trainingSetTable)

	if err := store.Delete(trainingSetLocation); err != nil {
		var notFoundErr *fferr.DatasetNotFoundError
		if errors.As(err, &notFoundErr) {
			logger.Infow("Table doesn't exist at location, continuing...", "location", trainingSetLocation)
			// continue
		} else {
			logger.Errorw("Failed to delete training set", "error", err)
			return err
		}
	}
	if err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Training Set deleted..."); err != nil {
		logger.Errorw("Unable to add run log", "error", err)
		return err
	}

	logger.Debugw("Finalizing delete")
	if err := t.metadata.FinalizeDelete(ctx, tsId); err != nil {
		logger.Errorw("Failed to finalize delete", "error", err)
		return err
	}

	return nil
}

func (t *TrainingSetTask) getLabelSourceMapping(ctx context.Context, label *metadata.LabelVariant) (provider.SourceMapping, error) {
	logger := t.logger.With("Label", label.Name(), "variant", label.Variant())
	labelProvider, err := label.FetchProvider(t.metadata, ctx)
	if err != nil {
		logger.Errorw("could not fetch label provider", "error", err)
		return provider.SourceMapping{}, err
	}
	logger.Debugw("Label Provider", "type", labelProvider.Type())
	switch pt.Type(labelProvider.Type()) {
	case pt.SnowflakeOffline, pt.BigQueryOffline, pt.PostgresOffline:
		logger.Debugw("Getting label source mapping from source ...")
		return t.getLabelSourceMappingFromSource(label, labelProvider, ctx)
	default:
		logger.Debugw("Getting label source mapping from resource table ...")
		return t.getLabelSourceMappingFromResourceTable(label, labelProvider, ctx)
	}
}

func (t *TrainingSetTask) getLabelSourceMappingFromResourceTable(label *metadata.LabelVariant, labelProvider *metadata.Provider, ctx context.Context) (provider.SourceMapping, error) {
	logger := t.logger.With("source", label.Source())
	logger.Debugw("Getting label source mapping from resource table ...")
	if err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Getting label source mapping from resource table ..."); err != nil {
		return provider.SourceMapping{}, err
	}
	labelSource, err := ps.ResourceToTableName(provider.Label.String(), label.Name(), label.Variant())
	if err != nil {
		logger.Errorw("could not get label source table name", "error", err)
		return provider.SourceMapping{}, err
	}
	labelLocation, err := t.getResourceLocation(labelProvider, labelSource)
	if err != nil {
		logger.Errorw("could not get label location", "error", err)
		return provider.SourceMapping{}, err
	}
	logger.Debugw("Label resource location", "location", labelLocation)
	srcMapping := provider.SourceMapping{
		Source:         labelSource,
		ProviderType:   pt.Type(labelProvider.Type()),
		ProviderConfig: labelProvider.SerializedConfig(),
		Location:       labelLocation,
	}

	lblEntityMappings, err := label.Location()
	if err != nil {
		logger.Errorw("could not get label location", "error", err)
		return provider.SourceMapping{}, err
	}
	// Given a label will always be created as a resource table, which uses stable naming conventions for columns,
	// we can hardcode the column names for both single-entity and multi-entity labels to "value" for the VALUE column
	// and "ts" for the TS column. For multi-entity labels, whereas the entity column for single-entity labels is simply
	// "entity", we will use "entity_<entity_name>" for multi-entity labels, so this translation is done here prior to
	// running the training set job so that all source mappings are consistent prior to being use in CreateTrainingSet.
	//**NOTE**: if we used label.LocationColumns() here, we would get
	// the column names from the source table, and not the resource table.
	logger.Debugw("Label entity mappings", "mappings", lblEntityMappings)
	srcMapping.EntityMappings = &metadata.EntityMappings{
		Mappings:        make([]metadata.EntityMapping, len(lblEntityMappings.Mappings)),
		ValueColumn:     "value",
		TimestampColumn: "ts",
	}
	for i, mapping := range lblEntityMappings.Mappings {
		entityCol := fmt.Sprintf("entity_%s", mapping.Name)
		if label.IsLegacyLocation() {
			entityCol = "entity"
		}
		srcMapping.EntityMappings.Mappings[i] = metadata.EntityMapping{
			Name:         mapping.Name,
			EntityColumn: entityCol,
		}
	}
	logger.Debugw("Label source mapping", "mapping", srcMapping)
	return srcMapping, nil
}

func (t *TrainingSetTask) getLabelSourceMappingFromSource(label *metadata.LabelVariant, labelProvider *metadata.Provider, ctx context.Context) (provider.SourceMapping, error) {
	logger := t.logger.With("source", label.Source())
	logger.Debugw("Getting label source mapping from source ...")
	if err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Getting label source mapping from source ..."); err != nil {
		return provider.SourceMapping{}, err
	}
	source, err := label.FetchSource(t.metadata, ctx)
	if err != nil {
		logger.Errorw("could not fetch source", "error", err)
		return provider.SourceMapping{}, err
	}
	var location pl.Location
	switch {
	case source.IsPrimaryData():
		logger.Debugw("Getting primary location ...")
		location, err = source.GetPrimaryLocation()
		if err != nil {
			logger.Errorw("could not get primary location", "source", source.Definition(), "error", err)
			return provider.SourceMapping{}, err
		}
	case source.IsTransformation():
		logger.Debugw("Getting transformation location ...")
		location, err = source.GetTransformationLocation()
		if err != nil {
			logger.Errorw("could not get transformation location", "source", source.Definition(), "error", err)
			return provider.SourceMapping{}, err
		}
	default:
		logger.Errorw("source is neither primary data nor transformation", "definition", source.Definition())
		return provider.SourceMapping{}, fferr.NewInternalErrorf("unsupported source type: %T", source.Definition())
	}
	lblEntityMappings, err := label.Location()
	if err != nil {
		logger.Errorw("could not get label location", "label", label.Name(), "variant", label.Variant(), "error", err)
		return provider.SourceMapping{}, err
	}
	logger.Debugw("Successfully got label source mapping from source", "entity_mappings", lblEntityMappings, "loc", location)
	return provider.SourceMapping{
		ProviderType:   pt.Type(labelProvider.Type()),
		ProviderConfig: labelProvider.SerializedConfig(),
		Location:       location,
		EntityMappings: &lblEntityMappings,
	}, nil
}

// **NOTE**: Given a feature's provider will always be an online store, we actually need its source's provider to grab the data for the training set.
func (t *TrainingSetTask) getFeatureSourceMapping(ctx context.Context, feature *metadata.FeatureVariant, source *metadata.SourceVariant) (provider.SourceMapping, error) {
	logger := t.logger.With("Feature", feature.Name(), "variant", feature.Variant())
	sourceProvider, err := source.FetchProvider(t.metadata, ctx)
	if err != nil {
		return provider.SourceMapping{}, err
	}
	logger.Debugw("Feature Source Provider", "type", sourceProvider.Type())
	featureSource, err := t.getFeatureSourceTableName(sourceProvider, feature)
	if err != nil {
		return provider.SourceMapping{}, err
	}
	featureLocation, err := t.getResourceLocation(sourceProvider, featureSource)
	if err != nil {
		return provider.SourceMapping{}, err
	}
	locCols := feature.LocationColumns()
	cols, isResourceCols := locCols.(metadata.ResourceVariantColumns)
	if !isResourceCols {
		logger.Errorf("expected ResourceVariantColumns, got %T", locCols)
		return provider.SourceMapping{}, fferr.NewInternalErrorf("expected ResourceVariantColumns, got %T", locCols)
	}
	logger.Debugw("Feature resource variant columns", "columns", cols)
	return provider.SourceMapping{
		Source:         featureSource,
		ProviderType:   pt.Type(sourceProvider.Type()),
		ProviderConfig: sourceProvider.SerializedConfig(),
		Location:       featureLocation,
		Columns:        &cols,
		EntityMappings: &metadata.EntityMappings{
			Mappings: []metadata.EntityMapping{
				{Name: feature.Entity(), EntityColumn: cols.Entity},
			},
		},
	}, nil
}

func (t *TrainingSetTask) AwaitPendingFeature(ctx context.Context, featureNameVariant metadata.NameVariant) (*metadata.FeatureVariant, error) {
	featureStatus := scheduling.PENDING
	for featureStatus != scheduling.READY {
		feature, err := t.metadata.GetFeatureVariant(ctx, featureNameVariant)
		if err != nil {
			return nil, err
		}
		featureStatus = feature.Status()
		if featureStatus == scheduling.FAILED {
			err := fferr.NewResourceFailedError(featureNameVariant.Name, featureNameVariant.Variant, fferr.FEATURE_VARIANT, fmt.Errorf("required feature is in a failed state"))
			return nil, err
		}
		if featureStatus == scheduling.READY {
			return feature, nil
		}
		time.Sleep(1 * time.Second)
	}
	return t.metadata.GetFeatureVariant(ctx, featureNameVariant)
}

func (t *TrainingSetTask) AwaitPendingLabel(ctx context.Context, labelNameVariant metadata.NameVariant) (*metadata.LabelVariant, error) {
	labelStatus := scheduling.PENDING
	for labelStatus != scheduling.READY {
		label, err := t.metadata.GetLabelVariant(ctx, labelNameVariant)
		if err != nil {
			return nil, err
		}
		labelStatus = label.Status()
		if labelStatus == scheduling.FAILED {
			err := fferr.NewResourceFailedError(labelNameVariant.Name, labelNameVariant.Variant, fferr.LABEL_VARIANT, fmt.Errorf("required label is in a failed state"))
			return nil, err
		}
		if labelStatus == scheduling.READY {
			return label, nil
		}
		time.Sleep(1 * time.Second)
	}
	return t.metadata.GetLabelVariant(ctx, labelNameVariant)
}

// TODO: (Erik) expand to handle other provider types and fully qualified table names (i.e. with database and schema)
func (t *TrainingSetTask) getResourceLocation(provider *metadata.Provider, tableName string) (pl.Location, error) {
	var location pl.Location
	var err error
	// TODO: Handle this in a generic way.
	switch pt.Type(provider.Type()) {
	case pt.SnowflakeOffline:
		config := pc.SnowflakeConfig{}
		if err := config.Deserialize(provider.SerializedConfig()); err != nil {
			return nil, err
		}
		// TODO: (Erik) determine if we want to use the Catalog location instead of SQL location; technically,
		// Snowflake references tables in a catalog no differently than it does other table types.
		location = pl.NewFullyQualifiedSQLLocation(config.Database, config.Schema, tableName)
	case pt.BigQueryOffline:
		config := pc.BigQueryConfig{}
		if err := config.Deserialize(provider.SerializedConfig()); err != nil {
			return nil, err
		}
		location = pl.NewFullyQualifiedSQLLocation(config.ProjectId, config.DatasetId, tableName)
	case pt.PostgresOffline:
		config := pc.PostgresConfig{}
		if err := config.Deserialize(provider.SerializedConfig()); err != nil {
			return nil, err
		}
		location = pl.NewFullyQualifiedSQLLocation(config.Database, config.Schema, tableName)
	default:
		t.logger.Errorf("unsupported provider type: %s", provider.Type())
	}
	return location, err
}

func (t *TrainingSetTask) getFeatureSourceTableName(p *metadata.Provider, feature *metadata.FeatureVariant) (string, error) {
	var resourceType provider.OfflineResourceType
	switch pt.Type(p.Type()) {
	case pt.SnowflakeOffline, pt.BigQueryOffline, pt.PostgresOffline:
		return t.getSourceTableNameForNonMaterializedProviders(feature)
	case pt.MemoryOffline, pt.MySqlOffline, pt.ClickHouseOffline, pt.RedshiftOffline, pt.SparkOffline, pt.K8sOffline:
		resourceType = provider.Feature
	default:
		t.logger.Errorw("unsupported provider type", "type", p.Type())
		return "", fferr.NewInternalErrorf("unsupported provider type: %s", p.Type())
	}

	return ps.ResourceToTableName(resourceType.String(), feature.Name(), feature.Variant())
}

// getSourceTableNameForNonMaterializedProviders returns the fully qualified table name for a feature's source variant.
// **NOTE:** In the past, we used the feature materialization as the source of a feature for a training set;
// however, this was incorrect because that data set will only ever have one row per entity, and in many cases,
// we need all rows for a given entity to correctly create a training set. Therefore, we now use the source variant
// as the source of a feature and allow the training set query to determine how to handle the source data based on
// the presence/absence of timestamps in both the features and label.
func (t *TrainingSetTask) getSourceTableNameForNonMaterializedProviders(feature *metadata.FeatureVariant) (string, error) {
	sourceNv := feature.Source()
	sv, err := t.metadata.GetSourceVariant(context.TODO(), sourceNv)
	if err != nil {
		t.logger.Errorw("could not get source variant", "name_variant", sourceNv, "err", err)
		return "", err
	}
	if sv.IsPrimaryData() {
		loc, err := sv.GetPrimaryLocation()
		if err != nil {
			t.logger.Errorw("could not get primary location", "source", sv.Definition(), "err", err)
			return "", err
		}
		return loc.Location(), nil
	} else if sv.IsTransformation() {
		loc, err := sv.GetTransformationLocation()
		if err != nil {
			return "", err
		}
		return loc.Location(), nil
	} else {
		return "", fferr.NewInternalErrorf("unsupported source type: %T", sv.Definition())
	}
}

func (t *TrainingSetTask) runTrainingSetJob(def provider.TrainingSetDef, offlineStore provider.OfflineStore) error {
	t.logger.Debugw("Running training set job", "id", def.ID)
	if err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Starting Training Set Creation..."); err != nil {
		t.logger.Errorw("Unable to add run log", "error", err)
		// We can continue without the run log
	}

	var trainingSetFnType func(provider.TrainingSetDef) error
	if t.isUpdate {
		trainingSetFnType = offlineStore.UpdateTrainingSet
	} else {
		trainingSetFnType = offlineStore.CreateTrainingSet
	}

	t.logger.Debugw("Running training set task")
	tsWatcher := &runner.SyncWatcher{
		ResultSync:  &runner.ResultSync{},
		DoneChannel: make(chan interface{}),
	}
	go func() {
		if err := trainingSetFnType(def); err != nil {
			t.logger.Errorw("Training set failed, ending watch", "error", err)
			tsWatcher.EndWatch(err)
			return
		}
		tsWatcher.EndWatch(nil)
	}()

	if err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Waiting for training set job to complete..."); err != nil {
		t.logger.Errorw("Unable to add run log", "error", err)
		// We can continue without the run log
	}

	t.logger.Infow("Waiting for training set job to complete")
	if err := tsWatcher.Wait(); err != nil {
		t.logger.Errorw("Training set job failed", "error", err)
		return err
	}

	t.logger.Infow("Training set job completed")

	if err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Training Set creation complete."); err != nil {
		t.logger.Errorw("Unable to add run log", "error", err)
		// We can continue without the run log
	}

	return nil
}
