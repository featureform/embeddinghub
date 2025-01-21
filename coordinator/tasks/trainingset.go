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
	"github.com/featureform/logging"
	"time"

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
	nv, ok := t.taskDef.Target.(scheduling.NameVariant)
	if !ok {
		return fferr.NewInternalErrorf("cannot create a source from target type: %s", t.taskDef.TargetType)
	}

	tsId := metadata.ResourceID{Name: nv.Name, Variant: nv.Variant, Type: metadata.TRAINING_SET_VARIANT}
	logger := t.logger.WithResource(logging.TrainingSetVariant, tsId.Name, tsId.Variant)
	logger.Info("Running training set job on resource")
	if err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Starting Training Set Creation..."); err != nil {
		return err
	}

	if err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Fetching Training Set configuration..."); err != nil {
		return err
	}

	if t.isDelete {
		return t.handleDeletion(tsId, logger)
	}

	ts, err := t.metadata.GetTrainingSetVariant(context.Background(), metadata.NameVariant{Name: nv.Name, Variant: nv.Variant})
	if err != nil {
		return err
	}

	store, getStoreErr := getStore(t.BaseTask, t.metadata, ts)
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
	if _, err := store.GetTrainingSet(providerResID); err == nil {
		return err
	}

	if err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Waiting for dependencies to complete..."); err != nil {
		return err
	}
	featureSourceMappings := make([]provider.SourceMapping, len(ts.Features()))
	features := ts.Features()
	featureList := make([]provider.ResourceID, len(features))
	for i, feature := range features {
		featureList[i] = provider.ResourceID{Name: feature.Name, Variant: feature.Variant, Type: provider.Feature}
		featureResource, err := t.metadata.GetFeatureVariant(context.Background(), feature)
		if err != nil {
			return err
		}
		sourceNameVariant := featureResource.Source()
		sourceVariant, err := t.awaitPendingSource(sourceNameVariant)
		if err != nil {
			return err
		}
		_, err = t.AwaitPendingFeature(metadata.NameVariant{Name: feature.Name, Variant: feature.Variant})
		if err != nil {
			return err
		}
		featureSourceMappings[i], err = t.getFeatureSourceMapping(featureResource, sourceVariant)
		if err != nil {
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

	label, err := ts.FetchLabel(t.metadata, context.Background())
	if err != nil {
		return err
	}
	labelSourceNameVariant := label.Source()
	_, err = t.awaitPendingSource(labelSourceNameVariant)
	if err != nil {
		return err
	}
	label, err = t.AwaitPendingLabel(metadata.NameVariant{Name: label.Name(), Variant: label.Variant()})
	if err != nil {
		return err
	}
	labelSourceMapping, err := t.getLabelSourceMapping(label)
	if err != nil {
		return err
	}
	resourceSnowflakeConfig := &metadata.ResourceSnowflakeConfig{}
	if store.Type() == pt.SnowflakeOffline {
		tempConfig, err := ts.ResourceSnowflakeConfig()
		if err != nil {
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
	}

	return t.runTrainingSetJob(trainingSetDef, store)
}

func (t *TrainingSetTask) handleDeletion(tsId metadata.ResourceID, logger logging.Logger) error {
	logger.Debugw("Deleting training set", "resource_id", tsId)
	tsToDelete, err := t.metadata.GetStagedForDeletionTrainingSetVariant(context.Background(),
		metadata.NameVariant{
			Name:    tsId.Name,
			Variant: tsId.Variant,
		})
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
	store, getStoreErr := getStore(t.BaseTask, t.metadata, tsToDelete)
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
	if err := t.metadata.FinalizeDelete(context.Background(), tsId); err != nil {
		return err
	}

	return nil
}

func (t *TrainingSetTask) getLabelSourceMapping(label *metadata.LabelVariant) (provider.SourceMapping, error) {
	logger := t.logger.With("Label", label.Name(), "variant", label.Variant())
	labelProvider, err := label.FetchProvider(t.metadata, context.Background())
	if err != nil {
		return provider.SourceMapping{}, err
	}
	logger.Debugw("Label Provider", "type", labelProvider.Type())
	labelSource, err := ps.ResourceToTableName(provider.Label.String(), label.Name(), label.Variant())
	if err != nil {
		return provider.SourceMapping{}, err
	}
	labelLocation, err := t.getResourceLocation(labelProvider, labelSource)
	if err != nil {
		return provider.SourceMapping{}, err
	}
	return provider.SourceMapping{
		Source:         labelSource,
		ProviderType:   pt.Type(labelProvider.Type()),
		ProviderConfig: labelProvider.SerializedConfig(),
		Location:       labelLocation,
		// Given a label will always be created as a resource table, which uses stable naming conventions for columns,
		// we can hardcode the column names here. **NOTE**: if we used label.LocationColumns() here, we would get
		// the column names from the source table, and not the resource table.
		Columns: metadata.ResourceVariantColumns{
			Entity: "entity",
			Value:  "value",
			TS:     "ts",
		},
	}, nil
}

// **NOTE**: Given a feature's provider will always be an online store, we actually need its source's provider to grab the data for the training set.
func (t *TrainingSetTask) getFeatureSourceMapping(feature *metadata.FeatureVariant, source *metadata.SourceVariant) (provider.SourceMapping, error) {
	logger := t.logger.With("Feature", feature.Name(), "variant", feature.Variant())
	sourceProvider, err := source.FetchProvider(t.metadata, context.Background())
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
		t.logger.Errorf("expected ResourceVariantColumns, got %T", locCols)
		return provider.SourceMapping{}, fferr.NewInternalErrorf("expected ResourceVariantColumns, got %T", locCols)
	}
	t.logger.Debugw("Feature resource variant columns", "columns", cols)
	return provider.SourceMapping{
		Source:         featureSource,
		ProviderType:   pt.Type(sourceProvider.Type()),
		ProviderConfig: sourceProvider.SerializedConfig(),
		Location:       featureLocation,
		Columns:        cols,
	}, nil
}

func (t *TrainingSetTask) AwaitPendingFeature(featureNameVariant metadata.NameVariant) (*metadata.FeatureVariant, error) {
	featureStatus := scheduling.PENDING
	for featureStatus != scheduling.READY {
		feature, err := t.metadata.GetFeatureVariant(context.Background(), featureNameVariant)
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
	return t.metadata.GetFeatureVariant(context.Background(), featureNameVariant)
}

func (t *TrainingSetTask) AwaitPendingLabel(labelNameVariant metadata.NameVariant) (*metadata.LabelVariant, error) {
	labelStatus := scheduling.PENDING
	for labelStatus != scheduling.READY {
		label, err := t.metadata.GetLabelVariant(context.Background(), labelNameVariant)
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
	return t.metadata.GetLabelVariant(context.Background(), labelNameVariant)
}

// TODO: (Erik) expand to handle other provider types and fully qualified table names (i.e. with database and schema)
func (t *TrainingSetTask) getResourceLocation(provider *metadata.Provider, tableName string) (pl.Location, error) {
	var location pl.Location
	var err error
	switch pt.Type(provider.Type()) {
	case pt.SnowflakeOffline:
		config := pc.SnowflakeConfig{}
		if err := config.Deserialize(provider.SerializedConfig()); err != nil {
			return nil, err
		}
		// TODO: (Erik) determine if we want to use the Catalog location instead of SQL location; technically,
		// Snowflake references tables in a catalog no differently than it does other table types.
		location = pl.NewSQLLocationWithDBSchemaTable(config.Database, config.Schema, tableName)
	default:
		t.logger.Errorw("unsupported provider type: %s", provider.Type())
	}
	return location, err
}

func (t *TrainingSetTask) getFeatureSourceTableName(p *metadata.Provider, feature *metadata.FeatureVariant) (string, error) {
	var resourceType provider.OfflineResourceType
	switch pt.Type(p.Type()) {
	case pt.SnowflakeOffline:
		return t.getSourceTableNameForSnowflake(feature)
	case pt.MemoryOffline, pt.MySqlOffline, pt.PostgresOffline, pt.ClickHouseOffline, pt.RedshiftOffline, pt.SparkOffline, pt.BigQueryOffline, pt.K8sOffline:
		resourceType = provider.Feature
	default:
		t.logger.Errorw("unsupported provider type", "type", p.Type())
		return "", fferr.NewInternalErrorf("unsupported provider type: %s", p.Type())
	}

	return ps.ResourceToTableName(resourceType.String(), feature.Name(), feature.Variant())
}

// getSourceTableNameForSnowflake returns the fully qualified table name for a feature's source variant.
// **NOTE:** In the past, we used the feature materialization as the source of a feature for a training set;
// however, this was incorrect because that data set will only ever have one row per entity, and in many cases,
// we need all rows for a given entity to correctly create a training set. Therefore, we now use the source variant
// as the source of a feature and allow the training set query to determine how to handle the source data based on
// the presence/absence of timestamps in both the features and label.
func (t *TrainingSetTask) getSourceTableNameForSnowflake(feature *metadata.FeatureVariant) (string, error) {
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
