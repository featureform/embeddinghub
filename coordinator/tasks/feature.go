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
	"github.com/featureform/provider/provider_schema"
	"time"

	"github.com/featureform/fferr"
	"github.com/featureform/filestore"
	"github.com/featureform/helpers"
	"github.com/featureform/metadata"
	"github.com/featureform/provider"
	pl "github.com/featureform/provider/location"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/provider/types"
	"github.com/featureform/runner"
	"github.com/featureform/scheduling"
)

type FeatureTask struct {
	BaseTask
}

func (t *FeatureTask) Run() error {
	nv, ok := t.taskDef.Target.(scheduling.NameVariant)
	if !ok {
		return fferr.NewInternalErrorf("cannot create a feature from target type: %s", t.taskDef.TargetType)
	}

	logger := t.logger.WithResource(logging.FeatureVariant, nv.Name, nv.Variant)
	resID := metadata.ResourceID{Name: nv.Name, Variant: nv.Variant, Type: metadata.FEATURE_VARIANT}
	if t.isDelete {
		return t.handleDeletion(resID, logger)
	}

	logger.Info("Running feature materialization job on resource: ", nv)
	if err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Fetching Feature details..."); err != nil {
		return err
	}

	feature, err := t.metadata.GetFeatureVariant(context.Background(), metadata.NameVariant{Name: nv.Name, Variant: nv.Variant})
	if err != nil {
		return err
	}
	logger.Infow("Running task", "source", feature.Source(), "location", feature.Location(), "location_col", feature.LocationColumns())
	if err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Waiting for dependencies to complete..."); err != nil {
		return err
	}

	sourceNameVariant := feature.Source()
	source, err := t.awaitPendingSource(sourceNameVariant)
	if err != nil {
		return err
	}

	if err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Fetching Offline Store..."); err != nil {
		return err
	}

	sourceProvider, err := source.FetchProvider(t.metadata, context.Background())
	if err != nil {
		return err
	}
	p, err := provider.Get(pt.Type(sourceProvider.Type()), sourceProvider.SerializedConfig())
	if err != nil {
		return err
	}
	sourceStore, err := p.AsOfflineStore()
	if err != nil {
		return err
	}

	var featureProvider *metadata.Provider // this is the inference store
	if feature.Provider() != "" {
		featureProvider, err = feature.FetchProvider(t.metadata, context.Background())
		if err != nil {
			return err
		}
	}

	vType, typeErr := feature.Type()
	if typeErr != nil {
		return typeErr
	}

	logger.Debugw("Getting feature's source location", "name", source.Name(), "variant", source.Variant())
	var sourceLocation pl.Location
	var sourceLocationErr error
	if source.IsSQLTransformation() || source.IsDFTransformation() {
		sourceLocation, sourceLocationErr = source.GetTransformationLocation()
	} else if source.IsPrimaryData() {
		sourceLocation, sourceLocationErr = source.GetPrimaryLocation()
	}

	if sourceLocationErr != nil {
		return sourceLocationErr
	}
	logger.Debugw("Feature's source location", "location", sourceLocation, "location_type", sourceLocation.Type())
	featID := provider.ResourceID{
		Name:    nv.Name,
		Variant: nv.Variant,
		Type:    provider.Feature,
	}

	tmpSchema := feature.LocationColumns().(metadata.ResourceVariantColumns)
	schema := provider.ResourceSchema{
		Entity:      tmpSchema.Entity,
		Value:       tmpSchema.Value,
		TS:          tmpSchema.TS,
		SourceTable: sourceLocation,
	}
	logger.Debugw("Creating Resource Table", "id", featID, "schema", schema)
	if err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Registering Feature from dataset..."); err != nil {
		return err
	}

	if _, err := sourceStore.RegisterResourceFromSourceTable(featID, schema); err != nil {
		return err
	}
	logger.Debugw("Resource Table Created", "id", featID, "schema", schema)

	maxJobDurationEnv := helpers.GetEnv("MAX_JOB_DURATION", "48h")
	maxJobDuration, err := time.ParseDuration(maxJobDurationEnv)

	if err != nil {
		return fferr.NewInternalErrorf("could not parse MAX_JOB_DURATION: %v", err)
	}

	resourceSnowflakeConfig := &metadata.ResourceSnowflakeConfig{}
	if sourceStore.Type() == pt.SnowflakeOffline {
		tempConfig, err := feature.ResourceSnowflakeConfig()
		if err != nil {
			return err
		}
		resourceSnowflakeConfig = tempConfig
	}

	providerResID := provider.ResourceID{Name: nv.Name, Variant: nv.Variant, Type: provider.Feature}
	materializedRunnerConfig := runner.MaterializedRunnerConfig{
		OfflineType:   pt.Type(sourceProvider.Type()),
		OfflineConfig: sourceProvider.SerializedConfig(),
		ResourceID:    providerResID,
		VType:         types.ValueTypeJSONWrapper{ValueType: vType},
		Cloud:         runner.LocalMaterializeRunner,
		IsUpdate:      t.isUpdate,
		Options: provider.MaterializationOptions{
			Output:                  filestore.Parquet,
			ShouldIncludeHeaders:    true,
			MaxJobDuration:          maxJobDuration,
			JobName:                 fmt.Sprintf("featureform-materialization--%s--%s", nv.Name, nv.Variant),
			ResourceSnowflakeConfig: resourceSnowflakeConfig,
			Schema:                  schema,
		},
	}

	if featureProvider != nil {
		materializedRunnerConfig.OnlineType = pt.Type(featureProvider.Type())
		materializedRunnerConfig.OnlineConfig = featureProvider.SerializedConfig()
	} else {
		materializedRunnerConfig.OnlineType = pt.NONE
	}

	isImportToS3Enabled, err := t.checkS3Import(featureProvider)
	if err != nil {
		return err
	}

	supportsDirectCopy := false
	var onlineStore provider.OnlineStore
	if featureProvider != nil {
		onlineProvider, err := provider.Get(pt.Type(featureProvider.Type()), featureProvider.SerializedConfig())
		if err != nil {
			return err
		}
		casted, err := onlineProvider.AsOnlineStore()
		if err != nil {
			return err
		}
		onlineStore = casted
		// Direct copy means the provider can copy to the online store itself
		matOpt := provider.DirectCopyOptionType(onlineStore)
		supports, err := sourceStore.SupportsMaterializationOption(matOpt)
		if err != nil {
			return err
		}
		supportsDirectCopy = supports
	}

	if err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Starting Materialization..."); err != nil {
		return err
	}

	var materializationErr error
	if supportsDirectCopy {
		if err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Materializing via direct copy..."); err != nil {
			return err
		}
		// Create the table to copy into
		if _, err := onlineStore.CreateTable(nv.Name, nv.Variant, vType); err != nil {
			_, isTableExistsErr := err.(*fferr.DatasetAlreadyExistsError)
			if !isTableExistsErr {
				return err
			}
		}
		_, materializationErr = sourceStore.CreateMaterialization(providerResID, provider.MaterializationOptions{
			MaxJobDuration: maxJobDuration,
			JobName:        fmt.Sprintf("featureform-materialization--%s--%s", nv.Name, nv.Variant),
			DirectCopyTo:   onlineStore,
		})
	} else if isImportToS3Enabled {
		materializationErr = t.materializeFeatureViaS3Import(resID, materializedRunnerConfig, sourceStore)
	} else {
		materializationErr = t.materializeFeature(resID, materializedRunnerConfig)
	}
	if materializationErr != nil {
		return materializationErr
	}

	logger.Debugw("Setting status to ready")
	if err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Materialization Complete..."); err != nil {
		return err
	}
	return nil
}

func (t *FeatureTask) handleDeletion(resID metadata.ResourceID, logger logging.Logger) error {
	logger.Infow("Deleting feature")
	featureTableName, tableNameErr := provider_schema.ResourceToTableName(provider_schema.Materialization, resID.Name, resID.Variant)
	if tableNameErr != nil {
		logger.Errorw("Failed to get table name for feature", "error", tableNameErr)
		return tableNameErr
	}
	featureLocation := pl.NewSQLLocation(featureTableName)
	logger.Debugw("Deleting feature at location", "location", featureLocation)

	nv := metadata.NameVariant{
		Name:    resID.Name,
		Variant: resID.Variant,
	}

	logger.Debugw("Fetching feature that's staged for deletion")
	featureToDelete, err := t.metadata.GetStagedForDeletionFeatureVariant(context.Background(), nv)
	if err != nil {
		return err
	}

	sourceStore, err := getStore(t.BaseTask, t.metadata, featureToDelete)
	if err != nil {
		logger.Errorw("Failed to get store", "error", err)
		return err
	}

	logger.Debugw("Deleting feature from offline store")
	if deleteErr := sourceStore.Delete(featureLocation); deleteErr != nil {
		var notFoundErr *fferr.DatasetNotFoundError
		if errors.As(deleteErr, &notFoundErr) {
			logger.Infow("Table doesn't exist at location, continuing...", "location", featureLocation)
		} else {
			logger.Errorw("Failed to delete feature from offline store", "error", deleteErr)
			return deleteErr
		}
	}

	logger.Infow("Successfully deleted feature at location", "location", featureLocation)

	var featureProvider *metadata.Provider // this is the inference store
	if featureToDelete.Provider() != "" {
		featureProvider, err = featureToDelete.FetchProvider(t.metadata, context.Background())
		if err != nil {
			logger.Errorw("Failed to fetch inference store", "error", err)
			return err
		}
	}

	logger.Debugw("Attempting to delete feature from online store", "name", nv.Name, "variant", nv.Variant)
	if featureProvider != nil {
		logger.Debugw("Deleting feature from online store")
		onlineProvider, err := provider.Get(pt.Type(featureProvider.Type()), featureProvider.SerializedConfig())
		if err != nil {
			logger.Errorw("Failed to get online provider", "error", err)
			return err
		}
		casted, err := onlineProvider.AsOnlineStore()
		if err != nil {
			return err
		}

		onlineDeleteErr := casted.DeleteTable(nv.Name, nv.Variant)
		if onlineDeleteErr != nil {
			var notFoundErr *fferr.DatasetNotFoundError
			if errors.As(onlineDeleteErr, &notFoundErr) {
				logger.Infow("Table not found in online store, continuing...")
				// continuing
			} else {
				logger.Errorw("Failed to delete feature from online store", "error", onlineDeleteErr)
				return onlineDeleteErr
			}
		}
		logger.Debugw("Deleted feature from online store", "name", nv.Name, "variant", nv.Variant)
	}

	if err := t.metadata.FinalizeDelete(context.Background(), resID); err != nil {
		return err
	}

	return nil
}

func (t *FeatureTask) checkS3Import(featureProvider *metadata.Provider) (bool, error) {
	if featureProvider != nil && featureProvider.Type() == string(pt.DynamoDBOnline) {
		t.logger.Debugw("Feature provider is DynamoDB")
		config := pc.DynamodbConfig{}
		if err := config.Deserialize(featureProvider.SerializedConfig()); err != nil {
			return false, err
		}
		return config.ImportFromS3, nil
	}
	return false, nil
}

func (t *FeatureTask) materializeFeatureViaS3Import(id metadata.ResourceID, config runner.MaterializedRunnerConfig, sourceStore provider.OfflineStore) error {
	t.logger.Infow("Materializing Feature Via S3 Import", "id", id)
	err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Starting Materialization via S3 to Dynamo Import...")
	if err != nil {
		return err
	}
	sparkOfflineStore, isSparkOfflineStore := sourceStore.(*provider.SparkOfflineStore)
	if !isSparkOfflineStore {
		return fferr.NewInvalidArgumentError(fmt.Errorf("offline store is not spark offline store"))
	}
	if sparkOfflineStore.Store.FilestoreType() != filestore.S3 {
		return fferr.NewInvalidArgumentError(fmt.Errorf("offline file store must be S3; %s is not supported", sparkOfflineStore.Store.FilestoreType()))
	}
	serialized, err := config.Serialize()
	if err != nil {
		return err
	}
	jobRunner, err := t.spawner.GetJobRunner(runner.S3_IMPORT_DYNAMODB, serialized, id)
	if err != nil {
		return err
	}
	completionWatcher, err := jobRunner.Run()
	if err != nil {
		return err
	}

	err = t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Waiting for Materialization to complete...")
	if err != nil {
		return err
	}

	if err := completionWatcher.Wait(); err != nil {
		return err
	}
	t.logger.Info("Successfully materialized feature via S3 import to DynamoDB", "id", id)
	return nil
}

func (t *FeatureTask) materializeFeature(id metadata.ResourceID, config runner.MaterializedRunnerConfig) error {
	t.logger.Infow("Starting Feature Materialization", "id", id)
	err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Starting Materialization via Copy...")
	if err != nil {
		return err
	}
	serialized, err := config.Serialize()
	if err != nil {
		return err
	}
	jobRunner, err := t.spawner.GetJobRunner(runner.MATERIALIZE, serialized, id)
	if err != nil {
		return err
	}
	completionWatcher, err := jobRunner.Run()
	if err != nil {
		return err
	}

	err = t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Waiting for Materialization to complete...")
	if err != nil {
		return err
	}

	if err := completionWatcher.Wait(); err != nil {
		return err
	}
	return nil
}
