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

	"github.com/featureform/provider/provider_schema"

	"github.com/featureform/fferr"
	"github.com/featureform/logging"
	"github.com/featureform/metadata"
	"github.com/featureform/provider"
	pl "github.com/featureform/provider/location"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/scheduling"
)

type LabelTask struct {
	BaseTask
}

func (t *LabelTask) Run() error {
	_, ctx, logger := t.logger.InitializeRequestID(context.TODO())
	nv, ok := t.taskDef.Target.(scheduling.NameVariant)
	if !ok {
		return fferr.NewInternalErrorf("cannot create a label from target type: %s", t.taskDef.TargetType)
	}

	nameVariant := metadata.NameVariant{Name: nv.Name, Variant: nv.Variant}
	err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Fetching Label details...")
	if err != nil {
		return err
	}
	resID := metadata.ResourceID{Name: nv.Name, Variant: nv.Variant, Type: metadata.LABEL_VARIANT}
	logger = logger.WithResource(logging.LabelVariant, resID.Name, resID.Variant).
		With("task_id", t.taskDef.TaskId, "task_run_id", t.taskDef.ID)

	if t.isDelete {
		logger.Debugw("Handling deletion")
		return t.handleDeletion(ctx, resID, logger)
	}

	label, err := t.metadata.GetLabelVariant(ctx, nameVariant)
	if err != nil {
		return err
	}

	logger = t.logger.With("source", label.Source())
	sourceNameVariant := label.Source()
	loc, err := label.Location()
	if err != nil {
		logger.Errorw("Failed to get label location", "error", err)
		return err
	}
	logger.Debugw("Label Location", "location", loc)

	if err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Waiting for dependencies to complete..."); err != nil {
		logger.Errorw("Failed to add run log", "error", err)
		return err
	}

	source, err := t.awaitPendingSource(sourceNameVariant)
	if err != nil {
		logger.Errorw("Failed to await pending source", "error", err)
		return err
	}

	if err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Fetching Offline Store..."); err != nil {
		logger.Errorw("Failed to add run log", "error", err)
		return err
	}

	sourceStore, getStoreErr := getOfflineStore(ctx, t.BaseTask, t.metadata, source, logger)
	if getStoreErr != nil {
		return getStoreErr
	}
	defer func(sourceStore provider.OfflineStore, logger logging.Logger) {
		err := sourceStore.Close()
		if err != nil {
			logger.Errorf("could not close offline store: %v", err)
		}
		logger.Debugw("Closed offline store")
	}(sourceStore, logger)
	var sourceLocation pl.Location
	var sourceLocationErr error
	if source.IsSQLTransformation() || source.IsDFTransformation() {
		logger.Debugw("Getting transformation location")
		sourceLocation, sourceLocationErr = source.GetTransformationLocation()
	} else if source.IsPrimaryData() {
		logger.Debugw("Getting primary location")
		sourceLocation, sourceLocationErr = source.GetPrimaryLocation()
	}

	if sourceLocationErr != nil {
		logger.Errorw("Failed to get source location", "error", sourceLocationErr)
		return sourceLocationErr
	}

	labelID := provider.ResourceID{
		Name:    nameVariant.Name,
		Variant: nameVariant.Variant,
		Type:    provider.Label,
	}
	schema := provider.ResourceSchema{
		SourceTable:    sourceLocation,
		EntityMappings: loc,
	}
	logger.Debugw("Creating Label Resource Table", "id", labelID, "schema", schema)

	if err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Registering Label from dataset..."); err != nil {
		logger.Errorw("Failed to add run log", "error", err)
		return err
	}
	logger.Debugw("Checking source store type", "type", fmt.Sprintf("%T", sourceStore))
	opts := make([]provider.ResourceOption, 0)
	if sourceStore.Type() == pt.SnowflakeOffline {
		logger.Debugw("Source store is snowflake")
		tempConfig, err := label.ResourceSnowflakeConfig()
		if err != nil {
			logger.Errorw("Failed to get snowflake config", "error", err)
			return err
		}
		snowflakeDynamicTableConfigOpts := &provider.ResourceSnowflakeConfigOption{
			Config:    tempConfig.DynamicTableConfig,
			Warehouse: tempConfig.Warehouse,
		}
		opts = append(opts, snowflakeDynamicTableConfigOpts)
	}

	if _, err := sourceStore.RegisterResourceFromSourceTable(labelID, schema, opts...); err != nil {
		logger.Errorw("Failed to register resource from source table", "id", labelID, "opts length", len(opts), "error", err)
		return err
	}
	logger.Debugw("Resource Table Created", "id", labelID, "schema", schema)

	if err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Registration complete..."); err != nil {
		logger.Errorw("Failed to add run log", "error", err)
		return err
	}
	logger.Debugw("Label Registration Complete", "id", labelID)
	return nil
}

func (t *LabelTask) handleDeletion(ctx context.Context, resID metadata.ResourceID, logger logging.Logger) error {
	logger.Infow("Deleting label")
	labelToDelete, err := t.metadata.GetStagedForDeletionLabelVariant(
		ctx,
		metadata.NameVariant{
			Name:    resID.Name,
			Variant: resID.Variant,
		},
		logger,
	)
	if err != nil {
		logger.Errorw("Failed to get staged for deletion label", "error", err)
		return err
	}

	logger.Infow("Deleting label", "resource_id", resID)
	labelTableName, tableNameErr := provider_schema.ResourceToTableName(provider_schema.Label, resID.Name, resID.Variant)
	if tableNameErr != nil {
		logger.Errorw("Failed to get table name", "error", tableNameErr)
		return tableNameErr
	}

	sourceStore, err := getOfflineStore(ctx, t.BaseTask, t.metadata, labelToDelete, logger)
	if err != nil {
		logger.Errorw("Failed to get store", "error", err)
		return err
	}

	labelLocation := pl.NewSQLLocation(labelTableName)
	logger = logger.With("location", labelLocation)

	logger.Debugw("Deleting label at location")
	if deleteErr := sourceStore.Delete(labelLocation); deleteErr != nil {
		var notFoundErr *fferr.DatasetNotFoundError
		if errors.As(deleteErr, &notFoundErr) {
			logger.Infow("Table doesn't exist at location, continuing...")
		} else {
			return deleteErr
		}
	}
	logger.Infow("Successfully deleted label at location")

	logger.Debugw("Finalizing delete")
	if err := t.metadata.FinalizeDelete(ctx, resID); err != nil {
		logger.Errorw("Failed to finalize delete", "error", err)
		return err
	}

	return nil
}
