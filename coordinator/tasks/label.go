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

	"github.com/featureform/fferr"
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
	logger := t.logger.WithResource(logging.LabelVariant, resID.Name, resID.Variant)

	if t.isDelete {
		return t.handleDeletion(resID, logger)
	}

	label, err := t.metadata.GetLabelVariant(context.Background(), nameVariant)
	if err != nil {
		return err
	}

	sourceNameVariant := label.Source()
	logger.Infow("feature obj", "name", label.Name(), "source", label.Source(), "location", label.Location(), "location_col", label.LocationColumns())

	if err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Waiting for dependencies to complete..."); err != nil {
		return err
	}

	source, err := t.awaitPendingSource(sourceNameVariant)
	if err != nil {
		return err
	}

	if err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Fetching Offline Store..."); err != nil {
		return err
	}

	sourceStore, getStoreErr := getStore(t.BaseTask, t.metadata, source)
	if getStoreErr != nil {
		return getStoreErr
	}

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

	labelID := provider.ResourceID{
		Name:    nameVariant.Name,
		Variant: nameVariant.Variant,
		Type:    provider.Label,
	}
	tmpSchema := label.LocationColumns().(metadata.ResourceVariantColumns)
	schema := provider.ResourceSchema{
		Entity:      tmpSchema.Entity,
		Value:       tmpSchema.Value,
		TS:          tmpSchema.TS,
		SourceTable: sourceLocation,
	}
	logger.Debugw("Creating Label Resource Table", "id", labelID, "schema", schema)

	if err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Registering Label from dataset..."); err != nil {
		return err
	}
	logger.Debugw("Checking source store type", "type", fmt.Sprintf("%T", sourceStore))
	opts := make([]provider.ResourceOption, 0)
	if sourceStore.Type() == pt.SnowflakeOffline {
		tempConfig, err := label.ResourceSnowflakeConfig()
		if err != nil {
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
		return err
	}

	return nil
}

func (t *LabelTask) handleDeletion(resID metadata.ResourceID, logger logging.Logger) error {
	logger.Infow("Deleting label")
	labelToDelete, err := t.metadata.GetStagedForDeletionLabelVariant(
		context.Background(),
		metadata.NameVariant{
			Name:    resID.Name,
			Variant: resID.Variant,
		},
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

	sourceStore, err := getStore(t.BaseTask, t.metadata, labelToDelete)
	if err != nil {
		logger.Errorw("Failed to get store", "error", err)
		return err
	}

	labelLocation := pl.NewSQLLocation(labelTableName)

	logger.Debugw("Deleting label at location", "location", labelLocation)
	if deleteErr := sourceStore.Delete(labelLocation); deleteErr != nil {
		var notFoundErr *fferr.DatasetNotFoundError
		if errors.As(deleteErr, &notFoundErr) {
			logger.Infow("Table doesn't exist at location, continuing...", "location", labelLocation)
		} else {
			return deleteErr
		}
	}

	logger.Infow("Successfully deleted label at location", "location", labelLocation)

	if err := t.metadata.FinalizeDelete(context.Background(), resID); err != nil {
		return err
	}

	return nil
}
