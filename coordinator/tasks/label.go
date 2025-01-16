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

	if t.isDelete {
		return t.handleDeletion(resID)
	}

	label, err := t.metadata.GetLabelVariant(context.Background(), nameVariant)
	if err != nil {
		return err
	}

	sourceNameVariant := label.Source()
	t.logger.Infow("feature obj", "name", label.Name(), "source", label.Source(), "location", label.Location(), "location_col", label.LocationColumns())

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

	defer func(sourceStore provider.OfflineStore) {
		err := sourceStore.Close()
		if err != nil {
			t.logger.Errorf("could not close offline store: %v", err)
		}
	}(sourceStore)
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
	t.logger.Debugw("Creating Label Resource Table", "id", labelID, "schema", schema)

	if err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Registering Label from dataset..."); err != nil {
		return err
	}
	t.logger.Debugw("Checking source store type", "type", fmt.Sprintf("%T", sourceStore))
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
		t.logger.Errorw("Failed to register resource from source table", "id", labelID, "opts length", len(opts), "error", err)
		return err
	}
	t.logger.Debugw("Resource Table Created", "id", labelID, "schema", schema)

	if err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Registration complete..."); err != nil {
		return err
	}

	return nil
}

func (t *LabelTask) handleDeletion(resID metadata.ResourceID) error {
	labelToDelete, err := t.metadata.GetStagedForDeletionLabelVariant(
		context.Background(),
		metadata.NameVariant{
			Name:    resID.Name,
			Variant: resID.Variant,
		},
	)
	if err != nil {
		return err
	}

	t.logger.Infow("Deleting label", "resource_id", resID)
	labelTableName, tableNameErr := provider_schema.ResourceToTableName(provider_schema.Label, resID.Name, resID.Variant)
	if tableNameErr != nil {
		t.logger.Debugw("Failed to get table name for label", "error", tableNameErr)
		return err
	}

	sourceStore, err := getStore(t.BaseTask, t.metadata, labelToDelete)
	if err != nil {
		return err
	}

	location := pl.NewSQLLocation(labelTableName)

	t.logger.Debugw("Deleting label at location", "location", location)

	if deleteErr := sourceStore.Delete(location); deleteErr != nil {
		var notFoundErr *fferr.DatasetNotFoundError
		if errors.As(deleteErr, &notFoundErr) {
			t.logger.Infow("Table doesn't exist at location, continuing...", "location", location)
		} else {
			return deleteErr
		}
	} else {
		t.logger.Infow("Successfully deleted label at location", "location", location)
	}

	if err := t.metadata.FinalizeDelete(context.Background(), resID); err != nil {
		return err
	}

	return nil
}
