// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider

import (
	"fmt"

	"github.com/featureform/fferr"
	"github.com/featureform/logging"
	"github.com/featureform/metadata"
	pl "github.com/featureform/provider/location"
	pc "github.com/featureform/provider/provider_config"
	ps "github.com/featureform/provider/provider_schema"
	pt "github.com/featureform/provider/provider_type"

	tsq "github.com/featureform/provider/tsquery"
	snowflake "github.com/snowflakedb/gosnowflake"
)

// sqlColumnType is used to specify the column type of a resource value.
type snowflakeColumnType string

const (
	sfInt       snowflakeColumnType = "integer"
	sfNumber    snowflakeColumnType = "NUMBER"
	sfFloat     snowflakeColumnType = "FLOAT"
	sfString    snowflakeColumnType = "varchar"
	sfBool      snowflakeColumnType = "BOOLEAN"
	sfTimestamp snowflakeColumnType = "TIMESTAMP_NTZ"
)

func snowflakeOfflineStoreFactory(config pc.SerializedConfig) (Provider, error) {
	sc := pc.SnowflakeConfig{}
	if err := sc.Deserialize(config); err != nil {
		return nil, err
	}
	queries := snowflakeSQLQueries{}
	queries.setVariableBinding(MySQLBindingStyle)
	connectionString, err := sc.ConnectionString(sc.Database, sc.Schema)
	if err != nil {
		return nil, fferr.NewInternalErrorf("could not get snowflake connection string: %v", err)
	}
	sgConfig := SQLOfflineStoreConfig{
		Config:                  config,
		ConnectionURL:           connectionString,
		Driver:                  "snowflake",
		ProviderType:            pt.SnowflakeOffline,
		QueryImpl:               &queries,
		ConnectionStringBuilder: sc.ConnectionString,
	}

	store, err := NewSQLOfflineStore(sgConfig)
	if err != nil {
		return nil, err
	}

	return &snowflakeOfflineStore{
		store,
		logging.NewLogger("snowflake_offline_store"),
		&queries,
	}, nil
}

type snowflakeOfflineStore struct {
	*sqlOfflineStore
	logger    logging.Logger
	sfQueries *snowflakeSQLQueries
}

func (sf *snowflakeOfflineStore) CreateTransformation(config TransformationConfig, opts ...TransformationOption) error {
	sf.logger.Debugw("Snowflake offline store creating transformation ...", "config", config)
	if len(opts) > 0 {
		sf.logger.Errorw("Snowflake off does not support transformation options")
		return fferr.NewInternalErrorf("Snowflake off does not support transformation options")
	}
	tableName, err := sf.sqlOfflineStore.getTransformationTableName(config.TargetTableID)
	if err != nil {
		sf.logger.Errorw("Failed to get transformation table name", "error", err)
		return err
	}
	var snowflakeConfig pc.SnowflakeConfig
	if err := snowflakeConfig.Deserialize(sf.sqlOfflineStore.Config()); err != nil {
		sf.logger.Errorw("Failed to deserialize snowflake config", "error", err)
		return err
	}
	// Given our API design allows for users to define Dynamic Iceberg Table configurations at the catalog level, it's possible
	// that config.SnowflakeDynamicTableConfig is nil. In this case, we'll create a new empty config to facilitate the merge with the catalog config.
	resConfig := config.ResourceSnowflakeConfig
	if resConfig == nil {
		resConfig = &metadata.ResourceSnowflakeConfig{}
	}
	if err := resConfig.Merge(&snowflakeConfig); err != nil {
		sf.logger.Errorw("Failed to merge dynamic table config", "error", err)
		return err
	}
	query, err := sf.sfQueries.dynamicIcebergTableCreate(tableName, config.Query, *resConfig)
	if err != nil {
		sf.logger.Errorw("Failed to create dynamic iceberg table query", "error", err)
		return err
	}
	sf.logger.Debugw("Creating Dynamic Iceberg Table for source", "query", query)
	if _, err := sf.sqlOfflineStore.db.Exec(query); err != nil {
		wrapped := fferr.NewResourceExecutionError(pt.SnowflakeOffline.String(), config.TargetTableID.Name, config.TargetTableID.Variant, fferr.ResourceType(config.TargetTableID.Type.String()), err)
		sf.logger.Errorw("Failed to create dynamic iceberg table", "error", err)
		return sf.handleErr(wrapped, err)
	}
	return nil
}

func (sf *snowflakeOfflineStore) UpdateTransformation(config TransformationConfig, opts ...TransformationOption) error {
	return fferr.NewInternalErrorf("Snowflake Offline Store does not currently support updating transformations")
}

// RegisterResourceFromSourceTable creates a new table in the database for labels, but not features. Unlike the sqlOfflineStore implementation, which
// creates a resource table for features, there's no need here for the intermediate table between source transformation and materialization table.
func (sf *snowflakeOfflineStore) RegisterResourceFromSourceTable(id ResourceID, schema ResourceSchema, opts ...ResourceOption) (OfflineTable, error) {
	sf.logger.Debugw("Snowflake offline store creating resource table...", "id", id, "schema", schema, "opts", opts)
	if id.Type == Feature {
		sf.logger.Debugw("Snowflake Offline Store does not support creating resource tables for features")
		return nil, nil
	}
	if err := id.check(Label); err != nil {
		return nil, err
	}
	if len(opts) == 0 || opts[0].Type() != SnowflakeDynamicTableResource {
		return nil, fferr.NewInternalErrorf("Snowflake Offline Store requires Dynamic Table Resource Options")
	}
	opt, isSnowflakeConfigOpt := opts[0].(*ResourceSnowflakeConfigOption)
	if !isSnowflakeConfigOpt {
		return nil, fferr.NewInternalErrorf("Snowflake Offline Store requires Resource Config Options; received %T", opts[0])
	}
	tableConfig := opt.Config
	if tableConfig == nil {
		tableConfig = &metadata.SnowflakeDynamicTableConfig{}
	}
	resConfig := &metadata.ResourceSnowflakeConfig{
		DynamicTableConfig: tableConfig,
		Warehouse:          opt.Warehouse,
	}
	var snowflakeConfig pc.SnowflakeConfig
	if err := snowflakeConfig.Deserialize(sf.sqlOfflineStore.Config()); err != nil {
		sf.logger.Errorw("Failed to deserialize snowflake config", "error", err)
		return nil, err
	}
	if err := resConfig.Merge(&snowflakeConfig); err != nil {
		sf.logger.Errorw("Failed to merge dynamic table config", "error", err)
		return nil, err
	}
	if exists, err := sf.sqlOfflineStore.tableExistsForResourceId(id); err != nil {
		return nil, err
	} else if exists {
		return nil, fferr.NewDatasetAlreadyExistsError(id.Name, id.Variant, nil)
	}
	if err := schema.Validate(); err != nil {
		sf.logger.Errorw("Failed to validate schema", "error", err)
		return nil, err
	}
	resourceAsQuery, err := sf.sfQueries.resourceTableAsQuery(schema, schema.TS != "")
	if err != nil {
		sf.logger.Errorw("Failed to create resource table as query", "error", err)
		return nil, err
	}
	tableName, err := ps.ResourceToTableName(id.Type.String(), id.Name, id.Variant)
	if err != nil {
		return nil, err
	}
	query, err := sf.sfQueries.dynamicIcebergTableCreate(tableName, resourceAsQuery, *resConfig)
	if err != nil {
		sf.logger.Errorw("Failed to create dynamic iceberg table query", "error", err)
		return nil, err
	}
	sf.logger.Debugw("Creating Dynamic Iceberg Table for label variant", "query", query)
	if _, err := sf.sqlOfflineStore.db.Exec(query); err != nil {
		wrapped := fferr.NewResourceExecutionError(pt.SnowflakeOffline.String(), id.Name, id.Variant, fferr.LABEL_VARIANT, err)
		sf.logger.Errorw("Failed to create dynamic iceberg table", "error", err)
		return nil, sf.handleErr(wrapped, err)
	}
	return &snowflakeOfflineTable{
		sqlOfflineTable: sqlOfflineTable{
			db:           sf.sqlOfflineStore.db,
			name:         tableName,
			query:        sf.sqlOfflineStore.query,
			providerType: pt.SnowflakeOffline,
		},
		location: pl.NewSQLLocationWithDBSchemaTable(snowflakeConfig.Database, snowflakeConfig.Schema, tableName),
	}, nil
}

func (sf *snowflakeOfflineStore) GetResourceTable(id ResourceID) (OfflineTable, error) {
	sf.logger.Debugw("Snowflake offline store getting resource table...", "id", id)
	if id.Type == Feature {
		sf.logger.Debugw("Snowflake Offline Store does not support resource tables for features")
		return nil, nil
	}
	if err := id.check(Label); err != nil {
		return nil, err
	}
	config := pc.SnowflakeConfig{}
	if err := config.Deserialize(sf.sqlOfflineStore.Config()); err != nil {
		return nil, err
	}
	tableName, err := ps.ResourceToTableName(id.Type.String(), id.Name, id.Variant)
	if err != nil {
		return nil, err
	}
	var schema = config.Schema
	if schema == "" {
		schema = "PUBLIC"
	}
	loc := pl.NewSQLLocationWithDBSchemaTable(config.Database, schema, tableName)

	if exists, err := sf.sqlOfflineStore.tableExists(loc); err != nil {
		return nil, err
	} else if !exists {
		return nil, fferr.NewDatasetNotFoundError(id.Name, id.Variant, nil)
	}

	return &snowflakeOfflineTable{
		sqlOfflineTable: sqlOfflineTable{
			db:           sf.sqlOfflineStore.db,
			name:         tableName,
			query:        sf.sqlOfflineStore.query,
			providerType: pt.SnowflakeOffline,
		},
		location: loc,
	}, nil
}

func (sf *snowflakeOfflineStore) CreateMaterialization(id ResourceID, opts MaterializationOptions) (Materialization, error) {
	if err := id.check(Feature); err != nil {
		return nil, err
	}
	var snowflakeConfig pc.SnowflakeConfig
	if err := snowflakeConfig.Deserialize(sf.sqlOfflineStore.Config()); err != nil {
		sf.logger.Errorw("Failed to deserialize snowflake config", "error", err)
		return nil, err
	}
	resConfig := opts.ResourceSnowflakeConfig
	if resConfig == nil {
		resConfig = &metadata.ResourceSnowflakeConfig{}
	}
	sf.logger.Debugw("Dynamic Table Config before Merge with Snowflake Config", "config", resConfig)
	if err := resConfig.Merge(&snowflakeConfig); err != nil {
		sf.logger.Errorw("Failed to merge dynamic table config", "error", err)
		return nil, err
	}

	// It seems that Snowflake can mistakenly choose a full refresh for a query that's compliant
	// with an incremental refresh, and given our materialization query has been written to be
	// incremental, we're hardcoding INCREMENTAL here to avoid the issue where our materialization
	// tables update fully.
	resConfig.DynamicTableConfig.RefreshMode = metadata.IncrementalRefresh

	sf.logger.Debugw(("Dynamic Table Config after Merge with Snowflake Config"), "config", resConfig)
	tableName, err := ps.ResourceToTableName(FeatureMaterialization.String(), id.Name, id.Variant)
	if err != nil {
		return nil, err
	}
	if err := opts.Schema.Validate(); err != nil {
		sf.logger.Errorw("Failed to validate schema", "error", err)
		return nil, err
	}
	sqlLoc, isSqlLoc := opts.Schema.SourceTable.(*pl.SQLLocation)
	if !isSqlLoc {
		return nil, fferr.NewInvalidArgumentErrorf("source table is not an SQL location")
	}
	materializationAsQuery := sf.sfQueries.materializationCreateAsQuery(opts.Schema.Entity, opts.Schema.Value, opts.Schema.TS, SanitizeSqlLocation(sqlLoc.TableLocation()))
	query, err := sf.sfQueries.dynamicIcebergTableCreate(tableName, materializationAsQuery, *resConfig)
	if err != nil {
		sf.logger.Errorw("Failed to create dynamic iceberg table query", "error", err)
		return nil, err
	}
	sf.logger.Debugw("Creating Dynamic Iceberg Table for materialization", "query", query)
	if _, err := sf.sqlOfflineStore.db.Exec(query); err != nil {
		wrapped := fferr.NewResourceExecutionError(pt.SnowflakeOffline.String(), id.Name, id.Variant, fferr.FEATURE_MATERIALIZATION, err)
		sf.logger.Errorw("Failed to create dynamic iceberg table", "error", err)
		return nil, sf.handleErr(wrapped, err)
	}
	return &sqlMaterialization{
		id:           MaterializationID(fmt.Sprintf("%s__%s", id.Name, id.Variant)),
		db:           sf.sqlOfflineStore.db,
		tableName:    tableName,
		location:     pl.NewSQLLocation(tableName),
		query:        sf.sfQueries,
		providerType: pt.SnowflakeOffline,
	}, nil
}

func (sf *snowflakeOfflineStore) UpdateMaterialization(id ResourceID, opts MaterializationOptions) (Materialization, error) {
	return nil, fferr.NewInternalErrorf("Snowflake Offline Store does not currently support updating materializations")
}

func (sf *snowflakeOfflineStore) CreateTrainingSet(def TrainingSetDef) error {
	if err := def.check(); err != nil {
		return err
	}
	var snowflakeConfig pc.SnowflakeConfig
	if err := snowflakeConfig.Deserialize(sf.sqlOfflineStore.Config()); err != nil {
		sf.logger.Errorw("Failed to deserialize snowflake config", "error", err)
		return err
	}
	resConfig := def.ResourceSnowflakeConfig
	if resConfig == nil {
		resConfig = &metadata.ResourceSnowflakeConfig{}
	}
	if err := resConfig.Merge(&snowflakeConfig); err != nil {
		sf.logger.Errorw("Failed to merge dynamic table config", "error", err)
		return err
	}
	tableName, err := sf.sqlOfflineStore.getTrainingSetName(def.ID)
	if err != nil {
		return err
	}
	ts, err := sf.buildTrainingSetQuery(def)
	if err != nil {
		return err
	}
	query, err := sf.sfQueries.dynamicIcebergTableCreate(tableName, ts, *resConfig)
	if err != nil {
		sf.logger.Errorw("Failed to create dynamic iceberg table query", "error", err)
		return err
	}
	sf.logger.Debugw("Creating Dynamic Iceberg Table for training set", "query", query)
	if _, err := sf.sqlOfflineStore.db.Exec(query); err != nil {
		wrapped := fferr.NewResourceExecutionError(pt.SnowflakeOffline.String(), def.ID.Name, def.ID.Variant, fferr.TRAINING_SET_VARIANT, err)
		sf.logger.Errorw("Failed to create dynamic iceberg table", "error", err)
		return sf.handleErr(wrapped, err)
	}
	return nil
}

func (sf *snowflakeOfflineStore) UpdateTrainingSet(def TrainingSetDef) error {
	return fferr.NewInternalErrorf("Snowflake Offline Store does not currently support updating training sets")
}

// AsOfflineStore returns the OfflineStore interface for the snowflakeOfflineStore.
// NOTE: snowflakeOfflineStore _must_ implement this method for consumers to call the correct methods on the interface
// (i.e. without implementing this method, consumers would be calling into sqlOfflineStore methods instead). Given this,
// DO NOT REMOVE THIS METHOD.
func (sf *snowflakeOfflineStore) AsOfflineStore() (OfflineStore, error) {
	return sf, nil
}

func (sf snowflakeOfflineStore) Delete(location pl.Location) error {
	if exists, err := sf.sqlOfflineStore.tableExists(location); err != nil {
		return err
	} else if !exists {
		return fferr.NewDatasetLocationNotFoundError(location.Location(), nil)
	}

	sqlLoc, isSqlLoc := location.(*pl.SQLLocation)
	if !isSqlLoc {
		return fferr.NewInternalErrorf("location is not an SQL location")
	}

	query := sf.sfQueries.dropTableQuery(*sqlLoc)
	sf.logger.Debugw("Deleting table", "query", query)

	if _, err := sf.db.Exec(query); err != nil {
		return sf.handleErr(fferr.NewExecutionError(pt.SnowflakeOffline.String(), err), err)
	}

	return nil
}

// handleErr attempts to add the Snowflake query and session IDs to a wrapped error to aid
// in further debugging in the Snowflake UI; note that handleErr will not fail even if the
// returned error isn't an instance of gosnowflake.SnowflakeError or if the query to get
// the session ID fails.
func (sf snowflakeOfflineStore) handleErr(wrapped fferr.Error, execErr error) error {
	sfErr, isSnowflakeErr := execErr.(*snowflake.SnowflakeError)
	if isSnowflakeErr {
		wrapped.AddDetail("query_id", sfErr.QueryID)
	}
	var sessionID string
	r := sf.db.QueryRow("SELECT CURRENT_SESSION()")
	if err := r.Scan(&sessionID); err != nil {
		sf.logger.Errorf("failed to query session ID for err: %v", err)
	} else {
		wrapped.AddDetail("session_id", sessionID)
	}
	return wrapped
}

func (sf snowflakeOfflineStore) buildTrainingSetQuery(def TrainingSetDef) (string, error) {
	sf.logger.Debugw("Building training set query...", "def", def)
	params, err := sf.adaptTsDefToBuilderParams(def)
	if err != nil {
		return "", err
	}
	sf.logger.Debugw("Training set builder params", "params", params)
	ts := tsq.NewTrainingSet(params)
	return ts.CompileSQL()
}

// **NOTE:** As the name suggests, this method is adapts the TrainingSetDef to the BuilderParams to avoid
// using TrainingSetDef directly in the tsquery package, which would create a circular dependency. In the future,
// we should move TrainingSetDef to the provider/types package to avoid this issue.
func (sf snowflakeOfflineStore) adaptTsDefToBuilderParams(def TrainingSetDef) (tsq.BuilderParams, error) {
	lblCols := def.LabelSourceMapping.Columns
	lblLoc, isSQLLocation := def.LabelSourceMapping.Location.(*pl.SQLLocation)
	if !isSQLLocation {
		return tsq.BuilderParams{}, fferr.NewInternalErrorf("label location is not an SQL location")
	}
	lblTableName := SanitizeSnowflakeIdentifier(lblLoc.TableLocation())

	ftCols := make([]metadata.ResourceVariantColumns, len(def.FeatureSourceMappings))
	ftTableNames := make([]string, len(def.FeatureSourceMappings))
	ftNameVariants := make([]metadata.ResourceID, len(def.FeatureSourceMappings))

	for i, ft := range def.FeatureSourceMappings {
		ftCols[i] = ft.Columns
		ftLoc, isSQLLocation := ft.Location.(*pl.SQLLocation)
		if !isSQLLocation {
			return tsq.BuilderParams{}, fferr.NewInternalErrorf("feature location is not an SQL location")
		}
		ftTableNames[i] = SanitizeSnowflakeIdentifier(ftLoc.TableLocation())
		id := def.Features[i]
		ftNameVariants[i] = metadata.ResourceID{
			Name:    id.Name,
			Variant: id.Variant,
			Type:    metadata.FEATURE_VARIANT,
		}
	}
	return tsq.BuilderParams{
		LabelColumns:           lblCols,
		SanitizedLabelTable:    lblTableName,
		FeatureColumns:         ftCols,
		SanitizedFeatureTables: ftTableNames,
		FeatureNameVariants:    ftNameVariants,
	}, nil
}
