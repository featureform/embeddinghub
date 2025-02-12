// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/featureform/fferr"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/provider/types"
	_ "github.com/lib/pq"
)

type redshiftColumnType string

const (
	rsInt       redshiftColumnType = "integer"
	rsBigInt    redshiftColumnType = "bigint"
	rsFloat     redshiftColumnType = "real"
	rsString    redshiftColumnType = "varchar"
	rsBool      redshiftColumnType = "boolean"
	rsTimestamp redshiftColumnType = "timestamp with time zone"
)

func redshiftOfflineStoreFactory(config pc.SerializedConfig) (Provider, error) {
	sc := pc.RedshiftConfig{}
	if err := sc.Deserialize(config); err != nil {
		return nil, err
	}

	// We are doing this to support older versions of
	// featureform that did not have the sslmode field
	// on the client side.
	sslMode := sc.SSLMode
	if sslMode == "" {
		sslMode = "disable"
	}

	queries := redshiftSQLQueries{}
	queries.setVariableBinding(PostgresBindingStyle)
	sgConfig := SQLOfflineStoreConfig{
		Config:        config,
		ConnectionURL: fmt.Sprintf("sslmode=%s user=%v password=%s host=%v port=%v dbname=%v", sslMode, sc.Username, sc.Password, sc.Host, sc.Port, sc.Database),
		Driver:        "postgres",
		ProviderType:  pt.RedshiftOffline,
		QueryImpl:     &queries,
		ConnectionStringBuilder: func(database, schema string) (string, error) {
			redshiftDb := database
			if redshiftDb == "" {
				redshiftDb = sc.Database
			}
			sch := schema
			if schema == "" {
				sch = "public"
			}
			return fmt.Sprintf("sslmode=%s user=%v password=%s host=%v port=%v dbname=%v search_path=%v", sslMode, sc.Username, sc.Password, sc.Host, sc.Port, redshiftDb, sch), nil
		},
	}

	store, err := NewSQLOfflineStore(sgConfig)
	if err != nil {
		return nil, err
	}
	return store, nil
}

type redshiftSQLQueries struct {
	defaultOfflineSQLQueries
}

func (q redshiftSQLQueries) tableExists() string {
	return "SELECT COUNT(*) FROM svv_tables WHERE table_schema='public' AND table_type='BASE TABLE' AND table_name=$1"
}

func (q redshiftSQLQueries) viewExists() string {
	return "SELECT COUNT(*) FROM svv_tables WHERE table_schema='public' AND table_type='VIEW' AND table_name=$1"
}

func (q redshiftSQLQueries) registerResources(db *sql.DB, tableName string, schema ResourceSchema) error {
	var query string
	if schema.TS != "" {
		query = fmt.Sprintf("CREATE VIEW %s AS SELECT %s as entity, %s as value, %s as ts FROM %s", sanitize(tableName),
			sanitize(schema.Entity), sanitize(schema.Value), sanitize(schema.TS), sanitize(schema.SourceTable.Location()))
	} else {
		query = fmt.Sprintf("CREATE VIEW %s AS SELECT %s as entity, %s as value, to_timestamp('%s', 'YYYY-DD-MM HH24:MI:SS +0000 UTC')::TIMESTAMPTZ as ts FROM %s", sanitize(tableName),
			sanitize(schema.Entity), sanitize(schema.Value), time.UnixMilli(0).UTC(), sanitize(schema.SourceTable.Location()))
	}
	if _, err := db.Exec(query); err != nil {
		wrapped := fferr.NewExecutionError(pt.RedshiftOffline.String(), err)
		wrapped.AddDetail("table_name", tableName)
		return wrapped
	}
	return nil
}

func (q redshiftSQLQueries) primaryTableRegister(tableName string, sourceName string) string {
	query := fmt.Sprintf("CREATE VIEW %s AS SELECT * FROM %s", sanitize(tableName), sourceName)
	return query
}

func (q redshiftSQLQueries) materializationCreate(tableName string, schema ResourceSchema) []string {
	return []string{
		fmt.Sprintf(
			"CREATE TABLE %s AS (SELECT entity, value, ts, row_number() over(ORDER BY (entity)) as row_number FROM ("+
				"SELECT entity, value, ts, row_number() OVER (PARTITION BY entity ORDER BY entity, ts DESC) as rn "+
				"FROM %s) WHERE rn=1 ORDER BY entity)", sanitize(tableName), sanitize(schema.SourceTable.Location())),
	}
}

func (q redshiftSQLQueries) materializationUpdate(db *sql.DB, tableName string, sourceName string) error {
	sanitizedTable := sanitize(tableName)
	tempTable := sanitize(fmt.Sprintf("tmp_%s", tableName))
	oldTable := sanitize(fmt.Sprintf("old_%s", tableName))
	query := fmt.Sprintf(
		"BEGIN TRANSACTION;"+
			"CREATE TABLE %s AS (SELECT entity, value, ts, row_number() over(ORDER BY (SELECT NULL)) as row_number FROM "+
			"(SELECT entity, ts, value, row_number() OVER (PARTITION BY entity ORDER BY ts desc) "+
			"AS rn FROM %s) t WHERE rn=1);"+
			"ALTER TABLE %s RENAME TO %s;"+
			"ALTER TABLE %s RENAME TO %s;"+
			"DROP TABLE %s;"+
			"COMMIT;"+
			"", tempTable, sanitize(sourceName), sanitizedTable, oldTable, tempTable, sanitizedTable, oldTable)

	if _, err := db.Exec(query); err != nil {
		wrapped := fferr.NewExecutionError(pt.RedshiftOffline.String(), err)
		wrapped.AddDetail("table_name", tableName)
		wrapped.AddDetail("source_name", sourceName)
		return wrapped
	}
	return nil
}

func (q redshiftSQLQueries) materializationDrop(tableName string) string {
	return fmt.Sprintf("DROP TABLE %s", sanitize(tableName))
}

func (q redshiftSQLQueries) determineColumnType(valueType types.ValueType) (string, error) {
	switch valueType {
	case types.Int, types.Int32, types.Int64:
		return "BIGINT", nil
	case types.Float32, types.Float64:
		return "DOUBLE PRECISION", nil
	case types.String:
		return "VARCHAR", nil
	case types.Bool:
		return "BOOLEAN", nil
	case types.Timestamp:
		return "TIMESTAMPTZ", nil
	case types.NilType:
		return "VARCHAR", nil
	default:
		return "", fferr.NewDataTypeNotFoundErrorf(valueType, "could not determine column type")
	}
}

func (q redshiftSQLQueries) newSQLOfflineTable(name string, columnType string) string {
	return fmt.Sprintf("CREATE TABLE %s (entity VARCHAR, value %s, ts TIMESTAMPTZ, UNIQUE (entity, ts))", sanitize(name), columnType)
}

func (q redshiftSQLQueries) createValuePlaceholderString(columns []TableColumn) string {
	placeholders := make([]string, 0)
	for i := range columns {
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+1))
	}
	return strings.Join(placeholders, ", ")
}

func (q redshiftSQLQueries) trainingSetCreate(store *sqlOfflineStore, def TrainingSetDef, tableName string, labelName string) error {
	return q.trainingSetQuery(store, def, tableName, labelName, false)
}

func (q redshiftSQLQueries) trainingSetUpdate(store *sqlOfflineStore, def TrainingSetDef, tableName string, labelName string) error {
	return q.trainingSetQuery(store, def, tableName, labelName, true)
}

func (q redshiftSQLQueries) trainingSetQuery(store *sqlOfflineStore, def TrainingSetDef, tableName string, labelName string, isUpdate bool) error {
	columns := make([]string, 0)
	selectColumns := make([]string, 0)
	query := ""
	for i, feature := range def.Features {
		tableName, err := store.getResourceTableName(feature)
		if err != nil {
			return err
		}
		santizedName := sanitize(tableName)
		tableJoinAlias := fmt.Sprintf("t%d", i+1)
		selectColumns = append(selectColumns, fmt.Sprintf("%s_rnk", tableJoinAlias))
		columns = append(columns, santizedName)
		query = fmt.Sprintf("%s LEFT OUTER JOIN (SELECT entity, value AS %s, ts, RANK() OVER (ORDER BY ts DESC) AS %s_rnk FROM %s ORDER BY ts desc) AS %s ON (%s.entity=t0.entity AND %s.ts <= t0.ts)",
			query, santizedName, tableJoinAlias, santizedName, tableJoinAlias, tableJoinAlias, tableJoinAlias)
		if i == len(def.Features)-1 {
			query = fmt.Sprintf("%s )) WHERE rn=1", query)
		}
	}
	columnStr := strings.Join(columns, ", ")
	selectColumnStr := strings.Join(selectColumns, ", ")

	if !isUpdate {
		fullQuery := fmt.Sprintf(
			"CREATE TABLE %s AS (SELECT %s, label FROM ("+
				"SELECT *, row_number() over(PARTITION BY e, label, time ORDER BY \"time\", %s DESC) AS rn FROM ( "+
				"SELECT t0.entity AS e, t0.value AS label, t0.ts AS time, %s, %s FROM %s AS t0 %s )",
			sanitize(tableName), columnStr, selectColumnStr, columnStr, selectColumnStr, sanitize(labelName), query)
		if _, err := store.db.Exec(fullQuery); err != nil {
			wrapped := fferr.NewResourceExecutionError(pt.RedshiftOffline.String(), def.ID.Name, def.ID.Variant, fferr.ResourceType(def.ID.Type.String()), err)
			wrapped.AddDetail("table_name", tableName)
			wrapped.AddDetail("label_name", labelName)
			return wrapped
		}
	} else {
		tempTable := sanitize(fmt.Sprintf("tmp_%s", tableName))
		fullQuery := fmt.Sprintf(
			"CREATE TABLE %s AS (SELECT %s, label FROM ("+
				"SELECT *, row_number() over(PARTITION BY e, label, time ORDER BY \"time\", %s desc) AS rn FROM ( "+
				"SELECT t0.entity AS e, t0.value AS label, t0.ts AS time, %s, %s FROM %s AS t0 %s )",
			tempTable, columnStr, selectColumnStr, columnStr, selectColumnStr, sanitize(labelName), query)

		if err := q.atomicUpdate(store.db, tableName, tempTable, fullQuery); err != nil {
			return err
		}
	}
	return nil
}

func (q redshiftSQLQueries) castTableItemType(v interface{}, t interface{}) interface{} {
	if v == nil {
		return v
	}
	switch t {
	case rsInt:
		return int32(v.(int64))
	case rsBigInt:
		return int(v.(int64))
	case rsFloat:
		return v.(float64)
	case rsString:
		return v.(string)
	case rsBool:
		return v.(bool)
	case rsTimestamp:
		return v.(time.Time).UTC()
	default:
		return v
	}
}

func (q redshiftSQLQueries) getValueColumnType(t *sql.ColumnType) interface{} {
	switch t.ScanType().String() {
	case "string":
		return rsString
	case "int32":
		return rsBigInt
	case "int64":
		return rsBigInt
	case "float32", "float64", "interface {}":
		return rsFloat
	case "bool":
		return rsBool
	case "time.Time":
		return rsTimestamp
	}
	return rsString
}

func (q redshiftSQLQueries) numRows(n interface{}) (int64, error) {
	return n.(int64), nil
}

func (q redshiftSQLQueries) transformationCreate(name string, query string) []string {
	return []string{fmt.Sprintf("CREATE TABLE %s AS %s", sanitize(name), query)}
}

func (q redshiftSQLQueries) transformationUpdate(db *sql.DB, tableName string, query string) error {
	tempName := sanitize(fmt.Sprintf("tmp_%s", tableName))
	fullQuery := fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM ( %s )", tempName, query)
	err := q.atomicUpdate(db, tableName, tempName, fullQuery)
	if err != nil {
		return err
	}
	return nil
}
