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
	"text/template"
	"time"

	"github.com/featureform/fferr"
	helper "github.com/featureform/helpers/postgres"
	"github.com/featureform/logging"
	pl "github.com/featureform/provider/location"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	tsq "github.com/featureform/provider/tsquery"
	"github.com/featureform/provider/types"
	_ "github.com/lib/pq"
)

type postgresColumnType string

const (
	pgInt       postgresColumnType = "integer"
	pgBigInt    postgresColumnType = "bigint"
	pgFloat     postgresColumnType = "float8"
	pgString    postgresColumnType = "varchar"
	pgBool      postgresColumnType = "boolean"
	pgTimestamp postgresColumnType = "timestamp with time zone"
)

func postgresOfflineStoreFactory(config pc.SerializedConfig) (Provider, error) {
	sc := pc.PostgresConfig{}
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

	queries := postgresSQLQueries{}
	queries.setVariableBinding(PostgresBindingStyle)
	connectionUrlBuilder := PostgresConnectionBuilderFunc(sc)
	connUrl, _ := connectionUrlBuilder(sc.Database, sc.Schema)
	sgConfig := SQLOfflineStoreConfig{
		Config:                  config,
		ConnectionURL:           connUrl,
		Driver:                  "postgres",
		ProviderType:            pt.PostgresOffline,
		QueryImpl:               &queries,
		ConnectionStringBuilder: connectionUrlBuilder,
		useDbConnectionCache:    true,
	}

	store, err := NewSQLOfflineStore(sgConfig)
	if err != nil {
		return nil, err
	}

	// We override the default getDb method, as when the db or schema
	// is empty, we use the default one configured.
	prevGetDb := store.getDb
	store.getDb = func(database, schema string) (*sql.DB, error) {
		if database == "" || schema == "" {
			return prevGetDb(sc.Database, sc.Schema)
		} else {
			return prevGetDb(database, schema)
		}
	}

	return store, nil
}

type postgresSQLQueries struct {
	defaultOfflineSQLQueries
}

func (q postgresSQLQueries) tableExists() string {
	return "SELECT COUNT(*) FROM pg_tables WHERE tablename  = $1 AND schemaname = CURRENT_SCHEMA()"
}

func (q postgresSQLQueries) viewExists() string {
	return "select " +
		"(select count(*) from pg_matviews where matviewname = $1 AND schemaname = CURRENT_SCHEMA())" +
		"+ (select count(*) from pg_views where viewname = $1 AND schemaname = CURRENT_SCHEMA())"
}

func (q postgresSQLQueries) registerResources(db *sql.DB, tableName string, schema ResourceSchema) error {
	return fferr.NewInternalErrorf("Postgres Offline store does not support registering resources")
}

func (q postgresSQLQueries) primaryTableRegister(tableName string, sourceName string) string {
	return fmt.Sprintf("CREATE VIEW %s AS SELECT * FROM %s", sanitize(tableName), sanitize(sourceName))
}

func (q postgresSQLQueries) materializationCreate(tableName string, schema ResourceSchema) []string {
	const materializationCreateTemplate = `
CREATE MATERIALIZED VIEW IF NOT EXISTS {{.tableName}} AS
WITH OrderedSource AS (
  SELECT
    {{.entity}} AS entity,
    {{.value}} AS value,
    {{.tsSelectStatement}} AS ts,
    ROW_NUMBER() OVER (PARTITION BY {{.entity}} {{.tsOrderByStatement}}) AS rn
  FROM {{.sourceLocation}}
)
SELECT
  entity,
  value,
  ts,
  ROW_NUMBER() OVER (ORDER BY (entity)) AS row_number
FROM OrderedSource
WHERE rn = 1
`
	tmpl := template.Must(template.New("materializationCreateTemplate").Parse(materializationCreateTemplate))

	var tsSelectStatement, tsOrderByStatement string
	if schema.TS != "" {
		tsSelectStatement = fmt.Sprintf("%s", schema.TS)
		tsOrderByStatement = fmt.Sprintf("ORDER BY %s DESC", schema.TS)
	} else {
		tsSelectStatement = fmt.Sprintf("to_timestamp('%s', 'YYYY-DD-MM HH24:MI:SS +0000 UTC')::TIMESTAMPTZ", time.UnixMilli(0).UTC())
		tsOrderByStatement = ""
	}

	values := map[string]any{
		"tableName":          sanitize(tableName),
		"entity":             schema.Entity,
		"value":              schema.Value,
		"tsSelectStatement":  tsSelectStatement,
		"tsOrderByStatement": tsOrderByStatement,
		// TODO: Error checking for SQLLocation
		"sourceLocation": helper.SanitizeLocation(*schema.SourceTable.(*pl.SQLLocation)),
	}

	var sb strings.Builder
	err := tmpl.Execute(&sb, values)
	if err != nil {
		panic("TODO: Refactor to make error-able")
	}

	return []string{
		sb.String(),
		//fmt.Sprintf(
		//	"CREATE MATERIALIZED VIEW IF NOT EXISTS %s AS (SELECT entity, value, ts, row_number() over(ORDER BY (SELECT NULL)) as row_number FROM "+
		//		"(SELECT entity, ts, value, row_number() OVER (PARTITION BY entity ORDER BY ts desc) "+
		//		"AS rn FROM %s) t WHERE rn=1);", sanitize(tableName), sanitize(sourceName)),
		fmt.Sprintf("CREATE UNIQUE INDEX ON %s (entity);", sanitize(tableName)),
	}
}

func (q postgresSQLQueries) materializationUpdate(db *sql.DB, tableName string, sourceName string) error {
	if _, err := db.Exec(fmt.Sprintf("REFRESH MATERIALIZED VIEW CONCURRENTLY %s", sanitize(tableName))); err != nil {
		wrapped := fferr.NewExecutionError(pt.PostgresOffline.String(), err)
		wrapped.AddDetail("table_name", tableName)
		wrapped.AddDetail("source_name", sourceName)
		return wrapped
	}
	return nil
}

func (q postgresSQLQueries) materializationExists() string {
	return "SELECT * FROM pg_matviews WHERE matviewname = $1"
}

func (q postgresSQLQueries) determineColumnType(valueType types.ValueType) (string, error) {
	switch valueType {
	case types.Int, types.Int32, types.Int64:
		return "INT", nil
	case types.Float32, types.Float64:
		return "FLOAT8", nil
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

func (q postgresSQLQueries) newSQLOfflineTable(name string, columnType string) string {
	return fmt.Sprintf("CREATE TABLE %s (entity VARCHAR, value %s, ts TIMESTAMPTZ, UNIQUE (entity, ts))", sanitize(name), columnType)
}

func (q postgresSQLQueries) createValuePlaceholderString(columns []TableColumn) string {
	placeholders := make([]string, 0)
	for i := range columns {
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+1))
	}
	return strings.Join(placeholders, ", ")
}

func (q postgresSQLQueries) trainingSetCreate(store *sqlOfflineStore, def TrainingSetDef, tableName string, _ string) error {
	return q.trainingSetQuery(store, def, tableName, "", false)
}

func (q postgresSQLQueries) trainingSetUpdate(store *sqlOfflineStore, def TrainingSetDef, tableName string, _ string) error {
	return q.trainingSetQuery(store, def, tableName, "", true)
}

func (q postgresSQLQueries) adaptTsDefToBuilderParams(def TrainingSetDef) (tsq.BuilderParams, error) {
	sanitizeTableNameFn := func(loc pl.Location) (string, error) {
		lblLoc, isSQLLocation := loc.(*pl.SQLLocation)
		if !isSQLLocation {
			return "", fferr.NewInternalErrorf("label location is not an SQL location, actual %T. %v", lblLoc, lblLoc)
		}
		return helper.SanitizeLocation(*lblLoc), nil
	}

	// TODO: Create and pass in actual logger
	logger := logging.NewLogger("postgres-temp")
	return def.ToBuilderParams(logger, sanitizeTableNameFn)
}

func (q postgresSQLQueries) trainingSetQuery(store *sqlOfflineStore, def TrainingSetDef, tableName string, _ string, isUpdate bool) error {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE MATERIALIZED VIEW %s AS ", sanitize(tableName)))

	params, err := q.adaptTsDefToBuilderParams(def)
	if err != nil {
		return err
	}

	queryConfig := tsq.QueryConfig{
		UseAsOfJoin: false,
		QuoteChar:   "\"",
		QuoteTable:  false,
	}
	ts := tsq.NewTrainingSet(queryConfig, params)
	sql, err := ts.CompileSQL()
	if err != nil {
		return err
	}
	sb.WriteString(sql)
	if _, err := store.db.Exec(sb.String()); err != nil {
		wrapped := fferr.NewResourceExecutionError(pt.PostgresOffline.String(), def.ID.Name, def.ID.Variant, fferr.ResourceType(def.ID.Type.String()), err)
		wrapped.AddDetail("table_name", tableName)
		return wrapped
	}
	return nil
}

func (q postgresSQLQueries) castTableItemType(v interface{}, t interface{}) interface{} {
	if v == nil {
		return v
	}
	switch t {
	case pgInt:
		return int32(v.(int64))
	case pgBigInt:
		return int(v.(int64))
	case pgFloat:
		return v.(float64)
	case pgString:
		return v.(string)
	case pgBool:
		return v.(bool)
	case pgTimestamp:
		return v.(time.Time).UTC()
	default:
		return v
	}
}

func (q postgresSQLQueries) getValueColumnType(t *sql.ColumnType) interface{} {
	switch t.ScanType().String() {
	case "string":
		return pgString
	case "int32":
		return pgBigInt
	case "int64":
		return pgBigInt
	case "float32", "float64", "interface {}":
		return pgFloat
	case "bool":
		return pgBool
	case "time.Time":
		return pgTimestamp
	}
	return pgString
}

func (q postgresSQLQueries) numRows(n interface{}) (int64, error) {
	return n.(int64), nil
}

func (q postgresSQLQueries) transformationCreate(name string, query string) []string {
	return []string{
		fmt.Sprintf("CREATE TABLE  %s AS %s", sanitize(name), query),
	}
}

func (q postgresSQLQueries) transformationUpdate(db *sql.DB, tableName string, query string) error {
	tempName := sanitize(fmt.Sprintf("tmp_%s", tableName))
	fullQuery := fmt.Sprintf("CREATE TABLE %s AS %s", tempName, query)
	return q.atomicUpdate(db, tableName, tempName, fullQuery)
}

func (q postgresSQLQueries) transformationExists() string {
	return "SELECT * FROM pg_matviews WHERE matviewname = $1"
}

func (q postgresSQLQueries) getColumns(db *sql.DB, tableName string) ([]TableColumn, error) {
	var schemaName string
	err := db.QueryRow("SELECT current_schema()").Scan(&schemaName)
	if err != nil {
		return nil, fferr.NewExecutionError(pt.PostgresOffline.String(), err)
	}

	qry := `
		SELECT attname AS column_name
		FROM pg_attribute 
		WHERE attrelid = $1::regclass
		AND attnum > 0
		ORDER BY attnum;
	`

	// Assuming tableName might already include the schema, so we handle it appropriately
	qualifiedTableName := fmt.Sprintf("%s.%s", sanitize(schemaName), sanitize(tableName))

	rows, err := db.Query(qry, qualifiedTableName)
	if err != nil {
		wrapped := fferr.NewExecutionError(pt.PostgresOffline.String(), err)
		wrapped.AddDetail("schema_name", schemaName)
		wrapped.AddDetail("table_name", tableName)
		return nil, wrapped
	}
	defer rows.Close()

	columnNames := make([]TableColumn, 0)
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			wrapped := fferr.NewExecutionError(pt.PostgresOffline.String(), err)
			wrapped.AddDetail("schema_name", schemaName)
			wrapped.AddDetail("table_name", tableName)
			return nil, wrapped
		}
		columnNames = append(columnNames, TableColumn{Name: column})
	}

	if err := rows.Err(); err != nil {
		wrapped := fferr.NewExecutionError(pt.PostgresOffline.String(), err)
		wrapped.AddDetail("schema_name", schemaName)
		wrapped.AddDetail("table_name", tableName)
		return nil, wrapped
	}

	return columnNames, nil
}
