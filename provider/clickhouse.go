// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider

import (
	"crypto/tls"
	"database/sql"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/featureform/fferr"
	"github.com/featureform/metadata"
	pl "github.com/featureform/provider/location"
	pc "github.com/featureform/provider/provider_config"
	ps "github.com/featureform/provider/provider_schema"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/provider/types"
)

const (
	chInt     = "Int"
	chInt8    = "Int8"
	chInt16   = "Int16"
	chInt32   = "Int32"
	chInt64   = "Int64"
	chUInt8   = "UInt8"
	chUInt16  = "UInt16"
	chUInt32  = "UInt32"
	chUInt64  = "UInt64"
	chFloat64 = "Float64"
	chFloat32 = "Float32"
	chString  = "String"
	chBool    = "Bool"
	// we assume nanoseconds
	chDateTime = "DateTime64(9)"
)

// ClickHouse needs backticks for table names, not quotes
func SanitizeClickHouseIdentifier(ident string) string {
	s := strings.ReplaceAll(ident, string([]byte{0}), "")
	return "`" + s + "`"
}

type clickHouseOfflineStore struct {
	sqlOfflineStore
}

func (store *clickHouseOfflineStore) getResourceTableName(id ResourceID) (string, error) {
	return ps.ResourceToTableName(id.Type.String(), id.Name, id.Variant)
}

func (store *clickHouseOfflineStore) getTrainingSetName(id ResourceID) (string, error) {
	if err := id.check(TrainingSet); err != nil {
		return "", err
	}
	return ps.ResourceToTableName(id.Type.String(), id.Name, id.Variant)
}

func (store *clickHouseOfflineStore) tableExists(id ResourceID) (bool, error) {
	n := -1
	var tableName string
	var err error
	if id.check(Feature, Label) == nil {
		tableName, err = store.getResourceTableName(id)
	} else if id.check(TrainingSet) == nil {
		tableName, err = store.getTrainingSetName(id)
	} else if id.check(Primary) == nil || id.check(Transformation) == nil {
		tableName, err = GetPrimaryTableName(id)
	}
	if err != nil {
		return false, err
	}
	query := store.query.tableExists()
	err = store.db.QueryRow(query, tableName).Scan(&n)
	if n > 0 && err == nil {
		return true, nil
	} else if err != nil {
		return false, fferr.NewResourceExecutionError(pt.ClickHouseOffline.String(), id.Name, id.Variant, fferr.ResourceType(id.Type.String()), err)
	}
	query = store.query.viewExists()
	err = store.db.QueryRow(query, tableName).Scan(&n)
	if n > 0 && err == nil {
		return true, nil
	} else if err != nil {
		return false, fferr.NewResourceExecutionError(pt.ClickHouseOffline.String(), id.Name, id.Variant, fferr.ResourceType(id.Type.String()), err)
	}
	return false, nil
}

func (store *clickHouseOfflineStore) getTransformationTableName(id ResourceID) (string, error) {
	if err := id.check(Transformation); err != nil {
		return "", fferr.NewInternalErrorf("resource type must be %s: received %s", Transformation.String(), id.Type.String())
	}
	return ps.ResourceToTableName(id.Type.String(), id.Name, id.Variant)
}

func (store *clickHouseOfflineStore) createsqlPrimaryTableQuery(name string, schema TableSchema) (string, error) {
	columns := make([]string, 0)
	for _, column := range schema.Columns {
		columnType, err := store.query.determineColumnType(column.ValueType)
		if err != nil {
			return "", err
		}
		//enforce all columns as nullable
		columns = append(columns, fmt.Sprintf("%s Nullable(%s)", column.Name, columnType))
	}
	columnString := strings.Join(columns, ", ")
	return store.query.primaryTableCreate(name, columnString), nil
}

func (store *clickHouseOfflineStore) newsqlPrimaryTable(db *sql.DB, name string, schema TableSchema) (*clickhousePrimaryTable, error) {
	query, err := store.createsqlPrimaryTableQuery(name, schema)
	if err != nil {
		return nil, err
	}
	_, err = db.Exec(query)
	if err != nil {
		wrapped := fferr.NewExecutionError(pt.ClickHouseOffline.String(), err)
		wrapped.AddDetail("table_name", name)
		return nil, wrapped
	}
	return &clickhousePrimaryTable{
		db:     db,
		name:   name,
		schema: schema,
		query:  store.query,
	}, nil
}

func (store *clickHouseOfflineStore) getValueIndex(columns []TableColumn) int {
	for i, column := range columns {
		if column.Name == "value" {
			return i
		}
	}
	return -1
}

func (store *clickHouseOfflineStore) newsqlOfflineTable(db *sql.DB, name string, valueType types.ValueType) (*clickhouseOfflineTable, error) {
	columnType, err := determineColumnType(valueType)
	if err != nil {
		return nil, err
	}
	tableCreateQry := store.query.newSQLOfflineTable(name, columnType)
	_, err = db.Exec(tableCreateQry)
	if err != nil {
		wrapped := fferr.NewExecutionError(pt.ClickHouseOffline.String(), err)
		wrapped.AddDetail("table_name", name)
		return nil, wrapped
	}
	return &clickhouseOfflineTable{
		db:    db,
		name:  name,
		query: store.query,
	}, nil
}

func (store *clickHouseOfflineStore) getsqlResourceTable(id ResourceID) (*clickhouseOfflineTable, error) {
	if exists, err := store.tableExists(id); err != nil {
		return nil, err
	} else if !exists {
		return nil, fferr.NewDatasetNotFoundError(id.Name, id.Variant, nil)
	}
	table, err := store.getResourceTableName(id)
	if err != nil {
		return nil, err
	}
	return &clickhouseOfflineTable{
		db:    store.db,
		name:  table,
		query: store.query,
	}, nil
}

func (store *clickHouseOfflineStore) materializationExists(id MaterializationID) (bool, error) {
	name, variant, err := ps.MaterializationIDToResource(string(id))
	if err != nil {
		return false, err
	}
	tableName, err := store.getMaterializationTableName(ResourceID{name, variant, Feature})
	if err != nil {
		return false, err
	}
	getMatQry := store.query.materializationExists()
	n := -1
	execErr := store.db.QueryRow(getMatQry, tableName).Scan(&n)
	if execErr != nil {
		wrapped := fferr.NewExecutionError(pt.ClickHouseOffline.String(), err)
		wrapped.AddDetail("table_name", tableName)
		return false, wrapped
	}
	if n == 0 {
		return false, nil
	}
	return true, nil
}

func (store *clickHouseOfflineStore) getMaterializationTableName(id ResourceID) (string, error) {
	if err := id.check(Feature); err != nil {
		return "", err
	}
	// NOTE: Given Clickhouse uses intermediate resource tables, the inbound resource ID will be Feature;
	// however, the table must be named according to the FeatureMaterialization offline type.
	return ps.ResourceToTableName(FeatureMaterialization.String(), id.Name, id.Variant)
}

func clickhouseOfflineStoreFactory(config pc.SerializedConfig) (Provider, error) {
	store, err := NewClickHouseOfflineStore(config)
	if err != nil {
		return nil, err
	}
	return store, nil
}

func NewClickHouseOfflineStore(config pc.SerializedConfig) (*clickHouseOfflineStore, error) {
	cc := pc.ClickHouseConfig{}
	if err := cc.Deserialize(config); err != nil {
		return nil, err
	}
	var t *tls.Config
	if cc.SSL {
		t = &tls.Config{}
	}
	getDbFunc := func(database, schema string) (*sql.DB, error) {
		clickhouseDb := database
		if database != "" {
			clickhouseDb = cc.Database
		}
		return clickhouse.OpenDB(&clickhouse.Options{
			Addr: []string{fmt.Sprintf("%s:%d", cc.Host, cc.Port)},
			Auth: clickhouse.Auth{
				Database: clickhouseDb,
				Username: cc.Username,
				Password: cc.Password,
			},
			Settings: clickhouse.Settings{
				"final": "1",
			},
			TLS: t,
		}), nil
	}

	queries := clickhouseSQLQueries{}
	// numeric and should work
	queries.setVariableBinding(PostgresBindingStyle)
	sgConfig := SQLOfflineStoreConfig{
		Config:       config,
		Driver:       "clickhouse",
		ProviderType: pt.ClickHouseOffline,
		QueryImpl:    &queries,
	}
	db, getDbErr := getDbFunc(cc.Database, "") // clickhouse doesn't have a notion of schema
	if getDbErr != nil {
		return nil, fferr.NewConnectionError("failed to establish connection to ClickHouse: %v", getDbErr)
	}
	//we bypass NewSQLOfflineStore as we want to estalish our connection using non dsn syntax
	return &clickHouseOfflineStore{sqlOfflineStore{
		db:     db,
		parent: sgConfig,
		query:  &queries,
		BaseProvider: BaseProvider{
			ProviderType:   pt.ClickHouseOffline,
			ProviderConfig: config,
		},
		getDb: getDbFunc,
	}}, nil
}

type clickhouseOfflineTable struct {
	db    *sql.DB
	query OfflineTableQueries
	name  string
}

func (table *clickhouseOfflineTable) Write(rec ResourceRecord) error {
	rec = checkTimestamp(rec)
	tb := SanitizeClickHouseIdentifier(table.name)
	if err := rec.check(); err != nil {
		return err
	}
	insertQuery := table.query.writeInserts(tb)
	if _, err := table.db.Exec(insertQuery, rec.Entity, rec.Value, rec.TS); err != nil {
		wrapped := fferr.NewResourceExecutionError(pt.ClickHouseOffline.String(), rec.Entity, "", fferr.ENTITY, err)
		wrapped.AddDetail("table_name", table.name)
		return wrapped
	}
	return nil
}

const batchSize = 10000

func (table *clickhouseOfflineTable) WriteBatch(recs []ResourceRecord) error {
	tb := SanitizeClickHouseIdentifier(table.name)
	scope, err := table.db.Begin()
	if err != nil {
		wrapped := fferr.NewExecutionError(pt.ClickHouseOffline.String(), err)
		wrapped.AddDetail("table_name", table.name)
		return wrapped
	}
	batch, err := scope.Prepare(fmt.Sprintf("INSERT INTO %s (entity, value, ts)", tb))
	if err != nil {
		wrapped := fferr.NewExecutionError(pt.ClickHouseOffline.String(), err)
		wrapped.AddDetail("table_name", table.name)
		return wrapped
	}
	b := 0
	for i, _ := range recs {
		if recs[i].Entity == "" && recs[i].Value == nil && recs[i].TS.IsZero() {
			wrapped := fferr.NewInvalidArgumentError(fmt.Errorf("invalid record at offset %d", i))
			wrapped.AddDetail("table_name", table.name)
			return wrapped
		}
		ts := recs[i].TS
		// insert empty time.Time{} as 1970
		ts = checkZeroTime(recs[i].TS)
		_, err := batch.Exec(recs[i].Entity, recs[i].Value, ts)
		if err != nil {
			wrapped := fferr.NewExecutionError(pt.ClickHouseOffline.String(), err)
			wrapped.AddDetail("table_name", table.name)
			return wrapped
		}
		b += 1
		if b == batchSize {
			err = scope.Commit()
			if err != nil {
				wrapped := fferr.NewExecutionError(pt.ClickHouseOffline.String(), err)
				wrapped.AddDetail("table_name", table.name)
				return wrapped
			}
			batch, err = scope.Prepare(fmt.Sprintf("INSERT INTO %s (entity, value, ts)", tb))
			if err != nil {
				wrapped := fferr.NewExecutionError(pt.ClickHouseOffline.String(), err)
				wrapped.AddDetail("table_name", table.name)
				return wrapped
			}
			b = 0
		}
	}
	if err := scope.Commit(); err != nil {
		wrapped := fferr.NewExecutionError(pt.ClickHouseOffline.String(), err)
		wrapped.AddDetail("table_name", table.name)
		return wrapped
	}
	return nil
}

func (table *clickhouseOfflineTable) Location() pl.Location {
	return pl.NewSQLLocation(table.name)
}

type clickhousePrimaryTable struct {
	db     *sql.DB
	name   string
	query  OfflineTableQueries
	schema TableSchema
}

func (table *clickhousePrimaryTable) Write(rec GenericRecord) error {
	tb := SanitizeClickHouseIdentifier(table.name)
	columns := table.getColumnNameString()
	placeholder := table.query.createValuePlaceholderString(table.schema.Columns)
	upsertQuery := fmt.Sprintf(""+
		"INSERT INTO %s ( %s ) "+
		"VALUES ( %s ) ", tb, columns, placeholder)
	if _, err := table.db.Exec(upsertQuery, rec...); err != nil {
		wrapped := fferr.NewExecutionError(pt.ClickHouseOffline.String(), err)
		wrapped.AddDetail("table_name", table.name)
		return wrapped
	}
	return nil
}

func (table *clickhousePrimaryTable) WriteBatch(recs []GenericRecord) error {
	tb := SanitizeClickHouseIdentifier(table.name)
	columns := table.getColumnNameString()
	scope, err := table.db.Begin()
	if err != nil {
		wrapped := fferr.NewExecutionError(pt.ClickHouseOffline.String(), err)
		wrapped.AddDetail("table_name", table.name)
		return wrapped
	}
	batch, err := scope.Prepare(fmt.Sprintf("INSERT INTO %s (%s)", tb, columns))
	if err != nil {
		wrapped := fferr.NewExecutionError(pt.ClickHouseOffline.String(), err)
		wrapped.AddDetail("table_name", table.name)
		return wrapped
	}
	b := 0
	for i, _ := range recs {
		_, err := batch.Exec(recs[i]...)
		if err != nil {
			wrapped := fferr.NewExecutionError(pt.ClickHouseOffline.String(), err)
			wrapped.AddDetail("table_name", table.name)
			return wrapped
		}
		b += 1
		if b == batchSize {
			err = scope.Commit()
			if err != nil {
				wrapped := fferr.NewExecutionError(pt.ClickHouseOffline.String(), err)
				wrapped.AddDetail("table_name", table.name)
				return wrapped
			}
			batch, err = scope.Prepare(fmt.Sprintf("INSERT INTO %s (%s)", tb, columns))
			if err != nil {
				wrapped := fferr.NewExecutionError(pt.ClickHouseOffline.String(), err)
				wrapped.AddDetail("table_name", table.name)
				return wrapped
			}
			b = 0
		}
	}
	return scope.Commit()
}

func (table *clickhousePrimaryTable) getColumnNameString() string {
	columns := make([]string, 0)
	for _, column := range table.schema.Columns {
		columns = append(columns, column.Name)
	}
	return strings.Join(columns, ", ")
}

func (table *clickhousePrimaryTable) GetName() string {
	return table.name
}

func (table *clickhousePrimaryTable) IterateSegment(n int64) (GenericTableIterator, error) {
	columns, err := table.query.getColumns(table.db, table.name)
	if err != nil {
		return nil, err
	}
	columnNames := make([]string, 0)
	ecolumnNames := make([]string, 0)
	for _, col := range columns {
		columnNames = append(columnNames, col.Name)
		ecolumnNames = append(ecolumnNames, SanitizeClickHouseIdentifier(col.Name))
	}
	names := strings.Join(ecolumnNames[:], ", ")
	var query string
	if n == -1 {
		query = fmt.Sprintf("SELECT %s FROM %s", names, SanitizeClickHouseIdentifier(table.name))
	} else {
		query = fmt.Sprintf("SELECT %s FROM %s LIMIT %d", names, SanitizeClickHouseIdentifier(table.name), n)
	}
	rows, err := table.db.Query(query)
	if err != nil {
		wrapped := fferr.NewExecutionError(pt.ClickHouseOffline.String(), err)
		wrapped.AddDetail("table_name", table.name)
		return nil, wrapped
	}
	colTypes, err := table.getValueColumnTypes(table.name)
	if err != nil {
		return nil, err
	}
	return newClickHouseTableIterator(rows, colTypes, columnNames, table.query), nil
}

func (table *clickhousePrimaryTable) getValueColumnTypes(tb string) ([]interface{}, error) {
	query := table.query.getValueColumnTypes(tb)
	rows, err := table.db.Query(query)
	if err != nil {
		wrapped := fferr.NewExecutionError(pt.ClickHouseOffline.String(), err)
		wrapped.AddDetail("table_name", table.name)
		return nil, wrapped
	}
	defer rows.Close()
	colTypes := make([]interface{}, 0)
	if rows.Next() {
		rawType, err := rows.ColumnTypes()
		if err != nil {
			wrapped := fferr.NewExecutionError(pt.ClickHouseOffline.String(), err)
			wrapped.AddDetail("table_name", tb)
			return nil, wrapped
		}
		for _, t := range rawType {
			colTypes = append(colTypes, table.query.getValueColumnType(t))
		}
	}
	return colTypes, nil
}

func (table clickhousePrimaryTable) NumRows() (int64, error) {
	n := int64(0)
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", SanitizeClickHouseIdentifier(table.name))
	rows := table.db.QueryRow(query)

	err := rows.Scan(&n)
	if err != nil {
		wrapped := fferr.NewExecutionError(pt.ClickHouseOffline.String(), err)
		wrapped.AddDetail("table_name", table.name)
		return 0, wrapped
	}
	return n, nil
}

type clickHouseTableIterator struct {
	rows          *sql.Rows
	currentValues GenericRecord
	err           error
	columnTypes   []interface{}
	columnNames   []string
	query         OfflineTableQueries
}

func newClickHouseTableIterator(rows *sql.Rows, columnTypes []interface{}, columnNames []string, query OfflineTableQueries) GenericTableIterator {
	return &clickHouseTableIterator{
		rows:          rows,
		currentValues: nil,
		err:           nil,
		columnTypes:   columnTypes,
		columnNames:   columnNames,
		query:         query,
	}
}

// Next custom implementation for ClickHouse as we need to pass pointers to pointers for nullable types (so we can detect nil)
func (it *clickHouseTableIterator) Next() bool {
	if !it.rows.Next() {
		it.rows.Close()
		return false
	}
	columnTypes, err := it.rows.ColumnTypes()
	if err != nil {
		it.err = fferr.NewExecutionError(pt.ClickHouseOffline.String(), err)
		it.rows.Close()
		return false
	}
	pointers := make([]interface{}, len(columnTypes))
	for i := range columnTypes {
		elementType := columnTypes[i].ScanType()
		// Create a pointer to a pointer - we need for nulls
		pointers[i] = reflect.New(elementType).Interface()
	}
	if err := it.rows.Scan(pointers...); err != nil {
		it.err = fferr.NewExecutionError(pt.ClickHouseOffline.String(), err)
		it.rows.Close()
		return false
	}
	rowValues := make(GenericRecord, len(columnTypes))
	for i, value := range pointers {
		rowValues[i] = it.query.castTableItemType(value, it.columnTypes[i])
	}
	it.currentValues = rowValues
	return true
}

func (it *clickHouseTableIterator) Values() GenericRecord {
	return it.currentValues
}

func (it *clickHouseTableIterator) Columns() []string {
	return it.columnNames
}

func (it *clickHouseTableIterator) Err() error {
	return it.err
}

func (it *clickHouseTableIterator) Close() error {
	if err := it.rows.Close(); err != nil {
		return fferr.NewExecutionError(pt.ClickHouseOffline.String(), err)
	}
	return nil
}

func (store *clickHouseOfflineStore) RegisterResourceFromSourceTable(id ResourceID, schema ResourceSchema, opts ...ResourceOption) (OfflineTable, error) {
	if len(opts) > 0 {
		return nil, fferr.NewInternalErrorf("ClickHouse does not support resource options")
	}
	if err := id.check(Feature, Label); err != nil {
		return nil, err
	}
	if exists, err := store.tableExists(id); err != nil {
		return nil, err
	} else if exists {
		return nil, fferr.NewDatasetAlreadyExistsError(id.Name, id.Variant, nil)
	}
	if schema.Entity == "" || schema.Value == "" {
		return nil, fferr.NewInvalidArgumentError(fmt.Errorf("non-empty entity and value columns required"))
	}
	tableName, err := store.getResourceTableName(id)
	if err != nil {
		return nil, err
	}
	if schema.TS == "" {
		if err := store.query.registerResources(store.db, tableName, schema, false); err != nil {
			return nil, err
		}
	} else {
		if err := store.query.registerResources(store.db, tableName, schema, true); err != nil {
			return nil, err
		}
	}

	return &clickhouseOfflineTable{
		db:    store.db,
		name:  tableName,
		query: store.query,
	}, nil
}

func (store *clickHouseOfflineStore) AsOfflineStore() (OfflineStore, error) {
	return store, nil
}

func (store *clickHouseOfflineStore) CheckHealth() (bool, error) {
	err := store.db.Ping()
	if err != nil {
		wrapped := fferr.NewConnectionError(pt.ClickHouseOffline.String(), err)
		wrapped.AddDetail("action", "ping")
		return false, wrapped
	}
	return true, nil
}

func (store *clickHouseOfflineStore) Delete(location pl.Location) error {
	return fferr.NewInternalErrorf("delete not implemented")
}

func (store *clickHouseOfflineStore) RegisterPrimaryFromSourceTable(id ResourceID, tableLocation pl.Location) (PrimaryTable, error) {
	if err := id.check(Primary); err != nil {
		return nil, err
	}
	if exists, err := store.tableExists(id); err != nil {
		return nil, err
	} else if exists {
		return nil, fferr.NewDatasetAlreadyExistsError(id.Name, id.Variant, nil)
	}
	tableName, err := GetPrimaryTableName(id)
	if err != nil {
		return nil, err
	}
	query := store.query.primaryTableRegister(tableName, tableLocation.Location())
	if _, err := store.db.Exec(query); err != nil {
		wrapped := fferr.NewResourceExecutionError(pt.ClickHouseOffline.String(), id.Name, id.Variant, fferr.ResourceType(id.Type.String()), err)
		wrapped.AddDetail("table_name", tableName)
		wrapped.AddDetail("source_name", tableLocation.Location())
		return nil, wrapped
	}

	columnNames, err := store.query.getColumns(store.db, tableName)
	if err != nil {
		return nil, err
	}

	return &clickhousePrimaryTable{
		db:     store.db,
		name:   tableName,
		schema: TableSchema{Columns: columnNames},
		query:  store.query,
	}, nil
}

func (store *clickHouseOfflineStore) SupportsTransformationOption(opt TransformationOptionType) (bool, error) {
	return false, nil
}

func (store *clickHouseOfflineStore) CreateTransformation(config TransformationConfig, opts ...TransformationOption) error {
	if len(opts) > 0 {
		return fferr.NewInternalErrorf("ClickHouse does not support transformation options")
	}
	name, err := store.getTransformationTableName(config.TargetTableID)
	if err != nil {
		return err
	}
	queries := store.query.transformationCreate(name, config.Query)
	for _, query := range queries {
		if _, err := store.db.Exec(query); err != nil {
			wrapped := fferr.NewResourceExecutionError(pt.ClickHouseOffline.String(), config.TargetTableID.Name, config.TargetTableID.Variant, fferr.ResourceType(config.TargetTableID.Type.String()), err)
			wrapped.AddDetail("table_name", name)
			return wrapped
		}
	}
	return nil
}

func (store *clickHouseOfflineStore) GetTransformationTable(id ResourceID) (TransformationTable, error) {
	n := -1
	name, err := store.getTransformationTableName(id)
	if err != nil {
		return nil, err
	}
	existsQuery := store.query.tableExists()
	err = store.db.QueryRow(existsQuery, name).Scan(&n)
	if err != nil {
		return nil, fferr.NewResourceExecutionError(pt.ClickHouseOffline.String(), id.Name, id.Variant, fferr.ResourceType(id.Type.String()), err)
	}
	if n == 0 {
		return nil, fferr.NewDatasetNotFoundError(id.Name, id.Variant, nil)
	}
	columnNames, err := store.query.getColumns(store.db, name)
	if err != nil {
		return nil, err
	}
	return &clickhousePrimaryTable{
		db:     store.db,
		name:   name,
		schema: TableSchema{Columns: columnNames},
		query:  store.query,
	}, nil
}

func (store *clickHouseOfflineStore) UpdateTransformation(config TransformationConfig, opts ...TransformationOption) error {
	if len(opts) > 0 {
		return fferr.NewInternalErrorf("ClickHouse does not support transformation options")
	}
	name, err := store.getTransformationTableName(config.TargetTableID)
	if err != nil {
		return err
	}
	err = store.query.transformationUpdate(store.db, name, config.Query)
	if err != nil {
		return err
	}
	return nil
}

func (store *clickHouseOfflineStore) CreatePrimaryTable(id ResourceID, schema TableSchema) (PrimaryTable, error) {
	if err := id.check(Primary); err != nil {
		return nil, err
	}
	if exists, err := store.tableExists(id); err != nil {
		return nil, err
	} else if exists {
		return nil, fferr.NewDatasetAlreadyExistsError(id.Name, id.Variant, nil)
	}
	if len(schema.Columns) == 0 {
		return nil, fferr.NewInvalidArgumentError(fmt.Errorf("cannot create primary table without columns"))
	}
	tableName, err := GetPrimaryTableName(id)
	if err != nil {
		return nil, err
	}
	table, err := store.newsqlPrimaryTable(store.db, tableName, schema)
	if err != nil {
		return nil, err
	}
	return table, nil
}

func (store *clickHouseOfflineStore) GetPrimaryTable(id ResourceID, source metadata.SourceVariant) (PrimaryTable, error) {
	name, err := GetPrimaryTableName(id)
	if err != nil {
		return nil, err
	}
	if exists, err := store.tableExists(id); err != nil {
		return nil, err
	} else if !exists {
		return nil, fferr.NewDatasetNotFoundError(id.Name, id.Variant, nil)
	}
	columnNames, err := store.query.getColumns(store.db, name)
	if err != nil {
		return nil, err
	}
	return &clickhousePrimaryTable{
		db:     store.db,
		name:   name,
		schema: TableSchema{Columns: columnNames},
		query:  store.query,
	}, nil
}

func (store *clickHouseOfflineStore) CreateResourceTable(id ResourceID, schema TableSchema) (OfflineTable, error) {
	if err := id.check(Feature, Label); err != nil {
		return nil, err
	}

	if exists, err := store.tableExists(id); err != nil {
		return nil, err
	} else if exists {
		return nil, fferr.NewDatasetAlreadyExistsError(id.Name, id.Variant, nil)
	}
	tableName, err := store.getResourceTableName(id)
	if err != nil {
		return nil, err
	}
	var valueType types.ValueType
	if valueIndex := store.getValueIndex(schema.Columns); valueIndex > 0 {
		valueType = schema.Columns[valueIndex].ValueType
	} else {
		valueType = types.NilType

	}
	table, err := store.newsqlOfflineTable(store.db, tableName, valueType)
	if err != nil {
		return nil, err
	}
	return table, nil
}

func (store *clickHouseOfflineStore) GetResourceTable(id ResourceID) (OfflineTable, error) {
	return store.getsqlResourceTable(id)
}

func (store *clickHouseOfflineStore) GetBatchFeatures(ids []ResourceID) (BatchFeatureIterator, error) {

	// if tables is empty, return an empty iterator
	if len(ids) == 0 {
		return newsqlBatchFeatureIterator(nil, nil, nil, store.query, store.Type()), fferr.NewInvalidArgumentError(fmt.Errorf("no features provided"))
	}

	asEntity := ""
	withFeatures := ""
	joinTables := ""
	featureColumns := ""
	var matIDs []string
	tableName0, err := store.getMaterializationTableName(ids[0])
	if err != nil {
		return nil, err
	}
	for i, tableID := range ids {
		matID, err := NewMaterializationID(tableID)
		if err != nil {
			return nil, err
		}
		matIDs = append(matIDs, string(matID))
		matTableName, err := store.getMaterializationTableName(tableID)
		if err != nil {
			return nil, err
		}
		matTableName = SanitizeClickHouseIdentifier(matTableName)

		if i > 0 {
			joinTables += "FULL OUTER JOIN "
		}
		withFeatures += fmt.Sprintf(", %s.value AS feature%d, %s.ts AS TS%d ", matTableName, i+1, matTableName, i+1)
		featureColumns += fmt.Sprintf(", feature%d", i+1)
		joinTables += fmt.Sprintf("%s ", matTableName)
		if i == 1 {
			joinTables += fmt.Sprintf("ON %s = %s.entity ", asEntity, matTableName)
			asEntity += ", "
		}
		if i > 1 {
			joinTables += fmt.Sprintf("ON COALESCE(%s) = %s.entity ", asEntity, matTableName)
			asEntity += ", "
		}
		asEntity += fmt.Sprintf("nullIf(%s.entity, '')", matTableName)
	}

	sort.Strings(matIDs)
	joinedTableName := store.getJoinedMaterializationTableName(strings.Join(matIDs, "_"))
	createQuery := ""
	if len(ids) == 1 {
		createQuery = fmt.Sprintf("CREATE VIEW `%s` AS SELECT %s AS entity %s FROM %s", joinedTableName, asEntity, withFeatures, SanitizeClickHouseIdentifier(tableName0))
	} else {
		createQuery = fmt.Sprintf("CREATE VIEW `%s` AS SELECT COALESCE(%s) AS entity %s FROM %s", joinedTableName, asEntity, withFeatures, joinTables)
	}
	store.db.Query(createQuery)
	createQueryWithoutTS := fmt.Sprintf("CREATE VIEW `no_ts_%s` AS SELECT entity%s FROM \"%s\"", joinedTableName, featureColumns, joinedTableName)
	store.db.Query(createQueryWithoutTS)
	select_query := fmt.Sprintf("SELECT * FROM `no_ts_%s`", joinedTableName)
	resultRows, err := store.db.Query(select_query)
	if err != nil {
		return nil, fferr.NewExecutionError(pt.ClickHouseOffline.String(), err)
	}
	if resultRows == nil {
		return newsqlBatchFeatureIterator(nil, nil, nil, store.query, store.Type()), nil
	}
	columnTypes, err := store.getValueColumnTypes(fmt.Sprintf("no_ts_%s", joinedTableName))
	if err != nil {
		return nil, err
	}
	columns, err := store.query.getColumns(store.db, fmt.Sprintf("no_ts_%s", joinedTableName))
	if err != nil {
		return nil, err
	}
	dropQuery := fmt.Sprintf("DROP VIEW `no_ts_%s`", joinedTableName)
	store.db.Query(dropQuery)
	columnNames := make([]string, 0)
	for _, col := range columns {
		columnNames = append(columnNames, SanitizeClickHouseIdentifier(col.Name))
	}
	return newsqlBatchFeatureIterator(resultRows, columnTypes, columnNames, store.query, store.Type()), nil
}

func (store *clickHouseOfflineStore) CreateMaterialization(id ResourceID, opts MaterializationOptions) (Materialization, error) {
	if id.Type != Feature {
		return nil, fferr.NewInvalidResourceTypeError(id.Name, id.Variant, fferr.ResourceType(id.Type.String()), fmt.Errorf("only features can be materialized"))
	}
	resTable, err := store.getsqlResourceTable(id)
	if err != nil {
		return nil, err
	}

	matID := MaterializationID(fmt.Sprintf("%s__%s", id.Name, id.Variant))
	matTableName, err := store.getMaterializationTableName(id)
	if err != nil {
		return nil, err
	}
	materializeQueries := store.query.materializationCreate(matTableName, resTable.name)
	for _, materializeQry := range materializeQueries {
		_, err = store.db.Exec(materializeQry)
		if err != nil {
			wrapped := fferr.NewInvalidResourceTypeError(id.Name, id.Variant, fferr.ResourceType(id.Type.String()), err)
			wrapped.AddDetail("resource_table_name", resTable.name)
			wrapped.AddDetail("materialization_table_name", matTableName)
			return nil, wrapped
		}
	}
	return &clickHouseMaterialization{
		id:        matID,
		db:        store.db,
		tableName: matTableName,
		query:     store.query,
	}, nil
}

func (store *clickHouseOfflineStore) SupportsMaterializationOption(opt MaterializationOptionType) (bool, error) {
	return false, nil
}

func (store *clickHouseOfflineStore) GetMaterialization(id MaterializationID) (Materialization, error) {
	name, variant, err := ps.MaterializationIDToResource(string(id))
	if err != nil {
		return nil, err
	}
	tableName, err := store.getMaterializationTableName(ResourceID{name, variant, Feature})
	if err != nil {
		return nil, err
	}

	getMatQry := store.query.materializationExists()
	n := -1
	execErr := store.db.QueryRow(getMatQry, tableName).Scan(&n)
	if execErr != nil {
		wrapped := fferr.NewExecutionError(pt.ClickHouseOffline.String(), err)
		wrapped.AddDetail("table_name", tableName)
		return nil, wrapped
	}
	if n == 0 {
		return nil, fferr.NewDatasetNotFoundError(string(id), "", nil)
	}
	return &clickHouseMaterialization{
		id:        id,
		db:        store.db,
		tableName: tableName,
		query:     store.query,
	}, err
}

func (store *clickHouseOfflineStore) UpdateMaterialization(id ResourceID, opts MaterializationOptions) (Materialization, error) {
	matID, err := NewMaterializationID(id)
	if err != nil {
		return nil, err
	}
	tableName, err := store.getMaterializationTableName(id)
	if err != nil {
		return nil, err
	}
	getMatQry := store.query.materializationExists()
	resTable, err := store.getsqlResourceTable(id)
	if err != nil {
		return nil, err
	}

	rows, err := store.db.Query(getMatQry, tableName)
	if err != nil {
		return nil, fferr.NewResourceExecutionError(pt.ClickHouseOffline.String(), id.Name, id.Variant, fferr.ResourceType(id.Type.String()), err)
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, fferr.NewDatasetNotFoundError(id.Name, id.Variant, fmt.Errorf("table %s is empty", tableName))
	}
	err = store.query.materializationUpdate(store.db, tableName, resTable.name)
	if err != nil {
		return nil, err
	}
	return &clickHouseMaterialization{
		id:        matID,
		db:        store.db,
		tableName: tableName,
		query:     store.query,
	}, err
}

func (store *clickHouseOfflineStore) DeleteMaterialization(id MaterializationID) error {
	name, variant, err := ps.MaterializationIDToResource(string(id))
	if err != nil {
		return err
	}
	tableName, err := store.getMaterializationTableName(ResourceID{name, variant, Feature})
	if err != nil {
		return err
	}
	if exists, err := store.materializationExists(id); err != nil {
		return err
	} else if !exists {
		return fferr.NewDatasetNotFoundError(string(id), "", nil)
	}
	query := store.query.materializationDrop(tableName)
	if _, err := store.db.Exec(query); err != nil {
		wrapped := fferr.NewExecutionError(pt.ClickHouseOffline.String(), err)
		wrapped.AddDetail("table_name", tableName)
		return wrapped
	}
	return nil
}

func (store *clickHouseOfflineStore) CreateTrainingSet(def TrainingSetDef) error {
	if err := def.check(); err != nil {
		return err
	}
	label, err := store.getsqlResourceTable(def.Label)
	if err != nil {
		return err
	}
	tableName, err := store.getTrainingSetName(def.ID)
	if err != nil {
		return err
	}
	if err := store.query.trainingSetCreate(&store.sqlOfflineStore, def, tableName, label.name); err != nil {
		return err
	}

	return nil
}

func (store *clickHouseOfflineStore) UpdateTrainingSet(def TrainingSetDef) error {
	if err := def.check(); err != nil {
		return err
	}
	label, err := store.getsqlResourceTable(def.Label)
	if err != nil {
		return err
	}
	tableName, err := store.getTrainingSetName(def.ID)
	if err != nil {
		return err
	}
	if err := store.query.trainingSetUpdate(&store.sqlOfflineStore, def, tableName, label.name); err != nil {
		return err
	}

	return nil
}

type TrainingSetPreparation struct {
	TrainingSetName string
	Columns         string
}

func (store *clickHouseOfflineStore) prepareTrainingSetQuery(id ResourceID) (*TrainingSetPreparation, error) {
	if err := id.check(TrainingSet); err != nil {
		return nil, err
	}
	if exists, err := store.tableExists(id); err != nil {
		return nil, err
	} else if !exists {
		return nil, fferr.NewDatasetNotFoundError(id.Name, id.Variant, nil)
	}
	trainingSetName, err := store.getTrainingSetName(id)
	if err != nil {
		return nil, err
	}
	columnNames, err := store.query.getColumns(store.db, trainingSetName)
	if err != nil {
		return nil, err
	}
	features := make([]string, 0)
	for _, name := range columnNames {
		features = append(features, SanitizeClickHouseIdentifier(name.Name))
	}
	columns := strings.Join(features, ", ")

	return &TrainingSetPreparation{
		TrainingSetName: trainingSetName,
		Columns:         columns,
	}, nil
}

func (store *clickHouseOfflineStore) GetTrainingSet(id ResourceID) (TrainingSetIterator, error) {
	fmt.Printf("Getting Training Set: %v\n", id)
	prep, err := store.prepareTrainingSetQuery(id)
	if err != nil {
		return nil, err
	}
	trainingSetQry := store.query.trainingRowSelect(prep.Columns, prep.TrainingSetName)
	fmt.Printf("Training Set Query: %s\n", trainingSetQry)
	rows, err := store.db.Query(trainingSetQry)
	if err != nil {
		return nil, fferr.NewResourceExecutionError(pt.ClickHouseOffline.String(), id.Name, id.Variant, fferr.ResourceType(id.Type.String()), err)
	}
	colTypes, err := store.getValueColumnTypes(prep.TrainingSetName)
	if err != nil {
		return nil, err
	}
	return store.newsqlTrainingSetIterator(rows, colTypes), nil
}

func (store *clickHouseOfflineStore) CreateTrainTestSplit(def TrainTestSplitDef) (func() error, error) {
	prep, err := store.prepareTrainingSetQuery(ResourceID{Name: def.TrainingSetName, Variant: def.TrainingSetVariant})
	if err != nil {
		return nil, err
	}
	trainTestSplitTableName := store.getTrainTestSplitTableName(prep.TrainingSetName, def)

	// Generate ORDER BY clause for shuffling if needed
	orderByClause := ""
	randomState := def.RandomState
	if def.Shuffle {
		if def.RandomState == 0 {
			randomState = rand.Int() // Ensure a random state if 0 is provided
		}
		// Use ClickHouse's cityHash64 for deterministic shuffling, rowNumberInAllBlocks is a unique identifier for each row
		orderByClause = fmt.Sprintf("ORDER BY cityHash64(concat(toString(_row), toString(%d)))", randomState)
	}

	// Calculate the number of test rows based on the testSize parameter
	var totalRows int
	err = store.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", SanitizeClickHouseIdentifier(prep.TrainingSetName))).Scan(&totalRows)
	if err != nil {
		return nil, fmt.Errorf("failed to get table size: %v", err)
	}
	testRows := int(float32(totalRows) * def.TestSize)

	// Create the final view with an 'is_test' column
	createViewQuery := fmt.Sprintf(`
			CREATE VIEW IF NOT EXISTS %s AS
			SELECT *, IF(row_number() OVER (%s) <= %d, 1, 0) AS is_test
			FROM %s
    	`,
		SanitizeClickHouseIdentifier(trainTestSplitTableName),
		orderByClause,
		testRows,
		SanitizeClickHouseIdentifier(prep.TrainingSetName),
	)

	if _, err := store.db.Exec(createViewQuery); err != nil {
		return nil, fmt.Errorf("failed to create final view: %v", err)
	}

	// return callback to drop view
	dropFunc := func() error {
		dropQuery := fmt.Sprintf("DROP VIEW if exists %s", SanitizeClickHouseIdentifier(trainTestSplitTableName))
		_, err := store.db.Exec(dropQuery)
		if err != nil {
			return fmt.Errorf("could not drop test split: %v", err)
		}
		return nil
	}

	return dropFunc, nil
}

func (store *clickHouseOfflineStore) getTrainTestSplitTableName(trainingSetTable string, def TrainTestSplitDef) string {
	// Generate unique suffix for the view names
	tableNameSuffix := fmt.Sprintf("%s_%d_%t_%d", trainingSetTable, int(def.TestSize*100), def.Shuffle, def.RandomState)
	trainTestSplitViewName := fmt.Sprintf("%s_split", tableNameSuffix)
	return trainTestSplitViewName
}

func (store *clickHouseOfflineStore) GetTrainTestSplit(def TrainTestSplitDef) (TrainingSetIterator, TrainingSetIterator, error) {
	prep, err := store.prepareTrainingSetQuery(ResourceID{Name: def.TrainingSetName, Variant: def.TrainingSetVariant})
	if err != nil {
		return nil, nil, err
	}
	trainTestSplitTableName := store.getTrainTestSplitTableName(prep.TrainingSetName, def)
	train, test := store.query.trainingRowSplitSelect(prep.Columns, SanitizeClickHouseIdentifier(trainTestSplitTableName))

	fmt.Printf("Training Set Query: %s\n", train)
	fmt.Printf("Test Set Query: %s\n", test)

	testRows, err := store.db.Query(test)
	if err != nil {
		return nil, nil, fmt.Errorf("could not query test set: %v", err)
	}
	trainRows, err := store.db.Query(train)
	if err != nil {
		return nil, nil, fmt.Errorf("could not query train set: %v", err)

	}

	colTypes, err := store.getValueColumnTypes(trainTestSplitTableName)
	if err != nil {
		return nil, nil, fmt.Errorf("could not get column types: %v", err)
	}

	return store.newsqlTrainingSetIterator(trainRows, colTypes), store.newsqlTrainingSetIterator(testRows, colTypes), nil

}

func (store *clickHouseOfflineStore) ResourceLocation(id ResourceID, resource any) (pl.Location, error) {
	if exists, err := store.tableExistsForResourceId(id); err != nil {
		return nil, err
	} else if !exists {
		return nil, fmt.Errorf("table does not exist: %v", id)
	}

	tableName, err := ps.ResourceToTableName(id.Type.String(), id.Name, id.Variant)
	if err != nil {
		return nil, err
	}

	return pl.NewSQLLocation(tableName), err
}

func (store *clickHouseOfflineStore) Close() error {
	return store.db.Close()
}

type clickhouseSQLQueries struct {
	defaultOfflineSQLQueries
}

func (q clickhouseSQLQueries) tableExists() string {
	return "SELECT count() FROM system.tables WHERE table = $1 AND (database = currentDatabase())"
}

func (q clickhouseSQLQueries) viewExists() string {
	return "SELECT count() FROM system.tables WHERE table = $1 AND engine='View' AND (database = currentDatabase())"
}

func (q clickhouseSQLQueries) primaryTableCreate(name string, columnString string) string {
	return fmt.Sprintf("CREATE TABLE %s ( %s ) ENGINE=MergeTree ORDER BY ()", SanitizeClickHouseIdentifier(name), columnString)
}

func (q clickhouseSQLQueries) trainingRowSelect(columns string, trainingSetName string) string {
	// ensures random order - table is ordered by _row which is inserted at insert time
	return fmt.Sprintf("SELECT * EXCEPT _row FROM (SELECT %s FROM %s ORDER BY _row ASC)", columns, SanitizeClickHouseIdentifier(trainingSetName))
}

func (q clickhouseSQLQueries) trainingRowSplitSelect(columns string, trainingSetSplitName string) (string, string) {
	testSplitQuery := fmt.Sprintf("SELECT * EXCEPT _row FROM (SELECT %s FROM %s WHERE `is_test` = 1 ORDER BY _row ASC)", columns, trainingSetSplitName)
	trainSplitQuery := fmt.Sprintf("SELECT * EXCEPT _row FROM (SELECT %s FROM %s WHERE `is_test` = 0 ORDER BY _row ASC)", columns, trainingSetSplitName)

	return trainSplitQuery, testSplitQuery
}

func (q clickhouseSQLQueries) registerResources(db *sql.DB, tableName string, schema ResourceSchema, timestamp bool) error {
	var query string
	if timestamp {
		query = fmt.Sprintf("CREATE VIEW %s AS SELECT %s as entity, %s as value, %s as ts FROM %s", SanitizeClickHouseIdentifier(tableName),
			SanitizeClickHouseIdentifier(schema.Entity), SanitizeClickHouseIdentifier(schema.Value), SanitizeClickHouseIdentifier(schema.TS), SanitizeClickHouseIdentifier(schema.SourceTable.Location()))
	} else {
		query = fmt.Sprintf("CREATE VIEW %s AS SELECT %s as entity, %s as value, toDateTime64(0, 9) AS ts FROM %s", SanitizeClickHouseIdentifier(tableName),
			SanitizeClickHouseIdentifier(schema.Entity), SanitizeClickHouseIdentifier(schema.Value), SanitizeClickHouseIdentifier(schema.SourceTable.Location()))
	}
	fmt.Printf("Resource creation query: %s\n", query)
	if _, err := db.Exec(query); err != nil {
		wrapped := fferr.NewExecutionError(pt.ClickHouseOffline.String(), err)
		wrapped.AddDetail("table_name", tableName)
		return wrapped
	}
	return nil
}

func (q clickhouseSQLQueries) primaryTableRegister(tableName string, sourceName string) string {
	return fmt.Sprintf("CREATE VIEW %s AS SELECT * FROM %s", SanitizeClickHouseIdentifier(tableName), sourceName)
}

func (q clickhouseSQLQueries) materializationCreate(tableName string, sourceName string) []string {
	return []string{fmt.Sprintf("CREATE TABLE %s ENGINE = MergeTree ORDER BY (entity, ts) SETTINGS allow_nullable_key=1 EMPTY AS SELECT * FROM %s", SanitizeClickHouseIdentifier(tableName), SanitizeClickHouseIdentifier(sourceName)),
		fmt.Sprintf("ALTER TABLE %s ADD COLUMN row_number UInt64;", SanitizeClickHouseIdentifier(tableName)),
		fmt.Sprintf("INSERT INTO %s SELECT entity, value, tis AS ts, row_number() OVER () AS row_number FROM (SELECT entity, max(ts) AS tis, argMax(value, ts) AS value FROM %s GROUP BY entity ORDER BY entity ASC, value ASC);", SanitizeClickHouseIdentifier(tableName), SanitizeClickHouseIdentifier(sourceName)),
	}
}

func (q clickhouseSQLQueries) materializationUpdate(db *sql.DB, tableName string, sourceName string) error {
	// create a new table
	currentTime := time.Now()
	epochMilliseconds := currentTime.UnixNano() / int64(time.Millisecond)
	if _, err := db.Exec(fmt.Sprintf("CREATE TABLE %s AS %s", SanitizeClickHouseIdentifier(fmt.Sprintf("%s_%d", tableName, epochMilliseconds)), SanitizeClickHouseIdentifier(tableName))); err != nil {
		wrapped := fferr.NewExecutionError(pt.ClickHouseOffline.String(), err)
		wrapped.AddDetail("table_name", tableName)
		wrapped.AddDetail("source_name", sourceName)
		return wrapped
	}
	if _, err := db.Exec(fmt.Sprintf("INSERT INTO %s SELECT entity, value, tis AS ts, row_number() OVER () AS row_number FROM (SELECT entity, max(ts) AS tis, argMax(value, ts) AS value FROM %s GROUP BY entity ORDER BY entity ASC, value ASC);", SanitizeClickHouseIdentifier(fmt.Sprintf("%s_%d", tableName, epochMilliseconds)), SanitizeClickHouseIdentifier(sourceName))); err != nil {
		wrapped := fferr.NewExecutionError(pt.ClickHouseOffline.String(), err)
		wrapped.AddDetail("table_name", tableName)
		wrapped.AddDetail("source_name", sourceName)
		return wrapped
	}
	if _, err := db.Exec(fmt.Sprintf("EXCHANGE TABLES %s AND %s", SanitizeClickHouseIdentifier(fmt.Sprintf("%s_%d", tableName, epochMilliseconds)), SanitizeClickHouseIdentifier(tableName))); err != nil {
		wrapped := fferr.NewExecutionError(pt.ClickHouseOffline.String(), err)
		wrapped.AddDetail("table_name", tableName)
		wrapped.AddDetail("source_name", sourceName)
		return wrapped
	}
	if _, err := db.Exec(fmt.Sprintf("DROP TABLE %s", SanitizeClickHouseIdentifier(fmt.Sprintf("%s_%d", tableName, epochMilliseconds)))); err != nil {
		wrapped := fferr.NewExecutionError(pt.ClickHouseOffline.String(), err)
		wrapped.AddDetail("table_name", tableName)
		wrapped.AddDetail("source_name", sourceName)
		return wrapped
	}
	return nil
}

func (q clickhouseSQLQueries) materializationExists() string {
	return q.tableExists()
}

func (q clickhouseSQLQueries) determineColumnType(valueType types.ValueType) (string, error) {
	switch valueType {
	case types.Int:
		return chInt, nil
	case types.Int8:
		return chInt8, nil
	case types.Int16:
		return chInt16, nil
	case types.Int64:
		return chInt64, nil
	case types.Int32:
		return chInt32, nil
	case types.UInt8:
		return chUInt8, nil
	case types.UInt16:
		return chUInt16, nil
	case types.UInt32:
		return chUInt32, nil
	case types.UInt64:
		return chUInt64, nil
	case types.Float32:
		return chFloat32, nil
	case types.Float64:
		return chFloat64, nil
	case types.String:
		return chString, nil
	case types.Bool:
		return chBool, nil
	case types.Timestamp:
		//TODO: determine precision
		return chDateTime, nil
	case types.NilType:
		return "Null", nil
	default:
		return "", fferr.NewDataTypeNotFoundErrorf(valueType, "could not determine column type")
	}
}

func (q clickhouseSQLQueries) newSQLOfflineTable(name string, columnType string) string {
	// currently we allow nullable keys and use a ReplacingMergeTree to handle updates. We may wish to remove ts from the ordering key and remove nullable keys
	return fmt.Sprintf("CREATE TABLE %s (entity String, value Nullable(%s), ts DateTime64(9)) ENGINE = ReplacingMergeTree ORDER BY (entity, ts) SETTINGS allow_nullable_key=1", SanitizeClickHouseIdentifier(name), columnType)
}

func (q clickhouseSQLQueries) writeUpdate(table string) string {
	bind := q.newVariableBindingIterator()
	return fmt.Sprintf("INSERT INTO %s (entity, value, ts) VALUES (%s, %s, %s)", table, bind.Next(), bind.Next(), bind.Next())
}

func (q clickhouseSQLQueries) writeInserts(table string) string {
	bind := q.newVariableBindingIterator()
	return fmt.Sprintf("INSERT INTO %s (entity, value, ts) VALUES (%s, %s, %s)", table, bind.Next(), bind.Next(), bind.Next())
}

func (q clickhouseSQLQueries) createValuePlaceholderString(columns []TableColumn) string {
	placeholders := make([]string, 0)
	for i := range columns {
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+1))
	}
	return strings.Join(placeholders, ", ")
}

func (q clickhouseSQLQueries) trainingSetCreate(store *sqlOfflineStore, def TrainingSetDef, tableName string, labelName string) error {
	return q.trainingSetQuery(store, def, tableName, labelName, false)
}

func (q clickhouseSQLQueries) trainingSetUpdate(store *sqlOfflineStore, def TrainingSetDef, tableName string, labelName string) error {
	return q.trainingSetQuery(store, def, tableName, labelName, true)
}

func buildTrainingSelect(store *sqlOfflineStore, def TrainingSetDef, tableName string, labelName string) (string, error) {
	columns := make([]string, 0)
	query := ""
	for i, feature := range def.Features {
		tableName, err := store.getResourceTableName(feature)
		if err != nil {
			return "", err
		}
		santizedName := SanitizeClickHouseIdentifier(tableName)
		tableJoinAlias := fmt.Sprintf("t%d", i)
		columns = append(columns, fmt.Sprintf("%s.value AS %s", tableJoinAlias, santizedName))
		query = fmt.Sprintf("%s ASOF LEFT JOIN (SELECT entity, value, ts FROM %s) AS %s ON (%s.entity = l.entity) AND (%s.ts <= l.ts)",
			query, santizedName, tableJoinAlias, tableJoinAlias, tableJoinAlias)
	}
	columnStr := strings.Join(columns, ", ")
	// rand gives us a UInt32 to ensure random order
	query = fmt.Sprintf("SELECT %s, l.value as label, rand() as _row FROM %s AS l %s", columnStr, SanitizeClickHouseIdentifier(labelName), query)
	return query, nil
}

func (q clickhouseSQLQueries) trainingSetQuery(store *sqlOfflineStore, def TrainingSetDef, tableName string, labelName string, isUpdate bool) error {
	query, err := buildTrainingSelect(store, def, tableName, labelName)
	if err != nil {
		return err
	}
	if !isUpdate {
		// use a 2-step EMPTY create so ClickHouse Cloud compatible
		createQuery := fmt.Sprintf("CREATE TABLE %s ENGINE = MergeTree ORDER BY _row EMPTY AS (%s)", SanitizeClickHouseIdentifier(tableName), query)
		if _, err := store.db.Exec(createQuery); err != nil {
			wrapped := fferr.NewResourceExecutionError(pt.ClickHouseOffline.String(), def.ID.Name, def.ID.Variant, fferr.TRAINING_SET_VARIANT, err)
			wrapped.AddDetail("table_name", tableName)
			wrapped.AddDetail("label_name", labelName)
			return wrapped
		}
		insertQuery := fmt.Sprintf("INSERT INTO %s %s", SanitizeClickHouseIdentifier(tableName), query)
		if _, err := store.db.Exec(insertQuery); err != nil {
			wrapped := fferr.NewResourceExecutionError(pt.ClickHouseOffline.String(), def.ID.Name, def.ID.Variant, fferr.TRAINING_SET_VARIANT, err)
			wrapped.AddDetail("table_name", tableName)
			wrapped.AddDetail("label_name", labelName)
			return wrapped
		}
	} else {
		tempName := SanitizeClickHouseIdentifier(fmt.Sprintf("tmp_%s", tableName))
		createQuery := fmt.Sprintf("CREATE TABLE %s ENGINE = MergeTree ORDER BY _row EMPTY AS (%s)", tempName, query)
		if _, err := store.db.Exec(createQuery); err != nil {
			wrapped := fferr.NewResourceExecutionError(pt.ClickHouseOffline.String(), def.ID.Name, def.ID.Variant, fferr.TRAINING_SET_VARIANT, err)
			wrapped.AddDetail("table_name", tableName)
			wrapped.AddDetail("label_name", labelName)
			return wrapped
		}
		insertQuery := fmt.Sprintf("INSERT INTO %s %s", tempName, query)
		if _, err := store.db.Exec(insertQuery); err != nil {
			wrapped := fferr.NewResourceExecutionError(pt.ClickHouseOffline.String(), def.ID.Name, def.ID.Variant, fferr.TRAINING_SET_VARIANT, err)
			wrapped.AddDetail("table_name", tableName)
			wrapped.AddDetail("label_name", labelName)
			return wrapped
		}
		if _, err := store.db.Exec(fmt.Sprintf("EXCHANGE TABLES %s AND %s", SanitizeClickHouseIdentifier(tableName), tempName)); err != nil {
			wrapped := fferr.NewResourceExecutionError(pt.ClickHouseOffline.String(), def.ID.Name, def.ID.Variant, fferr.TRAINING_SET_VARIANT, err)
			wrapped.AddDetail("table_name", tableName)
			wrapped.AddDetail("label_name", labelName)
			return wrapped
		}
		if _, err := store.db.Exec(fmt.Sprintf("DROP TABLE %s", tempName)); err != nil {
			wrapped := fferr.NewResourceExecutionError(pt.ClickHouseOffline.String(), def.ID.Name, def.ID.Variant, fferr.TRAINING_SET_VARIANT, err)
			wrapped.AddDetail("table_name", tableName)
			wrapped.AddDetail("label_name", labelName)
			return wrapped
		}
	}
	return nil
}

const pattern = `Nullable\((.*)\)`

var nullableRe = regexp.MustCompile(pattern)

func deferencePointer(v interface{}) interface{} {
	rv := reflect.ValueOf(v)
	for rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			//return null as nil
			return nil
		}
		rv = rv.Elem()
	}
	return rv.Interface()
}

func checkZeroTime(t time.Time) time.Time {
	//1970-01-01 in datetime64 in ClickHouse is 0 but client returns this as time.Time{} which is 0 year
	if t.IsZero() {
		return time.UnixMilli(0).UTC()
	}
	return t.UTC()
}

func (q clickhouseSQLQueries) castTableItemType(v interface{}, t interface{}) interface{} {
	if v == nil {
		return v
	}
	// v might be a pointer to a pointer (so we can handle nulls)
	v = deferencePointer(v)
	//type might be nullable - identify underlying type e.g. Nullable(String) -> String
	match := nullableRe.FindStringSubmatch(t.(string))
	if len(match) == 2 {
		t = match[1]
	}
	switch t {
	// Where possible we force precision ints to int. This is mainly for test reasons, which assume its are returned
	// This shouldn't impact functionality. If beyond range we use the precision.
	case chInt:
		return v.(int)
	case chInt32:
		return int(v.(int32))
	case chUInt32:
		if intValue := v.(uint32); intValue <= math.MaxInt32 {
			return int(intValue)
		}
		return v.(uint32)
	case chInt64:
		if intValue := v.(int64); intValue >= math.MinInt && intValue <= math.MaxInt {
			return int(intValue)
		}
		return v.(int64)
	case chUInt64:
		if intValue := v.(uint64); intValue <= uint64(math.MaxInt) {
			return int(intValue)
		}
		return v.(uint64)
	case chDateTime:
		if t, ok := v.(time.Time); !ok {
			return time.UnixMilli(0).UTC()
		} else {
			return checkZeroTime(t)
		}
	default:
		// other types don't need checking as will be correct type by ClickHouse client
		return v
	}
}

func (q clickhouseSQLQueries) getValueColumnType(t *sql.ColumnType) interface{} {
	return t.DatabaseTypeName()
}

func (q clickhouseSQLQueries) numRows(n interface{}) (int64, error) {
	// not ideal but this is what the OfflineTableQueries supports
	return int64(n.(uint64)), nil
}

func (q clickhouseSQLQueries) transformationCreate(name string, query string) []string {
	// 2-step with EMPTY for ClickHouse Cloud support. Transformations are just another table.
	return []string{
		fmt.Sprintf("CREATE TABLE %s ENGINE = MergeTree ORDER BY tuple() EMPTY AS %s;",
			SanitizeClickHouseIdentifier(name), query),
		fmt.Sprintf(" INSERT INTO %s %s;", SanitizeClickHouseIdentifier(name), query),
	}
}

func (q clickhouseSQLQueries) transformationUpdate(db *sql.DB, tableName string, query string) error {
	// to update we just rebuild the whole transformation switching atomically using EXCHANGE TABLES
	tempName := SanitizeClickHouseIdentifier(fmt.Sprintf("tmp_%s", tableName))
	createQuery := fmt.Sprintf("CREATE TABLE %s ENGINE = MergeTree ORDER BY tuple() EMPTY AS %s", tempName, query)
	if _, err := db.Exec(createQuery); err != nil {
		wrapped := fferr.NewExecutionError(pt.ClickHouseOffline.String(), err)
		wrapped.AddDetail("table_name", tableName)
		return wrapped
	}
	insertQuery := fmt.Sprintf("INSERT INTO %s %s", tempName, query)
	if _, err := db.Exec(insertQuery); err != nil {
		wrapped := fferr.NewExecutionError(pt.ClickHouseOffline.String(), err)
		wrapped.AddDetail("table_name", tableName)
		return wrapped
	}
	exchangeTableQuery := fmt.Sprintf("EXCHANGE TABLES %s AND %s", tempName, SanitizeClickHouseIdentifier(tableName))
	if _, err := db.Exec(exchangeTableQuery); err != nil {
		wrapped := fferr.NewExecutionError(pt.ClickHouseOffline.String(), err)
		wrapped.AddDetail("table_name", tableName)
		return wrapped
	}
	dropTableQuery := fmt.Sprintf("DROP TABLE %s", tempName)
	if _, err := db.Exec(dropTableQuery); err != nil {
		wrapped := fferr.NewExecutionError(pt.ClickHouseOffline.String(), err)
		wrapped.AddDetail("table_name", tableName)
		return wrapped
	}
	return nil
}

func (q clickhouseSQLQueries) transformationExists() string {
	return q.tableExists()
}

func (q clickhouseSQLQueries) getColumns(db *sql.DB, tableName string) ([]TableColumn, error) {
	qry := "SELECT name FROM system.columns WHERE table = ?"
	rows, err := db.Query(qry, tableName)
	if err != nil {
		wrapped := fferr.NewExecutionError(pt.ClickHouseOffline.String(), err)
		wrapped.AddDetail("table_name", tableName)
		return nil, wrapped
	}
	defer rows.Close()
	columnNames := make([]TableColumn, 0)
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			wrapped := fferr.NewExecutionError(pt.ClickHouseOffline.String(), err)
			wrapped.AddDetail("table_name", tableName)
			return nil, wrapped
		}
		columnNames = append(columnNames, TableColumn{Name: column})
	}
	return columnNames, nil
}

func (q clickhouseSQLQueries) materializationDrop(tableName string) string {
	return fmt.Sprintf("DROP TABLE %s", SanitizeClickHouseIdentifier(tableName))
}

func (q clickhouseSQLQueries) materializationIterateSegment(tableName string) string {
	bind := q.newVariableBindingIterator()
	return fmt.Sprintf("SELECT entity, value, ts FROM (SELECT * FROM %s WHERE row_number>%s AND row_number<=%s) t1", SanitizeClickHouseIdentifier(tableName), bind.Next(), bind.Next())
}

type clickHouseMaterialization struct {
	id        MaterializationID
	db        *sql.DB
	tableName string
	query     OfflineTableQueries
}

func (mat *clickHouseMaterialization) ID() MaterializationID {
	return mat.id
}

func (mat *clickHouseMaterialization) NumRows() (int64, error) {
	var n interface{}
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", SanitizeClickHouseIdentifier(mat.tableName))
	rows := mat.db.QueryRow(query)
	err := rows.Scan(&n)
	if err != nil {
		wrapped := fferr.NewExecutionError(pt.ClickHouseOffline.String(), err)
		wrapped.AddDetail("table_name", mat.tableName)
		return 0, wrapped
	}
	if n == nil {
		return 0, nil
	}
	intVar, err := mat.query.numRows(n)
	if err != nil {
		return 0, nil
	}
	return intVar, nil
}

func (mat *clickHouseMaterialization) IterateSegment(start, end int64) (FeatureIterator, error) {
	query := mat.query.materializationIterateSegment(mat.tableName)
	rows, err := mat.db.Query(query, start, end)
	if err != nil {
		wrapped := fferr.NewExecutionError(pt.ClickHouseOffline.String(), err)
		wrapped.AddDetail("table_name", mat.tableName)
		return nil, wrapped
	}
	types, err := rows.ColumnTypes()
	if err != nil {
		wrapped := fferr.NewExecutionError(pt.ClickHouseOffline.String(), err)
		wrapped.AddDetail("table_name", mat.tableName)
		return nil, wrapped
	}
	colType := mat.query.getValueColumnType(types[1])
	if err != nil {
		return nil, err
	}
	return newClickHouseFeatureIterator(rows, colType, mat.query), nil
}

func (mat *clickHouseMaterialization) NumChunks() (int, error) {
	return genericNumChunks(mat, defaultRowsPerChunk)
}

func (mat *clickHouseMaterialization) IterateChunk(idx int) (FeatureIterator, error) {
	return genericIterateChunk(mat, defaultRowsPerChunk, idx)
}

func (mat *clickHouseMaterialization) Location() pl.Location {
	return pl.NewSQLLocation(mat.tableName)
}

func newClickHouseFeatureIterator(rows *sql.Rows, columnType interface{}, query OfflineTableQueries) FeatureIterator {
	return &clickHouseFeatureIterator{
		rows:         rows,
		err:          nil,
		currentValue: ResourceRecord{},
		columnType:   columnType,
		query:        query,
	}
}

type clickHouseFeatureIterator struct {
	rows         *sql.Rows
	err          error
	currentValue ResourceRecord
	columnType   interface{}
	query        OfflineTableQueries
}

func (iter *clickHouseFeatureIterator) Next() bool {
	if !iter.rows.Next() {
		iter.rows.Close()
		return false
	}
	var rec ResourceRecord
	var entity interface{}
	var value interface{}
	var ts time.Time
	if err := iter.rows.Scan(&entity, &value, &ts); err != nil {
		iter.err = fferr.NewExecutionError(pt.ClickHouseOffline.String(), err)
		iter.rows.Close()
		return false
	}
	if err := rec.SetEntity(entity); err != nil {
		iter.err = err
		return false
	}
	rec.Value = iter.query.castTableItemType(value, iter.columnType)
	rec.TS = checkZeroTime(ts)
	iter.currentValue = rec
	return true
}

func (iter *clickHouseFeatureIterator) Value() ResourceRecord {
	return iter.currentValue
}

func (iter *clickHouseFeatureIterator) Err() error {
	return nil
}

func (iter *clickHouseFeatureIterator) Close() error {
	if err := iter.rows.Close(); err != nil {
		return fferr.NewExecutionError(pt.ClickHouseOffline.String(), err)
	}
	return nil
}
