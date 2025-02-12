// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	sf "github.com/snowflakedb/gosnowflake"

	"github.com/google/uuid"
	db "github.com/jackc/pgx/v4"

	"github.com/featureform/fferr"
	"github.com/featureform/logging"
	"github.com/featureform/metadata"
	pl "github.com/featureform/provider/location"
	pc "github.com/featureform/provider/provider_config"
	ps "github.com/featureform/provider/provider_schema"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/provider/types"
)

func sanitize(ident string) string {
	return db.Identifier{ident}.Sanitize()
}

type SQLOfflineStoreConfig struct {
	Config                  pc.SerializedConfig
	ConnectionURL           string
	Driver                  string
	ProviderType            pt.Type
	QueryImpl               OfflineTableQueries
	ConnectionStringBuilder func(database, schema string) (string, error)
	useDbConnectionCache    bool
}

type OfflineTableQueries interface {
	setVariableBinding(b variableBindingStyle)
	tableExists() string
	viewExists() string
	resourceExists(tableName string) string
	registerResources(db *sql.DB, tableName string, schema ResourceSchema, timestamp bool) error
	primaryTableRegister(tableName string, sourceName string) string
	primaryTableCreate(name string, columnString string) string
	getColumns(db *sql.DB, tableName string) ([]TableColumn, error)
	getValueColumnTypes(tableName string) string
	determineColumnType(valueType types.ValueType) (string, error)
	materializationCreate(tableName string, sourceName string) []string
	materializationUpdate(db *sql.DB, tableName string, sourceName string) error
	materializationExists() string
	materializationDrop(tableName string) string
	getTable() string
	dropTable(tableName string) string
	materializationIterateSegment(tableName string) string
	newSQLOfflineTable(name string, columnType string) string
	writeUpdate(table string) string
	writeInserts(table string) string
	writeExists(table string) string
	createValuePlaceholderString(columns []TableColumn) string
	trainingSetCreate(store *sqlOfflineStore, def TrainingSetDef, tableName string, labelName string) error
	trainingSetUpdate(store *sqlOfflineStore, def TrainingSetDef, tableName string, labelName string) error
	trainingRowSelect(columns string, trainingSetName string) string
	trainingRowSplitSelect(columns string, trainingSetSplitName string) (string, string)
	castTableItemType(v interface{}, t interface{}) interface{}
	getValueColumnType(t *sql.ColumnType) interface{}
	numRows(n interface{}) (int64, error)
	transformationCreate(name string, query string) []string
	transformationUpdate(db *sql.DB, tableName string, query string) error
	transformationExists() string // this isn't used anywhere should I still keep it
	resourceTableColumns(obj pl.FullyQualifiedObject) (string, error)
}

type sqlOfflineStore struct {
	db     *sql.DB
	parent SQLOfflineStoreConfig
	query  OfflineTableQueries
	getDb  func(database, schema string) (*sql.DB, error)
	logger logging.Logger
	BaseProvider
}

// NewPostgresOfflineStore creates a connection to a postgres database
// and initializes a table to track currently active Resource tables.
func NewSQLOfflineStore(config SQLOfflineStoreConfig) (*sqlOfflineStore, error) {
	url := config.ConnectionURL
	pgDb, err := sql.Open(config.Driver, url)
	if err != nil {
		wrapped := fferr.NewConnectionError(config.ProviderType.String(), err)
		wrapped.AddDetail("action", "connection_initialization")
		wrapped.AddDetail("connection_url", url)
		return nil, wrapped
	}

	return &sqlOfflineStore{
		db:     pgDb,
		parent: config,
		query:  config.QueryImpl,
		getDb: func(database, schema string) (*sql.DB, error) {
			url, err := config.ConnectionStringBuilder(database, schema)
			if err != nil {
				return nil, err
			}

			return getOrCreateDbConnection(config.Driver, url, config.useDbConnectionCache)
		},
		BaseProvider: BaseProvider{
			ProviderType:   config.ProviderType,
			ProviderConfig: config.Config,
		},
		logger: logging.NewLogger(fmt.Sprintf("sql driver %s", config.ProviderType.String())),
	}, nil
}

var (
	dbCache      = make(map[string]*sql.DB)
	dbCacheMutex sync.Mutex
)

func getOrCreateDbConnection(driver, url string, useCache bool) (*sql.DB, error) {
	if useCache {
		dbCacheMutex.Lock()
		defer dbCacheMutex.Unlock()

		// Check if a connection already exists in the cache
		if dbConn, ok := dbCache[url]; ok {
			return dbConn, nil
		}

		dbConn, err := createDbConn(driver, url)
		if err != nil {
			return dbConn, err
		}

		// Cache the new connection
		dbCache[url] = dbConn

		return dbConn, nil
	} else {
		// Create a new connection without using the cache
		dbConn, err := createDbConn(driver, url)
		if err != nil {
			return dbConn, err
		}

		return dbConn, nil
	}
}

func createDbConn(driver string, url string) (*sql.DB, error) {
	// Create a new connection
	dbConn, err := sql.Open(driver, url)
	if err != nil {
		return nil, err
	}

	// Set connection pool settings
	dbConn.SetMaxOpenConns(25)
	dbConn.SetMaxIdleConns(25)
	dbConn.SetConnMaxLifetime(time.Hour)
	return dbConn, nil
}

// TODO: deprecate in favor of provider_schema.ResourceToTableName
func (store *sqlOfflineStore) getResourceTableName(id ResourceID) (string, error) {
	return ps.ResourceToTableName(id.Type.String(), id.Name, id.Variant)
}

func (store *sqlOfflineStore) getMaterializationTableName(id ResourceID) (string, error) {
	if err := id.check(Feature); err != nil {
		return "", err
	}
	return ps.ResourceToTableName(FeatureMaterialization.String(), id.Name, id.Variant)
}

func (store *sqlOfflineStore) getJoinedMaterializationTableName(materializationIDs string) string {
	joinedTableUUID := uuid.NewSHA1(uuid.NameSpaceDNS, []byte(materializationIDs))
	return fmt.Sprintf("featureform_batch_features_%s", joinedTableUUID)
}

func (store *sqlOfflineStore) getTrainingSetName(id ResourceID) (string, error) {
	if err := id.check(TrainingSet); err != nil {
		return "", err
	}
	return ps.ResourceToTableName(id.Type.String(), id.Name, id.Variant)
}

func GetPrimaryTableName(id ResourceID) (string, error) {
	if err := id.check(Primary); err != nil {
		return "", err
	}
	return ps.ResourceToTableName(id.Type.String(), id.Name, id.Variant)
}

func (store *sqlOfflineStore) tableExists(location pl.Location) (bool, error) {
	sqlLocation, ok := location.(*pl.SQLLocation)
	if !ok {
		return false, fferr.NewInvalidArgumentErrorf("location is not a SQLLocation")
	}

	return store.checkExists(sqlLocation)
}

func (store *sqlOfflineStore) checkExists(sqlLocation *pl.SQLLocation) (bool, error) {
	// Check if the table exists in the database across tables and views
	var n int

	dbConn, err := store.getDb(sqlLocation.GetDatabase(), sqlLocation.GetSchema())
	if err != nil {
		return false, err
	}

	queries := []string{store.query.viewExists(), store.query.tableExists()}
	for _, query := range queries {
		err := dbConn.QueryRow(query, sqlLocation.GetTable()).Scan(&n)
		if err != nil {
			return false, fferr.NewExecutionError(store.Type().String(), err)
		}
		if n > 0 {
			return true, nil
		}
	}
	return false, nil
}

func (store *sqlOfflineStore) tableExistsForResourceId(id ResourceID) (bool, error) {
	tableName, err := store.getResourceTableName(id)
	if err != nil {
		return false, err
	}

	return store.tableExists(pl.NewSQLLocation(tableName))
}

func (store *sqlOfflineStore) AsOfflineStore() (OfflineStore, error) {
	return store, nil
}

func (store *sqlOfflineStore) Close() error {
	if err := store.db.Close(); err != nil {
		return fferr.NewConnectionError(store.Type().String(), err)
	}
	return nil
}

func (store *sqlOfflineStore) CheckHealth() (bool, error) {
	err := store.db.Ping()
	if err != nil {
		wrapped := fferr.NewConnectionError(store.Type().String(), err)
		wrapped.AddDetail("action", "ping")
		return false, wrapped
	}
	return true, nil
}

func (store sqlOfflineStore) Delete(location pl.Location) error {
	return fferr.NewInternalErrorf("delete not implemented")
}

func (store *sqlOfflineStore) RegisterResourceFromSourceTable(id ResourceID, schema ResourceSchema, opts ...ResourceOption) (OfflineTable, error) {
	logger := logging.NewLogger("sql").WithProvider(store.Type().String(), "SQL")
	logger.Debugw("Registering resource from source table", "id", id, "schema", schema, "options", opts)
	if len(opts) > 0 {
		logger.Errorw("resource options not supported", "options", opts)
		return nil, fferr.NewInvalidArgumentError(fmt.Errorf("resource options not supported"))
	}
	if err := id.check(Feature, Label); err != nil {
		logger.Errorw("id check failed", "id", id, "error", err)
		return nil, err
	}
	if exists, err := store.tableExistsForResourceId(id); err != nil {
		logger.Errorw("table exists check failed", "id", id, "error", err)
		return nil, err
	} else if exists {
		logger.Errorw("table already exists", "id", id)
		return nil, fferr.NewDatasetAlreadyExistsError(id.Name, id.Variant, nil)
	}
	if schema.Entity == "" || schema.Value == "" {
		logger.Errorw("non-empty entity and value columns required", "schema", schema)
		return nil, fferr.NewInvalidArgumentError(fmt.Errorf("non-empty entity and value columns required"))
	}
	tableName, err := store.getResourceTableName(id)
	if err != nil {
		logger.Errorw("table name generation failed", "id", id, "error", err)
		return nil, err
	}
	if schema.TS == "" {
		if err := store.query.registerResources(store.db, tableName, schema, false); err != nil {
			logger.Errorw("Register resources without timestamp failed", "table", tableName, "schema", schema, "error", err)
			return nil, err
		}
	} else {
		if err := store.query.registerResources(store.db, tableName, schema, true); err != nil {
			logger.Errorw("Register resources with timestamp failed", "table", tableName, "schema", schema, "error", err)
			return nil, err
		}
	}

	return &sqlOfflineTable{
		db:           store.db,
		name:         tableName,
		query:        store.query,
		providerType: store.Type(),
	}, nil
}

func (store *sqlOfflineStore) RegisterPrimaryFromSourceTable(id ResourceID, tableLocation pl.Location) (PrimaryTable, error) {
	if err := id.check(Primary); err != nil {
		return nil, err
	}

	// check type of tableLocation to ensure it is a SQLLocation
	sqlLocation, ok := tableLocation.(*pl.SQLLocation)
	if !ok {
		return nil, fferr.NewInvalidArgumentErrorf("tableLocation is not a SQL")
	}

	sourceExists, err := store.tableExists(tableLocation)
	if err != nil {
		return nil, err
	}

	if !sourceExists {
		return nil, fferr.NewDatasetNotFoundError(id.Name, id.Variant, fmt.Errorf("source table '%s' does not exist", tableLocation.Location()))
	}

	dbConn, err := store.getDb(sqlLocation.GetDatabase(), sqlLocation.GetSchema())
	if err != nil {
		return nil, fferr.NewConnectionError(store.Type().String(), err)
	}

	columnNames, err := store.query.getColumns(dbConn, sqlLocation.GetTable())
	if err != nil {
		return nil, err
	}

	return &sqlPrimaryTable{
		db:          dbConn,
		name:        sqlLocation.Location(),
		sqlLocation: sqlLocation,
		schema:      TableSchema{Columns: columnNames},
		query:       store.query,
	}, nil
}

func (store *sqlOfflineStore) CreatePrimaryTable(id ResourceID, schema TableSchema) (PrimaryTable, error) {
	if err := id.check(Primary); err != nil {
		return nil, err
	}
	if exists, err := store.tableExistsForResourceId(id); err != nil {
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
	getDb, err := store.getDb("", "")
	if err != nil {
		return nil, fferr.NewConnectionError(store.Type().String(), err)
	}
	table, err := store.newsqlPrimaryTable(getDb, tableName, schema)
	if err != nil {
		return nil, err
	}
	return table, nil
}

func (store *sqlOfflineStore) newsqlPrimaryTable(db *sql.DB, name string, schema TableSchema) (*sqlPrimaryTable, error) {
	query, err := store.createsqlPrimaryTableQuery(name, schema)
	if err != nil {
		return nil, err
	}
	_, err = db.Exec(query)
	if err != nil {
		return nil, err
	}

	// get current schema and database
	var dbName, schemaName string
	err = db.QueryRow("SELECT current_database(), current_schema()").Scan(&dbName, &schemaName)
	if err != nil {
		return nil, err
	}

	location, _ := pl.NewFullyQualifiedSQLLocation(dbName, schemaName, name).(*pl.SQLLocation)
	return &sqlPrimaryTable{
		db:           db,
		name:         name, // TODO get rid of this and just use location
		sqlLocation:  location,
		schema:       schema,
		query:        store.query,
		providerType: store.Type(),
	}, nil
}

// primaryTableCreate creates a query for table creation based on the
// specified TableSchema. Returns the query if successful. Returns an error
// if there is an invalid column type.
func (store *sqlOfflineStore) createsqlPrimaryTableQuery(name string, schema TableSchema) (string, error) {
	columns := make([]string, 0)
	for _, column := range schema.Columns {
		columnType, err := store.query.determineColumnType(column.ValueType)
		if err != nil {
			return "", err
		}
		columns = append(columns, fmt.Sprintf("%s %s", column.Name, columnType))
	}
	columnString := strings.Join(columns, ", ")
	return store.query.primaryTableCreate(name, columnString), nil
}

func (store *sqlOfflineStore) GetPrimaryTable(id ResourceID, source metadata.SourceVariant) (PrimaryTable, error) {
	location, err := source.GetPrimaryLocation()
	if err != nil {
		return nil, fferr.NewInvalidArgumentErrorf("Source Primary Location is empty: %v", err)
	}
	sqlLocation, ok := location.(*pl.SQLLocation)
	if !ok {
		return nil, fferr.NewInvalidArgumentErrorf("location is not a SQLLocation")
	}
	if location.Location() == "" {
		return nil, fferr.NewInvalidArgumentErrorf("source variant does not have a primary table name")
	}
	if exists, err := store.tableExists(sqlLocation); err != nil {
		return nil, fferr.NewInternalErrorf("could not check if table exists: %v", err)
	} else if !exists {
		return nil, fferr.NewDatasetNotFoundError(id.Name, id.Variant, nil)
	}

	dbConn, getDbErr := store.getDb(sqlLocation.GetDatabase(), sqlLocation.GetSchema())
	if getDbErr != nil {
		return nil, fferr.NewConnectionError(store.Type().String(), getDbErr)
	}

	columnNames, err := store.query.getColumns(dbConn, sqlLocation.GetTable())
	if err != nil {
		return nil, err
	}

	return &sqlPrimaryTable{
		db:           dbConn,
		name:         sqlLocation.GetTable(),
		sqlLocation:  sqlLocation,
		schema:       TableSchema{Columns: columnNames},
		query:        store.query,
		providerType: store.Type(),
	}, nil
}

func (store *sqlOfflineStore) GetTransformationTable(id ResourceID) (TransformationTable, error) {
	if err := id.check(Transformation); err != nil {
		return nil, err
	}
	name, err := ps.ResourceToTableName(id.Type.String(), id.Name, id.Variant)
	if err != nil {
		return nil, err
	}

	existsQuery := store.query.tableExists()
	rows, err := store.db.Query(existsQuery, name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, fferr.NewTransformationNotFoundError(name, id.Variant, err)
	}
	columnNames, err := store.query.getColumns(store.db, name)
	if err != nil {
		return nil, err
	}

	var dbName, schemaName string
	err = store.db.QueryRow("SELECT current_database(), current_schema()").Scan(&dbName, &schemaName)
	if err != nil {
		return nil, err
	}
	sqlLocation := pl.NewFullyQualifiedSQLLocation(dbName, schemaName, name).(*pl.SQLLocation)

	return &sqlPrimaryTable{
		db:           store.db,
		name:         name,
		sqlLocation:  sqlLocation,
		schema:       TableSchema{Columns: columnNames},
		query:        store.query,
		providerType: store.Type(),
	}, nil
}

// CreateResourceTable creates a new Resource table.
// Returns a table if it does not already exist and stores the table ID in the resource index table.
// Returns an error if the table already exists or if table is the wrong type.
func (store *sqlOfflineStore) CreateResourceTable(id ResourceID, schema TableSchema) (OfflineTable, error) {
	if err := id.check(Feature, Label); err != nil {
		return nil, err
	}

	if exists, err := store.tableExistsForResourceId(id); err != nil {
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

// getValueIndex returns the index of the value column in the schema.
// Returns -1 if an entity column is not found
func (store *sqlOfflineStore) getValueIndex(columns []TableColumn) int {
	for i, column := range columns {
		if column.Name == "value" {
			return i
		}
	}
	return -1
}

func (store *sqlOfflineStore) GetResourceTable(id ResourceID) (OfflineTable, error) {
	return store.getsqlResourceTable(id)
}

func (store *sqlOfflineStore) ResourceLocation(id ResourceID, resource any) (pl.Location, error) {
	if id.check(Primary) == nil {
		if sv, ok := resource.(metadata.SourceVariant); !ok {
			return nil, fferr.NewInvalidArgumentErrorf("resource is not a SourceVariant")
		} else {
			return pl.NewSQLLocation(sv.PrimaryDataSQLTableName()), nil
		}
	}

	if exists, err := store.tableExistsForResourceId(id); err != nil {
		return nil, fmt.Errorf("could not check if table exists: %v", err)
	} else if !exists {
		return nil, fmt.Errorf("table does not exist: %v", id)
	}

	var tableName string
	var err error
	switch id.Type {
	case Feature, Label:
		tableName, err = store.getResourceTableName(id)
	case TrainingSet:
		tableName, err = store.getTrainingSetName(id)
	case Primary:
		tableName, err = GetPrimaryTableName(id)
	case Transformation:
		tableName, err = store.getTransformationTableName(id)
	default:
		err = fferr.NewInvalidArgumentError(fmt.Errorf("unsupported resource type: %s", id.Type))
	}

	return pl.NewSQLLocation(tableName), err
}

type sqlMaterialization struct {
	id           MaterializationID
	db           *sql.DB
	tableName    string
	query        OfflineTableQueries
	providerType pt.Type
	location     pl.Location
}

func (mat *sqlMaterialization) ID() MaterializationID {
	return mat.id
}

// NumRows checks for the count of rows to return as the number of rows.
// If there are no rows in the table, the interface n is checked for Nil,
// otherwise the interface is converted from a string to an int64
func (mat *sqlMaterialization) NumRows() (int64, error) {
	var n interface{}
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", sanitize(mat.tableName))
	rows := mat.db.QueryRow(query)
	err := rows.Scan(&n)
	if err != nil {
		wrapped := fferr.NewExecutionError(mat.providerType.String(), err)
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

func (mat *sqlMaterialization) IterateSegment(start, end int64) (FeatureIterator, error) {
	query := mat.query.materializationIterateSegment(mat.tableName)
	rows, err := mat.db.Query(query, start, end)
	if err != nil {
		wrapped := fferr.NewExecutionError(mat.providerType.String(), err)
		wrapped.AddDetail("table_name", mat.tableName)
		return nil, wrapped
	}
	types, err := rows.ColumnTypes()
	if err != nil {
		wrapped := fferr.NewExecutionError(mat.providerType.String(), err)
		wrapped.AddDetail("table_name", mat.tableName)
		return nil, wrapped
	}
	colType := mat.query.getValueColumnType(types[1])
	return newsqlFeatureIterator(rows, colType, mat.query, mat.providerType), nil
}

func (mat *sqlMaterialization) NumChunks() (int, error) {
	return genericNumChunks(mat, defaultRowsPerChunk)
}

func (mat *sqlMaterialization) IterateChunk(idx int) (FeatureIterator, error) {
	return genericIterateChunk(mat, defaultRowsPerChunk, idx)
}

func (mat *sqlMaterialization) Location() pl.Location {
	return mat.location
}

type sqlFeatureIterator struct {
	rows         *sql.Rows
	err          error
	currentValue ResourceRecord
	columnType   interface{}
	query        OfflineTableQueries
	providerType pt.Type
}

func newsqlFeatureIterator(rows *sql.Rows, columnType interface{}, query OfflineTableQueries, providerType pt.Type) FeatureIterator {
	return &sqlFeatureIterator{
		rows:         rows,
		err:          nil,
		currentValue: ResourceRecord{},
		columnType:   columnType,
		query:        query,
		providerType: providerType,
	}
}

func (iter *sqlFeatureIterator) Next() bool {
	if !iter.rows.Next() {
		iter.rows.Close()
		return false
	}
	var rec ResourceRecord
	var entity interface{}
	var value interface{}
	var ts *time.Time
	if err := iter.rows.Scan(&entity, &value, &ts); err != nil {
		iter.rows.Close()
		iter.err = fferr.NewExecutionError(iter.providerType.String(), err)
		return false
	}
	if err := rec.SetEntity(entity); err != nil {
		iter.err = err
		return false
	}
	rec.Value = iter.query.castTableItemType(value, iter.columnType)
	if ts != nil {
		rec.TS = ts.UTC()
	}
	iter.currentValue = rec
	return true
}

func (iter *sqlFeatureIterator) Value() ResourceRecord {
	return iter.currentValue
}

func (iter *sqlFeatureIterator) Err() error {
	return nil
}

func (iter *sqlFeatureIterator) Close() error {
	if err := iter.rows.Close(); err != nil {
		return fferr.NewConnectionError(string(iter.providerType), err)
	}
	return nil
}

// Batch Feature Iterator
type sqlBatchFeatureIterator struct {
	rows          *sql.Rows
	err           error
	currentValues GenericRecord
	columnTypes   []interface{}
	columnNames   []string
	query         OfflineTableQueries
	providerType  pt.Type
}

// Need to figure out columntype
func newsqlBatchFeatureIterator(rows *sql.Rows, columnTypes []interface{}, columnNames []string, query OfflineTableQueries, providerType pt.Type) BatchFeatureIterator {
	return &sqlBatchFeatureIterator{
		rows:          rows,
		err:           nil,
		currentValues: nil,
		columnTypes:   columnTypes,
		columnNames:   columnNames,
		query:         query,
		providerType:  providerType,
	}
}

func (it *sqlBatchFeatureIterator) Next() bool {
	if !it.rows.Next() {
		it.rows.Close()
		return false
	}
	columnNames, err := it.rows.Columns()
	if err != nil {
		it.err = fferr.NewExecutionError(it.providerType.String(), err)
		it.rows.Close()
		return false
	}
	values := make([]interface{}, len(columnNames))
	pointers := make([]interface{}, len(columnNames))
	for i, _ := range values {
		pointers[i] = &values[i]
	}
	if err := it.rows.Scan(pointers...); err != nil {
		it.err = fferr.NewExecutionError(it.providerType.String(), err)
		it.rows.Close()
		return false
	}
	rowValues := make(GenericRecord, len(columnNames))
	for i, value := range values {
		if value == nil {
			continue
		}
		rowValues[i] = it.query.castTableItemType(value, it.columnTypes[i])
	}
	it.currentValues = rowValues
	return true
}

func (it *sqlBatchFeatureIterator) Entity() interface{} {
	return (it.currentValues)[0]
}

func (it *sqlBatchFeatureIterator) Features() GenericRecord {
	return (it.currentValues)[1:]
}

func (it *sqlBatchFeatureIterator) Columns() []string {
	return it.columnNames
}

func (it *sqlBatchFeatureIterator) Err() error {
	return it.err
}

func (it *sqlBatchFeatureIterator) Close() error {
	if err := it.rows.Close(); err != nil {
		return fferr.NewConnectionError(it.providerType.String(), err)
	}
	return nil
}

// Takes a list of feature resource IDs and creates a table view joining all the feature values based on the entity
// Note: This table view doesn't store timestamps
func (store *sqlOfflineStore) GetBatchFeatures(ids []ResourceID) (BatchFeatureIterator, error) {

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
		matTableName = sanitize(matTableName)

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
		asEntity += fmt.Sprintf("%s.entity", matTableName)
	}

	sort.Strings(matIDs)
	joinedTableName := store.getJoinedMaterializationTableName(strings.Join(matIDs, "_"))
	createQuery := ""
	if len(ids) == 1 {
		createQuery = fmt.Sprintf("CREATE VIEW \"%s\" AS SELECT %s AS entity %s FROM %s", joinedTableName, asEntity, withFeatures, sanitize(tableName0))
	} else {
		createQuery = fmt.Sprintf("CREATE VIEW \"%s\" AS SELECT COALESCE(%s) AS entity %s FROM %s", joinedTableName, asEntity, withFeatures, joinTables)
	}
	store.db.Query(createQuery)
	createQueryWithoutTS := fmt.Sprintf("CREATE VIEW \"no_ts_%s\" AS SELECT entity%s FROM \"%s\"", joinedTableName, featureColumns, joinedTableName)
	store.db.Query(createQueryWithoutTS)
	select_query := fmt.Sprintf("SELECT * FROM \"no_ts_%s\"", joinedTableName)
	resultRows, err := store.db.Query(select_query)
	if err != nil {
		return nil, err
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
	dropQuery := fmt.Sprintf("DROP VIEW \"no_ts_%s\"", joinedTableName)
	store.db.Query(dropQuery)
	columnNames := make([]string, 0)
	for _, col := range columns {
		columnNames = append(columnNames, sanitize(col.Name))
	}

	return newsqlBatchFeatureIterator(resultRows, columnTypes, columnNames, store.query, store.Type()), nil
}
func (store *sqlOfflineStore) CreateMaterialization(id ResourceID, opts MaterializationOptions) (Materialization, error) {
	if id.Type != Feature {
		return nil, fferr.NewInvalidArgumentError(fmt.Errorf("received %s; only features can be materialized", id.Type))
	}
	resTable, err := store.getsqlResourceTable(id)
	if err != nil {
		return nil, err
	}

	matID, err := NewMaterializationID(id)
	if err != nil {
		return nil, err
	}
	matTableName, err := store.getMaterializationTableName(id)
	if err != nil {
		return nil, err
	}
	materializeQueries := store.query.materializationCreate(matTableName, resTable.name)
	for _, materializeQry := range materializeQueries {
		_, err = store.db.Exec(materializeQry)
		if err != nil {
			return nil, fferr.NewResourceExecutionError(store.Type().String(), id.Name, id.Variant, fferr.ResourceType(id.Type.String()), err)
		}
	}
	return &sqlMaterialization{
		id:           matID,
		db:           store.db,
		tableName:    matTableName,
		query:        store.query,
		providerType: store.Type(),
	}, nil
}

func (store *sqlOfflineStore) SupportsMaterializationOption(opt MaterializationOptionType) (bool, error) {
	return false, nil
}

func (store *sqlOfflineStore) GetMaterialization(id MaterializationID) (Materialization, error) {
	name, variant, err := ps.MaterializationIDToResource(string(id))
	if err != nil {
		return nil, err
	}

	tableName, err := store.getMaterializationTableName(ResourceID{name, variant, Feature})
	if err != nil {
		return nil, err
	}

	getMatQry := store.query.materializationExists()

	rows, err := store.db.Query(getMatQry, tableName)
	if err != nil {
		wrapped := fferr.NewExecutionError(store.Type().String(), err)
		wrapped.AddDetail("table_name", tableName)
		return nil, wrapped
	}
	defer rows.Close()

	rowCount := 0
	if rows.Next() {
		rowCount++
	}
	if rowCount == 0 {
		return nil, fferr.NewDatasetNotFoundError(string(id), "", nil)
	}
	return &sqlMaterialization{
		id:           id,
		db:           store.db,
		tableName:    tableName,
		query:        store.query,
		providerType: store.Type(),
		location:     pl.NewSQLLocation(tableName),
	}, err
}

func (store *sqlOfflineStore) UpdateMaterialization(id ResourceID, opts MaterializationOptions) (Materialization, error) {
	tableName, err := store.getMaterializationTableName(id)
	if err != nil {
		return nil, err
	}
	matID, err := ps.ResourceToMaterializationID(id.Type.String(), id.Name, id.Variant)
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
		return nil, fferr.NewResourceExecutionError(store.Type().String(), id.Name, id.Variant, fferr.ResourceType(id.Type.String()), err)
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, fferr.NewDatasetNotFoundError(id.Name, id.Variant, nil)
	}
	err = store.query.materializationUpdate(store.db, tableName, resTable.name)
	if err != nil {
		return nil, err
	}
	return &sqlMaterialization{
		id:           MaterializationID(matID),
		db:           store.db,
		tableName:    tableName,
		query:        store.query,
		providerType: store.Type(),
	}, err
}

func (store *sqlOfflineStore) DeleteMaterialization(id MaterializationID) error {
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
		return fferr.NewDatasetNotFoundError(string(id), "", nil)
	}
	return nil
}

func (store *sqlOfflineStore) materializationExists(id MaterializationID) (bool, error) {
	name, variant, err := ps.MaterializationIDToResource(string(id))
	if err != nil {
		return false, err
	}
	tableName, err := store.getMaterializationTableName(ResourceID{name, variant, Feature})
	if err != nil {
		return false, err
	}
	getMatQry := store.query.materializationExists()
	rows, err := store.db.Query(getMatQry, tableName)
	if err != nil {
		return false, fferr.NewDatasetNotFoundError(string(id), "", nil)
	}
	defer rows.Close()
	rowCount := 0
	if rows.Next() {
		rowCount++
	}
	if rowCount == 0 {
		return false, nil
	}
	return true, nil
}

func (store *sqlOfflineStore) CreateTrainingSet(def TrainingSetDef) error {
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
	if err := store.query.trainingSetCreate(store, def, tableName, label.name); err != nil {
		return err
	}

	return nil
}

func (store *sqlOfflineStore) UpdateTrainingSet(def TrainingSetDef) error {
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
	if err := store.query.trainingSetUpdate(store, def, tableName, label.name); err != nil {
		return err
	}

	return nil
}

func (store *sqlOfflineStore) GetTrainingSet(id ResourceID) (TrainingSetIterator, error) {
	logger := store.logger.WithResource(logging.TrainingSetVariant, id.Name, id.Variant)
	logger.Debugw("Getting training set")
	if err := id.check(TrainingSet); err != nil {
		return nil, err
	}
	logger.Debugw("Checking if Training Set exists")
	if exists, err := store.tableExistsForResourceId(id); err != nil {
		logger.Errorw("Error checking if Training Set exists", "error", err)
		return nil, err
	} else if !exists {
		logger.Errorw("Training Set does not exist")
		return nil, fferr.NewDatasetNotFoundError(id.Name, id.Variant, nil)
	}
	trainingSetName, err := store.getTrainingSetName(id)
	if err != nil {
		logger.Errorw("Error getting Training Set name", "error", err)
		return nil, err
	}
	columnNames, err := store.query.getColumns(store.db, trainingSetName)
	if err != nil {
		logger.Errorw("Error getting columns", "error", err)
		return nil, err
	}
	features := make([]string, 0)
	for _, name := range columnNames {
		features = append(features, sanitize(name.Name))
	}
	columns := strings.Join(features[:], ", ")
	trainingSetQry := store.query.trainingRowSelect(columns, trainingSetName)
	store.logger.Debugw("Training Set Query", "query", trainingSetQry)
	rows, err := store.db.Query(trainingSetQry)
	if err != nil {
		logger.Errorw("Error querying Training Set", "error", err, "store", store.Type().String())
		return nil, fferr.NewResourceExecutionError(store.Type().String(), id.Name, id.Variant, fferr.ResourceType(id.Type.String()), err)
	}
	colTypes, err := store.getValueColumnTypes(trainingSetName)
	if err != nil {
		logger.Errorw("Error getting column types", "error", err, "training_set_name", trainingSetName)
		return nil, err
	}
	logger.Debugw("Returning Training Set Iterator")
	return store.newsqlTrainingSetIterator(rows, colTypes), nil
}

func (store *sqlOfflineStore) CreateTrainTestSplit(def TrainTestSplitDef) (func() error, error) {
	return nil, fmt.Errorf("not Implemented")
}

func (store *sqlOfflineStore) GetTrainTestSplit(def TrainTestSplitDef) (TrainingSetIterator, TrainingSetIterator, error) {
	return nil, nil, fmt.Errorf("not Implemented")
}

// getValueColumnTypes returns a list of column types. Columns consist of feature and label values
// within a training set.
func (store *sqlOfflineStore) getValueColumnTypes(table string) ([]interface{}, error) {
	query := store.query.getValueColumnTypes(table)
	rows, err := store.db.Query(query)
	if err != nil {
		wrapped := fferr.NewExecutionError(store.Type().String(), err)
		wrapped.AddDetail("table_name", table)
		return nil, wrapped
	}
	defer rows.Close()
	colTypes := make([]interface{}, 0)

	if rows.Next() {

		rawType, err := rows.ColumnTypes()
		if err != nil {
			wrapped := fferr.NewExecutionError(store.Type().String(), err)
			wrapped.AddDetail("table_name", table)
			return nil, wrapped
		}
		for _, t := range rawType {
			colTypes = append(colTypes, store.query.getValueColumnType(t))
		}
	}
	return colTypes, nil
}

type sqlTrainingRowsIterator struct {
	rows            *sql.Rows
	currentFeatures []interface{}
	currentLabel    interface{}
	err             error
	columnTypes     []interface{}
	isHeaderRow     bool
	query           OfflineTableQueries
	store           *sqlOfflineStore
}

func (store *sqlOfflineStore) newsqlTrainingSetIterator(rows *sql.Rows, columnTypes []interface{}) TrainingSetIterator {
	return &sqlTrainingRowsIterator{
		rows:            rows,
		currentFeatures: nil,
		currentLabel:    nil,
		err:             nil,
		columnTypes:     columnTypes,
		isHeaderRow:     true,
		query:           store.query,
		store:           store,
	}
}

func (it *sqlTrainingRowsIterator) Next() bool {
	if !it.rows.Next() {
		it.rows.Close()
		return false
	}
	columnNames, err := it.rows.Columns()
	if err != nil {
		it.err = fferr.NewExecutionError(it.store.ProviderType.String(), err)
		it.rows.Close()
		return false
	}
	values := make([]interface{}, len(columnNames))
	pointers := make([]interface{}, len(columnNames))
	for i, _ := range values {
		pointers[i] = &values[i]
	}
	if err := it.rows.Scan(pointers...); err != nil {
		it.err = fferr.NewExecutionError(it.store.ProviderType.String(), err)
		it.rows.Close()
		return false
	}
	var label interface{}
	numFeatures := len(columnNames) - 1
	featureVals := make([]interface{}, numFeatures)
	for i, value := range values {
		if value == nil {
			continue
		}
		if i < numFeatures {
			featureVals[i] = it.query.castTableItemType(value, it.columnTypes[i])
		} else {
			label = it.query.castTableItemType(value, it.columnTypes[i])
		}
	}
	it.currentFeatures = featureVals
	it.currentLabel = label

	return true
}

func (it *sqlTrainingRowsIterator) Err() error {
	return it.err
}

func (it *sqlTrainingRowsIterator) Features() []interface{} {
	return it.currentFeatures
}

func (it *sqlTrainingRowsIterator) Label() interface{} {
	return it.currentLabel
}

func (store *sqlOfflineStore) getsqlResourceTable(id ResourceID) (*sqlOfflineTable, error) {
	if exists, err := store.tableExistsForResourceId(id); err != nil {
		return nil, err
	} else if !exists {
		return nil, fferr.NewDatasetNotFoundError(id.Name, id.Variant, nil)
	}
	table, err := store.getResourceTableName(id)
	if err != nil {
		return nil, err
	}
	return &sqlOfflineTable{
		db:           store.db,
		name:         table,
		query:        store.query,
		providerType: store.Type(),
	}, nil
}

type sqlOfflineTable struct {
	db           *sql.DB
	query        OfflineTableQueries
	name         string
	providerType pt.Type
}

type sqlPrimaryTable struct {
	db           *sql.DB
	name         string
	sqlLocation  *pl.SQLLocation
	query        OfflineTableQueries
	schema       TableSchema
	providerType pt.Type
}

func (table *sqlPrimaryTable) GetName() string {
	return table.name
}

func (table *sqlPrimaryTable) Write(rec GenericRecord) error {
	tb := sanitize(table.name)
	columns := table.getColumnNameString()
	placeholder := table.query.createValuePlaceholderString(table.schema.Columns)
	upsertQuery := fmt.Sprintf(""+
		"INSERT INTO %s ( %s ) "+
		"VALUES ( %s ) ", tb, columns, placeholder)
	if _, err := table.db.Exec(upsertQuery, rec...); err != nil {
		wrapped := fferr.NewExecutionError(table.providerType.String(), err)
		wrapped.AddDetail("table_name", table.name)
		return wrapped
	}
	return nil
}

func (table *sqlPrimaryTable) WriteBatch(recs []GenericRecord) error {
	for _, rec := range recs {
		if err := table.Write(rec); err != nil {
			return err
		}
	}
	return nil
}

func (table *sqlPrimaryTable) getColumnNameString() string {
	columns := make([]string, 0)
	for _, column := range table.schema.Columns {
		columns = append(columns, column.Name)
	}
	return strings.Join(columns, ", ")
}

func (pt *sqlPrimaryTable) IterateSegment(n int64) (GenericTableIterator, error) {
	columns, err := pt.query.getColumns(pt.db, pt.name)
	if err != nil {
		return nil, err
	}
	columnNames := make([]string, 0)
	for _, col := range columns {
		columnNames = append(columnNames, sanitize(col.Name))
	}
	names := strings.Join(columnNames[:], ", ")
	var query string
	if n == -1 {
		query = fmt.Sprintf("SELECT %s FROM %s", names, sanitize(pt.name))
	} else {
		query = fmt.Sprintf("SELECT %s FROM %s LIMIT %d", names, sanitize(pt.name), n)
	}
	rows, err := pt.db.Query(query)
	if err != nil {
		wrapped := fferr.NewExecutionError(pt.providerType.String(), err)
		wrapped.AddDetail("table_name", pt.name)
		return nil, wrapped
	}
	colTypes, err := pt.getValueColumnTypes(pt.name)
	if err != nil {
		return nil, err
	}
	return newsqlGenericTableIterator(rows, colTypes, columnNames, pt.query, pt.providerType), nil
}

func (pt *sqlPrimaryTable) getValueColumnTypes(table string) ([]interface{}, error) {
	query := pt.query.getValueColumnTypes(table)
	rows, err := pt.db.Query(query)
	if err != nil {
		wrapped := fferr.NewExecutionError(pt.providerType.String(), err)
		wrapped.AddDetail("table_name", pt.name)
		return nil, wrapped
	}
	defer rows.Close()
	colTypes := make([]interface{}, 0)
	if rows.Next() {

		rawType, err := rows.ColumnTypes()
		if err != nil {
			wrapped := fferr.NewExecutionError(pt.providerType.String(), err)
			wrapped.AddDetail("table_name", pt.name)
			return nil, wrapped
		}
		for _, t := range rawType {
			colTypes = append(colTypes, pt.query.getValueColumnType(t))
		}
	}

	return colTypes, nil
}

func (pt *sqlPrimaryTable) NumRows() (int64, error) {
	n := int64(0)
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", sanitize(pt.name))
	rows := pt.db.QueryRow(query)

	err := rows.Scan(&n)
	if err != nil {
		wrapped := fferr.NewExecutionError(pt.providerType.String(), err)
		wrapped.AddDetail("table_name", pt.name)
		return 0, wrapped
	}
	return n, nil
}

func determineColumnType(valueType types.ValueType) (string, error) {
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

func (store *sqlOfflineStore) newsqlOfflineTable(db *sql.DB, name string, valueType types.ValueType) (*sqlOfflineTable, error) {
	columnType, err := determineColumnType(valueType)
	if err != nil {
		return nil, err
	}
	tableCreateQry := store.query.newSQLOfflineTable(name, columnType)
	_, err = db.Exec(tableCreateQry)
	if err != nil {
		wrapped := fferr.NewExecutionError(store.Type().String(), err)
		wrapped.AddDetail("table_name", name)
		return nil, wrapped
	}
	return &sqlOfflineTable{
		db:           db,
		name:         name,
		query:        store.query,
		providerType: store.Type(),
	}, nil
}

func (table *sqlOfflineTable) Write(rec ResourceRecord) error {
	rec = checkTimestamp(rec)
	tb := sanitize(table.name)
	if err := rec.check(); err != nil {
		return err
	}

	n := -1
	existsQuery := table.query.writeExists(tb)

	if err := table.db.QueryRow(existsQuery, rec.Entity, rec.TS).Scan(&n); err != nil {
		wrapped := fferr.NewResourceExecutionError(table.providerType.String(), rec.Entity, "", fferr.ENTITY, err)
		wrapped.AddDetail("table_name", table.name)
		return wrapped
	}
	if n == 0 {
		insertQuery := table.query.writeInserts(tb)
		if _, err := table.db.Exec(insertQuery, rec.Entity, rec.Value, rec.TS); err != nil {
			wrapped := fferr.NewResourceExecutionError(table.providerType.String(), rec.Entity, "", fferr.ENTITY, err)
			wrapped.AddDetail("table_name", table.name)
			return wrapped
		}
	} else if n > 0 {
		updateQuery := table.query.writeUpdate(tb)
		if _, err := table.db.Exec(updateQuery, rec.Value, rec.Entity, rec.TS); err != nil {
			wrapped := fferr.NewResourceExecutionError(table.providerType.String(), rec.Entity, "", fferr.ENTITY, err)
			wrapped.AddDetail("table_name", table.name)
			return wrapped
		}
	}
	return nil
}

func (table *sqlOfflineTable) WriteBatch(recs []ResourceRecord) error {
	for _, rec := range recs {
		if err := table.Write(rec); err != nil {
			return err
		}
	}
	return nil
}

func (table *sqlOfflineTable) Location() pl.Location {
	return pl.NewSQLLocation(table.name)
}

func (table *sqlOfflineTable) resourceExists(rec ResourceRecord) (bool, error) {
	rec = checkTimestamp(rec)
	query := table.query.resourceExists(table.name)

	rows, err := table.db.Query(query, rec.Entity, rec.TS)
	if err != nil {
		return false, fferr.NewResourceExecutionError(table.providerType.String(), rec.Entity, "", fferr.ENTITY, err)
	}
	defer rows.Close()
	rowCount := 0
	for rows.Next() {
		rowCount++
	}
	if rowCount == 0 {
		return false, nil
	}
	return true, nil
}

func (store *sqlOfflineStore) SupportsTransformationOption(opt TransformationOptionType) (bool, error) {
	return false, nil
}

func (store *sqlOfflineStore) CreateTransformation(config TransformationConfig, opts ...TransformationOption) error {
	if len(opts) > 0 {
		return fferr.NewInternalErrorf("OfflineStore does not support transformation options")
	}
	name, err := store.getTransformationTableName(config.TargetTableID)
	if err != nil {
		return err
	}
	queries := store.query.transformationCreate(name, config.Query)
	for _, query := range queries {
		if _, err := store.db.Exec(query); err != nil {
			return fferr.NewResourceExecutionError(store.Type().String(), config.TargetTableID.Name, config.TargetTableID.Variant, fferr.ResourceType(config.TargetTableID.Type.String()), err)
		}
	}
	return nil
}

func (store *sqlOfflineStore) UpdateTransformation(config TransformationConfig, opts ...TransformationOption) error {
	if len(opts) > 0 {
		return fferr.NewInternalErrorf("OfflineStore does not support transformation options")
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

func (store *sqlOfflineStore) getTransformationTableName(id ResourceID) (string, error) {
	if err := id.check(Transformation); err != nil {
		return "", err
	}
	return ps.ResourceToTableName(id.Type.String(), id.Name, id.Variant)
}

type sqlGenericTableIterator struct {
	rows          *sql.Rows
	currentValues GenericRecord
	err           error
	columnTypes   []interface{}
	columnNames   []string
	query         OfflineTableQueries
	providerType  pt.Type
}

func newsqlGenericTableIterator(rows *sql.Rows, columnTypes []interface{}, columnNames []string, query OfflineTableQueries, providerType pt.Type) GenericTableIterator {
	return &sqlGenericTableIterator{
		rows:          rows,
		currentValues: nil,
		err:           nil,
		columnTypes:   columnTypes,
		columnNames:   columnNames,
		query:         query,
		providerType:  providerType,
	}
}

func (it *sqlGenericTableIterator) Next() bool {
	if !it.rows.Next() {
		it.rows.Close()
		return false
	}
	columnNames, err := it.rows.Columns()
	if err != nil {
		it.err = fferr.NewExecutionError(it.providerType.String(), err)
		it.rows.Close()
		return false
	}
	values := make([]interface{}, len(columnNames))
	pointers := make([]interface{}, len(columnNames))
	for i, _ := range values {
		pointers[i] = &values[i]
	}
	if err := it.rows.Scan(pointers...); err != nil {
		it.err = fferr.NewExecutionError(it.providerType.String(), err)
		it.rows.Close()
		return false
	}

	rowValues := make(GenericRecord, len(columnNames))
	for i, value := range values {
		if value == nil {
			continue
		}
		rowValues[i] = it.query.castTableItemType(value, it.columnTypes[i])
	}
	it.currentValues = rowValues
	return true
}

func (it *sqlGenericTableIterator) Values() GenericRecord {
	return it.currentValues
}

func (it *sqlGenericTableIterator) Columns() []string {
	return it.columnNames
}

func (it *sqlGenericTableIterator) Err() error {
	return it.err
}

func (it *sqlGenericTableIterator) Close() error {
	if err := it.rows.Close(); err != nil {
		return fferr.NewConnectionError(it.providerType.String(), err)
	}
	return nil
}

type defaultOfflineSQLQueries struct {
	BindingStyle variableBindingStyle
}

func (q *defaultOfflineSQLQueries) newVariableBindingIterator() VariableBindingIterator {
	return VariableBindingIterator{
		Current: 0,
		Style:   q.BindingStyle,
	}
}

type variableBindingStyle string

const (
	PostgresBindingStyle variableBindingStyle = "POSTGRESBIND"
	MySQLBindingStyle    variableBindingStyle = "SQLBIND"
)

type VariableBindingIterator struct {
	Current int
	Style   variableBindingStyle
}

func (it *VariableBindingIterator) Next() string {
	if it.Style == PostgresBindingStyle {
		it.Current = it.Current + 1
		return fmt.Sprintf("$%d", it.Current)
	} else {
		return "?"
	}
}

func (q *defaultOfflineSQLQueries) setVariableBinding(b variableBindingStyle) {
	q.BindingStyle = b
}

const genericExists = `SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ? AND table_schema = CURRENT_SCHEMA()`

func (q defaultOfflineSQLQueries) tableExists() string {
	return genericExists
}

func (q defaultOfflineSQLQueries) viewExists() string {
	return genericExists
}

func (q defaultOfflineSQLQueries) registerResources(db *sql.DB, tableName string, schema ResourceSchema, timestamp bool) error {
	var query string
	if timestamp {
		query = fmt.Sprintf("CREATE VIEW %s AS SELECT IDENTIFIER('%s') as entity,  IDENTIFIER('%s') as value,  IDENTIFIER('%s') as ts FROM TABLE('%s')", sanitize(tableName),
			schema.Entity, schema.Value, schema.TS, sanitize(schema.SourceTable.Location()))
	} else {
		query = fmt.Sprintf("CREATE VIEW %s AS SELECT IDENTIFIER('%s') as entity, IDENTIFIER('%s') as value, to_timestamp_ntz('%s', 'YYYY-DD-MM HH24:MI:SS +0000 UTC')::TIMESTAMP_NTZ as ts FROM TABLE('%s')", sanitize(tableName),
			schema.Entity, schema.Value, time.UnixMilli(0).UTC(), sanitize(schema.SourceTable.Location()))
	}
	if _, err := db.Exec(query); err != nil {
		wrapped := fferr.NewExecutionError("SQL", err)
		wrapped.AddDetail("table_name", tableName)
		return wrapped
	}
	return nil
}

func (q defaultOfflineSQLQueries) primaryTableRegister(tableName string, sourceName string) string {
	sprintf := fmt.Sprintf("CREATE VIEW %s AS SELECT * FROM TABLE('%s')", sanitize(tableName), sanitize(sourceName))
	return sprintf
}

func (q defaultOfflineSQLQueries) getColumns(db *sql.DB, name string) ([]TableColumn, error) {
	bind := q.newVariableBindingIterator()
	qry := fmt.Sprintf("SELECT column_name FROM information_schema.columns WHERE table_name = %s and table_schema = CURRENT_SCHEMA() order by ordinal_position", bind.Next())
	rows, err := db.Query(qry, name)
	if err != nil {
		wrapped := fferr.NewExecutionError("SQL", err)
		wrapped.AddDetail("table_name", name)
		return nil, wrapped
	}
	defer rows.Close()
	columnNames := make([]TableColumn, 0)
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			wrapped := fferr.NewExecutionError("SQL", err)
			wrapped.AddDetail("table_name", name)
			return nil, wrapped
		}
		columnNames = append(columnNames, TableColumn{Name: column})
	}
	return columnNames, nil
}

func (q defaultOfflineSQLQueries) primaryTableCreate(name string, columnString string) string {
	return fmt.Sprintf("CREATE TABLE %s ( %s )", sanitize(name), columnString)
}

func (q defaultOfflineSQLQueries) materializationCreate(tableName string, sourceName string) []string {
	return []string{
		fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s AS (SELECT entity, value, ts, row_number() over(ORDER BY (SELECT NULL)) as row_number FROM "+
				"(SELECT entity, ts, value, row_number() OVER (PARTITION BY entity ORDER BY ts desc) "+
				"AS rn FROM %s) t WHERE rn=1)", sanitize(tableName), sanitize(sourceName)),
	}
}

func (q defaultOfflineSQLQueries) materializationUpdate(db *sql.DB, tableName string, sourceName string) error {
	sanitizedTable := sanitize(tableName)
	tempTable := sanitize(fmt.Sprintf("tmp_%s", tableName))
	oldTable := sanitize(fmt.Sprintf("old_%s", tableName))
	query := fmt.Sprintf(
		"BEGIN TRANSACTION;"+
			"CREATE TABLE IF NOT EXISTS %s AS (SELECT entity, value, ts, row_number() over(ORDER BY (SELECT NULL)) as row_number FROM "+
			"(SELECT entity, ts, value, row_number() OVER (PARTITION BY entity ORDER BY ts desc) "+
			"AS rn FROM %s) t WHERE rn=1);"+
			"ALTER TABLE %s RENAME TO %s;"+
			"ALTER TABLE %s RENAME TO %s;"+
			"DROP TABLE %s;"+
			"COMMIT;"+
			"", tempTable, sanitize(sourceName), sanitizedTable, oldTable, tempTable, sanitizedTable, oldTable)
	var numStatements = 6
	stmt, _ := sf.WithMultiStatement(context.TODO(), numStatements)
	if _, err := db.QueryContext(stmt, query); err != nil {
		wrapped := fferr.NewExecutionError("SQL", err)
		wrapped.AddDetail("table_name", tableName)
		return wrapped
	}
	return nil
}

func (q defaultOfflineSQLQueries) getTable() string {
	bind := q.newVariableBindingIterator()
	return fmt.Sprintf("SELECT DISTINCT (table_name) FROM information_schema.tables WHERE table_name=%s and table_schema = CURRENT_SCHEMA()", bind.Next())
}

func (q defaultOfflineSQLQueries) materializationExists() string {
	bind := q.newVariableBindingIterator()
	return fmt.Sprintf("SELECT DISTINCT (table_name) FROM information_schema.tables WHERE table_name=%s and table_schema = CURRENT_SCHEMA()", bind.Next())
}

func (q defaultOfflineSQLQueries) materializationDrop(tableName string) string {
	return fmt.Sprintf("DROP MATERIALIZED VIEW %s", sanitize(tableName))
}

func (q defaultOfflineSQLQueries) dropTable(tableName string) string {
	return fmt.Sprintf("DROP TABLE %s", sanitize(tableName))
}

func (q defaultOfflineSQLQueries) trainingRowSelect(columns string, trainingSetName string) string {
	return fmt.Sprintf("SELECT %s FROM %s", columns, sanitize(trainingSetName))
}

func (q defaultOfflineSQLQueries) trainingRowSplitSelect(columns string, trainingSetSplitName string) (string, string) {
	// throw unimiplemented error
	return "", ""
}
func (q defaultOfflineSQLQueries) getValueColumnTypes(tableName string) string {
	return fmt.Sprintf("SELECT * FROM %s", sanitize(tableName))
}

func (q defaultOfflineSQLQueries) determineColumnType(valueType types.ValueType) (string, error) {
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
		return "TIMESTAMP_NTZ", nil
	case types.NilType:
		return "VARCHAR", nil
	default:
		return "", fferr.NewDataTypeNotFoundErrorf(valueType, "could not determine column type")
	}
}

func (q defaultOfflineSQLQueries) newSQLOfflineTable(name string, columnType string) string {
	return fmt.Sprintf("CREATE TABLE %s (entity VARCHAR, value %s, ts TIMESTAMP_NTZ, UNIQUE (entity, ts))", sanitize(name), columnType)
}

func (q defaultOfflineSQLQueries) resourceExists(tableName string) string {
	bind := q.newVariableBindingIterator()
	return fmt.Sprintf("SELECT entity, value, ts FROM %s WHERE entity=%s AND ts=%s ", sanitize(tableName), bind.Next(), bind.Next())
}
func (q defaultOfflineSQLQueries) writeUpdate(table string) string {
	bind := q.newVariableBindingIterator()
	return fmt.Sprintf("UPDATE %s SET value=%s WHERE entity=%s AND ts=%s ", table, bind.Next(), bind.Next(), bind.Next())
}
func (q defaultOfflineSQLQueries) writeInserts(table string) string {
	bind := q.newVariableBindingIterator()
	return fmt.Sprintf("INSERT INTO %s (entity, value, ts) VALUES (%s, %s, %s)", table, bind.Next(), bind.Next(), bind.Next())
}
func (q defaultOfflineSQLQueries) writeExists(table string) string {
	bind := q.newVariableBindingIterator()
	return fmt.Sprintf("SELECT COUNT (*) FROM %s WHERE entity=%s AND ts=%s", table, bind.Next(), bind.Next())
}

func (q defaultOfflineSQLQueries) materializationIterateSegment(tableName string) string {
	bind := q.newVariableBindingIterator()
	return fmt.Sprintf("SELECT entity, value, ts FROM ( SELECT * FROM %s WHERE row_number>%s AND row_number<=%s)t1;", sanitize(tableName), bind.Next(), bind.Next())
}

func (q defaultOfflineSQLQueries) createValuePlaceholderString(columns []TableColumn) string {
	placeholders := make([]string, 0)
	for _ = range columns {
		placeholders = append(placeholders, fmt.Sprintf("?"))
	}
	return strings.Join(placeholders, ", ")
}

func (q defaultOfflineSQLQueries) trainingSetQuery(store *sqlOfflineStore, def TrainingSetDef, tableName string, labelName string, isUpdate bool) error {
	columns := make([]string, 0)
	query := ""
	for i, feature := range def.Features {
		tableName, err := store.getResourceTableName(feature)
		santizedName := sanitize(tableName)
		if err != nil {
			return err
		}
		tableJoinAlias := fmt.Sprintf("t%d", i+1)
		columns = append(columns, santizedName)
		query = fmt.Sprintf("%s LEFT OUTER JOIN (SELECT entity, value as %s, ts FROM %s ORDER BY ts desc) as %s ON (%s.entity=t0.entity AND %s.ts <= t0.ts)",
			query, santizedName, santizedName, tableJoinAlias, tableJoinAlias, tableJoinAlias)

	}
	for i, lagFeature := range def.LagFeatures {
		lagFeaturesOffset := len(def.Features)
		tableName, err := store.getResourceTableName(ResourceID{lagFeature.FeatureName, lagFeature.FeatureVariant, Feature})
		if err != nil {
			return err
		}
		lagColumnName := sanitize(lagFeature.LagName)
		if lagFeature.LagName == "" {
			lagColumnName = sanitize(fmt.Sprintf("%s_lag_%s", tableName, lagFeature.LagDelta))
		}
		columns = append(columns, lagColumnName)
		sanitizedName := sanitize(tableName)
		tableJoinAlias := fmt.Sprintf("t%d", lagFeaturesOffset+i+1)
		timeDeltaSeconds := lagFeature.LagDelta.Seconds()
		query = fmt.Sprintf("%s LEFT OUTER JOIN (SELECT entity, value as %s, ts FROM %s ORDER BY ts desc) as %s ON (%s.entity=t0.entity AND (%s.ts + INTERVAL '%f') <= t0.ts)",
			query, lagColumnName, sanitizedName, tableJoinAlias, tableJoinAlias, tableJoinAlias, timeDeltaSeconds)
	}

	query = fmt.Sprintf("%s )) WHERE rn=1", query)
	columnStr := strings.Join(columns, ", ")
	if !isUpdate {
		fullQuery := fmt.Sprintf(
			"CREATE TABLE %s AS (SELECT %s, label FROM ("+
				"SELECT *, row_number() over(PARTITION BY e, label, time ORDER BY time desc) as rn FROM ( "+
				"SELECT t0.entity as e, t0.value as label, t0.ts as time, %s from %s as t0 %s )",
			sanitize(tableName), columnStr, columnStr, sanitize(labelName), query)
		if _, err := store.db.Exec(fullQuery); err != nil {
			wrapped := fferr.NewExecutionError("SQL", err)
			wrapped.AddDetail("table_name", tableName)
			return wrapped
		}
	} else {
		tempTable := sanitize(fmt.Sprintf("tmp_%s", tableName))
		fullQuery := fmt.Sprintf(
			"CREATE TABLE %s AS (SELECT %s, label FROM ("+
				"SELECT *, row_number() over(PARTITION BY e, label, time ORDER BY time desc) as rn FROM ( "+
				"SELECT t0.entity as e, t0.value as label, t0.ts as time, %s from %s as t0 %s )",
			tempTable, columnStr, columnStr, sanitize(labelName), query)
		err := q.atomicUpdate(store.db, tableName, tempTable, fullQuery)
		return err
	}
	return nil
}

func (q defaultOfflineSQLQueries) atomicUpdate(db *sql.DB, tableName string, tempName string, query string) error {
	sanitizedTable := sanitize(tableName)
	transaction := fmt.Sprintf(
		"BEGIN TRANSACTION;"+
			"%s;"+
			"TRUNCATE TABLE %s;"+ // this doesn't work in a trx for BIGQUERY and REDSHIFT
			"INSERT INTO %s SELECT * FROM %s;"+
			"DROP TABLE %s;"+
			"COMMIT;"+
			"", query, sanitizedTable, sanitizedTable, tempName, tempName)
	var numStatements = 6
	// Gets around the fact that the go redshift driver doesn't support multi statement trx queries
	stmt, _ := sf.WithMultiStatement(context.TODO(), numStatements)
	if _, err := db.QueryContext(stmt, transaction); err != nil {
		wrapped := fferr.NewExecutionError("SQL", err)
		wrapped.AddDetail("table_name", tableName)
		return wrapped
	}
	return nil
}

func (q defaultOfflineSQLQueries) trainingSetCreate(store *sqlOfflineStore, def TrainingSetDef, tableName string, labelName string) error {
	return q.trainingSetQuery(store, def, tableName, labelName, false)
}

func (q defaultOfflineSQLQueries) trainingSetUpdate(store *sqlOfflineStore, def TrainingSetDef, tableName string, labelName string) error {
	return q.trainingSetQuery(store, def, tableName, labelName, true)
}

func (q defaultOfflineSQLQueries) castTableItemType(v interface{}, t interface{}) interface{} {
	if v == nil {
		return nil
	}

	switch t {
	case sfInt, sfNumber:
		if intVar, err := strconv.Atoi(v.(string)); err != nil {
			return v
		} else {
			return intVar
		}
	case sfFloat:
		if casted, ok := v.(float64); ok {
			return casted
		}
		if s, err := strconv.ParseFloat(v.(string), 64); err != nil {
			return v
		} else {
			return s
		}
	case sfString:
		return v.(string)
	case sfBool:
		return v.(bool)
	case sfTimestamp:
		ts := v.(time.Time).UTC()
		return ts
	default:
		return v
	}
}

func (q defaultOfflineSQLQueries) getValueColumnType(t *sql.ColumnType) interface{} {
	switch t.ScanType().String() {
	case "string":
		return sfString
	case "int64":
		return sfInt
	// TODO: (Erik) determine if we want to handle int32 and int64 separately
	case "float32", "float64":
		return sfFloat
	case "bool":
		return sfBool
	case "time.Time":
		return sfTimestamp
	}
	return sfString
}

func (q defaultOfflineSQLQueries) numRows(n interface{}) (int64, error) {
	if intVar, err := strconv.Atoi(n.(string)); err != nil {
		return 0, fferr.NewInternalError(err)
	} else {
		return int64(intVar), nil
	}
}

func (q defaultOfflineSQLQueries) transformationCreate(name string, query string) []string {
	return []string{
		fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM ( %s )", sanitize(name), query),
	}
}

func (q defaultOfflineSQLQueries) transformationUpdate(db *sql.DB, tableName string, query string) error {
	tempName := sanitize(fmt.Sprintf("tmp_%s", tableName))
	fullQuery := fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM ( %s )", tempName, query)
	err := q.atomicUpdate(db, tableName, tempName, fullQuery)
	if err != nil {
		return err
	}
	return nil
}

func (q defaultOfflineSQLQueries) transformationExists() string {
	bind := q.newVariableBindingIterator()
	return fmt.Sprintf("SELECT DISTINCT (table_name) FROM information_schema.tables WHERE table_name=%s AND table_schema=CURRENT_SCHEMA()", bind.Next())
}

func (q defaultOfflineSQLQueries) resourceTableColumns(obj pl.FullyQualifiedObject) (string, error) {
	if obj.Database == "" {
		return "", fferr.NewInternalErrorf("database required for resource table columns query")
	}
	if obj.Table == "" {
		return "", fferr.NewInternalErrorf("table required for resource table columns query")
	}
	if obj.Schema == "" {
		obj.Schema = "PUBLIC"
	}
	var sb strings.Builder
	sb.WriteString("SELECT COLUMN_NAME FROM ")
	sb.WriteString(fmt.Sprintf("%s.INFORMATION_SCHEMA.COLUMNS ", obj.Database))
	sb.WriteString(fmt.Sprintf("WHERE TABLE_SCHEMA = '%s' ", obj.Schema))
	sb.WriteString(fmt.Sprintf("AND TABLE_NAME = '%s'", obj.Table))
	return sb.String(), nil
}

func GetTransformationTableName(id ResourceID) (string, error) {
	if err := id.check(Transformation); err != nil {
		return "", fferr.NewInternalErrorf("resource type must be %s: received %s", Transformation.String(), id.Type.String())
	}
	return ps.ResourceToTableName("Transformation", id.Name, id.Variant)
}

func SanitizeSqlLocation(obj pl.FullyQualifiedObject) string {
	ident := db.Identifier{}

	if obj.Database != "" && obj.Schema != "" {
		ident = append(ident, obj.Database)
	}

	if obj.Schema != "" {
		ident = append(ident, obj.Schema)
	}

	ident = append(ident, obj.Table)

	return ident.Sanitize()
}
