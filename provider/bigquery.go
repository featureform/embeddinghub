// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/featureform/logging"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/featureform/fferr"
	"github.com/featureform/metadata"
	pl "github.com/featureform/provider/location"
	pc "github.com/featureform/provider/provider_config"
	ps "github.com/featureform/provider/provider_schema"
	p_type "github.com/featureform/provider/provider_type"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/provider/types"
	_ "github.com/lib/pq"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const (
	bqInt       = "INTEGER"
	bqBigInt    = "BIGINT"
	bqFloat     = "DECIMAL"
	bqString    = "STRING"
	bqBool      = "BOOL"
	bqTimestamp = "TIMESTAMP"
)

const (
	sleepTime = 1 * time.Second
)

type BQOfflineStoreConfig struct {
	Config       pc.SerializedConfig
	ProjectId    string
	ProviderType p_type.Type
	QueryImpl    BQOfflineTableQueries
	logger       logging.Logger
}

type BQOfflineTableQueries interface {
	tableExists(tableName string) string
	viewExists(viewName string) string
	determineColumnType(valueType types.ValueType) (string, error)
	primaryTableCreate(name string, columnString string) string
	upsertQuery(tb string, columns string, placeholder string) string
	createValuePlaceholderString(columns []TableColumn) string
	registerResources(client *bigquery.Client, tableName string, schema ResourceSchema, timestamp bool) error
	writeUpdate(table string) string
	writeInserts(table string) string
	writeExists(table string) string
	newBQOfflineTable(name string, columnType string) string
	materializationCreate(tableName string, resultName string) string
	materializationIterateSegment(tableName string, start int64, end int64) string
	getNumRowsQuery(tableName string) string
	getTablePrefix() string
	setTablePrefix(prefix string)
	getContext() context.Context
	setContext()
	castTableItemType(v interface{}, t interface{}) interface{}
	materializationExists(tableName string) string
	materializationDrop(tableName string) string
	materializationUpdate(client *bigquery.Client, tableName string, sourceName string) error
	monitorJob(job *bigquery.Job) error
	transformationCreate(name string, query string) string
	getColumns(client *bigquery.Client, name string) ([]TableColumn, error)
	transformationUpdate(client *bigquery.Client, tableName string, query string) error
	trainingSetCreate(store *bqOfflineStore, def TrainingSetDef, tableName string, labelName string) error
	trainingSetUpdate(store *bqOfflineStore, def TrainingSetDef, tableName string, labelName string) error
	trainingSetQuery(store *bqOfflineStore, def TrainingSetDef, tableName string, labelName string, isUpdate bool) error
	atomicUpdate(client *bigquery.Client, tableName string, tempName string, query string) error
	trainingRowSelect(columns string, trainingSetName string) string
	primaryTableRegister(tableName string, sourceName string) string
	getTableName(tableName string) string
}

type defaultBQQueries struct {
	TablePrefix string
	Ctx         context.Context
}

type bqGenericTableIterator struct {
	iter         *bigquery.RowIterator
	currentValue GenericRecord
	err          error
	query        BQOfflineTableQueries
}

type bqPrimaryTable struct {
	client *bigquery.Client
	name   string
	query  BQOfflineTableQueries
	schema TableSchema
}

func (pt *bqPrimaryTable) GetName() string {
	return pt.name
}

func (pt *bqPrimaryTable) IterateSegment(n int64) (GenericTableIterator, error) {
	tableName := pt.query.getTableName(pt.name)
	var query string
	if n == -1 {
		query = fmt.Sprintf("SELECT * FROM `%s`", tableName)
	} else {
		query = fmt.Sprintf("SELECT * FROM `%s` LIMIT %d", tableName, n)
	}
	bqQ := pt.client.Query(query)
	it, err := bqQ.Read(pt.query.getContext())
	if err != nil {
		wrapped := fferr.NewExecutionError(p_type.BigQueryOffline.String(), err)
		wrapped.AddDetail("table_name", tableName)
		return nil, wrapped
	}
	return newBigQueryGenericTableIterator(it, pt.query), nil
}

func (pt *bqPrimaryTable) NumRows() (int64, error) {
	var n []bigquery.Value
	tableName := pt.query.getTableName(pt.name)
	query := fmt.Sprintf("SELECT COUNT(*) FROM `%s`", tableName)

	bqQ := pt.client.Query(query)

	it, err := bqQ.Read(pt.query.getContext())
	if err != nil {
		wrapped := fferr.NewExecutionError(p_type.BigQueryOffline.String(), err)
		wrapped.AddDetail("table_name", tableName)
		return 0, wrapped
	}

	err = it.Next(&n)
	if err != nil {
		wrapped := fferr.NewExecutionError(p_type.BigQueryOffline.String(), err)
		wrapped.AddDetail("table_name", tableName)
		return 0, wrapped
	}

	return n[0].(int64), nil
}

func (pt *bqPrimaryTable) Write(rec GenericRecord) error {
	tb := pt.name
	recordsParameter, columns, columnsString := pt.getNonNullRecords(rec)

	placeholder := pt.query.createValuePlaceholderString(columns)
	upsertQuery := pt.query.upsertQuery(tb, columnsString, placeholder)

	bqQ := pt.client.Query(upsertQuery)
	bqQ.Parameters = recordsParameter

	if _, err := bqQ.Read(pt.query.getContext()); err != nil {
		wrapped := fferr.NewExecutionError(p_type.BigQueryOffline.String(), err)
		wrapped.AddDetail("table_name", tb)
		return wrapped
	}

	return nil
}

func (pt *bqPrimaryTable) WriteBatch(recs []GenericRecord) error {
	for _, rec := range recs {
		if err := pt.Write(rec); err != nil {
			return err
		}
	}
	return nil
}

func (pt *bqPrimaryTable) getNonNullRecords(rec GenericRecord) ([]bigquery.QueryParameter, []TableColumn, string) {
	recordsParameter := make([]bigquery.QueryParameter, 0)
	recordColumns := make([]TableColumn, 0)
	selectedColumns := make([]string, 0)
	for i, r := range rec {
		if r == nil {
			continue
		}
		recordsParameter = append(recordsParameter, bigquery.QueryParameter{Value: r})
		recordColumns = append(recordColumns, pt.schema.Columns[i])
		selectedColumns = append(selectedColumns, pt.schema.Columns[i].Name)
	}

	columns := strings.Join(selectedColumns, ",")

	return recordsParameter, recordColumns, columns
}

func (it *bqGenericTableIterator) Next() bool {
	var rowValues []bigquery.Value
	err := it.iter.Next(&rowValues)
	if err == iterator.Done {
		it.err = nil
		return false
	} else if err != nil {
		it.err = fferr.NewExecutionError(p_type.BigQueryOffline.String(), err)
		return false
	}

	genRows := make(GenericRecord, len(rowValues))
	for i, value := range rowValues {
		if value == nil {
			continue
		}

		colType := it.iter.Schema[i].Type
		genRows[i] = it.query.castTableItemType(value, colType)
	}
	it.currentValue = genRows
	return true
}

func (it *bqGenericTableIterator) Values() GenericRecord {
	return it.currentValue
}

func (it *bqGenericTableIterator) Columns() []string {
	var columns []string
	// As the documentation for bigquery.Schema notes:
	// > The schema of the table. In some scenarios it will only be available after the first call to Next(),
	//  like when a call to Query.Read uses the jobs.query API for an optimized query path.
	// Given this possibility, we should check if the schema is empty and if so, call Next() to populate it.
	// Given Columns is only called to fetch the data sources columns list, we can safely call Next() here
	// without concern for skipping a value in the iteration.
	if len(it.iter.Schema) == 0 {
		it.Next()
	}
	for _, col := range it.iter.Schema {
		columns = append(columns, col.Name)
	}
	return columns
}

func (it *bqGenericTableIterator) Err() error {
	return it.err
}

func (it *bqGenericTableIterator) Close() error {
	return nil
}

func newBigQueryGenericTableIterator(it *bigquery.RowIterator, query BQOfflineTableQueries) GenericTableIterator {
	return &bqGenericTableIterator{
		iter:         it,
		currentValue: nil,
		err:          nil,
		query:        query,
	}
}

func (q defaultBQQueries) registerResources(client *bigquery.Client, tableName string, schema ResourceSchema, timestamp bool) error {
	var query string
	if timestamp {
		query = fmt.Sprintf("CREATE VIEW `%s` AS SELECT `%s` as entity, `%s` as value, `%s` as ts, CURRENT_TIMESTAMP() as insert_ts FROM `%s`", q.getTableName(tableName),
			schema.Entity, schema.Value, schema.TS, q.getTableName(schema.SourceTable.Location()))
	} else {
		query = fmt.Sprintf("CREATE VIEW `%s` AS SELECT `%s` as entity, `%s` as value, PARSE_TIMESTAMP('%%Y-%%m-%%d %%H:%%M:%%S +0000 UTC', '%s') as ts, CURRENT_TIMESTAMP() as insert_ts FROM `%s`", q.getTableName(tableName),
			schema.Entity, schema.Value, time.UnixMilli(0).UTC(), q.getTableName(schema.SourceTable.Location()))
	}

	bqQ := client.Query(query)
	if _, err := bqQ.Read(q.getContext()); err != nil {
		wrapped := fferr.NewExecutionError(p_type.BigQueryOffline.String(), err)
		wrapped.AddDetail("table_name", tableName)
		return wrapped
	}
	return nil
}

func (q defaultBQQueries) writeUpdate(table string) string {
	return fmt.Sprintf("UPDATE `%s` SET value=? WHERE entity=? AND ts=? ", q.getTableName(table))
}
func (q defaultBQQueries) writeInserts(table string) string {
	return fmt.Sprintf("INSERT INTO `%s` (entity, value, ts, insert_ts) VALUES (?, ?, ?, CURRENT_TIMESTAMP())", q.getTableName(table))
}

func (q defaultBQQueries) writeExists(table string) string {
	return fmt.Sprintf("SELECT COUNT(*) FROM `%s` WHERE entity=\"%s\" AND ts=timestamp(\"%s\")", q.getTableName(table), "?", "?")
}

func (q defaultBQQueries) tableExists(tableName string) string {
	return fmt.Sprintf("SELECT COUNT(*) AS total FROM `%s.INFORMATION_SCHEMA.TABLES` WHERE table_type='BASE TABLE' AND table_name='%s'", q.getTablePrefix(), tableName)
}

func (q defaultBQQueries) viewExists(viewName string) string {
	return fmt.Sprintf("SELECT COUNT(*) AS total FROM `%s.INFORMATION_SCHEMA.TABLES` WHERE table_type='VIEW' AND table_name='%s'", q.getTablePrefix(), viewName)
}

func (q defaultBQQueries) determineColumnType(valueType types.ValueType) (string, error) {
	switch valueType {
	case types.Int:
		return "INTEGER", nil
	case types.Int32, types.Int64:
		return "BIGINT", nil
	case types.Float32, types.Float64:
		return "FLOAT64", nil
	case types.String:
		return "STRING", nil
	case types.Bool:
		return "BOOLEAN", nil
	case types.Timestamp:
		return "TIMESTAMP", nil
	case types.NilType:
		return "STRING", nil
	default:
		return "", fferr.NewDataTypeNotFoundErrorf(valueType, "cannot find column type for value type")
	}
}

func (q defaultBQQueries) primaryTableCreate(name string, columnString string) string {
	query := fmt.Sprintf("CREATE TABLE `%s` ( %s )", q.getTableName(name), columnString)
	return query
}

func (q defaultBQQueries) upsertQuery(tb string, columns string, placeholder string) string {
	return fmt.Sprintf("INSERT INTO `%s` ( %s ) VALUES ( %s )", q.getTableName(tb), columns, placeholder)
}

func (q defaultBQQueries) createValuePlaceholderString(columns []TableColumn) string {
	placeholders := make([]string, 0)
	for range columns {
		placeholders = append(placeholders, "?")
	}
	return strings.Join(placeholders, ", ")
}

func (q defaultBQQueries) newBQOfflineTable(name string, columnType string) string {
	return fmt.Sprintf("CREATE TABLE `%s` (entity STRING, value %s, ts TIMESTAMP, insert_ts TIMESTAMP)", q.getTableName(name), columnType)
}

func (q defaultBQQueries) materializationCreate(tableName string, resultName string) string {
	query := fmt.Sprintf(
		"CREATE TABLE `%s` AS (SELECT entity, value, ts, row_number() over(ORDER BY entity) as row_number FROM "+
			"(SELECT entity, ts, value, row_number() OVER (PARTITION BY entity ORDER BY ts DESC, insert_ts DESC) "+
			"AS rn FROM `%s`) t WHERE rn=1)", q.getTableName(tableName), q.getTableName(resultName))

	return query
}

func (q defaultBQQueries) materializationIterateSegment(tableName string, start int64, end int64) string {
	return fmt.Sprintf("SELECT entity, value, ts FROM ( SELECT * FROM `%s` WHERE row_number > %v AND row_number <= %v)", q.getTableName(tableName), start, end)
}

func (q defaultBQQueries) getNumRowsQuery(tableName string) string {
	return fmt.Sprintf("SELECT COUNT(*) FROM `%s`", q.getTableName(tableName))
}

func (q *defaultBQQueries) getTablePrefix() string {
	return q.TablePrefix
}

func (q *defaultBQQueries) setTablePrefix(prefix string) {
	q.TablePrefix = prefix
}

func (q *defaultBQQueries) setContext() {
	q.Ctx = context.Background()
}

func (q *defaultBQQueries) getContext() context.Context {
	return q.Ctx
}

func (q defaultBQQueries) castTableItemType(v interface{}, t interface{}) interface{} {
	if v == nil {
		return v
	}

	t = fmt.Sprintf("%s", t)
	switch t {
	case bqInt:
		return int(v.(int64))
	case bqBigInt:
		return int64(v.(int64))
	case bqFloat:
		return v.(float64)
	case bqString:
		v := v.(string)
		return v
	case bqBool:
		return v.(bool)
	case bqTimestamp:
		return v.(time.Time).UTC()
	default:
		return v
	}
}

func (q defaultBQQueries) materializationExists(tableName string) string {
	return fmt.Sprintf("SELECT DISTINCT(table_name) FROM `%s.INFORMATION_SCHEMA.TABLES` WHERE table_type='BASE TABLE' AND table_name='%s'", q.getTablePrefix(), tableName)
}

func (q defaultBQQueries) materializationDrop(tableName string) string {
	return fmt.Sprintf("DROP TABLE `%s`", q.getTableName(tableName))
}

func (q defaultBQQueries) materializationUpdate(client *bigquery.Client, tableName string, sourceName string) error {
	sanitizedTable := tableName
	tempTable := fmt.Sprintf("tmp_%s", tableName)
	oldTable := fmt.Sprintf("old_%s", tableName)

	materializationCreateQuery := fmt.Sprintf("CREATE TABLE `%s` AS (SELECT entity, value, ts, row_number() over(ORDER BY (entity)) as row_number FROM "+
		"(SELECT entity, ts, value, row_number() OVER (PARTITION BY entity ORDER BY ts DESC, insert_ts DESC) "+
		"AS rn FROM `%s`) t WHERE rn=1);", q.getTableName(tempTable), q.getTableName(sourceName))

	alterTables := fmt.Sprintf(
		"ALTER TABLE `%s` RENAME TO `%s`;"+
			"ALTER TABLE `%s` RENAME TO `%s`;", q.getTableName(sanitizedTable), oldTable, q.getTableName(tempTable), sanitizedTable)

	dropTable := fmt.Sprintf("DROP TABLE `%s`;", q.getTableName(oldTable))

	query := fmt.Sprintf("%s %s %s", materializationCreateQuery, alterTables, dropTable)

	bqQ := client.Query(query)
	job, err := bqQ.Run(q.getContext())
	if err != nil {
		wrapped := fferr.NewExecutionError(p_type.BigQueryOffline.String(), err)
		wrapped.AddDetail("table_name", tableName)
		wrapped.AddDetail("source_name", sourceName)
		return wrapped
	}

	err = q.monitorJob(job)
	return err
}

func (q defaultBQQueries) monitorJob(job *bigquery.Job) error {
	for {
		time.Sleep(sleepTime)
		status, err := job.Status(q.getContext())
		if err != nil {
			wrapped := fferr.NewExecutionError(p_type.BigQueryOffline.String(), err)
			wrapped.AddDetail("job_id", job.ID())
			return wrapped
		} else if status.Err() != nil {
			wrapped := fferr.NewExecutionError(p_type.BigQueryOffline.String(), status.Err())
			wrapped.AddDetail("job_id", job.ID())
			return wrapped
		}

		switch status.State {
		case bigquery.Done:
			return nil
		default:
			continue
		}
	}
}

func (q defaultBQQueries) transformationCreate(name string, query string) string {
	qry := fmt.Sprintf("CREATE TABLE `%s` AS %s", q.getTableName(name), query)
	return qry
}

func (q defaultBQQueries) getColumns(client *bigquery.Client, name string) ([]TableColumn, error) {
	qry := fmt.Sprintf("SELECT column_name FROM `%s.INFORMATION_SCHEMA.COLUMNS` WHERE table_name=\"%s\" ORDER BY ordinal_position", q.getTablePrefix(), name)

	bqQ := client.Query(qry)
	it, err := bqQ.Read(q.getContext())
	if err != nil {
		wrapped := fferr.NewExecutionError(p_type.BigQueryOffline.String(), err)
		wrapped.AddDetail("table_name", name)
		return nil, wrapped
	}

	columnNames := make([]TableColumn, 0)
	for {
		var column []bigquery.Value
		err := it.Next(&column)
		if err == iterator.Done {
			break
		} else if err != nil {
			wrapped := fferr.NewExecutionError(p_type.BigQueryOffline.String(), err)
			wrapped.AddDetail("table_name", name)
			return nil, wrapped
		}
		columnNames = append(columnNames, TableColumn{Name: column[0].(string)})
	}

	return columnNames, nil
}

func (q defaultBQQueries) transformationUpdate(client *bigquery.Client, tableName string, query string) error {
	tempName := fmt.Sprintf("tmp_%s", tableName)
	fullQuery := fmt.Sprintf("CREATE TABLE `%s` AS %s", q.getTableName(tempName), query)

	err := q.atomicUpdate(client, tableName, tempName, fullQuery)
	if err != nil {
		wrapped := fferr.NewExecutionError(p_type.BigQueryOffline.String(), err)
		wrapped.AddDetail("table_name", tableName)
		return wrapped
	}
	return nil
}

func (q defaultBQQueries) atomicUpdate(client *bigquery.Client, tableName string, tempName string, query string) error {
	bqTableName := q.getTableName(tableName)
	bqTempTableName := q.getTableName(tempName)
	updateQuery := fmt.Sprintf(
		"%s;"+
			"TRUNCATE TABLE `%s`;"+ // this doesn't work in a trx
			"INSERT INTO `%s` SELECT * FROM `%s`;"+
			"DROP TABLE `%s`;"+
			"", query, bqTableName, bqTableName, bqTempTableName, bqTempTableName)

	bdQ := client.Query(updateQuery)
	job, err := bdQ.Run(q.getContext())
	if err != nil {
		wrapped := fferr.NewExecutionError(p_type.BigQueryOffline.String(), err)
		wrapped.AddDetail("table_name", tableName)
		return wrapped
	}

	err = q.monitorJob(job)
	return err
}

func (q defaultBQQueries) trainingSetCreate(store *bqOfflineStore, def TrainingSetDef, tableName string, labelName string) error {
	return q.trainingSetQuery(store, def, tableName, labelName, false)
}

func (q defaultBQQueries) trainingSetUpdate(store *bqOfflineStore, def TrainingSetDef, tableName string, labelName string) error {
	return q.trainingSetQuery(store, def, tableName, labelName, true)
}

func (q defaultBQQueries) trainingSetQuery(store *bqOfflineStore, def TrainingSetDef, tableName string, labelName string, isUpdate bool) error {
	columns := make([]string, 0)
	selectColumns := make([]string, 0)
	query := ""
	for i, feature := range def.Features {
		tableName, err := store.getResourceTableName(feature)
		if err != nil {
			return err
		}
		santizedName := strings.Replace(tableName, "-", "_", -1)
		tableJoinAlias := fmt.Sprintf("t%d", i+1)
		selectColumns = append(selectColumns, fmt.Sprintf("%s_rnk", tableJoinAlias))
		columns = append(columns, santizedName)
		query = fmt.Sprintf("%s LEFT OUTER JOIN (SELECT entity, value AS `%s`, ts, RANK() OVER (ORDER BY ts DESC, insert_ts DESC) AS %s_rnk FROM `%s` ORDER BY ts desc) AS %s ON (%s.entity=t0.entity AND %s.ts <= t0.ts)",
			query, santizedName, tableJoinAlias, q.getTableName(tableName), tableJoinAlias, tableJoinAlias, tableJoinAlias)
		if i == len(def.Features)-1 {
			query = fmt.Sprintf("%s )) WHERE rn=1", query)
		}
	}
	columnStr := strings.Join(columns, ", ")
	selectColumnStr := strings.Join(selectColumns, ", ")

	if !isUpdate {
		fullQuery := fmt.Sprintf(
			"CREATE TABLE `%s` AS (SELECT %s, label FROM ("+
				"SELECT *, row_number() over(PARTITION BY e, label, time ORDER BY \"time\", %s DESC) AS rn FROM ( "+
				"SELECT t0.entity AS e, t0.value AS label, t0.ts AS time, %s, %s FROM `%s` AS t0 %s )",
			q.getTableName(tableName), columnStr, selectColumnStr, columnStr, selectColumnStr, q.getTableName(labelName), query)

		bqQ := store.client.Query(fullQuery)
		job, err := bqQ.Run(store.query.getContext())
		if err != nil {
			return fferr.NewResourceExecutionError(p_type.BigQueryOffline.String(), def.ID.Name, def.ID.Variant, fferr.ResourceType(def.ID.Type.String()), err)
		}

		err = store.query.monitorJob(job)
		return err
	} else {
		tempTable := fmt.Sprintf("tmp_%s", tableName)
		fullQuery := fmt.Sprintf(
			"CREATE TABLE `%s` AS (SELECT %s, label FROM ("+
				"SELECT *, row_number() over(PARTITION BY e, label, time ORDER BY \"time\", %s desc) AS rn FROM ( "+
				"SELECT t0.entity AS e, t0.value AS label, t0.ts AS time, %s, %s FROM `%s` AS t0 %s )",
			q.getTableName(tempTable), columnStr, selectColumnStr, columnStr, selectColumnStr, q.getTableName(labelName), query)
		err := q.atomicUpdate(store.client, tableName, tempTable, fullQuery)
		return err
	}
}

func (q defaultBQQueries) trainingRowSelect(columns string, trainingSetName string) string {
	return fmt.Sprintf("SELECT %s FROM `%s`", columns, q.getTableName(trainingSetName))
}

func (q defaultBQQueries) primaryTableRegister(tableName string, sourceName string) string {
	return fmt.Sprintf("CREATE VIEW `%s` AS SELECT * FROM `%s`", q.getTableName(tableName), q.getTableName(sourceName))
}

func (q defaultBQQueries) getTableName(tableName string) string {
	return fmt.Sprintf("%s.%s", q.getTablePrefix(), tableName)
}

type bqMaterialization struct {
	id        MaterializationID
	client    *bigquery.Client
	tableName string
	query     BQOfflineTableQueries
}

func (mat *bqMaterialization) ID() MaterializationID {
	return mat.id
}

func (mat *bqMaterialization) NumRows() (int64, error) {
	var n []bigquery.Value
	query := mat.query.getNumRowsQuery(mat.tableName)

	bqQ := mat.client.Query(query)
	it, err := bqQ.Read(mat.query.getContext())
	if err != nil {
		wrapped := fferr.NewExecutionError(p_type.BigQueryOffline.String(), err)
		wrapped.AddDetail("table_name", mat.tableName)
		return 0, wrapped
	}

	err = it.Next(&n)
	if err != nil {
		wrapped := fferr.NewExecutionError(p_type.BigQueryOffline.String(), err)
		wrapped.AddDetail("table_name", mat.tableName)
		return 0, wrapped
	}
	if n == nil {
		return 0, nil
	}
	return n[0].(int64), nil

}

func (mat *bqMaterialization) IterateSegment(start, end int64) (FeatureIterator, error) {
	query := mat.query.materializationIterateSegment(mat.tableName, start, end)

	bqQ := mat.client.Query(query)
	it, err := bqQ.Read(mat.query.getContext())
	if err != nil {
		return nil, fferr.NewExecutionError(p_type.BigQueryOffline.String(), err)
	}
	return newbqFeatureIterator(it, mat.query), nil
}

func (mat *bqMaterialization) NumChunks() (int, error) {
	return genericNumChunks(mat, defaultRowsPerChunk)
}

func (mat *bqMaterialization) IterateChunk(idx int) (FeatureIterator, error) {
	return genericIterateChunk(mat, defaultRowsPerChunk, idx)
}

func (mat *bqMaterialization) Location() pl.Location {
	return pl.NewSQLLocation(mat.tableName)
}

type bqFeatureIterator struct {
	iter         *bigquery.RowIterator
	currentValue ResourceRecord
	err          error
	query        BQOfflineTableQueries
}

func newbqFeatureIterator(it *bigquery.RowIterator, query BQOfflineTableQueries) FeatureIterator {
	return &bqFeatureIterator{
		iter:         it,
		err:          nil,
		currentValue: ResourceRecord{},
		query:        query,
	}
}

func (it *bqFeatureIterator) Next() bool {
	var rowValue []bigquery.Value
	err := it.iter.Next(&rowValue)
	if err == iterator.Done {
		it.err = nil
		return false
	} else if err != nil {
		it.err = fferr.NewExecutionError(p_type.BigQueryOffline.String(), err)
		return false
	}

	var currValue ResourceRecord
	valueColType := it.iter.Schema[1].Type
	if err := currValue.SetEntity(rowValue[0]); err != nil {
		it.err = err
		return false
	}
	currValue.Value = it.query.castTableItemType(rowValue[1], valueColType)
	currValue.TS = rowValue[2].(time.Time)

	it.currentValue = currValue
	return true
}

func (it *bqFeatureIterator) Value() ResourceRecord {
	return it.currentValue
}

func (it *bqFeatureIterator) Err() error {
	return it.err
}

func (it *bqFeatureIterator) Close() error {
	return nil
}

type bqOfflineTable struct {
	client *bigquery.Client
	query  BQOfflineTableQueries
	name   string
}

func (table *bqOfflineTable) Write(rec ResourceRecord) error {
	rec = checkTimestamp(rec)
	tb := table.name
	if err := rec.check(); err != nil {
		return err
	}

	var n []bigquery.Value
	existsQuery := table.query.writeExists(tb)

	bqQ := table.client.Query(existsQuery)
	bqQ.Parameters = []bigquery.QueryParameter{
		{
			Value: rec.Entity,
		},
		{
			Value: rec.TS,
		},
	}

	iter, err := bqQ.Read(table.query.getContext())
	if err != nil {
		return fferr.NewResourceExecutionError(p_type.BigQueryOffline.String(), rec.Entity, "", fferr.ENTITY, err)
	}

	err = iter.Next(&n)
	if err != nil {
		return fferr.NewResourceExecutionError(p_type.BigQueryOffline.String(), rec.Entity, "", fferr.ENTITY, err)
	}

	if n == nil {
		return fferr.NewInternalError(fmt.Errorf("cannot find %s table", tb))
	}

	var writeQuery string
	var params []bigquery.QueryParameter
	if n[0].(int64) == 0 {
		writeQuery = table.query.writeInserts(tb)
		params = []bigquery.QueryParameter{bigquery.QueryParameter{Value: rec.Entity}, bigquery.QueryParameter{Value: rec.Value}, bigquery.QueryParameter{Value: rec.TS}}
	} else if n[0].(int64) > 0 {
		writeQuery = table.query.writeUpdate(tb)
		params = []bigquery.QueryParameter{bigquery.QueryParameter{Value: rec.Value}, bigquery.QueryParameter{Value: rec.Entity}, bigquery.QueryParameter{Value: rec.TS}}
	}

	bqQ = table.client.Query(writeQuery)
	bqQ.Parameters = params

	if _, err = bqQ.Read(table.query.getContext()); err != nil {
		return fferr.NewResourceExecutionError(p_type.BigQueryOffline.String(), rec.Entity, "", fferr.ENTITY, err)
	}

	return nil
}

func (table *bqOfflineTable) WriteBatch(recs []ResourceRecord) error {
	for _, rec := range recs {
		if err := table.Write(rec); err != nil {
			return err
		}
	}
	return nil
}

func (table *bqOfflineTable) Location() pl.Location {
	return pl.NewSQLLocation(table.name)
}

type bqOfflineStore struct {
	client *bigquery.Client
	parent BQOfflineStoreConfig
	query  BQOfflineTableQueries
	logger logging.Logger
	BaseProvider
}

func NewBQOfflineStore(config BQOfflineStoreConfig) (*bqOfflineStore, error) {
	sc := pc.BigQueryConfig{}
	if err := sc.Deserialize(config.Config); err != nil {
		return nil, err
	}

	creds, err := json.Marshal(sc.Credentials)
	if err != nil {
		return nil, fferr.NewProviderConfigError(string(pt.BigQueryOffline), err)
	}
	client, err := bigquery.NewClient(context.TODO(), config.ProjectId, option.WithCredentialsJSON(creds))
	if err != nil {
		return nil, fferr.NewConnectionError(string(pt.BigQueryOffline), err)
	}
	defer client.Close()

	return &bqOfflineStore{
		client: client,
		parent: config,
		query:  config.QueryImpl,
		logger: config.logger,
		BaseProvider: BaseProvider{
			ProviderType:   config.ProviderType,
			ProviderConfig: config.Config,
		},
	}, nil
}

func bigQueryOfflineStoreFactory(config pc.SerializedConfig) (Provider, error) {
	sc := pc.BigQueryConfig{}
	logger := logging.NewLogger("bigquery")
	if err := sc.Deserialize(config); err != nil {
		return nil, err
	}
	queries := defaultBQQueries{}
	queries.setTablePrefix(fmt.Sprintf("%s.%s", sc.ProjectId, sc.DatasetId))
	queries.setContext()

	sgConfig := BQOfflineStoreConfig{
		Config:       config,
		ProjectId:    sc.ProjectId,
		ProviderType: pt.BigQueryOffline,
		QueryImpl:    &queries,
		logger:       logger,
	}

	store, err := NewBQOfflineStore(sgConfig)
	if err != nil {
		return nil, err
	}
	return store, nil
}

func (store *bqOfflineStore) RegisterResourceFromSourceTable(id ResourceID, schema ResourceSchema, opts ...ResourceOption) (OfflineTable, error) {
	if len(opts) > 0 {
		return nil, fferr.NewInternalErrorf("BigQuery does not support resource options")
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
		if err := store.query.registerResources(store.client, tableName, schema, false); err != nil {
			return nil, err
		}
	} else {
		if err := store.query.registerResources(store.client, tableName, schema, true); err != nil {
			return nil, err
		}
	}

	return &bqOfflineTable{
		client: store.client,
		name:   tableName,
		query:  store.query,
	}, nil
}

func (store *bqOfflineStore) RegisterPrimaryFromSourceTable(id ResourceID, tableLocation pl.Location) (PrimaryTable, error) {
	logger := store.logger.WithValues(map[string]any{
		"resourceId": id,
	})
	logger.Infow("Registering primary from source table")

	if err := id.check(Primary); err != nil {
		logger.Errorw("Resource type is not primary", "err", err)
		return nil, err
	}

	if exists, err := store.tableExists(id); err != nil {
		logger.Errorw("Checking if table exists", "err", err)
		return nil, err
	} else if exists {
		logger.Errorw("Table already exists", "err", err)
		return nil, fferr.NewDatasetAlreadyExistsError(id.Name, id.Variant, nil)
	}

	tableName, err := GetPrimaryTableName(id)
	if err != nil {
		logger.Errorw("Mapping id to table name", "err", err)
		return nil, err
	}
	query := store.query.primaryTableRegister(tableName, tableLocation.Location())

	bqQ := store.client.Query(query)
	job, err := bqQ.Run(store.query.getContext())
	if err != nil {
		logger.Errorw("Running query", "err", err)
		return nil, fferr.NewResourceExecutionError(store.Type().String(), id.Name, id.Variant, fferr.ResourceType(id.Type.String()), err)
	}

	err = store.query.monitorJob(job)
	if err != nil {
		logger.Errorw("Monitoring BigQuery job", "err", err)
		return nil, err
	}

	columnNames, err := store.query.getColumns(store.client, tableName)
	if err != nil {
		logger.Errorw("Getting column names", "err", err)
		return nil, err
	}
	return &bqPrimaryTable{
		client: store.client,
		name:   tableName,
		schema: TableSchema{Columns: columnNames},
		query:  store.query,
	}, nil
}

func (store *bqOfflineStore) SupportsTransformationOption(opt TransformationOptionType) (bool, error) {
	return false, nil
}

func (store *bqOfflineStore) CreateTransformation(config TransformationConfig, opts ...TransformationOption) error {
	if len(opts) > 0 {
		return fferr.NewInternalErrorf("BigQuery does not support transformation options")
	}
	name, err := store.getTableName(config.TargetTableID)
	if err != nil {
		return err
	}
	query := store.query.transformationCreate(name, config.Query)

	bqQ := store.client.Query(query)
	job, err := bqQ.Run(store.query.getContext())
	if err != nil {
		return fferr.NewResourceExecutionError(store.Type().String(), config.TargetTableID.Name, config.TargetTableID.Variant, fferr.ResourceType(config.TargetTableID.Type.String()), err)
	}

	err = store.query.monitorJob(job)
	return err
}

func (store *bqOfflineStore) getTableName(id ResourceID) (string, error) {
	return ps.ResourceToTableName(id.Type.String(), id.Name, id.Variant)
}

func (store *bqOfflineStore) GetTransformationTable(id ResourceID) (TransformationTable, error) {
	name, err := store.getTableName(id)
	if err != nil {
		return nil, err
	}

	existsQuery := store.query.tableExists(name)
	bqQ := store.client.Query(existsQuery)
	it, err := bqQ.Read(store.query.getContext())
	if err != nil {
		return nil, fferr.NewDatasetNotFoundError(id.Name, id.Variant, err)
	}

	var row []bigquery.Value
	err = it.Next(&row)

	if err != nil {
		return nil, fferr.NewResourceExecutionError(store.Type().String(), id.Name, id.Variant, fferr.ResourceType(id.Type.String()), err)
	}
	if len(row) == 0 {
		return nil, fferr.NewTransformationNotFoundError(id.Name, id.Variant, nil)
	}

	columnNames, err := store.query.getColumns(store.client, name)
	if err != nil {
		return nil, err
	}

	return &bqPrimaryTable{
		client: store.client,
		name:   name,
		schema: TableSchema{Columns: columnNames},
		query:  store.query,
	}, nil
}

func (store *bqOfflineStore) UpdateTransformation(config TransformationConfig, opts ...TransformationOption) error {
	if len(opts) > 0 {
		return fferr.NewInternalErrorf("BigQuery does not support transformation options")
	}
	name, err := store.getTableName(config.TargetTableID)
	if err != nil {
		return err
	}
	err = store.query.transformationUpdate(store.client, name, config.Query)
	if err != nil {
		return err
	}

	return nil
}

func (store *bqOfflineStore) CreatePrimaryTable(id ResourceID, schema TableSchema) (PrimaryTable, error) {
	if err := id.check(Primary); err != nil {
		return nil, err
	}
	if exists, err := store.tableExists(id); err != nil {
		return nil, err
	} else if exists {
		return nil, fferr.NewDatasetAlreadyExistsError(id.Name, id.Variant, nil)
	}
	if len(schema.Columns) == 0 {
		return nil, fferr.NewDatasetNotFoundError(id.Name, id.Variant, fmt.Errorf("cannot create primary table without columns"))
	}
	tableName, err := GetPrimaryTableName(id)
	if err != nil {
		return nil, err
	}
	table, err := store.newBigQueryPrimaryTable(store.client, tableName, schema)
	if err != nil {
		return nil, fferr.NewResourceExecutionError(store.Type().String(), id.Name, id.Variant, fferr.ResourceType(id.Type.String()), err)
	}
	return table, nil
}

func (store *bqOfflineStore) GetPrimaryTable(id ResourceID, source metadata.SourceVariant) (PrimaryTable, error) {
	name, err := GetPrimaryTableName(id)
	if err != nil {
		return nil, err
	}
	if exists, err := store.tableExists(id); err != nil {
		return nil, err
	} else if !exists {
		return nil, fferr.NewDatasetNotFoundError(id.Name, id.Variant, nil)
	}
	columnNames, err := store.query.getColumns(store.client, name)
	if err != nil {
		return nil, err
	}

	return &bqPrimaryTable{
		client: store.client,
		name:   name,
		schema: TableSchema{Columns: columnNames},
		query:  store.query,
	}, nil
}

func (store *bqOfflineStore) CreateResourceTable(id ResourceID, schema TableSchema) (OfflineTable, error) {
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
	table, err := store.newbqOfflineTable(store.client, tableName, valueType)
	if err != nil {
		return nil, err
	}

	return table, nil
}

func (store *bqOfflineStore) getValueIndex(columns []TableColumn) int {
	for i, column := range columns {
		if column.Name == "value" {
			return i
		}
	}
	return -1
}

func (store *bqOfflineStore) newbqOfflineTable(client *bigquery.Client, name string, valueType types.ValueType) (*bqOfflineTable, error) {
	columnType, err := store.query.determineColumnType(valueType)
	if err != nil {
		return nil, err
	}
	tableCreateQry := store.query.newBQOfflineTable(name, columnType)
	bqQ := client.Query(tableCreateQry)
	_, err = bqQ.Read(store.query.getContext())
	if err != nil {
		wrapped := fferr.NewExecutionError(store.Type().String(), err)
		wrapped.AddDetail("table_name", name)
		return nil, wrapped
	}
	return &bqOfflineTable{
		client: client,
		name:   name,
		query:  store.query,
	}, nil
}

func (store *bqOfflineStore) GetResourceTable(id ResourceID) (OfflineTable, error) {
	return store.getbqResourceTable(id)
}

func (store *bqOfflineStore) GetBatchFeatures(tables []ResourceID) (BatchFeatureIterator, error) {
	return nil, fferr.NewInternalError(fmt.Errorf("batch features not implemented for this provider"))
}

func (store *bqOfflineStore) CreateMaterialization(id ResourceID, opts MaterializationOptions) (Materialization, error) {
	if id.Type != Feature {
		return nil, fferr.NewInvalidArgumentError(fmt.Errorf("received %s; only features can be materialized", id.Type.String()))
	}
	resTable, err := store.getbqResourceTable(id)
	if err != nil {
		return nil, err
	}

	matID := MaterializationID(fmt.Sprintf("%s__%s", id.Name, id.Variant))
	matTableName, err := store.getMaterializationTableName(id)
	if err != nil {
		return nil, err
	}
	materializeQry := store.query.materializationCreate(matTableName, resTable.name)

	bqQ := store.client.Query(materializeQry)
	_, err = bqQ.Read(store.query.getContext())
	if err != nil {
		return nil, fferr.NewResourceExecutionError(store.Type().String(), id.Name, id.Variant, fferr.ResourceType(id.Type.String()), err)
	}
	return &bqMaterialization{
		id:        matID,
		client:    store.client,
		tableName: matTableName,
		query:     store.query,
	}, nil
}

func (store *bqOfflineStore) SupportsMaterializationOption(opt MaterializationOptionType) (bool, error) {
	return false, nil
}

func (store *bqOfflineStore) getbqResourceTable(id ResourceID) (*bqOfflineTable, error) {
	if exists, err := store.tableExists(id); err != nil {
		return nil, err
	} else if !exists {
		return nil, fferr.NewDatasetNotFoundError(id.Name, id.Variant, nil)
	}

	table, err := store.getResourceTableName(id)
	if err != nil {
		return nil, err
	}
	return &bqOfflineTable{
		client: store.client,
		name:   table,
		query:  store.query,
	}, nil
}

func (store *bqOfflineStore) getMaterializationTableName(id ResourceID) (string, error) {
	if err := id.check(Feature); err != nil {
		return "", err
	}
	// NOTE: Given BiqQuery uses intermediate resource tables, the inbound resource ID will be Feature;
	// however, the table must be named according to the FeatureMaterialization offline type.
	return ps.ResourceToTableName(FeatureMaterialization.String(), id.Name, id.Variant)
}

func (store *bqOfflineStore) GetMaterialization(id MaterializationID) (Materialization, error) {
	name, variant, err := ps.MaterializationIDToResource(string(id))
	if err != nil {
		return nil, err
	}
	tableName, err := store.getMaterializationTableName(ResourceID{Name: name, Variant: variant, Type: Feature})
	if err != nil {
		return nil, err
	}
	getMatQry := store.query.materializationExists(tableName)

	bqQry := store.client.Query(getMatQry)
	it, err := bqQry.Read(store.query.getContext())
	if err != nil {
		wrapped := fferr.NewExecutionError(store.Type().String(), err)
		wrapped.AddDetail("table_name", tableName)
		wrapped.AddDetail("materialization_id", string(id))
		return nil, wrapped
	}

	var row []bigquery.Value
	err = it.Next(&row)
	if err != nil {
		wrapped := fferr.NewExecutionError(store.Type().String(), err)
		wrapped.AddDetail("table_name", tableName)
		wrapped.AddDetail("materialization_id", string(id))
		return nil, wrapped
	}

	if len(row) == 0 {
		return nil, fferr.NewDatasetNotFoundError(string(id), "", nil)
	}
	return &bqMaterialization{
		id:        id,
		client:    store.client,
		tableName: tableName,
		query:     store.query,
	}, err
}

func (store *bqOfflineStore) UpdateMaterialization(id ResourceID, opts MaterializationOptions) (Materialization, error) {
	matID, err := NewMaterializationID(id)
	if err != nil {
		return nil, err
	}
	tableName, err := store.getMaterializationTableName(id)
	if err != nil {
		return nil, err
	}
	getMatQry := store.query.materializationExists(tableName)
	resTable, err := store.getbqResourceTable(id)
	if err != nil {
		return nil, err
	}

	bqQ := store.client.Query(getMatQry)
	it, err := bqQ.Read(store.query.getContext())
	if err != nil {
		return nil, fferr.NewResourceExecutionError(store.Type().String(), id.Name, id.Variant, fferr.ResourceType(id.Type.String()), err)
	}
	var row []bigquery.Value
	err = it.Next(&row)
	if err != nil {
		return nil, fferr.NewResourceExecutionError(store.Type().String(), id.Name, id.Variant, fferr.ResourceType(id.Type.String()), err)
	}
	if len(row) == 0 {
		return nil, fferr.NewResourceExecutionError(store.Type().String(), id.Name, id.Variant, fferr.ResourceType(id.Type.String()), err)
	}

	err = store.query.materializationUpdate(store.client, tableName, resTable.name)
	if err != nil {
		return nil, err
	}

	return &bqMaterialization{
		id:        matID,
		client:    store.client,
		tableName: tableName,
		query:     store.query,
	}, err
}

func (store *bqOfflineStore) DeleteMaterialization(id MaterializationID) error {
	name, variant, err := ps.MaterializationIDToResource(string(id))
	if err != nil {
		return err
	}
	tableName, err := store.getMaterializationTableName(ResourceID{Name: name, Variant: variant, Type: Feature})
	if err != nil {
		return err
	}
	if exists, err := store.materializationExists(id); err != nil {
		return err
	} else if !exists {
		return fferr.NewDatasetNotFoundError(string(id), "", nil)
	}
	query := store.query.materializationDrop(tableName)
	bqQ := store.client.Query(query)
	if _, err := bqQ.Read(store.query.getContext()); err != nil {
		wrapped := fferr.NewExecutionError(store.Type().String(), err)
		wrapped.AddDetail("table_name", tableName)
		wrapped.AddDetail("materialization_id", string(id))
		return wrapped
	}

	return nil
}

func (store *bqOfflineStore) materializationExists(id MaterializationID) (bool, error) {
	name, variant, err := ps.MaterializationIDToResource(string(id))
	if err != nil {
		return false, err
	}
	tableName, err := store.getMaterializationTableName(ResourceID{Name: name, Variant: variant, Type: Feature})
	if err != nil {
		return false, err
	}
	getMatQry := store.query.materializationExists(tableName)

	bqQ := store.client.Query(getMatQry)
	it, err := bqQ.Read(store.query.getContext())
	if err != nil {
		wrapped := fferr.NewExecutionError(store.Type().String(), err)
		wrapped.AddDetail("table_name", tableName)
		wrapped.AddDetail("materialization_id", string(id))
		return false, wrapped
	}

	var row []bigquery.Value
	if err := it.Next(&row); err != nil {
		return false, nil
	} else {
		return true, nil
	}
}

func (store *bqOfflineStore) CreateTrainingSet(def TrainingSetDef) error {
	if err := def.check(); err != nil {
		return err
	}
	label, err := store.getbqResourceTable(def.Label)
	if err != nil {
		return err
	}
	tableName, err := store.getTrainingSetName(def.ID)
	if err != nil {
		return err
	}

	err = store.query.trainingSetCreate(store, def, tableName, label.name)
	return err
}

func (store *bqOfflineStore) UpdateTrainingSet(def TrainingSetDef) error {
	if err := def.check(); err != nil {
		return err
	}
	label, err := store.getbqResourceTable(def.Label)
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

func (store *bqOfflineStore) GetTrainingSet(id ResourceID) (TrainingSetIterator, error) {
	fmt.Printf("Getting Training Set: %v\n", id)
	if err := id.check(TrainingSet); err != nil {
		return nil, err
	}
	fmt.Printf("Checking if Training Set exists: %v\n", id)
	if exists, err := store.tableExists(id); err != nil {
		return nil, err
	} else if !exists {
		return nil, fferr.NewDatasetNotFoundError(id.Name, id.Variant, nil)
	}
	trainingSetName, err := store.getTrainingSetName(id)
	if err != nil {
		return nil, err
	}
	columnNames, err := store.query.getColumns(store.client, trainingSetName)
	if err != nil {
		return nil, err
	}
	features := make([]string, 0)
	for _, name := range columnNames {
		features = append(features, name.Name)
	}
	columns := strings.Join(features[:], ", ")
	trainingSetQry := store.query.trainingRowSelect(columns, trainingSetName)

	fmt.Printf("Training Set Query: %s\n", trainingSetQry)
	bqQ := store.client.Query(trainingSetQry)
	iter, err := bqQ.Read(store.query.getContext())
	if err != nil {
		return nil, fferr.NewResourceExecutionError(store.Type().String(), id.Name, id.Variant, fferr.ResourceType(id.Type.String()), err)
	}

	return store.newbqTrainingSetIterator(iter), nil
}

func (store *bqOfflineStore) CreateTrainTestSplit(def TrainTestSplitDef) (func() error, error) {
	return nil, fmt.Errorf("not Implemented")
}

func (store *bqOfflineStore) GetTrainTestSplit(def TrainTestSplitDef) (TrainingSetIterator, TrainingSetIterator, error) {
	return nil, nil, fmt.Errorf("not Implemented")
}

func (store *bqOfflineStore) CheckHealth() (bool, error) {
	return false, fferr.NewInternalError(fmt.Errorf("provider health check not implemented"))
}

func (store *bqOfflineStore) ResourceLocation(id ResourceID, resource any) (pl.Location, error) {
	if exists, err := store.tableExists(id); err != nil {
		return nil, err
	} else if !exists {
		return nil, fferr.NewDatasetNotFoundError(id.Name, id.Variant, nil)
	}

	tableName, err := ps.ResourceToTableName(id.Type.String(), id.Name, id.Variant)
	if err != nil {
		return nil, err
	}

	return pl.NewSQLLocation(tableName), err
}

func (store bqOfflineStore) Delete(location pl.Location) error {
	return fferr.NewInternalErrorf("delete not implemented")
}

type bqTrainingRowsIterator struct {
	iter            *bigquery.RowIterator
	currentFeatures []interface{}
	currentLabel    interface{}
	err             error
	isHeaderRow     bool
	query           BQOfflineTableQueries
}

func (store *bqOfflineStore) newbqTrainingSetIterator(iter *bigquery.RowIterator) TrainingSetIterator {
	return &bqTrainingRowsIterator{
		iter:            iter,
		currentFeatures: nil,
		currentLabel:    nil,
		err:             nil,
		isHeaderRow:     true,
		query:           store.query,
	}
}

func (it *bqTrainingRowsIterator) Next() bool {
	var rowValues []bigquery.Value
	err := it.iter.Next(&rowValues)
	if err == iterator.Done {
		it.err = nil
		return false
	} else if err != nil {
		it.err = fferr.NewExecutionError(p_type.BigQueryOffline.String(), err)
		return false
	}

	var label interface{}
	numFeatures := len(it.iter.Schema) - 1
	featureVals := make([]interface{}, numFeatures)
	for i, value := range rowValues {
		if value == nil {
			continue
		}
		colType := it.iter.Schema[i].Type
		if i < numFeatures {
			featureVals[i] = it.query.castTableItemType(value, colType)
		} else {
			label = it.query.castTableItemType(value, colType)
		}
	}
	it.currentFeatures = featureVals
	it.currentLabel = label

	return true
}

func (it *bqTrainingRowsIterator) Err() error {
	return it.err
}

func (it *bqTrainingRowsIterator) Features() []interface{} {
	return it.currentFeatures
}

func (it *bqTrainingRowsIterator) Label() interface{} {
	return it.currentLabel
}

func (store *bqOfflineStore) AsOfflineStore() (OfflineStore, error) {
	return store, nil
}

func (store *bqOfflineStore) Close() error {
	if err := store.client.Close(); err != nil {
		return fferr.NewConnectionError(store.Type().String(), err)
	}
	return nil
}

func (store *bqOfflineStore) tableExists(id ResourceID) (bool, error) {
	var n []bigquery.Value
	tableName, err := store.getTableName(id)
	if err != nil {
		return false, err
	}

	query := store.query.tableExists(tableName)
	bqQ := store.client.Query(query)

	iter, err := bqQ.Read(store.query.getContext())
	if err != nil {
		return false, fferr.NewResourceExecutionError(p_type.BigQueryOffline.String(), id.Name, id.Variant, fferr.ResourceType(id.Type.String()), err)
	}

	err = iter.Next(&n)
	if n != nil && n[0].(int64) > 0 && err == nil {
		return true, nil
	} else if err != nil {
		return false, err
	}

	query = store.query.viewExists(tableName)
	bqQ = store.client.Query(query)

	iter, err = bqQ.Read(store.query.getContext())
	if err != nil {
		return false, fferr.NewResourceExecutionError(p_type.BigQueryOffline.String(), id.Name, id.Variant, fferr.ResourceType(id.Type.String()), err)
	}

	err = iter.Next(&n)
	if n != nil && n[0].(int64) > 0 && err == nil {
		return true, nil
	} else if err != nil {
		return false, fferr.NewResourceExecutionError(p_type.BigQueryOffline.String(), id.Name, id.Variant, fferr.ResourceType(id.Type.String()), err)
	}
	return false, nil
}

func (store *bqOfflineStore) newBigQueryPrimaryTable(client *bigquery.Client, name string, schema TableSchema) (*bqPrimaryTable, error) {
	query, err := store.createBigQueryPrimaryTableQuery(name, schema)
	if err != nil {
		return nil, err
	}

	qry := client.Query(query)
	_, err = qry.Read(store.query.getContext())
	if err != nil {
		return nil, fferr.NewExecutionError(p_type.BigQueryOffline.String(), err)
	}
	return &bqPrimaryTable{
		client: client,
		name:   name,
		schema: schema,
		query:  store.query,
	}, nil
}

func (store *bqOfflineStore) createBigQueryPrimaryTableQuery(name string, schema TableSchema) (string, error) {
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

func (store *bqOfflineStore) getResourceTableName(id ResourceID) (string, error) {
	return ps.ResourceToTableName(id.Type.String(), id.Name, id.Variant)
}

func (store *bqOfflineStore) getTrainingSetName(id ResourceID) (string, error) {
	if err := id.check(TrainingSet); err != nil {
		return "", err
	}
	return ps.ResourceToTableName(id.Type.String(), id.Name, id.Variant)
}
