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
	"errors"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/featureform/fferr"
	"github.com/featureform/logging"
	"github.com/featureform/metadata"
	pl "github.com/featureform/provider/location"
	pc "github.com/featureform/provider/provider_config"
	ps "github.com/featureform/provider/provider_schema"
	p_type "github.com/featureform/provider/provider_type"
	pt "github.com/featureform/provider/provider_type"
	tsq "github.com/featureform/provider/tsquery"
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

type defaultBQQueries struct {
	ProjectId string
	DatasetId string
	Ctx       context.Context
	logger    logging.Logger
}

type bqGenericTableIterator struct {
	iter         *bigquery.RowIterator
	currentValue GenericRecord
	err          error
	query        defaultBQQueries
	columns      []TableColumn
}

type bqPrimaryTable struct {
	table  *bigquery.Table
	client *bigquery.Client
	name   string
	query  defaultBQQueries
	schema TableSchema
	logger logging.Logger
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

	columns, err := pt.query.getColumns(pt.client, pt.name)
	if err != nil {
		pt.logger.Errorw("Error getting columns", "error", err)
		wrapped := fferr.NewExecutionError(p_type.BigQueryOffline.String(), err)
		wrapped.AddDetail("table_name", tableName)
		return nil, wrapped
	}

	it, err := bqQ.Read(pt.query.getContext())
	if err != nil {
		pt.logger.Errorw("Error executing query", "error", err)
		wrapped := fferr.NewExecutionError(p_type.BigQueryOffline.String(), err)
		wrapped.AddDetail("table_name", tableName)
		return nil, wrapped
	}
	return newBigQueryGenericTableIterator(it, pt.query, columns), nil
}

func (pt *bqPrimaryTable) NumRows() (int64, error) {
	var n []bigquery.Value
	tableName := pt.query.getTableName(pt.name)
	query := fmt.Sprintf("SELECT COUNT(*) FROM `%s`", tableName)

	bqQ := pt.client.Query(query)

	it, err := bqQ.Read(pt.query.getContext())
	if err != nil {
		pt.logger.Errorw("Error executing query", "error", err)
		wrapped := fferr.NewExecutionError(p_type.BigQueryOffline.String(), err)
		wrapped.AddDetail("table_name", tableName)
		return 0, wrapped
	}

	err = it.Next(&n)
	if err != nil {
		pt.logger.Errorw("Error iterating over rows", "error", err)
		wrapped := fferr.NewExecutionError(p_type.BigQueryOffline.String(), err)
		wrapped.AddDetail("table_name", tableName)
		return 0, wrapped
	}

	return n[0].(int64), nil
}

func (pt *bqPrimaryTable) getTableName() string {
	return fmt.Sprintf("%s.%s", pt.table.DatasetID, pt.table.TableID)
}

func (pt *bqPrimaryTable) upsertQuery(columns string, placeholder string) string {
	return fmt.Sprintf("INSERT INTO `%s` ( %s ) VALUES ( %s )", pt.getTableName(), columns, placeholder)
}

func (pt *bqPrimaryTable) Write(rec GenericRecord) error {
	return pt.WriteBatch([]GenericRecord{rec})
}

// mapSaver is used solely for WriteBatch, which creates a bigquery.Inserter.
// This inserter takes in a struct that implements the ValueSaver interface,
// whose only method is the Save method below.
type mapSaver struct {
	record map[string]interface{}
}

func (ms *mapSaver) Save() (map[string]bigquery.Value, string, error) {
	record := make(map[string]bigquery.Value)

	for k, v := range ms.record {
		record[k] = v
	}

	return record, "", nil
}

func (pt *bqPrimaryTable) WriteBatch(recs []GenericRecord) error {
	var rows []*mapSaver

	for _, rec := range recs {
		var record = pt.getNonNullRecords(rec)
		rows = append(rows, &mapSaver{record})
	}

	return pt.table.Inserter().Put(context.TODO(), rows)
}

func (pt *bqPrimaryTable) getNonNullRecords(rec GenericRecord) map[string]interface{} {
	records := make(map[string]interface{})

	for i, r := range rec {
		if r == nil {
			continue
		}
		records[pt.schema.Columns[i].Name] = r
	}

	return records
}

func (it *bqGenericTableIterator) Next() bool {
	var rowValues []bigquery.Value
	err := it.iter.Next(&rowValues)
	if errors.Is(err, iterator.Done) {
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

	for _, col := range it.columns {
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

func newBigQueryGenericTableIterator(it *bigquery.RowIterator, query defaultBQQueries, columns []TableColumn) GenericTableIterator {
	return &bqGenericTableIterator{
		iter:         it,
		currentValue: nil,
		err:          nil,
		query:        query,
		columns:      columns,
	}
}

func (store *bqOfflineStore) newBigQueryPrimaryTable(name string) (*bqPrimaryTable, error) {
	logger := store.logger.With("table", name)

	table := store.client.Dataset(store.query.getDatasetId()).Table(name)

	columnNames, err := store.query.getColumns(store.client, name)
	if err != nil {
		store.logger.Errorw("Error getting column names", "error", err)
		return nil, err
	}

	logger.Debug("Successfully got client for primary table")

	return &bqPrimaryTable{
		client: store.client,
		table:  table,
		name:   name,
		schema: TableSchema{Columns: columnNames},
		query:  store.query,
		logger: logger,
	}, nil
}

func (q defaultBQQueries) registerResources(client *bigquery.Client, tableName string, schema ResourceSchema) error {
	logger := q.logger.With("table", tableName, "schema", schema)

	var sourceLocation, isSqlLocation = schema.SourceTable.(*pl.SQLLocation)
	if !isSqlLocation {
		logger.Errorw("Source table is not an SQL location", "location_type", fmt.Sprintf("%T", schema.SourceTable))
		return fferr.NewInvalidArgumentErrorf("source table is not an SQL location, actual %T", schema.SourceTable.Location())
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE VIEW `%s` AS SELECT ", q.getTableName(tableName)))

	for _, m := range schema.EntityMappings.Mappings {
		sb.WriteString(fmt.Sprintf("`%s` AS entity_%s, ", m.EntityColumn, m.Name))
	}
	sb.WriteString(fmt.Sprintf("`%s` AS value, ", schema.EntityMappings.ValueColumn))

	if schema.TS != "" {
		sb.WriteString(fmt.Sprintf("`%s` as ts ",
			schema.TS,
		))
	} else {
		sb.WriteString(fmt.Sprintf("PARSE_TIMESTAMP('%%Y-%%m-%%d %%H:%%M:%%S +0000 UTC', '%s') as ts ",
			time.UnixMilli(0).UTC(),
		))
	}

	sb.WriteString(fmt.Sprintf("FROM `%s`", q.getTableNameFromLocation(*sourceLocation)))

	logger.Infow("Running register resource query", "query", sb.String())
	bqQ := client.Query(sb.String())
	if _, err := bqQ.Read(q.getContext()); err != nil {
		logger.Errorw("Failed to register resource", "error", err)
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

func (q defaultBQQueries) determineColumnType(valueType types.ValueType) (bigquery.FieldType, error) {
	switch valueType {
	case types.Int, types.Int32, types.Int64:
		return bigquery.IntegerFieldType, nil
	case types.Float32, types.Float64:
		// The BigQuery client names the Float type differently internally than what BigQuery is itself expecting.
		return "FLOAT64", nil
	case types.String:
		return bigquery.StringFieldType, nil
	case types.Bool:
		return bigquery.BooleanFieldType, nil
	case types.Timestamp:
		return bigquery.TimestampFieldType, nil
	case types.NilType:
		return bigquery.StringFieldType, nil
	default:
		return "", fferr.NewDataTypeNotFoundErrorf(valueType, "cannot find column type for value type")
	}
}

func (q defaultBQQueries) primaryTableCreate(name string, columnString string) string {
	query := fmt.Sprintf("CREATE TABLE `%s` ( %s )", q.getTableName(name), columnString)
	return query
}

func (q defaultBQQueries) createValuePlaceholderString(columns []TableColumn) string {
	placeholders := make([]string, 0)
	for range columns {
		placeholders = append(placeholders, "?")
	}
	return strings.Join(placeholders, ", ")
}

func (q defaultBQQueries) newBQOfflineTableQuery(name string, columnType string) string {
	return fmt.Sprintf("CREATE TABLE `%s` (entity STRING, value %s, ts TIMESTAMP, insert_ts TIMESTAMP)", q.getTableName(name), columnType)
}

func (q defaultBQQueries) materializationCreate(tableName string, schema ResourceSchema, resourceLocation pl.SQLLocation) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE OR REPLACE VIEW `%s` AS ", tableName))

	// By default, we'll use and order by the provided timestamp.
	tsSelectStmt := fmt.Sprintf("`%s` AS ts", schema.TS)
	tsOrderByStmt := fmt.Sprintf("ORDER BY `%s` DESC", schema.TS)

	// If there's no timestamp, then we simply just hardcode it as 0 epoch time, and
	// ignore any order clause.
	if schema.TS == "" {
		tsSelectStmt = "TIMESTAMP_SECONDS(0) AS ts"
		tsOrderByStmt = ""
	}

	cteFormat := "WITH OrderedSource AS (SELECT `%s` AS entity, `%s` AS value, %s, ROW_NUMBER() OVER (PARTITION BY `%s` %s) AS rn FROM `%s`) "
	cteClause := fmt.Sprintf(cteFormat, schema.Entity, schema.Value, tsSelectStmt, schema.Entity, tsOrderByStmt, q.getTableNameFromLocation(resourceLocation))

	sb.WriteString(cteClause)
	sb.WriteString("SELECT entity, value, ts, ROW_NUMBER() OVER (ORDER BY (entity)) AS row_number FROM OrderedSource WHERE rn = 1")

	return sb.String()
}

func (q defaultBQQueries) materializationIterateSegment(tableName string, start int64, end int64) string {
	return fmt.Sprintf("SELECT entity, value, ts FROM ( SELECT * FROM `%s` WHERE row_number > %v AND row_number <= %v)", q.getTableName(tableName), start, end)
}

func (q defaultBQQueries) getNumRowsQuery(tableName string) string {
	return fmt.Sprintf("SELECT COUNT(*) FROM `%s`", q.getTableName(tableName))
}

func (q *defaultBQQueries) getTablePrefix() string {
	return fmt.Sprintf("%s.%s", q.ProjectId, q.DatasetId)
}

func (q *defaultBQQueries) setTablePrefix(project string, dataset string) {
	q.ProjectId = project
	q.DatasetId = dataset
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
	return fmt.Sprintf("SELECT DISTINCT(table_name) FROM `%s.INFORMATION_SCHEMA.TABLES` WHERE table_type='VIEW' AND table_name='%s'", q.getTablePrefix(), tableName)
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
		q.logger.Errorw("Failed to update materialization", "table", tableName, "err", err)
		wrapped := fferr.NewExecutionError(p_type.BigQueryOffline.String(), err)
		wrapped.AddDetail("table_name", tableName)
		wrapped.AddDetail("source_name", sourceName)
		return wrapped
	}

	err = q.monitorJob(job)
	return err
}

func (q defaultBQQueries) monitorJob(job *bigquery.Job) error {
	logger := q.logger.With("jobId", job.ID())
	for {
		time.Sleep(sleepTime)
		status, err := job.Status(q.getContext())
		if err != nil {
			logger.Errorw("Failed to get job status")
			wrapped := fferr.NewExecutionError(p_type.BigQueryOffline.String(), err)
			wrapped.AddDetail("job_id", job.ID())
			return wrapped
		} else if status.Err() != nil {
			logger.Errorw("Job failed", "error", status.Err())
			wrapped := fferr.NewExecutionError(p_type.BigQueryOffline.String(), status.Err())
			wrapped.AddDetail("job_id", job.ID())
			return wrapped
		}

		switch status.State {
		case bigquery.Done:
			logger.Infow("Job succeeded")
			return nil
		default:
			logger.Infow("Job is not in a finished state, sleeping and checking again", "sleepTime", sleepTime)
			continue
		}
	}
}

func (q defaultBQQueries) transformationCreate(location pl.SQLLocation, query string) string {
	qry := fmt.Sprintf("CREATE OR REPLACE VIEW `%s` AS %s", q.getTableNameFromLocation(location), query)
	return qry
}

func (q defaultBQQueries) getColumns(client *bigquery.Client, name string) ([]TableColumn, error) {
	qry := fmt.Sprintf("SELECT column_name FROM `%s.INFORMATION_SCHEMA.COLUMNS` WHERE table_name=\"%s\" ORDER BY ordinal_position", q.getTablePrefix(), name)

	bqQ := client.Query(qry)
	it, err := bqQ.Read(q.getContext())
	if err != nil {
		q.logger.Errorw("Failed to get columns", "table", name, "err", err)
		wrapped := fferr.NewExecutionError(p_type.BigQueryOffline.String(), err)
		wrapped.AddDetail("table_name", name)
		return nil, wrapped
	}

	columnNames := make([]TableColumn, 0)
	for {
		var column []bigquery.Value
		err := it.Next(&column)
		if errors.Is(err, iterator.Done) {
			break
		} else if err != nil {
			q.logger.Errorw("Failed to get columns", "table", name, "err", err)
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
		q.logger.Errorw("Failed to update transformation", "table", tableName, "err", err)
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
		q.logger.Errorw("Failed to update table", "table", tableName, "query", query, "err", err)
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
			q.logger.Errorw("Failed to get table name", "feature", feature, "err", err)
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

func (q defaultBQQueries) getTableName(tableName string) string {
	location := pl.FullyQualifiedObject{
		Database: q.ProjectId,
		Schema:   q.DatasetId,
		Table:    tableName,
	}

	return location.String()
}

func (q defaultBQQueries) getTableNameFromLocation(location pl.SQLLocation) string {
	rootLocation := q.getRootLocation()
	tableLocation := rootLocation.GetTableFromRoot(&location)

	return tableLocation.TableLocation().String()
}

func (q defaultBQQueries) getProjectId() string {
	return q.ProjectId
}

func (q defaultBQQueries) getDatasetId() string {
	return q.DatasetId
}

func (q defaultBQQueries) getRootLocation() *pl.SQLLocation {
	return pl.NewFullyQualifiedSQLLocation(q.ProjectId, q.DatasetId, "").(*pl.SQLLocation)
}

type bqMaterialization struct {
	id        MaterializationID
	client    *bigquery.Client
	tableName string
	query     defaultBQQueries
	logger    logging.Logger
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
		mat.logger.Errorw("Error reading rows")
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

	logger := mat.logger.With("matID", mat.id, "table", mat.tableName, "")
	return newbqFeatureIterator(logger, it, mat.query), nil
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
	query        defaultBQQueries
	logger       logging.Logger
}

func newbqFeatureIterator(logger logging.Logger, it *bigquery.RowIterator, query defaultBQQueries) FeatureIterator {
	return &bqFeatureIterator{
		iter:         it,
		err:          nil,
		currentValue: ResourceRecord{},
		query:        query,
		logger:       logger,
	}
}

func (it *bqFeatureIterator) Next() bool {
	var rowValue []bigquery.Value
	err := it.iter.Next(&rowValue)
	if errors.Is(err, iterator.Done) {
		it.err = nil
		return false
	} else if err != nil {
		it.logger.Errorw("Error iterating over table", "error", err)
		it.err = fferr.NewExecutionError(p_type.BigQueryOffline.String(), err)
		return false
	}

	var currValue ResourceRecord
	valueColType := it.iter.Schema[1].Type
	if err := currValue.SetEntity(rowValue[0]); err != nil {
		it.logger.Errorw("Error setting entity", "error", err)
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
	query  defaultBQQueries
	name   string
	logger logging.Logger
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
		table.logger.Errorw("Error reading table", "error", err)
		return fferr.NewResourceExecutionError(p_type.BigQueryOffline.String(), rec.Entity, "", fferr.ENTITY, err)
	}

	err = iter.Next(&n)
	if err != nil {
		table.logger.Errorw("Error reading table", "error", err)
		return fferr.NewResourceExecutionError(p_type.BigQueryOffline.String(), rec.Entity, "", fferr.ENTITY, err)
	}

	if n == nil {
		table.logger.Errorw("Cannot find table", "table", tb)
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
		table.logger.Errorw("Error reading table", "error", err)
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
	return pl.NewFullyQualifiedSQLLocation(table.query.DatasetId, "", table.name)
}

type bqOfflineStore struct {
	client *bigquery.Client
	config pc.BigQueryConfig
	query  defaultBQQueries
	logger logging.Logger
	BaseProvider
}

func NewBQOfflineStore(config pc.SerializedConfig, logger logging.Logger) (*bqOfflineStore, error) {
	sc := pc.BigQueryConfig{}
	if err := sc.Deserialize(config); err != nil {
		return nil, err
	}

	creds, err := json.Marshal(sc.Credentials)
	if err != nil {
		logger.Errorw("Error marshaling credentials", "error", err)
		return nil, fferr.NewProviderConfigError(string(pt.BigQueryOffline), err)
	}
	client, err := bigquery.NewClient(context.TODO(), sc.ProjectId, option.WithCredentialsJSON(creds))
	if err != nil {
		logger.Errorw("Error creating BigQuery client", "error", err)
		return nil, fferr.NewConnectionError(string(pt.BigQueryOffline), err)
	}
	defer client.Close()

	queries := defaultBQQueries{
		ProjectId: sc.ProjectId,
		DatasetId: sc.DatasetId,
		logger:    logger,
	}
	queries.setContext()

	return &bqOfflineStore{
		client: client,
		query:  queries,
		logger: logger,
		BaseProvider: BaseProvider{
			ProviderType:   pt.BigQueryOffline,
			ProviderConfig: config,
		},
	}, nil
}

func bigQueryOfflineStoreFactory(config pc.SerializedConfig) (Provider, error) {
	sc := pc.BigQueryConfig{}
	logger := logging.NewLogger("bigquery")
	if err := sc.Deserialize(config); err != nil {
		return nil, err
	}

	store, err := NewBQOfflineStore(config, logger)
	if err != nil {
		logger.Errorw("Error creating BigQuery store", "error", err)
		return nil, err
	}
	return store, nil
}

func (store *bqOfflineStore) RegisterResourceFromSourceTable(id ResourceID, schema ResourceSchema, opts ...ResourceOption) (OfflineTable, error) {
	logger := store.logger.With("id", id, "schema", schema, "opts", opts)

	logger.Debug("BigQuery store registering resource from source table")

	if len(opts) > 0 {
		errorMsg := "BigQuery does not support resource options"
		logger.Error(errorMsg)
		return nil, fferr.NewInternalErrorf(errorMsg)
	}
	if err := id.check(Feature, Label); err != nil {
		logger.Error("BigQuery only supports registering feature and label resources")
		return nil, err
	}
	if exists, err := store.tableExistsForResourceId(id); err != nil {
		logger.Error("Error checking if table exists for resource id", "error", err)
		return nil, err
	} else if exists {
		logger.Errorw("Resource already exists")
		return nil, fferr.NewDatasetAlreadyExistsError(id.Name, id.Variant, nil)
	}
	if err := schema.Validate(); err != nil {
		logger.Error("Error validating schema", "error", err)
		return nil, err
	}
	tableName, err := store.getResourceTableName(id)
	if err != nil {
		logger.Error("Error getting table name", "error", err)
		return nil, err
	}

	if err := store.query.registerResources(store.client, tableName, schema); err != nil {
		logger.Error("Error registering resources", "error", err)
		return nil, err
	}

	return store.newBqOfflineTable(tableName)
}

func (store *bqOfflineStore) RegisterPrimaryFromSourceTable(id ResourceID, tableLocation pl.Location) (PrimaryTable, error) {
	logger := store.logger.With("resourceId", id)

	logger.Debug("Registering primary from source table")

	sqlLocation, isSqlLocation := tableLocation.(*pl.SQLLocation)
	if !isSqlLocation {
		errorMsg := fmt.Sprintf("source table %s is not a SQLLocation, actual: %T", tableLocation, tableLocation)
		logger.Error(errorMsg)
		return nil, fferr.NewInvalidArgumentErrorf(errorMsg)
	}

	if err := id.check(Primary); err != nil {
		logger.Errorw("Resource type is not primary", "err", err)
		return nil, err
	}

	return store.newBigQueryPrimaryTable(sqlLocation.Location())
}

func (store *bqOfflineStore) SupportsTransformationOption(opt TransformationOptionType) (bool, error) {
	return false, nil
}

func (store *bqOfflineStore) CreateTransformation(config TransformationConfig, opts ...TransformationOption) error {
	logger := store.logger.With("config", config)

	logger.Debug("Creating transformation")
	if len(opts) > 0 {
		return fferr.NewInternalErrorf("BigQuery does not support transformation options")
	}
	name, err := store.getTableName(config.TargetTableID)
	if err != nil {
		logger.Errorw("Error getting table name", "error", err)
		return err
	}

	// TODO: We do just create it, but maybe still consider doing an error check here.
	location := pl.NewSQLLocation(name).(*pl.SQLLocation)
	query := store.query.transformationCreate(*location, config.Query)

	bqQ := store.client.Query(query)
	job, err := bqQ.Run(store.query.getContext())
	if err != nil {
		logger.Errorw("Error creating transformation", "error", err)
		return fferr.NewResourceExecutionError(store.Type().String(), config.TargetTableID.Name, config.TargetTableID.Variant, fferr.ResourceType(config.TargetTableID.Type.String()), err)
	}

	return store.query.monitorJob(job)
}

func (store *bqOfflineStore) getTableName(id ResourceID) (string, error) {
	return ps.ResourceToTableName(id.Type.String(), id.Name, id.Variant)
}

func (store *bqOfflineStore) GetTransformationTable(id ResourceID) (TransformationTable, error) {
	logger := store.logger.With("resourceId", id)

	logger.Debug("Getting transformation table")
	name, err := store.getTableName(id)
	if err != nil {
		store.logger.Errorw("Error getting table name", "error", err)
		return nil, err
	}

	existsQuery := store.query.tableExists(name)
	bqQ := store.client.Query(existsQuery)
	it, err := bqQ.Read(store.query.getContext())
	if err != nil {
		store.logger.Errorw("Error getting table name", "error", err)
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

	return store.newBigQueryPrimaryTable(name)
}

func (store *bqOfflineStore) UpdateTransformation(config TransformationConfig, opts ...TransformationOption) error {
	logger := store.logger.With("config", config)

	logger.Debug("Updating transformation")

	if len(opts) > 0 {
		logger.Errorw("BigQuery does not support transformation options", "options", opts)
		return fferr.NewInternalErrorf("BigQuery does not support transformation options")
	}
	name, err := store.getTableName(config.TargetTableID)
	if err != nil {
		logger.Errorw("Error getting table name", "error", err)
		return err
	}
	err = store.query.transformationUpdate(store.client, name, config.Query)
	if err != nil {
		logger.Errorw("Error updating transformation", "error", err)
		return err
	}

	logger.Info("Successfully updated transformation")

	return nil
}

func (store *bqOfflineStore) CreatePrimaryTable(id ResourceID, schema TableSchema) (PrimaryTable, error) {
	logger := store.logger.With("resourceId", id)

	logger.Debug("Creating primary table")

	if err := id.check(Primary); err != nil {
		logger.Errorw("Resource type is not primary", "err", err)
		return nil, err
	}
	if exists, err := store.tableExistsForResourceId(id); err != nil {
		logger.Errorw("Error checking if table exists", "error", err)
		return nil, err
	} else if exists {
		logger.Errorw("Table already exists", "id", id)
		return nil, fferr.NewDatasetAlreadyExistsError(id.Name, id.Variant, nil)
	}
	if len(schema.Columns) == 0 {
		logger.Errorw("Missing schema columns", "id", id)
		return nil, fferr.NewDatasetNotFoundError(id.Name, id.Variant, fmt.Errorf("cannot create primary table without columns"))
	}
	tableName, err := GetPrimaryTableName(id)
	if err != nil {
		logger.Errorw("Error getting table name", "error", err)
		return nil, err
	}
	table, err := store.createNewBigQueryPrimaryTable(store.client, tableName, schema)
	if err != nil {
		logger.Errorw("Error creating primary table", "error", err)
		return nil, fferr.NewResourceExecutionError(store.Type().String(), id.Name, id.Variant, fferr.ResourceType(id.Type.String()), err)
	}

	logger.Info("Successfully created primary table")
	return table, nil
}

func (store *bqOfflineStore) GetPrimaryTable(id ResourceID, source metadata.SourceVariant) (PrimaryTable, error) {
	logger := store.logger.With("resourceId", id)

	logger.Debug("Getting primary table")

	location, err := source.GetPrimaryLocation()
	if err != nil {
		logger.Errorw("Error getting primary location", "error", err)
		return nil, err
	}

	sqlLocation, isSqlLocation := location.(*pl.SQLLocation)
	if !isSqlLocation {
		logger.Errorw("Source is not a SQL location", "error", err)
		return nil, fferr.NewInvalidArgumentErrorf("source table is not an SQL location")
	}

	if exists, err := store.tableExists(sqlLocation); err != nil {
		logger.Errorw("Error checking if table exists", "error", err)
		return nil, err
	} else if !exists {
		logger.Errorw("Table does not exist", "id", id)
		return nil, fferr.NewDatasetNotFoundError(id.Name, id.Variant, nil)
	}

	name := sqlLocation.Location()

	return store.newBigQueryPrimaryTable(name)
}

func (store *bqOfflineStore) CreateResourceTable(id ResourceID, schema TableSchema) (OfflineTable, error) {
	logger := store.logger.With("resourceId", id)

	logger.Debug("Creating resource table")

	if err := id.check(Feature, Label); err != nil {
		logger.Errorw("Resource type is not feature or label", "error", err)
		return nil, err
	}

	if exists, err := store.tableExistsForResourceId(id); err != nil {
		logger.Errorw("Error checking if table exists", "error", err)
		return nil, err
	} else if exists {
		logger.Errorw("Table already exists")
		return nil, fferr.NewDatasetAlreadyExistsError(id.Name, id.Variant, nil)
	}
	tableName, err := store.getResourceTableName(id)
	if err != nil {
		logger.Errorw("Error getting table name", "error", err)
		return nil, err
	}
	var valueType types.ValueType
	if valueIndex := store.getValueIndex(schema.Columns); valueIndex > 0 {
		valueType = schema.Columns[valueIndex].ValueType
	} else {
		valueType = types.NilType
	}
	table, err := store.createBqOfflineTable(store.client, tableName, valueType)
	if err != nil {
		logger.Errorw("Error creating table", "error", err)
		return nil, err
	}

	logger.Info("Successfully created resource table")

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

func (store *bqOfflineStore) createBqOfflineTable(client *bigquery.Client, name string, valueType types.ValueType) (*bqOfflineTable, error) {
	columnType, err := store.query.determineColumnType(valueType)
	if err != nil {
		return nil, err
	}
	tableCreateQry := store.query.newBQOfflineTableQuery(name, string(columnType))
	bqQ := client.Query(tableCreateQry)
	_, err = bqQ.Read(store.query.getContext())
	if err != nil {
		wrapped := fferr.NewExecutionError(store.Type().String(), err)
		wrapped.AddDetail("table_name", name)
		return nil, wrapped
	}

	return store.newBqOfflineTable(name)
}

func (store *bqOfflineStore) GetResourceTable(id ResourceID) (OfflineTable, error) {
	return store.getbqResourceTable(id)
}

func (store *bqOfflineStore) GetBatchFeatures(tables []ResourceID) (BatchFeatureIterator, error) {
	return nil, fferr.NewInternalError(fmt.Errorf("batch features not implemented for this provider"))
}

func (store *bqOfflineStore) newMaterialization(id MaterializationID, tableName string) (*bqMaterialization, error) {
	logger := store.logger.With("id", id)

	logger.Debug("Successfully created client for materialization")

	return &bqMaterialization{
		id:        id,
		client:    store.client,
		tableName: tableName,
		query:     store.query,
		logger:    logger,
	}, nil
}

func (store *bqOfflineStore) CreateMaterialization(id ResourceID, opts MaterializationOptions) (Materialization, error) {
	logger := store.logger.With("resourceId", id, "opts", opts)

	logger.Debug("Creating materialization")

	if id.Type != Feature {
		logger.Errorw("Materialization source must be a feature", "type", id.Type)
		return nil, fferr.NewInvalidArgumentError(fmt.Errorf("received %s; only features can be materialized", id.Type.String()))
	}

	sqlLocation, isSqlLocation := opts.Schema.SourceTable.(*pl.SQLLocation)
	if !isSqlLocation {
		return nil, fferr.NewInvalidArgumentErrorf("source table is not an SQL location")
	}

	matID := MaterializationID(fmt.Sprintf("%s__%s", id.Name, id.Variant))
	matTableName, err := store.getMaterializationTableName(id)
	if err != nil {
		logger.Errorw("Error getting table name", "error", err)
		return nil, err
	}
	// TODO: Somehow combine this logic with all of the other interface methods that get a
	// relative location.
	// BigQuery requires the table name to be prefixed with a dataset when creating a new table.
	matTableName = fmt.Sprintf("%s.%s.%s", store.query.ProjectId, store.query.DatasetId, matTableName)
	materializeQry := store.query.materializationCreate(matTableName, opts.Schema, *sqlLocation)

	bqQ := store.client.Query(materializeQry)
	_, err = bqQ.Read(store.query.getContext())
	if err != nil {
		logger.Errorw("Error creating materialization", "error", err)
		return nil, fferr.NewResourceExecutionError(store.Type().String(), id.Name, id.Variant, fferr.ResourceType(id.Type.String()), err)
	}

	return store.newMaterialization(matID, matTableName)
}

func (store *bqOfflineStore) SupportsMaterializationOption(opt MaterializationOptionType) (bool, error) {
	return false, nil
}

func (store *bqOfflineStore) newBqOfflineTable(tableName string) (*bqOfflineTable, error) {
	logger := store.logger.With("table", tableName)

	logger.Debug("Successfully created client for BQ offline table")

	return &bqOfflineTable{
		client: store.client,
		name:   tableName,
		query:  store.query,
		logger: logger,
	}, nil
}

func (store *bqOfflineStore) getbqResourceTable(id ResourceID) (*bqOfflineTable, error) {
	logger := store.logger.With("resourceId", id)

	logger.Debug("Getting resource table")

	if exists, err := store.tableExistsForResourceId(id); err != nil {
		logger.Errorw("Error checking if table exists", "error", err)
		return nil, err
	} else if !exists {
		logger.Errorw("Table does not exist")
		return nil, fferr.NewDatasetNotFoundError(id.Name, id.Variant, nil)
	}

	table, err := store.getResourceTableName(id)
	if err != nil {
		logger.Errorw("Error getting table name", "error", err)
		return nil, err
	}

	return store.newBqOfflineTable(table)
}

func (store *bqOfflineStore) getMaterializationTableName(id ResourceID) (string, error) {
	if err := id.check(Feature); err != nil {
		store.logger.Errorw("Error checking if table exists", "id", id, "error", err)
		return "", err
	}
	// NOTE: Given BiqQuery uses intermediate resource tables, the inbound resource ID will be Feature;
	// however, the table must be named according to the FeatureMaterialization offline type.
	return ps.ResourceToTableName(FeatureMaterialization.String(), id.Name, id.Variant)
}

func (store *bqOfflineStore) GetMaterialization(id MaterializationID) (Materialization, error) {
	logger := store.logger.With("resourceId", id)

	logger.Debug("Getting materialization")

	name, variant, err := ps.MaterializationIDToResource(string(id))
	if err != nil {
		logger.Errorw("Error getting materialization", "error", err)
		return nil, err
	}
	tableName, err := store.getMaterializationTableName(ResourceID{Name: name, Variant: variant, Type: Feature})
	if err != nil {
		logger.Errorw("Error getting table name", "error", err)
		return nil, err
	}
	getMatQry := store.query.materializationExists(tableName)

	bqQry := store.client.Query(getMatQry)
	it, err := bqQry.Read(store.query.getContext())
	if err != nil {
		logger.Errorw("Error getting materialization", "error", err)
		wrapped := fferr.NewExecutionError(store.Type().String(), err)
		wrapped.AddDetail("table_name", tableName)
		wrapped.AddDetail("materialization_id", string(id))
		return nil, wrapped
	}

	var row []bigquery.Value
	err = it.Next(&row)
	if err != nil {
		logger.Errorw("Error iterating over table", "table", tableName, "error", err)
		wrapped := fferr.NewExecutionError(store.Type().String(), err)
		wrapped.AddDetail("table_name", tableName)
		wrapped.AddDetail("materialization_id", string(id))
		return nil, wrapped
	}

	if len(row) == 0 {
		return nil, fferr.NewDatasetNotFoundError(string(id), "", nil)
	}

	return store.newMaterialization(id, tableName)
}

func (store *bqOfflineStore) UpdateMaterialization(id ResourceID, opts MaterializationOptions) (Materialization, error) {
	logger := store.logger.With("resourceId", id)

	logger.Debug("Updating materialization")

	matID, err := NewMaterializationID(id)
	if err != nil {
		logger.Errorw("Error creating materialization", "error", err)
		return nil, err
	}
	tableName, err := store.getMaterializationTableName(id)
	if err != nil {
		logger.Errorw("Error getting table name", "error", err)
		return nil, err
	}
	getMatQry := store.query.materializationExists(tableName)
	resTable, err := store.getbqResourceTable(id)
	if err != nil {
		logger.Errorw("Error getting table name", "error", err)
		return nil, err
	}

	bqQ := store.client.Query(getMatQry)
	it, err := bqQ.Read(store.query.getContext())
	if err != nil {
		logger.Errorw("Error running materialization query", "error", err)
		return nil, fferr.NewResourceExecutionError(store.Type().String(), id.Name, id.Variant, fferr.ResourceType(id.Type.String()), err)
	}
	var row []bigquery.Value
	err = it.Next(&row)
	if err != nil {
		logger.Errorw("Error iterating over table", "table", tableName, "error", err)
		return nil, fferr.NewResourceExecutionError(store.Type().String(), id.Name, id.Variant, fferr.ResourceType(id.Type.String()), err)
	}
	if len(row) == 0 {
		logger.Errorw("Row has no columns", "table", tableName)
		return nil, fferr.NewResourceExecutionError(store.Type().String(), id.Name, id.Variant, fferr.ResourceType(id.Type.String()), err)
	}

	err = store.query.materializationUpdate(store.client, tableName, resTable.name)
	if err != nil {
		logger.Errorw("Error updating materialization", "error", err)
		return nil, err
	}

	return store.newMaterialization(matID, tableName)
}

func (store *bqOfflineStore) DeleteMaterialization(id MaterializationID) error {
	logger := store.logger.With("resourceId", id)

	logger.Debug("Deleting materialization")

	name, variant, err := ps.MaterializationIDToResource(string(id))
	if err != nil {
		logger.Errorw("Error getting materialization", "error", err)
		return err
	}
	tableName, err := store.getMaterializationTableName(ResourceID{Name: name, Variant: variant, Type: Feature})
	if err != nil {
		logger.Errorw("Error getting table name", "error", err)
		return err
	}
	if exists, err := store.materializationExists(id); err != nil {
		logger.Errorw("Error checking if materialization exists", "error", err)
		return err
	} else if !exists {
		logger.Errorw("Materialization does not exist")
		return fferr.NewDatasetNotFoundError(string(id), "", nil)
	}
	query := store.query.materializationDrop(tableName)
	bqQ := store.client.Query(query)
	if _, err := bqQ.Read(store.query.getContext()); err != nil {
		logger.Errorw("Error deleting materialization", "error", err)
		wrapped := fferr.NewExecutionError(store.Type().String(), err)
		wrapped.AddDetail("table_name", tableName)
		wrapped.AddDetail("materialization_id", string(id))
		return wrapped
	}

	logger.Info("Successfully deleted materialization")

	return nil
}

func (store *bqOfflineStore) materializationExists(id MaterializationID) (bool, error) {
	logger := store.logger.With("resourceId", id)

	logger.Debug("Checking if materialization exists")

	name, variant, err := ps.MaterializationIDToResource(string(id))
	if err != nil {
		logger.Errorw("Error mapping materialization to resource", "error", err)
		return false, err
	}
	tableName, err := store.getMaterializationTableName(ResourceID{Name: name, Variant: variant, Type: Feature})
	if err != nil {
		logger.Errorw("Error getting table name", "error", err)
		return false, err
	}
	getMatQry := store.query.materializationExists(tableName)

	bqQ := store.client.Query(getMatQry)
	it, err := bqQ.Read(store.query.getContext())
	if err != nil {
		logger.Errorw("Error checking if materialization exists", "error", err)
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

func (bq *bqOfflineStore) buildTrainingSetQuery(tableName string, def TrainingSetDef) (string, error) {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE OR REPLACE TABLE `%s.%s` AS ", bq.query.DatasetId, tableName))

	params, err := bq.adaptTsDefToBuilderParams(def)
	if err != nil {
		return "", err
	}

	queryConfig := tsq.QueryConfig{
		UseAsOfJoin: false,
		QuoteChar:   "`",
		QuoteTable:  true,
	}
	ts := tsq.NewTrainingSet(queryConfig, params)
	sql, err := ts.CompileSQL()
	if err != nil {
		return "", err
	}
	sb.WriteString(sql)
	return sb.String(), nil
}

func (bq *bqOfflineStore) CreateTrainingSet(def TrainingSetDef) error {
	logger := bq.logger.With("trainingSetDef", def)

	logger.Debug("Creating training set")

	if err := def.check(); err != nil {
		logger.Errorw("Error validating training set", "error", err)
		return err
	}
	tableName, err := bq.getTrainingSetName(def.ID)
	if err != nil {
		logger.Errorw("Error getting table name", "error", err)
		return err
	}
	query, err := bq.buildTrainingSetQuery(tableName, def)
	if err != nil {
		logger.Errorw("Error building training set query", "error", err)
		return err
	}
	qry := bq.client.Query(query)
	_, err = qry.Read(bq.query.getContext())
	if err != nil {
		logger.Errorw("Error running training set query", "error", err)
		return fferr.NewExecutionError(p_type.BigQueryOffline.String(), err)
	}

	logger.Info("Successfully created training set")

	return nil
}

func (store *bqOfflineStore) UpdateTrainingSet(def TrainingSetDef) error {
	logger := store.logger.With("trainingSetDef", def)

	logger.Debug("Updating training set")

	if err := def.check(); err != nil {
		logger.Errorw("Error validating training set", "error", err)
		return err
	}
	label, err := store.getbqResourceTable(def.Label)
	if err != nil {
		logger.Errorw("Error getting table", "error", err)
		return err
	}
	tableName, err := store.getTrainingSetName(def.ID)
	if err != nil {
		logger.Errorw("Error getting table name", "error", err)
		return err
	}
	if err := store.query.trainingSetUpdate(store, def, tableName, label.name); err != nil {
		logger.Errorw("Error updating training set", "error", err)
		return err
	}

	logger.Info("Successfully updated training set")

	return nil
}

func (store *bqOfflineStore) GetTrainingSet(id ResourceID) (TrainingSetIterator, error) {
	logger := store.logger.With("resourceId", id)

	logger.Debug("Getting training set")

	if err := id.check(TrainingSet); err != nil {
		logger.Errorw("Resource must be training set", "error", err)
		return nil, err
	}
	if exists, err := store.tableExistsForResourceId(id); err != nil {
		logger.Errorw("Error checking if training set exists", "error", err)
		return nil, err
	} else if !exists {
		logger.Errorw("Training set does not exist")
		return nil, fferr.NewDatasetNotFoundError(id.Name, id.Variant, nil)
	}
	trainingSetName, err := store.getTrainingSetName(id)
	if err != nil {
		logger.Errorw("Error getting training set name", "error", err)
		return nil, err
	}
	columnNames, err := store.query.getColumns(store.client, trainingSetName)
	if err != nil {
		logger.Errorw("Error getting column names", "error", err)
		return nil, err
	}
	features := make([]string, 0)
	for _, name := range columnNames {
		features = append(features, name.Name)
	}
	columns := strings.Join(features[:], ", ")
	trainingSetQry := store.query.trainingRowSelect(columns, trainingSetName)

	bqQ := store.client.Query(trainingSetQry)
	iter, err := bqQ.Read(store.query.getContext())
	if err != nil {
		logger.Errorw("Error getting training set rows", "error", err)
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
	logger := store.logger.With("resourceId", id)

	logger.Debug("Getting resource location")

	if exists, err := store.tableExistsForResourceId(id); err != nil {
		logger.Errorw("Error checking if resource exists", "error", err)
		return nil, err
	} else if !exists {
		logger.Error("Resource does not exist")
		return nil, fferr.NewDatasetNotFoundError(id.Name, id.Variant, nil)
	}

	tableName, err := ps.ResourceToTableName(id.Type.String(), id.Name, id.Variant)
	if err != nil {
		logger.Errorw("Error getting table name", "error", err)
		return nil, err
	}

	logger.Debug("Successfully got resource location")

	return pl.NewSQLLocation(tableName), nil
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
	query           defaultBQQueries
}

func (store *bqOfflineStore) newbqTrainingSetIterator(iter *bigquery.RowIterator) TrainingSetIterator {
	store.logger.Debug("Successfully created bq training set iterator client")

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
	if errors.Is(err, iterator.Done) {
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

func (store *bqOfflineStore) tableExistsForResourceId(id ResourceID) (bool, error) {
	tableName, err := store.getTableName(id)
	if err != nil {
		return false, err
	}

	res, err := store.tableExists(pl.NewSQLLocation(tableName))
	if err != nil {
		return false, fferr.NewResourceExecutionError(p_type.BigQueryOffline.String(), id.Name, id.Variant, fferr.ResourceType(id.Type.String()), err)
	}

	return res, nil
}

func (store *bqOfflineStore) tableExists(location pl.Location) (bool, error) {
	logger := store.logger.With("location", location)

	logger.Debug("Checking if table exists")

	sqlLocation, isSqlLocation := location.(*pl.SQLLocation)
	if !isSqlLocation {
		logger.Errorw("Location is not an SQLLocation", "locationType", sqlLocation.Type)
		return false, fferr.NewInvalidArgumentErrorf("location is not a SQLLocation")
	}

	var n []bigquery.Value
	query := store.query.tableExists(sqlLocation.Location())
	bqQ := store.client.Query(query)

	iter, err := bqQ.Read(store.query.getContext())
	if err != nil {
		logger.Errorw("Error querying table", "error", err)
		return false, fferr.NewExecutionError(p_type.BigQueryOffline.String(), err)
	}

	err = iter.Next(&n)
	if n != nil && n[0].(int64) > 0 && err == nil {
		return true, nil
	} else if err != nil {
		logger.Errorw("Error querying table", "error", err)
		return false, err
	}

	query = store.query.viewExists(sqlLocation.Location())
	bqQ = store.client.Query(query)

	iter, err = bqQ.Read(store.query.getContext())
	if err != nil {
		logger.Errorw("Error querying view", "error", err)
		return false, fferr.NewExecutionError(p_type.BigQueryOffline.String(), err)
	}

	err = iter.Next(&n)
	if n != nil && n[0].(int64) > 0 && err == nil {
		return true, nil
	} else if err != nil {
		logger.Errorw("Error querying view", "error", err)
		return false, fferr.NewExecutionError(p_type.BigQueryOffline.String(), err)
	}
	return false, nil
}

func (store *bqOfflineStore) createNewBigQueryPrimaryTable(client *bigquery.Client, name string, schema TableSchema) (*bqPrimaryTable, error) {
	logger := store.logger.With("name", name, "schema", schema)

	logger.Debug("Creating new bigquery primary table")

	query, err := store.createBigQueryPrimaryTableQuery(name, schema)
	if err != nil {
		logger.Errorw("Error creating query for new bigquery primary table", "error", err)
		return nil, err
	}

	qry := client.Query(query)
	_, err = qry.Read(store.query.getContext())
	if err != nil {
		logger.Errorw("Error creating new bigquery primary table", "error", err)
		return nil, fferr.NewExecutionError(p_type.BigQueryOffline.String(), err)
	}

	return store.newBigQueryPrimaryTable(name)
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

func (bq *bqOfflineStore) adaptTsDefToBuilderParams(def TrainingSetDef) (tsq.BuilderParams, error) {
	sanitizeTableNameFn := func(loc pl.Location) (string, error) {
		lblLoc, isSQLLocation := loc.(*pl.SQLLocation)
		if !isSQLLocation {
			return "", fferr.NewInternalErrorf("label location is not an SQL location")
		}
		return bq.query.getTableNameFromLocation(*lblLoc), nil
	}

	return def.ToBuilderParams(bq.logger, sanitizeTableNameFn)
}
