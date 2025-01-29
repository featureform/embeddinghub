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
	pl "github.com/featureform/provider/location"
	ps "github.com/featureform/provider/provider_schema"
	"github.com/google/uuid"
	"google.golang.org/api/googleapi"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/featureform/provider/location"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/joho/godotenv"
	"google.golang.org/api/option"
)

type bigQueryOfflineStoreTester struct {
	*bqOfflineStore
}

func (bq *bigQueryOfflineStoreTester) CreateDatabase(name string) error {
	metadata := &bigquery.DatasetMetadata{
		Name: name,
	}
	if err := bq.client.Dataset(name).Create(context.TODO(), metadata); err != nil {
		// If the dataset already exists, we just ignore the error (there are a few
		// tests that init data in the same dataset).
		var apiErr *googleapi.Error
		if !(errors.As(err, &apiErr) && apiErr.Code == 409) {
			return err
		}
	}

	return nil
}

func (bq *bigQueryOfflineStoreTester) DropDatabase(name string) error {
	return bq.client.Dataset(name).DeleteWithContents(context.TODO())
}

func (bq *bigQueryOfflineStoreTester) CreateSchema(database, schema string) error {
	// Intentional no-op. There's really nothing analogous to a schema in
	// BigQuery, so we ignore it.
	return nil
}

func (bq *bigQueryOfflineStoreTester) CreateTable(loc pl.Location, schema TableSchema) (PrimaryTable, error) {
	sqlLocation, ok := loc.(*location.SQLLocation)
	if !ok {
		return nil, fmt.Errorf("invalid location type")
	}

	var tableSchema bigquery.Schema

	for _, col := range schema.Columns {
		columnType, err := bq.query.determineColumnType(col.ValueType)
		if err != nil {
			return nil, err
		}
		tableSchema = append(tableSchema, &bigquery.FieldSchema{
			Name:     col.Name,
			Type:     columnType,
			Required: false,
		})
	}

	var datasetName = sqlLocation.GetDatabase()
	// There are a few tests that don't particularly care to create (or set)
	// db's to use. In this case, just default to the testing dataset.
	if datasetName == "" {
		datasetName = bq.query.DatasetId
	}

	var tableName = sqlLocation.GetTable()

	dataset := bq.client.Dataset(datasetName)

	tableMetadata := &bigquery.TableMetadata{
		Name:   tableName,
		Schema: tableSchema,
	}

	table := dataset.Table(tableName)
	if err = table.Create(context.TODO(), tableMetadata); err != nil {
		return nil, err
	}

	var newQueryClient = defaultBQQueries{
		ProjectId: bq.query.ProjectId,
		DatasetId: datasetName,
		Ctx:       bq.query.Ctx,
	}

	return &bqPrimaryTable{
		client: bq.client,
		table:  table,
		name:   tableName,
		query:  newQueryClient,
		schema: schema,
	}, nil
}

func getBigQueryConfig(t *testing.T) (pc.BigQueryConfig, error) {
	err := godotenv.Load("../.env")
	if err != nil {
		t.Logf("could not open .env file... Checking environment: %s", err)
	}

	credentials, ok := os.LookupEnv("BIGQUERY_CREDENTIALS")
	if !ok {
		t.Fatalf("missing BIGQUERY_CREDENTIALS variable")
	}
	projectID, ok := os.LookupEnv("BIGQUERY_PROJECT_ID")
	if !ok {
		t.Fatalf("missing BIGQUERY_PROJECT_ID variable")
	}

	JSONCredentials, err := ioutil.ReadFile(credentials)
	if err != nil {
		panic(fmt.Errorf("cannot find big query credentials: %v", err))
	}

	var credentialsDict map[string]interface{}
	err = json.Unmarshal(JSONCredentials, &credentialsDict)
	if err != nil {
		panic(fmt.Errorf("cannot unmarshal big query credentials: %v", err))
	}

	bigQueryDatasetId := strings.Replace(strings.ToUpper(uuid.NewString()), "-", "_", -1)
	os.Setenv("BIGQUERY_DATASET_ID", bigQueryDatasetId)
	t.Log("BigQuery Dataset: ", bigQueryDatasetId)

	var bigQueryConfig = pc.BigQueryConfig{
		ProjectId:   projectID,
		DatasetId:   bigQueryDatasetId,
		Credentials: credentialsDict,
	}

	return bigQueryConfig, nil
}

func createBigQueryDataset(c pc.BigQueryConfig) error {
	sCreds, err := json.Marshal(c.Credentials)
	if err != nil {
		return err
	}

	client, err := bigquery.NewClient(context.TODO(), c.ProjectId, option.WithCredentialsJSON(sCreds))
	if err != nil {
		return err
	}
	defer client.Close()

	meta := &bigquery.DatasetMetadata{
		Location:               "US",
		DefaultTableExpiration: 24 * time.Hour,
	}
	err = client.Dataset(c.DatasetId).Create(context.TODO(), meta)

	return err
}

func destroyBigQueryDataset(c pc.BigQueryConfig) error {
	sCreds, err := json.Marshal(c.Credentials)
	if err != nil {
		return err
	}

	time.Sleep(10 * time.Second)

	client, err := bigquery.NewClient(context.TODO(), c.ProjectId, option.WithCredentialsJSON(sCreds))
	if err != nil {
		return err
	}
	defer client.Close()

	err = client.Dataset(c.DatasetId).DeleteWithContents(context.TODO())

	return err
}

func RegisterBigQueryTransformationOnPrimaryDatasetTest(t *testing.T, tester offlineSqlTest) {
	test := newSQLTransformationTest(tester.storeTester, false, tester.transformationQuery)
	_ = initSqlPrimaryDataset(t, test.tester, test.data.location, test.data.schema, test.data.records)
	if err := test.tester.CreateTransformation(test.data.config); err != nil {
		t.Fatalf("could not create transformation: %v", err)
	}
	actual, err := test.tester.GetTransformationTable(test.data.config.TargetTableID)
	if err != nil {
		t.Fatalf("could not get transformation table: %v", err)
	}
	test.data.Assert(t, actual)
}

func RegisterBigQueryChainedTransformationsTest(t *testing.T, tester offlineSqlTest) {
	test := newSQLTransformationTest(tester.storeTester, tester.useSchema, tester.transformationQuery)
	_ = initSqlPrimaryDataset(t, test.tester, test.data.location, test.data.schema, test.data.records)
	if err := test.tester.CreateTransformation(test.data.config); err != nil {
		t.Fatalf("could not create transformation: %v", err)
	}
	// CHAIN `SELECT *` TRANSFORMATION ON 1ST TRANSFORMATION
	// Create chained transformation resource ID and table name
	id := ResourceID{Name: "DUMMY_TABLE_TF2", Variant: "test", Type: Transformation}
	table, err := ps.ResourceToTableName(Transformation.String(), id.Name, id.Variant)
	if err != nil {
		t.Fatalf("could not get transformation table: %v", err)
	}
	// Get the table name of the first transformation and create a SQL location for sanitization
	srcDataset, err := ps.ResourceToTableName(Transformation.String(), test.data.config.TargetTableID.Name, test.data.config.TargetTableID.Variant)
	if err != nil {
		t.Fatalf("could not get transformation table name from resource ID: %v", err)
	}
	// TODO: Nicer way of doing this.
	srcLoc := pl.NewFullyQualifiedSQLLocation(test.tester.(*bigQueryOfflineStoreTester).query.DatasetId, "", srcDataset).(*pl.SQLLocation)
	// Copy the original config and modify the query and source mapping
	config := test.data.config
	config.TargetTableID = id
	config.Query = fmt.Sprintf("SELECT * FROM `%s`", srcLoc.TableLocation())
	config.SourceMapping[0].Location = pl.NewSQLLocation(table)
	// Create, get and assert the chained transformation
	if err := test.tester.CreateTransformation(config); err != nil {
		t.Fatalf("could not create transformation: %v", err)
	}
	actual, err := test.tester.GetTransformationTable(id)
	if err != nil {
		t.Fatalf("could not get transformation table: %v", err)
	}
	test.data.Assert(t, actual)
}

func TestBigQueryTransformations(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	tester := getConfiguredBigQueryTester(t, false)

	testCases := map[string]func(t *testing.T, storeTester offlineSqlTest){
		"RegisterTransformationOnPrimaryDatasetTest": RegisterBigQueryTransformationOnPrimaryDatasetTest,
		"RegisterChainedTransformationsTest":         RegisterBigQueryChainedTransformationsTest,
	}

	for name, testCase := range testCases {
		constName := name
		constTestCase := testCase
		t.Run(constName, func(t *testing.T) {
			t.Parallel()
			constTestCase(t, tester)
		})
	}
}

func TestBigQueryMaterializations(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	tester := getConfiguredBigQueryTester(t, false)

	testCases := map[string]func(t *testing.T, storeTester offlineSqlTest){
		"RegisterMaterializationNoTimestampTest": RegisterMaterializationNoTimestampTest,
		"RegisterMaterializationTimestampTest":   RegisterMaterializationTimestampTest,
	}

	for name, testCase := range testCases {
		constName := name
		constTestCase := testCase
		t.Run(constName, func(t *testing.T) {
			t.Parallel()
			constTestCase(t, tester)
		})
	}
}

func TestBigQueryTrainingSets(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	tester := getConfiguredBigQueryTester(t, false)

	tsDatasetTypes := []trainingSetDatasetType{
		tsDatasetFeaturesLabelTS,
		tsDatasetFeaturesTSLabelNoTS,
		tsDatasetFeaturesNoTSLabelTS,
		tsDatasetFeaturesLabelNoTS,
	}

	for _, testCase := range tsDatasetTypes {
		constName := string(testCase)
		constTestCase := testCase
		t.Run(constName, func(t *testing.T) {
			t.Parallel()
			RegisterTrainingSet(t, tester, constTestCase)
		})
	}
}

func getConfiguredBigQueryTester(t *testing.T, useCrossDBJoins bool) offlineSqlTest {
	bigQueryConfig, err := getBigQueryConfig(t)
	if err != nil {
		t.Fatalf("could not get BigQuery config: %s", err)
	}

	if err := createBigQueryDataset(bigQueryConfig); err != nil {
		t.Fatalf("Cannot create BigQuery Dataset: %v", err)
	}

	t.Cleanup(func() {
		err := destroyBigQueryDataset(bigQueryConfig)
		if err != nil {
			t.Logf("failed to cleanup database: %s\n", err)
		}
	})

	store, err := GetOfflineStore(pt.BigQueryOffline, bigQueryConfig.Serialize())

	if err != nil {
		t.Fatalf("could not initialize store: %s\n", err)
	}

	offlineStoreTester := &bigQueryOfflineStoreTester{store.(*bqOfflineStore)}

	return offlineSqlTest{
		storeTester:         offlineStoreTester,
		testCrossDbJoins:    useCrossDBJoins,
		useSchema:           false,
		transformationQuery: "SELECT location_id, AVG(wind_speed) as avg_daily_wind_speed, AVG(wind_duration) as avg_daily_wind_duration, AVG(fetch_value) as avg_daily_fetch, TIMESTAMP(timestamp) as date FROM %s GROUP BY location_id, TIMESTAMP(timestamp)",
	}
}
