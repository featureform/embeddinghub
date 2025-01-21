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
	"encoding/json"
	"fmt"
	"github.com/featureform/fferr"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/featureform/metadata"
	"github.com/featureform/provider/location"
	pl "github.com/featureform/provider/location"
	pc "github.com/featureform/provider/provider_config"
	ps "github.com/featureform/provider/provider_schema"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/provider/types"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"github.com/stretchr/testify/assert"
)

const (
	floatTolerance = 1e-9
	// SHOW DYNAMIC TABLES returns 21 columns; however, we're only concerned with the 11th column which is the warehouse.
	showDynamicTableColCount        = 21
	showDynamicTableWarehouseColIdx = 11
)

type snowflakeOfflineStoreTester struct {
	*snowflakeOfflineStore
}

func (s *snowflakeOfflineStoreTester) CreateDatabase(name string) error {
	db, err := s.sqlOfflineStore.getDb("", "")
	if err != nil {
		return err
	}
	_, err = db.Exec("CREATE DATABASE IF NOT EXISTS " + sanitize(name))
	return err
}

func (s *snowflakeOfflineStoreTester) DropDatabase(name string) error {
	db, err := s.sqlOfflineStore.getDb("", "")
	if err != nil {
		return err
	}
	query := "DROP DATABASE IF EXISTS " + sanitize(name)
	_, err = db.Exec(query)
	if err != nil {
		return err
	}
	return nil
}

func (s *snowflakeOfflineStoreTester) CreateSchema(database, schema string) error {
	db, err := s.sqlOfflineStore.getDb(database, "")
	if err != nil {
		return err
	}
	query := "CREATE SCHEMA IF NOT EXISTS " + sanitize(schema)
	_, err = db.Exec(query)
	if err != nil {
		return err
	}
	return nil
}

func (s *snowflakeOfflineStoreTester) CreateTable(loc location.Location, schema TableSchema) (PrimaryTable, error) {
	sqlLocation, ok := loc.(*location.SQLLocation)
	if !ok {
		return nil, fmt.Errorf("invalid location type")
	}

	db, err := s.sqlOfflineStore.getDb(sqlLocation.GetDatabase(), sqlLocation.GetSchema())
	if err != nil {
		return nil, err
	}

	// don't need string builder here
	var queryBuilder strings.Builder
	queryBuilder.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (", sqlLocation.TableLocation().String()))

	// do strings .join after
	for i, column := range schema.Columns {
		if i > 0 {
			queryBuilder.WriteString(", ")
		}
		columnType, err := s.sqlOfflineStore.query.determineColumnType(column.ValueType)
		if err != nil {
			return nil, err
		}
		queryBuilder.WriteString(fmt.Sprintf("%s %s", column.Name, columnType))
	}
	queryBuilder.WriteString(")")

	query := queryBuilder.String()
	_, err = db.Exec(query)
	if err != nil {
		return nil, err
	}

	return &snowflakePrimaryTable{
		sqlPrimaryTable{
			db:           db,
			name:         sqlLocation.Location(),
			sqlLocation:  sqlLocation,
			query:        s.sqlOfflineStore.query,
			schema:       schema,
			providerType: s.sqlOfflineStore.ProviderType,
		},
	}, nil
}

func (s *snowflakeOfflineStoreTester) PollTableRefresh(id ResourceID, retryInterval time.Duration, timeout time.Duration) error {
	if err := id.check(Feature, TrainingSet); err != nil {
		return err
	}
	var resourceType string
	if id.Type == TrainingSet {
		resourceType = TrainingSet.String()
	} else {
		resourceType = FeatureMaterialization.String()
	}
	tableName, err := ps.ResourceToTableName(resourceType, id.Name, id.Variant)
	if err != nil {
		return err
	}
	db, err := s.sqlOfflineStore.getDb("", "")
	if err != nil {
		return err
	}

	query := fmt.Sprintf(`SELECT count(*) FROM  TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(NAME => '%s')) WHERE REFRESH_TRIGGER != 'CREATION' AND REFRESH_ACTION != 'NO_DATA' AND STATE = 'SUCCEEDED' ORDER BY REFRESH_END_TIME DESC;`, sanitize(tableName))
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(retryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("polling timed out after %v", timeout)
		case <-ticker.C:
			count := int64(0)
			r := db.QueryRow(query)
			if err := r.Scan(&count); err != nil {
				return err
			}
			if count >= 1 {
				return nil
			}
		}
	}
}

func (s *snowflakeOfflineStoreTester) CheckWarehouse(id ResourceID, expectedWh string) (bool, error) {
	db, err := s.sqlOfflineStore.getDb("", "")
	if err != nil {
		return false, err
	}

	tableName, err := ps.ResourceToTableName(FeatureMaterialization.String(), id.Name, id.Variant)
	if err != nil {
		return false, err
	}

	query := fmt.Sprintf("SHOW DYNAMIC TABLES LIKE '%s'", tableName)

	r := db.QueryRow(query)

	var warehouse string
	// SHOW DYNAMIC TABLES returns 21 columns; however, we're only concerned with the 11th column which is the warehouse.
	// To satisfy the scan function, we need to create a slice of 21 interfaces with the 11th element being the warehouse.
	var placeholders [showDynamicTableColCount]interface{}

	// Assign the warehouse to the 11th element of the slice
	placeholders[showDynamicTableWarehouseColIdx] = &warehouse

	for i := 0; i < len(placeholders); i++ {
		// Skip the 11th element since we've already assigned it
		if i == showDynamicTableWarehouseColIdx {
			continue
		}
		placeholders[i] = new(interface{})
	}

	if err := r.Scan(placeholders[:]...); err != nil {
		if err == sql.ErrNoRows {
			// Handle the case where no rows were returned
			fmt.Println("No dynamic tables found with the specified name.")
			return false, nil
		}
		return false, err
	}

	return warehouse == expectedWh, nil
}

// TESTS

func TestSnowflakeTransformations(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	tester := getConfiguredTester(t, false)

	testCases := map[string]func(t *testing.T, storeTester offlineSqlTest){
		"RegisterTransformationOnPrimaryDatasetTest": RegisterTransformationOnPrimaryDatasetTest,
		"RegisterChainedTransformationsTest":         RegisterChainedTransformationsTest,
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

func TestSnowflakeMaterializations(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	tester := getConfiguredTester(t, false)

	testCases := map[string]func(t *testing.T, storeTester offlineSqlTest){
		"RegisterMaterializationNoTimestampTest":            RegisterMaterializationNoTimestampTest,
		"RegisterMaterializationWithDefaultTargetLagTest":   RegisterMaterializationWithDefaultTargetLagTest,
		"RegisterMaterializationTimestampTest":              RegisterMaterializationTimestampTest,
		"RegisterMaterializationWithDifferentWarehouseTest": RegisterMaterializationWithDifferentWarehouseTest,
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

func TestSnowflakeTrainingSets(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	tester := getConfiguredTester(t, false)

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

func TestSnowflakeSchemas(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	tester := getConfiguredTester(t, true)

	testCases := map[string]func(t *testing.T, storeTester offlineSqlTest){
		"RegisterTableInDifferentDatabaseTest":           RegisterTableInDifferentDatabaseTest,
		"RegisterTableInSameDatabaseDifferentSchemaTest": RegisterTableInSameDatabaseDifferentSchemaTest,
		"RegisterTwoTablesInSameSchemaTest":              RegisterTwoTablesInSameSchemaTest,
		"CrossDatabaseJoinTest":                          CrossDatabaseJoinTest,
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

func TestSnowflakeConfigHasLegacyCredentials(t *testing.T) {
	type fields struct {
		Username       string
		Password       string
		AccountLocator string
		Organization   string
		Account        string
		Database       string
		Schema         string
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{"Empty", fields{}, false},
		{"Has Legacy", fields{AccountLocator: "abcdefg"}, true},
		{"Has Current", fields{Account: "account", Organization: "organization"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sf := &pc.SnowflakeConfig{
				Username:       tt.fields.Username,
				Password:       tt.fields.Password,
				AccountLocator: tt.fields.AccountLocator,
				Organization:   tt.fields.Organization,
				Account:        tt.fields.Account,
				Database:       tt.fields.Database,
				Schema:         tt.fields.Schema,
			}
			if got := sf.HasLegacyCredentials(); got != tt.want {
				t.Errorf("HasLegacyCredentials() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSnowflakeConfigHasCurrentCredentials(t *testing.T) {
	type fields struct {
		Username       string
		Password       string
		AccountLocator string
		Organization   string
		Account        string
		Database       string
		Schema         string
	}
	tests := []struct {
		name    string
		fields  fields
		want    bool
		wantErr bool
	}{
		{"Empty", fields{}, false, false},
		{"Has Legacy", fields{AccountLocator: "abcdefg"}, false, false},
		{"Has Current", fields{Account: "account", Organization: "organization"}, true, false},
		{"Only Account", fields{Account: "account"}, false, true},
		{"Only Organization", fields{Organization: "organization"}, false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sf := &pc.SnowflakeConfig{
				Username:       tt.fields.Username,
				Password:       tt.fields.Password,
				AccountLocator: tt.fields.AccountLocator,
				Organization:   tt.fields.Organization,
				Account:        tt.fields.Account,
				Database:       tt.fields.Database,
				Schema:         tt.fields.Schema,
			}
			got, err := sf.HasCurrentCredentials()
			if (err != nil) != tt.wantErr {
				t.Errorf("HasCurrentCredentials() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("HasCurrentCredentials() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSnowflakeConfigConnectionString(t *testing.T) {
	type fields struct {
		Username       string
		Password       string
		AccountLocator string
		Organization   string
		Account        string
		Database       string
		Schema         string
		Role           string
		Warehouse      string
		SessionParams  map[string]string
	}
	tests := []struct {
		name    string
		fields  fields
		want    string
		wantErr bool
	}{
		{"Empty", fields{}, "", true},
		{
			"Has Legacy",
			fields{Username: "u", Password: "p", AccountLocator: "accountlocator", Database: "d", Schema: "s"},
			"u:p@accountlocator/d/s",
			false,
		},
		{
			"Has Current",
			fields{Username: "u", Password: "p", Account: "account", Organization: "org", Database: "d", Schema: "s"},
			"u:p@org-account/d/s",
			false,
		},
		{
			"Has Role Parameter",
			fields{Username: "u", Password: "p", Account: "account", Organization: "org", Database: "d", Schema: "s", Role: "myrole"},
			"u:p@org-account/d/s?role=myrole",
			false,
		},
		{
			"Has Warehouse Parameter",
			fields{Username: "u", Password: "p", Account: "account", Organization: "org", Database: "d", Schema: "s", Warehouse: "wh"},
			"u:p@org-account/d/s?warehouse=wh",
			false,
		},
		{
			"Has Warehouse and Role Parameter",
			fields{Username: "u", Password: "p", Account: "account", Organization: "org", Database: "d", Schema: "s", Warehouse: "wh", Role: "myrole"},
			"u:p@org-account/d/s?warehouse=wh&role=myrole",
			false,
		},
		{
			"Only Account",
			fields{Username: "u", Password: "p", Account: "account", Database: "d", Schema: "s"},
			"",
			true,
		},
		{
			"Only Organization",
			fields{Username: "u", Password: "p", Organization: "org", Database: "d", Schema: "s"},
			"",
			true,
		},
		{
			"Both Current And Legacy",
			fields{Username: "u", Password: "p", Account: "account", Organization: "org", AccountLocator: "accountlocator", Database: "d", Schema: "s"},
			"",
			true,
		},
		{
			"Neither Current Nor Legacy",
			fields{Username: "u", Password: "p", Database: "d", Schema: "s"},
			"",
			true,
		},
		{
			"Has Session Params",
			fields{Username: "u", Password: "p", Account: "account", Organization: "org", Database: "d", Schema: "s", SessionParams: map[string]string{"query_tag": "t"}},
			"u:p@org-account/d/s?query_tag=t",
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sf := &pc.SnowflakeConfig{
				Username:       tt.fields.Username,
				Password:       tt.fields.Password,
				AccountLocator: tt.fields.AccountLocator,
				Organization:   tt.fields.Organization,
				Account:        tt.fields.Account,
				Database:       tt.fields.Database,
				Schema:         tt.fields.Schema,
				Role:           tt.fields.Role,
				Warehouse:      tt.fields.Warehouse,
				SessionParams:  tt.fields.SessionParams,
			}
			got, err := sf.ConnectionString(tt.fields.Database, tt.fields.Schema)
			if (err != nil) != tt.wantErr {
				t.Errorf("ConnectionString() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ConnectionString() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSnowflakeDeserializeCurrentCredentials(t *testing.T) {
	expected := pc.SnowflakeConfig{
		Username:      "username",
		Password:      "password",
		Organization:  "org",
		Account:       "account",
		Database:      "database",
		Schema:        "schema",
		SessionParams: map[string]string{"query_tag": "t"},
	}
	credentialsMap := make(map[string]interface{})
	credentialsMap["Username"] = expected.Username
	credentialsMap["Password"] = expected.Password
	credentialsMap["Organization"] = expected.Organization
	credentialsMap["Account"] = expected.Account
	credentialsMap["Database"] = expected.Database
	credentialsMap["Schema"] = expected.Schema
	credentialsMap["SessionParams"] = expected.SessionParams
	b, err := json.Marshal(credentialsMap)
	if err != nil {
		t.Fatalf("could not marshal test data: %s", err.Error())
	}
	config := pc.SnowflakeConfig{}
	if err := config.Deserialize(pc.SerializedConfig(b)); err != nil {
		t.Fatalf("could not deserialize config: %s", err.Error())
	}
	if !reflect.DeepEqual(expected, config) {
		t.Fatalf("Expected: %v, Got %v", expected, config)
	}
}

func TestSnowflakeDeserializeLegacyCredentials(t *testing.T) {
	expected := pc.SnowflakeConfig{
		Username:       "username",
		Password:       "password",
		AccountLocator: "accountlocator",
		Database:       "database",
		Schema:         "schema",
	}
	credentialsMap := make(map[string]string)
	credentialsMap["Username"] = expected.Username
	credentialsMap["Password"] = expected.Password
	credentialsMap["AccountLocator"] = expected.AccountLocator
	credentialsMap["Database"] = expected.Database
	credentialsMap["Schema"] = expected.Schema
	b, err := json.Marshal(credentialsMap)
	if err != nil {
		t.Fatalf("could not marshal test data: %s", err.Error())
	}
	config := pc.SnowflakeConfig{}
	if err := config.Deserialize(pc.SerializedConfig(b)); err != nil {
		t.Fatalf("could not deserialize config: %s", err.Error())
	}
	if !reflect.DeepEqual(expected, config) {
		t.Fatalf("Expected: %v, Got %v", expected, config)
	}
}

func TestSnowflakeDelete(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}
	tester := getConfiguredTester(t, true)
	testCases := map[string]func(t *testing.T, storeTester offlineSqlTest){
		// "DeleteTableTest": DeleteTableTest,
		"DeleteNotExistingTableTest": DeleteNotExistingTableTest,
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

// TEST FUNCTION

func CrossDatabaseJoinTest(t *testing.T, tester offlineSqlTest) {
	if !tester.testCrossDbJoins {
		t.Skip("skipping cross database join test")
	}

	dbName := fmt.Sprintf("DB_%s", strings.ToUpper(uuid.NewString()[:5]))
	t.Logf("Database Name1: %s\n", dbName)
	if err := tester.storeTester.CreateDatabase(dbName); err != nil {
		t.Fatalf("could not create database: %v", err)
	}

	dbName2 := fmt.Sprintf("DB_%s", strings.ToUpper(uuid.NewString()[:5]))
	t.Logf("Database Name2: %s\n", dbName2)
	if err := tester.storeTester.CreateDatabase(dbName2); err != nil {
		t.Fatalf("could not create database: %v", err)
	}

	t.Cleanup(func() {
		if err := tester.storeTester.DropDatabase(dbName); err != nil {
			t.Fatalf("could not drop database: %v", err)
		}
		if err := tester.storeTester.DropDatabase(dbName2); err != nil {
			t.Fatalf("could not drop database: %v", err)
		}
	})

	tableName1 := "DUMMY_TABLE"
	sqlLocation := location.NewSQLLocationWithDBSchemaTable(dbName, "PUBLIC", tableName1).(*location.SQLLocation)
	records, err := createDummyTable(tester.storeTester, *sqlLocation, 3)
	if err != nil {
		t.Fatalf("could not create table: %v", err)
	}

	tableName2 := "DUMMY_TABLE2"
	sqlLocation2 := location.NewSQLLocationWithDBSchemaTable(dbName2, "PUBLIC", tableName2).(*location.SQLLocation)
	records2, err := createDummyTable(tester.storeTester, *sqlLocation2, 10)
	if err != nil {
		t.Fatalf("could not create table: %v", err)
	}

	// Register the tables
	primary1, primaryErr := tester.storeTester.RegisterPrimaryFromSourceTable(
		ResourceID{Name: tableName1, Variant: "test", Type: Primary},
		sqlLocation,
	)
	if primaryErr != nil {
		t.Fatalf("could not register primary table: %v", primaryErr)
	}

	primary2, primaryErr := tester.storeTester.RegisterPrimaryFromSourceTable(
		ResourceID{Name: tableName2, Variant: "test", Type: Primary},
		sqlLocation2,
	)
	if primaryErr != nil {
		t.Fatalf("could not register primary table: %v", primaryErr)
	}

	// Verify the table contents
	verifyPrimaryTable(t, primary1, records)
	verifyPrimaryTable(t, primary2, records2)

	targetTableId := ResourceID{Name: "DUMMY_TABLE_TF", Variant: "test", Type: Transformation}
	// union the tables using a transformation
	tfConfig := TransformationConfig{
		Type:          SQLTransformation,
		TargetTableID: targetTableId,
		Query:         fmt.Sprintf("SELECT NAME FROM %s UNION SELECT NAME FROM %s", sqlLocation.TableLocation().String(), sqlLocation2.TableLocation().String()),
	}

	err = tester.storeTester.CreateTransformation(tfConfig)
	if err != nil {
		t.Fatalf("could not create transformation: %v", err)
	}

	// Verify the union table contents
	tfTable, err := tester.storeTester.GetTransformationTable(targetTableId)
	if err != nil {
		t.Fatalf("could not get transformation table: %v", err)
	}

	numRows, err := tfTable.NumRows()
	if err != nil {
		t.Fatalf("could not get number of rows: %v", err)
	}

	assert.Equal(t, int64(13), numRows, "expected 13 rows")
}

func RegisterTwoTablesInSameSchemaTest(t *testing.T, tester offlineSqlTest) {
	schemaName1 := fmt.Sprintf("SCHEMA_%s", strings.ToUpper(uuid.NewString()[:5]))
	schemaName2 := fmt.Sprintf("SCHEMA_%s", strings.ToUpper(uuid.NewString()[:5]))
	if err := tester.storeTester.CreateSchema("", schemaName1); err != nil {
		t.Fatalf("could not create schema: %v", err)
	}

	if err := tester.storeTester.CreateSchema("", schemaName2); err != nil {
		t.Fatalf("could not create schema: %v", err)
	}

	// Create the first table
	tableName := "DUMMY_TABLE"
	sqlLocation := location.NewSQLLocationWithDBSchemaTable("", schemaName1, tableName).(*location.SQLLocation)
	records, err := createDummyTable(tester.storeTester, *sqlLocation, 3)
	if err != nil {
		t.Fatalf("could not create table: %v", err)
	}

	// Create the second table using the same table name
	sqlLocation2 := location.NewSQLLocationWithDBSchemaTable("", schemaName2, tableName).(*location.SQLLocation)
	records2, err := createDummyTable(tester.storeTester, *sqlLocation2, 10)
	if err != nil {
		t.Fatalf("could not create table: %v", err)
	}

	primary1, primaryErr := tester.storeTester.RegisterPrimaryFromSourceTable(
		ResourceID{Name: "tb1", Variant: "test", Type: Primary},
		sqlLocation,
	)
	if primaryErr != nil {
		t.Fatalf("could not register primary table: %v", primaryErr)
	}

	primary2, primaryErr := tester.storeTester.RegisterPrimaryFromSourceTable(
		ResourceID{Name: "tb2", Variant: "test", Type: Primary},
		sqlLocation2,
	)
	if primaryErr != nil {
		t.Fatalf("could not register primary table: %v", primaryErr)
	}

	// Verify the table contents
	verifyPrimaryTable(t, primary1, records)
	verifyPrimaryTable(t, primary2, records2)
}

func RegisterTableInDifferentDatabaseTest(t *testing.T, storeTester offlineSqlTest) {
	dbName := fmt.Sprintf("DB_%s", strings.ToUpper(uuid.NewString()[:5]))

	err := storeTester.storeTester.CreateDatabase(dbName)
	if err != nil {
		t.Fatalf("could not create database: %v", err)
	}

	t.Cleanup(func() {
		if err := storeTester.storeTester.DropDatabase(dbName); err != nil {
			t.Fatalf("could not drop database: %v", err)
		}
	})

	// Create the schema
	schemaName := fmt.Sprintf("SCHEMA_%s", strings.ToUpper(uuid.NewString()[:5]))
	if err := storeTester.storeTester.CreateSchema(dbName, schemaName); err != nil {
		t.Fatalf("could not create schema: %v", err)
	}

	// Create the table
	tableName := "DUMMY_TABLE"
	sqlLocation := location.NewSQLLocationWithDBSchemaTable(dbName, schemaName, tableName).(*location.SQLLocation)
	records, err := createDummyTable(storeTester.storeTester, *sqlLocation, 3)
	if err != nil {
		t.Fatalf("could not create table: %v", err)
	}

	primary, primaryErr := storeTester.storeTester.RegisterPrimaryFromSourceTable(
		ResourceID{Name: tableName, Variant: "test", Type: Primary},
		sqlLocation,
	)
	if primaryErr != nil {
		t.Fatalf("could not register primary table: %v", primaryErr)
	}

	// Verify the table contents
	verifyPrimaryTable(t, primary, records)
}

func RegisterTableInSameDatabaseDifferentSchemaTest(t *testing.T, storeTester offlineSqlTest) {
	schemaName := fmt.Sprintf("SCHEMA_%s", strings.ToUpper(uuid.NewString()[:5]))
	if err := storeTester.storeTester.CreateSchema("", schemaName); err != nil {
		t.Fatalf("could not create schema: %v", err)
	}

	// Create the table
	tableName := "DUMMY_TABLE"
	sqlLocation := location.NewSQLLocationWithDBSchemaTable("", schemaName, tableName).(*location.SQLLocation)
	records, err := createDummyTable(storeTester.storeTester, *sqlLocation, 3)
	if err != nil {
		t.Fatalf("could not create table: %v", err)
	}

	primary, primaryErr := storeTester.storeTester.RegisterPrimaryFromSourceTable(
		ResourceID{Name: tableName, Variant: "test", Type: Primary},
		sqlLocation,
	)
	if primaryErr != nil {
		t.Fatalf("could not register primary table: %v", primaryErr)
	}

	// Verify the table contents
	verifyPrimaryTable(t, primary, records)
}

func RegisterTransformationOnPrimaryDatasetTest(t *testing.T, tester offlineSqlTest) {
	test := newSQLTransformationTest(tester.storeTester)
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

func RegisterChainedTransformationsTest(t *testing.T, tester offlineSqlTest) {
	test := newSQLTransformationTest(tester.storeTester)
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
	srcLoc := pl.NewSQLLocation(srcDataset).(*pl.SQLLocation)
	// Copy the original config and modify the query and source mapping
	config := test.data.config
	config.TargetTableID = id
	config.Query = fmt.Sprintf("SELECT * FROM %s", SanitizeSnowflakeIdentifier(srcLoc.TableLocation()))
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

func RegisterMaterializationNoTimestampTest(t *testing.T, tester offlineSqlTest) {
	useTimestamps := false
	isIncremental := false
	matTest := newSQLMaterializationTest(tester.storeTester, useTimestamps)
	_ = initSqlPrimaryDataset(t, matTest.tester, matTest.data.location, matTest.data.schema, matTest.data.records)
	mat, err := matTest.tester.CreateMaterialization(matTest.data.id, matTest.data.opts)
	if err != nil {
		t.Fatalf("could not create materialization: %v", err)
	}
	mat, err = matTest.tester.GetMaterialization(mat.ID())
	if err != nil {
		t.Fatalf("could not get materialization: %v", err)
	}

	matTest.data.Assert(t, mat, isIncremental)
}

func RegisterMaterializationTimestampTest(t *testing.T, tester offlineSqlTest) {
	useTimestamps := true
	isIncremental := false
	matTest := newSQLMaterializationTest(tester.storeTester, useTimestamps)
	_ = initSqlPrimaryDataset(t, matTest.tester, matTest.data.location, matTest.data.schema, matTest.data.records)
	mat, err := matTest.tester.CreateMaterialization(matTest.data.id, matTest.data.opts)
	if err != nil {
		t.Fatalf("could not create materialization: %v", err)
	}
	mat, err = matTest.tester.GetMaterialization(mat.ID())
	if err != nil {
		t.Fatalf("could not get materialization: %v", err)
	}

	matTest.data.Assert(t, mat, isIncremental)
}

func RegisterMaterializationWithDefaultTargetLagTest(t *testing.T, tester offlineSqlTest) {
	useTimestamps := true
	isIncremental := true
	matTest := newSQLMaterializationTest(tester.storeTester, useTimestamps)
	primary := initSqlPrimaryDataset(t, matTest.tester, matTest.data.location, matTest.data.schema, matTest.data.records)
	matTest.data.opts.ResourceSnowflakeConfig = &metadata.ResourceSnowflakeConfig{
		DynamicTableConfig: &metadata.SnowflakeDynamicTableConfig{
			TargetLag: "90 seconds",
		},
	}
	mat, err := matTest.tester.CreateMaterialization(matTest.data.id, matTest.data.opts)
	if err != nil {
		t.Fatalf("could not create materialization: %v", err)
	}

	if err := primary.WriteBatch(matTest.data.incrementalRecords); err != nil {
		t.Fatalf("could not write batch: %v", err)
	}

	snowflakeTester, isSnowflakeTester := tester.storeTester.(*snowflakeOfflineStoreTester)
	if !isSnowflakeTester {
		t.Fatalf("expected store tester to be snowflakeOfflineStoreTester")
	}

	if err := snowflakeTester.PollTableRefresh(matTest.data.id, 90*time.Second, 3*time.Minute); err != nil {
		t.Fatalf("expected table to refresh: %v", err)
	}

	matIncr, err := matTest.tester.GetMaterialization(mat.ID())
	if err != nil {
		t.Fatalf("could not get materialization: %v", err)
	}

	matTest.data.Assert(t, matIncr, isIncremental)
}

func RegisterMaterializationWithDifferentWarehouseTest(t *testing.T, tester offlineSqlTest) {
	useTimestamps := true
	isIncremental := true
	matTest := newSQLMaterializationTest(tester.storeTester, useTimestamps)
	primary := initSqlPrimaryDataset(t, matTest.tester, matTest.data.location, matTest.data.schema, matTest.data.records)
	warehouse := "TEST_WH"
	matTest.data.opts.ResourceSnowflakeConfig = &metadata.ResourceSnowflakeConfig{
		DynamicTableConfig: &metadata.SnowflakeDynamicTableConfig{
			TargetLag: "90 seconds",
		},
		Warehouse: warehouse,
	}
	mat, err := matTest.tester.CreateMaterialization(matTest.data.id, matTest.data.opts)
	if err != nil {
		t.Fatalf("could not create materialization: %v", err)
	}

	if err := primary.WriteBatch(matTest.data.incrementalRecords); err != nil {
		t.Fatalf("could not write batch: %v", err)
	}

	snowflakeTester, isSnowflakeTester := tester.storeTester.(*snowflakeOfflineStoreTester)
	if !isSnowflakeTester {
		t.Fatalf("expected store tester to be snowflakeOfflineStoreTester")
	}

	if err := snowflakeTester.PollTableRefresh(matTest.data.id, 90*time.Second, 3*time.Minute); err != nil {
		t.Fatalf("expected table to refresh: %v", err)
	}

	usesWarehouse, err := snowflakeTester.CheckWarehouse(matTest.data.id, warehouse)
	if err != nil {
		t.Fatalf("failed to check warehouse: %v", err)
	}
	if !usesWarehouse {
		t.Fatalf("expected materialization to use warehouse %s", warehouse)
	}

	matIncr, err := matTest.tester.GetMaterialization(mat.ID())
	if err != nil {
		t.Fatalf("could not get materialization: %v", err)
	}

	matTest.data.Assert(t, matIncr, isIncremental)
}

func RegisterTrainingSet(t *testing.T, tester offlineSqlTest, tsDatasetType trainingSetDatasetType) {
	tsTest := newSQLTrainingSetTest(tester.storeTester, tsDatasetType)
	_ = initSqlPrimaryDataset(t, tsTest.tester, tsTest.data.location, tsTest.data.schema, tsTest.data.records)
	_ = initSqlPrimaryDataset(t, tsTest.tester, tsTest.data.labelLocation, tsTest.data.labelSchema, tsTest.data.labelRecords)

	res, err := tsTest.tester.RegisterResourceFromSourceTable(tsTest.data.labelID, tsTest.data.labelResourceSchema, &ResourceSnowflakeConfigOption{})
	if err != nil {
		t.Fatalf("could not register label table: %v", err)
	}
	tsTest.data.def.LabelSourceMapping.Location = res.Location()
	if err := tsTest.tester.CreateTrainingSet(tsTest.data.def); err != nil {
		t.Fatalf("could not create training set: %v", err)
	}
	ts, err := tsTest.tester.GetTrainingSet(tsTest.data.id)
	if err != nil {
		t.Fatalf("could not get training set: %v", err)
	}
	tsTest.data.Assert(t, ts)
}

func DeleteTableTest(t *testing.T, tester offlineSqlTest) {
	dbName := fmt.Sprintf("DB_%s", strings.ToUpper(uuid.NewString()[:5]))
	t.Logf("Database Name1: %s\n", dbName)
	if err := tester.storeTester.CreateDatabase(dbName); err != nil {
		t.Fatalf("could not create database: %v", err)
	}
	// Create the table
	tableName := "DUMMY_TABLE"
	sqlLocation := location.NewSQLLocationWithDBSchemaTable(dbName, "PUBLIC", tableName).(*location.SQLLocation)
	_, err := createDummyTable(tester.storeTester, *sqlLocation, 3)
	if err != nil {
		t.Fatalf("could not create table: %v", err)
	}
	if err := tester.storeTester.Delete(sqlLocation); err != nil {
		t.Fatalf("could not delete table: %v", err)
	}
}
func DeleteNotExistingTableTest(t *testing.T, tester offlineSqlTest) {
	dbName := fmt.Sprintf("DB_%s", strings.ToUpper(uuid.NewString()[:5]))
	t.Logf("Database Name1: %s\n", dbName)
	if err := tester.storeTester.CreateDatabase(dbName); err != nil {
		t.Fatalf("could not create database: %v", err)
	}
	loc := location.NewSQLLocationWithDBSchemaTable(dbName, "PUBLIC", "NOT_EXISTING_TABLE").(*location.SQLLocation)
	deleteErr := tester.storeTester.Delete(loc)
	if deleteErr == nil {
		t.Fatalf("expected error deleting table")
	}
	if _, ok := deleteErr.(*fferr.DatasetNotFoundError); !ok {
		t.Fatalf("expected DatasetNotFoundError")
	}
}

// HELPER FUNCTIONS

func createDummyTable(storeTester offlineSqlStoreTester, sqlLocation location.SQLLocation, numRows int) ([]GenericRecord, error) {
	// Create the table
	// create simple Schema
	schema := TableSchema{
		Columns: []TableColumn{
			{
				Name:      "ID",
				ValueType: types.Int,
			},
			{
				Name:      "NAME",
				ValueType: types.String,
			},
		},
	}

	primaryTable, err := storeTester.CreateTable(&sqlLocation, schema)
	if err != nil {
		return nil, err
	}

	genericRecords := make([]GenericRecord, 0)
	randomNum := uuid.NewString()[:5]
	for i := 0; i < numRows; i++ {
		genericRecords = append(genericRecords, []interface{}{i, fmt.Sprintf("Name_%d_%s", i, randomNum)})
	}

	if err := primaryTable.WriteBatch(genericRecords); err != nil {
		return nil, err
	}

	return genericRecords, nil
}

func verifyPrimaryTable(t *testing.T, primary PrimaryTable, records []GenericRecord) {
	t.Helper()
	numRows, err := primary.NumRows()
	if err != nil {
		t.Fatalf("could not get number of rows: %v", err)
	}

	if numRows == 0 {
		t.Fatalf("expected more than 0 rows")
	}

	iterator, err := primary.IterateSegment(100)
	if err != nil {
		t.Fatalf("Could not get generic iterator: %v", err)
	}

	i := 0
	for iterator.Next() {
		for j, v := range iterator.Values() {
			// NOTE: we're handling float64 differently hear given the values returned by Snowflake have less precision
			// and therefore are not equal unless we round them; if tests require handling of other types, we can add
			// additional cases here, otherwise the default case will cover all other types
			switch v.(type) {
			case float64:
				assert.True(t, floatsAreClose(v.(float64), records[i][j].(float64), floatTolerance), "expected same values")
			case time.Time:
				assert.Equal(t, records[i][j].(time.Time).Truncate(time.Microsecond), v.(time.Time).Truncate(time.Microsecond), "expected same values")
			default:
				assert.Equal(t, v, records[i][j], "expected same values")
			}
		}
		i++
	}
}

func getSnowflakeConfig(t *testing.T, dbName string) (pc.SnowflakeConfig, error) {
	err := godotenv.Load("../.env")
	if err != nil {
		t.Logf("could not open .env file... Checking environment: %s", err)
	}

	snowFlakeDatabase := strings.ToUpper(uuid.NewString())
	if dbName != "" {
		snowFlakeDatabase = dbName
	}
	t.Log("Snowflake Database: ", snowFlakeDatabase)

	username, ok := os.LookupEnv("SNOWFLAKE_USERNAME")
	if !ok {
		t.Fatalf("missing SNOWFLAKE_USERNAME variable")
	}
	password, ok := os.LookupEnv("SNOWFLAKE_PASSWORD")
	if !ok {
		t.Fatalf("missing SNOWFLAKE_PASSWORD variable")
	}
	org, ok := os.LookupEnv("SNOWFLAKE_ORG")
	if !ok {
		t.Fatalf("missing SNOWFLAKE_ORG variable")
	}
	account, ok := os.LookupEnv("SNOWFLAKE_ACCOUNT")
	if !ok {
		t.Fatalf("missing SNOWFLAKE_ACCOUNT variable")
	}
	externalVolume, ok := os.LookupEnv("SNOWFLAKE_EXTERNAL_VOLUME")
	if !ok {
		t.Fatalf("missing SNOWFLAKE_EXTERNAL_VOLUME variable")
	}
	baseLocation, ok := os.LookupEnv("SNOWFLAKE_BASE_LOCATION")
	if !ok {
		t.Fatalf("missing SNOWFLAKE_BASE_LOCATION variable")
	}
	snowflakeConfig := pc.SnowflakeConfig{
		Username:     username,
		Password:     password,
		Organization: org,
		Account:      account,
		Database:     snowFlakeDatabase,
		Warehouse:    "COMPUTE_WH",
		Catalog: &pc.SnowflakeCatalogConfig{
			ExternalVolume: externalVolume,
			BaseLocation:   baseLocation,
			TableConfig: pc.SnowflakeTableConfig{
				TargetLag:   "DOWNSTREAM",
				RefreshMode: "AUTO",
				Initialize:  "ON_CREATE",
			},
		},
	}

	return snowflakeConfig, nil
}

func createSnowflakeDatabase(c pc.SnowflakeConfig) error {
	url := fmt.Sprintf("%s:%s@%s-%s", c.Username, c.Password, c.Organization, c.Account)
	db, err := sql.Open("snowflake", url)
	if err != nil {
		return err
	}
	databaseQuery := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", sanitize(c.Database))
	if _, err := db.Exec(databaseQuery); err != nil {
		return err
	}
	return nil
}

func destroySnowflakeDatabase(c pc.SnowflakeConfig) error {
	url := fmt.Sprintf("%s:%s@%s-%s", c.Username, c.Password, c.Organization, c.Account)
	db, err := sql.Open("snowflake", url)
	if err != nil {
		return err
	}
	databaseQuery := fmt.Sprintf("DROP DATABASE IF EXISTS %s", sanitize(c.Database))
	if _, err := db.Exec(databaseQuery); err != nil {
		return err
	}
	return nil
}

func getConfiguredTester(t *testing.T, useCrossDBJoins bool) offlineSqlTest {
	dbName := fmt.Sprintf("DB_%s", strings.ToUpper(uuid.NewString()[:5]))
	t.Logf("Creating Parent Database: %s\n", dbName)
	snowflakeConfig, err := getSnowflakeConfig(t, dbName)
	if err != nil {
		t.Fatalf("could not get snowflake config: %s", err)
	}
	if err := createSnowflakeDatabase(snowflakeConfig); err != nil {
		t.Fatalf("%v", err)
	}

	t.Cleanup(func() {
		t.Logf("Dropping Parent Database: %s\n", dbName)
		err := destroySnowflakeDatabase(snowflakeConfig)
		if err != nil {
			t.Logf("failed to cleanup database: %s\n", err)
		}
	})

	store, err := GetOfflineStore(pt.SnowflakeOffline, snowflakeConfig.Serialize())
	if err != nil {
		t.Fatalf("could not initialize store: %s\n", err)
	}

	offlineStoreTester := &snowflakeOfflineStoreTester{store.(*snowflakeOfflineStore)}

	return offlineSqlTest{
		storeTester:      offlineStoreTester,
		testCrossDbJoins: useCrossDBJoins,
	}
}
