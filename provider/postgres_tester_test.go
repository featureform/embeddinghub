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
	"os"
	"strings"
	"testing"

	"github.com/featureform/provider/retriever"

	"github.com/joho/godotenv"
	"github.com/lib/pq"

	"github.com/featureform/provider/location"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/google/uuid"
	_ "github.com/lib/pq" // PostgreSQL driver
)

// sqlOfflineStore for PostgreSQL implementation
type postgresOfflineStoreTester struct {
	defaultDbName string
	*sqlOfflineStore
}

func (p *postgresOfflineStoreTester) GetTestDatabase() string {
	return p.defaultDbName
}

func (p *postgresOfflineStoreTester) CreateDatabase(name string) error {
	db, err := p.sqlOfflineStore.getDb("", "")
	if err != nil {
		return err
	}

	// Postgres doesn't have a CREATE DATABASE IF EXISTS clause, so we just drop and recreate it.
	query := fmt.Sprintf("DROP DATABASE IF EXISTS %s", pq.QuoteIdentifier(name))
	_, err = db.Exec(query)
	if err != nil {
		return err
	}

	query = fmt.Sprintf("CREATE DATABASE %s", pq.QuoteIdentifier(name))
	_, err = db.Exec(query)
	if err != nil {
		return err
	}

	return nil
}

func (p *postgresOfflineStoreTester) DropDatabase(name string) error {
	// First, get the connection to the PostgreSQL server.
	db, err := p.sqlOfflineStore.getDb("", "")
	if err != nil {
		return err
	}

	// Terminate all connections to the target database.
	terminateQuery := fmt.Sprintf(`
        SELECT pg_terminate_backend(pg_stat_activity.pid)
        FROM pg_stat_activity
        WHERE pg_stat_activity.datname = %s
        AND pid <> pg_backend_pid()
    `, pq.QuoteLiteral(name)) // Use QuoteLiteral for the database name to ensure it's properly escaped.

	_, err = db.Exec(terminateQuery)
	if err != nil {
		return err
	}

	// Now, drop the database.
	dropQuery := fmt.Sprintf("DROP DATABASE IF EXISTS %s", pq.QuoteIdentifier(name))
	_, err = db.Exec(dropQuery)
	return err
}

func (p *postgresOfflineStoreTester) CreateSchema(database, schema string) error {
	db, err := p.sqlOfflineStore.getDb(database, "")
	if err != nil {
		return err
	}
	query := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", pq.QuoteIdentifier(schema))
	_, err = db.Exec(query)
	return err
}

func (p *postgresOfflineStoreTester) CreateTable(loc location.Location, schema TableSchema) (PrimaryTable, error) {
	sqlLocation, ok := loc.(*location.SQLLocation)
	if !ok {
		return nil, fmt.Errorf("invalid location type")
	}

	var currentDb, currentSchema string
	query := fmt.Sprintf("SELECT current_database(), current_schema()")
	err := p.sqlOfflineStore.db.QueryRow(query).Scan(&currentDb, &currentSchema)
	if err != nil {
		return nil, err
	}
	fmt.Println("Current Database: ", currentDb)
	fmt.Println("Current Schema: ", currentSchema)

	db, err := p.sqlOfflineStore.getDb(sqlLocation.GetDatabase(), sqlLocation.GetSchema())
	if err != nil {
		return nil, err
	}

	var queryBuilder strings.Builder
	queryBuilder.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (", SanitizeSqlLocation(sqlLocation.TableLocation())))
	for i, column := range schema.Columns {
		if i > 0 {
			queryBuilder.WriteString(", ")
		}
		columnType, err := p.sqlOfflineStore.query.determineColumnType(column.ValueType)
		if err != nil {
			return nil, err
		}
		queryBuilder.WriteString(fmt.Sprintf("%s %s", column.Name, columnType))
	}
	queryBuilder.WriteString(")")

	query = queryBuilder.String()
	_, tblErr := db.Exec(query)
	if tblErr != nil {
		return nil, tblErr
	}

	return &sqlPrimaryTable{
		db:           db,
		name:         sqlLocation.Location(),
		sqlLocation:  sqlLocation,
		query:        p.sqlOfflineStore.query,
		schema:       schema,
		providerType: p.sqlOfflineStore.ProviderType,
	}, nil
}

func TestPostgresSchemas(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	dbName := fmt.Sprintf("db_%s", strings.ToLower(uuid.NewString()[:5]))
	t.Logf("Parent Database Name: %s", dbName)
	postgresConfig, err := getPostgresConfig(t, "")
	if err != nil {
		t.Fatalf("could not retrieve Postgres config: %v", err)
	}

	store, err := GetOfflineStore(pt.PostgresOffline, postgresConfig.Serialize())
	if err != nil {
		t.Fatalf("could not initialize store: %s\n", err)
	}

	offlineStoreTester := &postgresOfflineStoreTester{
		defaultDbName:   dbName,
		sqlOfflineStore: store.(*sqlOfflineStore),
	}

	tester := offlineSqlTest{
		storeTester:      offlineStoreTester,
		testCrossDbJoins: false,
	}

	testCases := map[string]func(t *testing.T, storeTester offlineSqlTest){
		//"RegisterTableInDifferentDatabaseTest": RegisterTableInDifferentDatabaseTest,
		//"RegisterTableInSameDatabaseDifferentSchemaTest": RegisterTableInSameDatabaseDifferentSchemaTest,
		//"RegisterTwoTablesInSameSchemaTest":              RegisterTwoTablesInSameSchemaTest,
		//"CrossDatabaseJoinTest":                          CrossDatabaseJoinTest,
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

// Sample implementation to create and drop a Postgres database
func createPostgresDatabase(config pc.PostgresConfig) error {
	connString := PostgresConnectionBuilder(config)
	db, err := sql.Open("postgres", connString)
	if err != nil {
		return err
	}
	defer db.Close()

	_, err = db.Exec(fmt.Sprintf("CREATE DATABASE %s", config.Database))
	return err
}

func destroyPostgresDatabase(config pc.PostgresConfig) error {
	connString := PostgresConnectionBuilder(config)
	db, err := sql.Open("postgres", connString)
	if err != nil {
		return err
	}
	defer db.Close()

	_, err = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", pq.QuoteIdentifier(config.Database)))
	return err
}

// Assuming you have a function to load Postgres config
func getPostgresConfig(t *testing.T, dbName string) (pc.PostgresConfig, error) {
	err := godotenv.Load("../.env")
	if err != nil {
		t.Logf("could not open .env file... Checking environment: %s", err)
	}

	if dbName == "" {
		var ok bool
		dbName, ok = os.LookupEnv("POSTGRES_DB")
		if !ok {
			t.Fatalf("missing POSTGRES_DB variable")
		}
	}

	user, ok := os.LookupEnv("POSTGRES_USER")
	if !ok {
		t.Fatalf("missing POSTGRES_USER variable")
	}
	password, ok := os.LookupEnv("POSTGRES_PASSWORD")
	if !ok {
		t.Fatalf("missing POSTGRES_PASSWORD variable")
	}

	schema := uuid.NewString()[:10]

	postgresConfig := pc.PostgresConfig{
		Host:     "localhost",
		Port:     "5432",
		Database: dbName,
		Username: user,
		Password: retriever.NewStaticValue[string](password),
		SSLMode:  "disable",
		Schema:   schema,
	}

	return postgresConfig, nil
}
