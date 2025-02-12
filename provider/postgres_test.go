// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider

import (
	"testing"

	pl "github.com/featureform/provider/location"
	pt "github.com/featureform/provider/provider_type"
)

func TestOfflineStorePostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	postgresConfig, err := getPostgresConfig(t, "")
	if err != nil {
		t.Fatalf("could not get postgres config: %s\n", err)
	}

	store, err := GetOfflineStore(pt.PostgresOffline, postgresConfig.Serialize())
	if err != nil {
		t.Fatalf("could not initialize store: %s\n", err)
	}

	test := OfflineStoreTest{
		t:     t,
		store: store,
	}
	//test.Run()
	test.RunSQL()
}

func getConfiguredPostgresTester(t *testing.T, useCrossDBJoins bool) offlineSqlTest {
	postgresConfig, err := getPostgresConfig(t, "")
	if err != nil {
		t.Fatalf("could not get postgres config: %s\n", err)
	}

	store, err := GetOfflineStore(pt.PostgresOffline, postgresConfig.Serialize())
	if err != nil {
		t.Fatalf("could not initialize store: %s\n", err)
	}

	offlineStore, err := store.AsOfflineStore()
	if err != nil {
		t.Fatalf("could not initialize offline store: %s\n", err)
	}

	dbName := postgresConfig.Database
	storeTester := postgresOfflineStoreTester{
		defaultDbName:   dbName,
		sqlOfflineStore: offlineStore.(*sqlOfflineStore),
	}

	t.Logf("Creating Parent Database: %s\n", dbName)

	//err = storeTester.CreateDatabase(dbName)
	//if err != nil {
	//	t.Fatalf("could not create database: %s\n", err)
	//}

	//t.Cleanup(func() {
	//	t.Logf("Dropping Parent Database: %s\n", dbName)
	//	err := storeTester.DropDatabase(dbName)
	//	if err != nil {
	//		t.Logf("failed to cleanup database: %s\n", err)
	//	}
	//})

	sanitizeTableName := func(obj pl.FullyQualifiedObject) string {
		loc := pl.NewFullyQualifiedSQLLocation(obj.Database, obj.Schema, obj.Table).(*pl.SQLLocation)
		return quotePostgresTable(*loc)
	}

	return offlineSqlTest{
		storeTester:         &storeTester,
		testCrossDbJoins:    useCrossDBJoins,
		transformationQuery: "SELECT LOCATION_ID, AVG(WIND_SPEED) as AVG_DAILY_WIND_SPEED, AVG(WIND_DURATION) as AVG_DAILY_WIND_DURATION, AVG(FETCH_VALUE) as AVG_DAILY_FETCH, DATE(TIMESTAMP) as DATE FROM %s GROUP BY LOCATION_ID, DATE(TIMESTAMP)",
		sanitizeTableName:   sanitizeTableName,
	}
}
