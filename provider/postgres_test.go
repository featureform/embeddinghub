// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider

import (
	"testing"

	helper "github.com/featureform/helpers/postgres"
	pl "github.com/featureform/provider/location"
	pt "github.com/featureform/provider/provider_type"
)

func TestOfflineStorePostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	// Disabling these tests, as the majority of them assume older behavior of providers
	// (ex. utilizing deprecated ResourceSchema locations, etc.). Currently we just
	// run the tests in correctness_tests instead.

	// TODO: Refactor the tests, and re-enable.
	//postgresTester := getConfiguredPostgresTester(t, false)
	//test := OfflineStoreTest{
	//	t:     t,
	//	store: postgresTester.storeTester,
	//}

	//test.Run()
	//test.RunSQL()
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

	if err := storeTester.CreateSchema(postgresConfig.Database, postgresConfig.Schema); err != nil {
		t.Fatalf("could not create schema: %s\n", err)
	}
	// TODO: Drop Schema

	sanitizeTableName := func(obj pl.FullyQualifiedObject) string {
		loc := pl.NewFullyQualifiedSQLLocation(obj.Database, obj.Schema, obj.Table).(*pl.SQLLocation)
		return helper.SanitizeLocation(*loc)
	}

	return offlineSqlTest{
		storeTester:         &storeTester,
		testCrossDbJoins:    useCrossDBJoins,
		transformationQuery: "SELECT LOCATION_ID, AVG(WIND_SPEED) as AVG_DAILY_WIND_SPEED, AVG(WIND_DURATION) as AVG_DAILY_WIND_DURATION, AVG(FETCH_VALUE) as AVG_DAILY_FETCH, DATE(TIMESTAMP) as DATE FROM %s GROUP BY LOCATION_ID, DATE(TIMESTAMP)",
		sanitizeTableName:   sanitizeTableName,
	}
}
