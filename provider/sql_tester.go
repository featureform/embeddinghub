// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider

import (
	"github.com/featureform/provider/location"
)

type offlineSqlTest struct {
	storeTester         offlineSqlStoreTester
	testCrossDbJoins    bool
	useSchema           bool
	transformationQuery string
}

type offlineSqlStoreCoreTester interface {
	AsOfflineStore() (OfflineStore, error)
	CreateDatabase(name string) error
	DropDatabase(name string) error
	CreateSchema(database, schema string) error
	CreateTable(loc location.Location, schema TableSchema) (PrimaryTable, error)
}

type offlineSqlStoreTester interface {
	offlineSqlStoreCoreTester
	OfflineStore
}

type offlineSqlStoreDatasetTester interface {
	offlineSqlStoreCoreTester
	OfflineStoreDataset
}

type offlineMaterializationSqlStoreTester interface {
	offlineSqlStoreCoreTester
	OfflineStoreDataset
	OfflineStoreMaterialization
}

type offlineTrainingSetSqlStoreTester interface {
	offlineSqlStoreCoreTester
	OfflineStoreDataset
	OfflineStoreMaterialization
	OfflineStoreTrainingSet
}
