// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider

import (
	"fmt"
	pl "github.com/featureform/provider/location"

	"github.com/featureform/fferr"
	fs "github.com/featureform/filestore"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/provider/types"
)

var cassandraTypeMap = map[string]string{
	"string":  "text",
	"int":     "int",
	"int64":   "bigint",
	"float32": "float",
	"float64": "double",
	"bool":    "boolean",
}

func GetOnlineStore(t pt.Type, c pc.SerializedConfig) (OnlineStore, error) {
	provider, err := Get(t, c)
	if err != nil {
		return nil, err
	}
	store, err := provider.AsOnlineStore()
	if err != nil {
		return nil, err
	}
	return store, nil
}

type OnlineStore interface {
	GetTable(feature, variant string) (OnlineStoreTable, error)
	CreateTable(feature, variant string, valueType types.ValueType) (OnlineStoreTable, error)
	DeleteTable(feature, variant string) error
	Close() error
	Provider
}

type ImportID string

type Import interface {
	Status() string
	ErrorMessage() string
}

// This interface provides an abstraction for online stores that offer
// bulk creation via import and was created to avoid having to make specific
// online store implementation public for the purpose of calling specialized
// methods on them. Currently, DynamoDB is the only online store that implements
// this interface for the purpose of support the S3 import feature.
type ImportableOnlineStore interface {
	OnlineStore
	ImportTable(feature, variant string, valueType types.ValueType, source fs.Filepath) (ImportID, error)
	GetImport(id ImportID) (Import, error)
}

type OnlineStoreTable interface {
	Set(entity string, value interface{}) error
	Get(entity string) (interface{}, error)
}

type VectorStore interface {
	CreateIndex(feature, variant string, vectorType types.VectorType) (VectorStoreTable, error)
	DeleteIndex(feature, variant string) error
	OnlineStore
}

type VectorStoreTable interface {
	OnlineStoreTable
	Nearest(feature, variant string, vector []float32, k int32) ([]string, error)
}

type BatchOnlineTable interface {
	OnlineStoreTable
	BatchSet([]SetItem) error
	MaxBatchSize() (int, error)
}

type SetItem struct {
	Entity string
	Value  interface{}
}

type tableKey struct {
	feature, variant string
}

func localOnlineStoreFactory(pc.SerializedConfig) (Provider, error) {
	return NewLocalOnlineStore(), nil
}

type localOnlineStore struct {
	tables map[tableKey]localOnlineTable
	BaseProvider
}

func NewLocalOnlineStore() *localOnlineStore {
	return &localOnlineStore{
		make(map[tableKey]localOnlineTable),
		BaseProvider{
			ProviderType:   pt.LocalOnline,
			ProviderConfig: []byte{},
		},
	}
}

func (store *localOnlineStore) AsOnlineStore() (OnlineStore, error) {
	return store, nil
}

func (store *localOnlineStore) GetTable(feature, variant string) (OnlineStoreTable, error) {
	table, has := store.tables[tableKey{feature, variant}]
	if !has {
		wrapped := fferr.NewDatasetNotFoundError(feature, variant, nil)
		wrapped.AddDetail("provider", store.ProviderType.String())
		return nil, wrapped
	}
	return table, nil
}

func (store *localOnlineStore) CreateTable(feature, variant string, valueType types.ValueType) (OnlineStoreTable, error) {
	key := tableKey{feature, variant}
	if _, has := store.tables[key]; has {
		wrapped := fferr.NewDatasetAlreadyExistsError(feature, variant, nil)
		wrapped.AddDetail("provider", store.ProviderType.String())
		return nil, wrapped
	}
	table := make(localOnlineTable)
	store.tables[key] = table
	return table, nil
}

func (store *localOnlineStore) DeleteTable(feaute, variant string) error {
	return nil
}

func (store *localOnlineStore) Close() error {
	return nil
}

func (store *localOnlineStore) CheckHealth() (bool, error) {
	return false, fmt.Errorf("provider health check not implemented")
}

func (store localOnlineStore) Delete(location pl.Location) error {
	return fferr.NewInternalErrorf("delete not implemented")
}

type localOnlineTable map[string]interface{}

func (table localOnlineTable) Set(entity string, value interface{}) error {
	table[entity] = value
	return nil
}

func (table localOnlineTable) Get(entity string) (interface{}, error) {
	val, has := table[entity]
	if !has {
		return nil, fferr.NewEntityNotFoundError("", "", entity, nil)
	}
	return val, nil
}
