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
	pl "github.com/featureform/provider/location"

	"github.com/featureform/fferr"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/provider/types"
	"github.com/gocql/gocql"
	sn "github.com/mrz1836/go-sanitize"
)

type cassandraTableKey struct {
	Keyspace, Feature, Variant string
}

func (t cassandraTableKey) String() string {
	marshalled, err := json.Marshal(t)
	if err != nil {
		return err.Error()
	}
	return string(marshalled)
}

type cassandraOnlineStore struct {
	session  *gocql.Session
	keyspace string
	BaseProvider
}

type cassandraOnlineTable struct {
	session   *gocql.Session
	key       cassandraTableKey
	valueType types.ValueType
}

func cassandraOnlineStoreFactory(serialized pc.SerializedConfig) (Provider, error) {
	cassandraConfig := &pc.CassandraConfig{}
	if err := cassandraConfig.Deserialize(serialized); err != nil {
		return nil, err
	}
	if cassandraConfig.Keyspace == "" {
		cassandraConfig.Keyspace = "Featureform_table__"
	}

	return NewCassandraOnlineStore(cassandraConfig)
}

func NewCassandraOnlineStore(options *pc.CassandraConfig) (*cassandraOnlineStore, error) {
	cassandraCluster := gocql.NewCluster(options.Addr)
	cassandraCluster.Authenticator = gocql.PasswordAuthenticator{
		Username: options.Username,
		Password: options.Password,
	}
	err := cassandraCluster.Consistency.UnmarshalText([]byte(options.Consistency))
	if err != nil {
		return nil, fferr.NewExecutionError(pt.CassandraOnline.String(), err)
	}
	newSession, err := cassandraCluster.CreateSession()
	if err != nil {
		return nil, fferr.NewExecutionError(pt.CassandraOnline.String(), err)
	}

	query := fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class' : 'SimpleStrategy','replication_factor' : %d }", options.Keyspace, options.Replication)
	err = newSession.Query(query).WithContext(context.TODO()).Exec()
	if err != nil {
		return nil, fferr.NewExecutionError(pt.CassandraOnline.String(), err)
	}
	cassandraCluster.Keyspace = options.Keyspace

	query = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (tableName text PRIMARY KEY, tableType text)", fmt.Sprintf("%s.featureform__metadata", options.Keyspace))
	err = newSession.Query(query).WithContext(context.TODO()).Exec()
	if err != nil {
		return nil, fferr.NewExecutionError(pt.CassandraOnline.String(), err)
	}

	return &cassandraOnlineStore{newSession, options.Keyspace, BaseProvider{
		ProviderType:   pt.CassandraOnline,
		ProviderConfig: options.Serialized(),
	},
	}, nil
}

func (store *cassandraOnlineStore) AsOnlineStore() (OnlineStore, error) {
	return store, nil
}

func (store *cassandraOnlineStore) Close() error {
	store.session.Close()
	if !store.session.Closed() {
		return fferr.NewExecutionError(pt.CassandraOnline.String(), fmt.Errorf("could not close cassandra online store session"))
	}
	return nil
}

func GetTableName(keyspace, feature, variant string) string {
	tableName := fmt.Sprintf("%s.featureform__%s__%s", sn.Custom(keyspace, "[^a-zA-Z0-9_]"), sn.Custom(feature, "[^a-zA-Z0-9_]"), sn.Custom(variant, "[^a-zA-Z0-9_]"))
	return tableName
}

func GetMetadataTableName(keyspace string) string {
	metadataTableName := fmt.Sprintf("%s.featureform__metadata", keyspace)
	return metadataTableName
}

func (store *cassandraOnlineStore) CreateTable(feature, variant string, valueType types.ValueType) (OnlineStoreTable, error) {
	tableName := GetTableName(store.keyspace, feature, variant)
	vType := cassandraTypeMap[string(valueType.Scalar())]
	key := cassandraTableKey{store.keyspace, feature, variant}
	table, _ := store.GetTable(feature, variant)
	if table != nil {
		return nil, fferr.NewDatasetAlreadyExistsError(feature, variant, nil)
	}
	if table != nil {
		wrapped := fferr.NewDatasetAlreadyExistsError(feature, variant, nil)
		wrapped.AddDetail("provider", store.ProviderType.String())
		return nil, wrapped
	}

	metadataTableName := GetMetadataTableName(store.keyspace)
	query := fmt.Sprintf("INSERT INTO %s (tableName, tableType) VALUES (?, ?)", metadataTableName)
	err := store.session.Query(query, tableName, string(valueType.Scalar())).WithContext(context.TODO()).Exec()
	if err != nil {
		wrapped := fferr.NewResourceExecutionError(pt.CassandraOnline.String(), feature, variant, fferr.FEATURE_VARIANT, err)
		wrapped.AddDetail("table_name", tableName)
		return nil, wrapped
	}

	query = fmt.Sprintf("CREATE TABLE %s (entity text PRIMARY KEY, value %s)", tableName, vType)
	err = store.session.Query(query).WithContext(context.TODO()).Exec()
	if err != nil {
		wrapped := fferr.NewResourceExecutionError(pt.CassandraOnline.String(), feature, variant, fferr.FEATURE_VARIANT, err)
		wrapped.AddDetail("table_name", tableName)
		return nil, wrapped
	}

	return &cassandraOnlineTable{
		session:   store.session,
		key:       key,
		valueType: valueType,
	}, nil
}

func (store *cassandraOnlineStore) GetTable(feature, variant string) (OnlineStoreTable, error) {
	tableName := GetTableName(store.keyspace, feature, variant)
	key := cassandraTableKey{store.keyspace, feature, variant}

	var vType string
	metadataTableName := GetMetadataTableName(store.keyspace)
	query := fmt.Sprintf("SELECT tableType FROM %s WHERE tableName = '%s'", metadataTableName, tableName)
	err := store.session.Query(query).WithContext(context.TODO()).Scan(&vType)
	if err == gocql.ErrNotFound {
		wrapped := fferr.NewDatasetNotFoundError(feature, variant, nil)
		wrapped.AddDetail("provider", store.ProviderType.String())
		return nil, wrapped
	}
	if err != nil {
		wrapped := fferr.NewResourceExecutionError(store.ProviderType.String(), feature, variant, fferr.FEATURE_VARIANT, err)
		wrapped.AddDetail("table_name", tableName)
		return nil, wrapped
	}

	table := &cassandraOnlineTable{
		session:   store.session,
		key:       key,
		valueType: types.ScalarType(vType),
	}

	return table, nil
}

func (store *cassandraOnlineStore) DeleteTable(feature, variant string) error {
	tableName := GetTableName(store.keyspace, feature, variant)
	metadataTableName := GetMetadataTableName(store.keyspace)
	query := fmt.Sprintf("DELETE FROM %s WHERE tableName = '%s' IF EXISTS", metadataTableName, tableName)
	err := store.session.Query(query).WithContext(context.TODO()).Exec()
	if err != nil {
		wrapped := fferr.NewResourceExecutionError(store.ProviderType.String(), feature, variant, fferr.FEATURE_VARIANT, err)
		wrapped.AddDetail("table_name", tableName)
		return wrapped
	}
	query = fmt.Sprintf("DROP TABLE [IF EXISTS] %s", tableName)
	err = store.session.Query(query).WithContext(context.TODO()).Exec()
	if err != nil {
		wrapped := fferr.NewResourceExecutionError(store.ProviderType.String(), feature, variant, fferr.FEATURE_VARIANT, err)
		wrapped.AddDetail("table_name", tableName)
		return wrapped
	}

	return nil
}

func (store *cassandraOnlineStore) CheckHealth() (bool, error) {
	return false, fferr.NewInternalError(fmt.Errorf("not implemented"))
}

func (store cassandraOnlineStore) Delete(location pl.Location) error {
	return fferr.NewInternalErrorf("delete not implemented")
}

func (table cassandraOnlineTable) Set(entity string, value interface{}) error {
	key := table.key
	tableName := GetTableName(key.Keyspace, key.Feature, key.Variant)

	query := fmt.Sprintf("INSERT INTO %s (entity, value) VALUES (?, ?)", tableName)
	err := table.session.Query(query, entity, value).WithContext(context.TODO()).Exec()
	if err != nil {
		wrapped := fferr.NewResourceExecutionError(pt.CassandraOnline.String(), entity, "", fferr.ENTITY, err)
		wrapped.AddDetail("table_name", tableName)
		return wrapped
	}

	return nil
}

func (table cassandraOnlineTable) Get(entity string) (interface{}, error) {

	key := table.key
	tableName := GetTableName(key.Keyspace, key.Feature, key.Variant)

	var ptr interface{}
	switch table.valueType {
	case types.Int:
		ptr = new(int)
	case types.Int64:
		ptr = new(int64)
	case types.Float32:
		ptr = new(float32)
	case types.Float64:
		ptr = new(float64)
	case types.Bool:
		ptr = new(bool)
	case types.String, types.NilType:
		ptr = new(string)
	default:
		return nil, fferr.NewDataTypeNotFoundErrorf(table.valueType, "could not determine column type")
	}

	query := fmt.Sprintf("SELECT value FROM %s WHERE entity = '%s'", tableName, entity)
	err := table.session.Query(query).WithContext(context.TODO()).Scan(ptr)
	if err == gocql.ErrNotFound {
		wrapped := fferr.NewEntityNotFoundError(key.Feature, key.Variant, entity, nil)
		wrapped.AddDetail("table_name", tableName)
		return nil, wrapped
	}
	if err != nil {
		wrapped := fferr.NewExecutionError(pt.CassandraOnline.String(), err)
		wrapped.AddDetail("table_name", tableName)
		return nil, wrapped
	}

	var val interface{}
	switch casted := ptr.(type) {
	case *int:
		val = *casted
	case *int64:
		val = *casted
	case *float32:
		val = *casted
	case *float64:
		val = *casted
	case *bool:
		val = *casted
	case *string:
		val = *casted
	default:
		return nil, fferr.NewDataTypeNotFoundErrorf(table.valueType, "could not determine column type")
	}
	return val, nil

}
