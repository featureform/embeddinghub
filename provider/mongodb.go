// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider

import (
	"context"
	"fmt"
	pl "github.com/featureform/provider/location"

	"github.com/featureform/fferr"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/provider/types"
	sn "github.com/mrz1836/go-sanitize"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

type mongoDBMetadataRow struct {
	Name string
	T    string
}

type mongoDBOnlineStore struct {
	client          *mongo.Client
	database        string
	tableThroughput int
	BaseProvider
}

type mongoDBOnlineTable struct {
	client    *mongo.Client
	database  string
	name      string
	valueType types.ValueType
}

func mongoOnlineStoreFactory(serialized pc.SerializedConfig) (Provider, error) {
	mongoConfig := &pc.MongoDBConfig{}
	if err := mongoConfig.Deserialize(serialized); err != nil {
		return nil, err
	}

	return NewMongoDBOnlineStore(mongoConfig)
}

func NewMongoDBOnlineStore(config *pc.MongoDBConfig) (*mongoDBOnlineStore, error) {
	uri := fmt.Sprintf("mongodb://%s:%s@%s:%s/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000", config.Username, config.Password, config.Host, config.Port)
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(uri))
	if err != nil {
		return nil, fferr.NewConnectionError(pt.MongoDBOnline.String(), err)
	}
	cur, err := client.Database(config.Database).ListCollections(context.TODO(), bson.D{{Key: "name", Value: "featureform__metadata"}})
	if err != nil {
		return nil, fferr.NewExecutionError(pt.MongoDBOnline.String(), err)
	}
	var res []interface{}
	err = cur.All(context.TODO(), &res)
	if err != nil {
		return nil, fferr.NewInternalError(err)
	}
	if len(res) == 0 {
		command := bson.D{{Key: "customAction", Value: "CreateCollection"}, {Key: "collection", Value: "featureform__metadata"}, {Key: "autoScaleSettings", Value: bson.D{{Key: "maxThroughput", Value: 1000}}}}
		var cmdResult interface{}
		wConcern := writeconcern.New(writeconcern.J(true), writeconcern.WMajority())
		err := client.Database(config.Database, &options.DatabaseOptions{
			WriteConcern: wConcern,
		}).RunCommand(context.TODO(), command).Decode(&cmdResult)
		if err != nil {
			return nil, fferr.NewExecutionError(pt.MongoDBOnline.String(), err)
		}
	}

	return &mongoDBOnlineStore{
		client:          client,
		database:        config.Database,
		tableThroughput: config.Throughput,
		BaseProvider: BaseProvider{
			ProviderType:   pt.MongoDBOnline,
			ProviderConfig: config.Serialized(),
		},
	}, nil
}

func (store *mongoDBOnlineStore) AsOnlineStore() (OnlineStore, error) {
	return store, nil
}

func (store *mongoDBOnlineStore) Close() error {
	err := store.client.Disconnect(context.TODO())
	if err != nil {
		return fferr.NewConnectionError(pt.MongoDBOnline.String(), err)
	}
	return nil
}

func (store *mongoDBOnlineStore) GetTableName(feature, variant string) string {
	tableName := fmt.Sprintf("featureform__%s__%s", sn.Custom(feature, "[^a-zA-Z0-9_]"), sn.Custom(variant, "[^a-zA-Z0-9_]"))
	return tableName
}

func (store *mongoDBOnlineStore) GetMetadataTableName() string {
	return "featureform__metadata"
}

func (store *mongoDBOnlineStore) CreateTable(feature, variant string, valueType types.ValueType) (OnlineStoreTable, error) {
	tableName := store.GetTableName(feature, variant)
	vType := string(valueType.Scalar())
	getTable, _ := store.GetTable(feature, variant)
	if getTable != nil {
		return nil, fferr.NewDatasetAlreadyExistsError(feature, variant, nil)
	}

	metadataTableName := store.GetMetadataTableName()
	wConcern := writeconcern.New(writeconcern.J(true), writeconcern.WMajority())
	_, err := store.client.Database(store.database, &options.DatabaseOptions{
		WriteConcern: wConcern,
	}).Collection(metadataTableName).InsertOne(context.TODO(), mongoDBMetadataRow{tableName, vType})
	if err != nil {
		wrapped := fferr.NewResourceExecutionError(pt.MongoDBOnline.String(), feature, variant, fferr.FEATURE_VARIANT, err)
		wrapped.AddDetail("table_name", tableName)
		return nil, wrapped
	}

	command := bson.D{{Key: "customAction", Value: "CreateCollection"}, {Key: "collection", Value: tableName}, {Key: "autoScaleSettings", Value: bson.D{{Key: "maxThroughput", Value: store.tableThroughput}}}}
	var cmdResult interface{}
	err = store.client.Database(store.database).RunCommand(context.TODO(), command).Decode(&cmdResult)
	if err != nil {
		wrapped := fferr.NewResourceExecutionError(pt.MongoDBOnline.String(), feature, variant, fferr.FEATURE_VARIANT, err)
		wrapped.AddDetail("table_name", tableName)
		return nil, wrapped
	}

	table := &mongoDBOnlineTable{
		client:    store.client,
		database:  store.database,
		name:      tableName,
		valueType: valueType,
	}

	return table, nil
}

func (store *mongoDBOnlineStore) GetTable(feature, variant string) (OnlineStoreTable, error) {
	tableName := store.GetTableName(feature, variant)
	cur, err := store.client.Database(store.database).ListCollections(context.TODO(), bson.D{{Key: "name", Value: tableName}})
	if err != nil {
		wrapped := fferr.NewResourceExecutionError(pt.MongoDBOnline.String(), feature, variant, fferr.FEATURE_VARIANT, err)
		wrapped.AddDetail("table_name", tableName)
		return nil, wrapped
	}
	var res []interface{}
	err = cur.All(context.TODO(), &res)
	if err != nil {
		wrapped := fferr.NewResourceExecutionError(pt.MongoDBOnline.String(), feature, variant, fferr.FEATURE_VARIANT, err)
		wrapped.AddDetail("table_name", tableName)
		return nil, wrapped
	}
	if len(res) == 0 {
		return nil, fferr.NewDatasetNotFoundError(feature, variant, nil)
	}

	var row mongoDBMetadataRow
	err = store.client.Database(store.database).Collection(store.GetMetadataTableName()).FindOne(context.TODO(), bson.D{{Key: "name", Value: tableName}}).Decode(&row)
	if err != nil {
		wrapped := fferr.NewResourceExecutionError(pt.MongoDBOnline.String(), feature, variant, fferr.FEATURE_VARIANT, err)
		wrapped.AddDetail("table_name", tableName)
		return nil, wrapped
	}
	table := &mongoDBOnlineTable{
		client:    store.client,
		database:  store.database,
		name:      tableName,
		valueType: types.ScalarType(row.T),
	}
	return table, nil
}

func (store *mongoDBOnlineStore) DeleteTable(feature, variant string) error {
	tableName := store.GetTableName(feature, variant)
	err := store.client.Database(store.database).Collection(tableName).Drop(context.TODO())
	if err != nil {
		wrapped := fferr.NewResourceExecutionError(pt.MongoDBOnline.String(), feature, variant, fferr.FEATURE_VARIANT, err)
		wrapped.AddDetail("table_name", tableName)
		return wrapped
	}
	_, err = store.client.Database(store.database).Collection(store.GetMetadataTableName()).DeleteOne(context.TODO(), bson.D{{Key: "name", Value: tableName}})
	if err != nil {
		wrapped := fferr.NewResourceExecutionError(pt.MongoDBOnline.String(), feature, variant, fferr.FEATURE_VARIANT, err)
		wrapped.AddDetail("table_name", tableName)
		return wrapped
	}
	return nil
}

func (store *mongoDBOnlineStore) CheckHealth() (bool, error) {
	return false, fferr.NewInternalError(fmt.Errorf("not implemented"))
}

func (store mongoDBOnlineStore) Delete(location pl.Location) error {
	return fferr.NewInternalErrorf("delete not implemented")
}
func (table mongoDBOnlineTable) Set(entity string, value interface{}) error {
	upsert := true
	_, err := table.client.Database(table.database).
		Collection(table.name).
		UpdateOne(
			context.TODO(),
			bson.D{{Key: "entity", Value: entity}},
			bson.D{{Key: "$set", Value: bson.D{{Key: "entity", Value: entity}, {Key: "value", Value: value}}}},
			&options.UpdateOptions{
				Upsert: &upsert,
			},
		)
	if err != nil {
		wrapped := fferr.NewResourceExecutionError(pt.MongoDBOnline.String(), entity, "", fferr.ENTITY, err)
		wrapped.AddDetail("table", table.name)
		return wrapped
	}
	return nil
}

func (table mongoDBOnlineTable) Get(entity string) (interface{}, error) {
	type tableRow struct {
		ID     primitive.ObjectID `bson:"_id"`
		Entity string             `bson:"entity"`
		Value  interface{}        `bson:"value"`
	}
	var row tableRow
	err := table.client.Database(table.database).Collection(table.name).FindOne(context.TODO(), bson.D{{Key: "entity", Value: entity}}).Decode(&row)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			fmt.Printf("could not get table value: %s: %s: %s", table.name, entity, err.Error())
			wrapped := fferr.NewEntityNotFoundError("", "", entity, nil)
			wrapped.AddDetail("table", table.name)
			return nil, wrapped
		}
		wrapped := fferr.NewResourceExecutionError(pt.MongoDBOnline.String(), entity, "", fferr.ENTITY, err)
		wrapped.AddDetail("table", table.name)
		return nil, wrapped
	}

	switch table.valueType {
	case types.Int:
		return int(row.Value.(int32)), nil
	case types.Int64:
		return row.Value.(int64), nil
	case types.Float32:
		return float32(row.Value.(float64)), nil
	case types.Float64:
		return row.Value.(float64), nil
	case types.Bool:
		return row.Value.(bool), nil
	case types.String, types.NilType:
		return row.Value.(string), nil
	default:
		return nil, fferr.NewDataTypeNotFoundErrorf(table.valueType, "could not get table value")
	}

}
