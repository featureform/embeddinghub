// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	pl "github.com/featureform/provider/location"
	"strconv"
	"time"

	"github.com/featureform/fferr"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/provider/types"

	"github.com/redis/rueidis"
)

type redisTableKey struct {
	Prefix, Feature, Variant string
}

func (t redisTableKey) String() string {
	marshalled, _ := json.Marshal(t)
	return string(marshalled)
}

type redisOnlineStore struct {
	client rueidis.Client
	prefix string
	BaseProvider
}

func redisOnlineStoreFactory(serialized pc.SerializedConfig) (Provider, error) {
	redisConfig := &pc.RedisConfig{}
	if err := redisConfig.Deserialize(serialized); err != nil {
		return nil, err
	}
	if redisConfig.Prefix == "" {
		redisConfig.Prefix = "Featureform_table__"
	}
	return NewRedisOnlineStore(redisConfig)
}

func NewRedisOnlineStore(options *pc.RedisConfig) (*redisOnlineStore, error) {
	redisOptions := rueidis.ClientOption{
		InitAddress: []string{options.Addr},
		Password:    options.Password,
		SelectDB:    options.DB,
		/*
			The rueidis client opts-in to server-assisted client-side caching by default.
			Given we're not making use of this feature (i.e. via the commands `DoCach` or
			`DoMultiCache`), and given this is normally a feature that a user would have to
			explicitly opt into for a connection via the Redis command `CLIENT TRACKING ON`,
			we disable this feature by default to avoid any issues with Redis servers versions
			under 6.0.

			See details here:
				* rueidis client-side caching: https://github.com/redis/rueidis#client-side-caching
				* Redis CLIENT TRACKING command: https://redis.io/commands/client-tracking/
		*/
		DisableCache: true,
	}
	redisClient, err := rueidis.NewClient(redisOptions)
	if err != nil {
		wrapped := fferr.NewConnectionError(pt.RedisOnline.String(), err)
		wrapped.AddDetail("action", "client initialization")
		wrapped.AddDetail("addr", options.Addr)
		return nil, wrapped
	}
	return &redisOnlineStore{redisClient, options.Prefix, BaseProvider{
		ProviderType:   pt.RedisOnline,
		ProviderConfig: options.Serialized(),
	},
	}, nil
}

func (store *redisOnlineStore) AsOnlineStore() (OnlineStore, error) {
	return store, nil
}

func (store *redisOnlineStore) Close() error {
	store.client.Close()
	return nil
}

func (store *redisOnlineStore) GetTable(feature, variant string) (OnlineStoreTable, error) {
	key := redisTableKey{store.prefix, feature, variant}
	cmd := store.client.B().
		Hget().
		Key(fmt.Sprintf("%s__tables", store.prefix)).
		Field(key.String()).
		Build()
	vType, err := store.client.Do(context.TODO(), cmd).ToString()
	if err != nil && rueidis.IsRedisNil(err) {
		return nil, fferr.NewDatasetNotFoundError(feature, variant, err)
	} else if err != nil {
		return nil, fferr.NewResourceExecutionError(store.ProviderType.String(), feature, variant, fferr.FEATURE_VARIANT, err)
	}
	var table OnlineStoreTable
	// This maintains backwards compatibility with the previous implementation,
	// which wrote the scalar type string as the value to the field under the
	// tables hash.
	if _, isScalarString := types.ScalarTypes[types.ScalarType(vType)]; isScalarString {
		return &redisOnlineTable{
			client:    store.client,
			key:       key,
			valueType: types.ScalarType(vType),
		}, nil
	}
	valueTypeJSON := &types.ValueTypeJSONWrapper{}
	err = json.Unmarshal([]byte(vType), valueTypeJSON)
	if err != nil {
		wrapped := fferr.NewInternalError(err)
		wrapped.AddDetail("value_type", vType)
		return nil, wrapped
	}
	switch valueTypeJSON.ValueType.(type) {
	case types.VectorType:
		table = &redisOnlineIndex{
			client: store.client,
			key: redisIndexKey{
				Prefix:  store.prefix,
				Feature: feature,
				Variant: variant,
			},
			valueType: valueTypeJSON.ValueType,
		}
	case types.ScalarType:
		table = &redisOnlineTable{
			client:    store.client,
			key:       key,
			valueType: valueTypeJSON.ValueType,
		}
	default:
		return nil, fferr.NewInvalidArgumentError(fmt.Errorf("unknown value type: %T", valueTypeJSON.ValueType))
	}
	return table, nil
}

func (store *redisOnlineStore) CreateTable(feature, variant string, valueType types.ValueType) (OnlineStoreTable, error) {
	key := redisTableKey{store.prefix, feature, variant}
	cmd := store.client.B().
		Hexists().
		Key(fmt.Sprintf("%s__tables", store.prefix)).
		Field(key.String()).
		Build()
	exists, err := store.client.Do(context.TODO(), cmd).AsBool()
	if err != nil {
		return nil, fferr.NewResourceExecutionError(store.ProviderType.String(), feature, variant, fferr.FEATURE_VARIANT, err)
	}
	if exists {
		return nil, fferr.NewDatasetAlreadyExistsError(feature, variant, nil)
	}
	serialized, err := json.Marshal(types.ValueTypeJSONWrapper{valueType})
	if err != nil {
		wrapped := fferr.NewInternalError(err)
		wrapped.AddDetail("value_type", fmt.Sprintf("%v", valueType))
		return nil, wrapped
	}
	cmd = store.client.B().
		Hset().
		Key(fmt.Sprintf("%s__tables", store.prefix)).
		FieldValue().
		FieldValue(key.String(), string(serialized)).
		Build()
	if resp := store.client.Do(context.TODO(), cmd); resp.Error() != nil {
		return nil, fferr.NewResourceExecutionError(store.ProviderType.String(), feature, variant, fferr.FEATURE_VARIANT, resp.Error())
	}
	var table OnlineStoreTable
	switch valueType.(type) {
	case types.VectorType:
		table = &redisOnlineIndex{
			client: store.client,
			key: redisIndexKey{
				Prefix:  store.prefix,
				Feature: feature,
				Variant: variant,
			},
			valueType: valueType,
		}
	case types.ScalarType:
		table = &redisOnlineTable{
			client:    store.client,
			key:       key,
			valueType: valueType,
		}
	default:
		return nil, fferr.NewInvalidArgumentError(fmt.Errorf("unknown value type: %T", valueType))
	}
	return table, nil
}

func (store *redisOnlineStore) DeleteTable(feature, variant string) error {
	return nil
}

func (store *redisOnlineStore) CheckHealth() (bool, error) {
	cmd := store.client.B().Ping().Build()
	resp, err := store.client.Do(context.Background(), cmd).ToString()
	if err != nil {
		wrapped := fferr.NewConnectionError(pt.RedisOnline.String(), err)
		wrapped.AddDetail("action", "ping")
		return false, wrapped
	}
	if resp != "PONG" {
		wrapped := fferr.NewConnectionError(pt.RedisOnline.String(), fmt.Errorf("expected 'PONG' from Redis server; received: %s", resp))
		wrapped.AddDetail("action", "ping")
		return false, wrapped
	}
	return true, nil
}

func (store redisOnlineStore) Delete(location pl.Location) error {
	return fferr.NewInternalErrorf("delete not implemented")
}

func (store *redisOnlineStore) CreateIndex(feature, variant string, vectorType types.VectorType) (VectorStoreTable, error) {
	key := redisIndexKey{Prefix: store.prefix, Feature: feature, Variant: variant}
	cmd, err := store.createIndexCmd(key, vectorType)
	if err != nil {
		return nil, fferr.NewResourceExecutionError(store.ProviderType.String(), feature, variant, fferr.FEATURE_VARIANT, err)
	}
	resp := store.client.Do(context.Background(), cmd)
	if resp.Error() != nil {
		return &redisOnlineIndex{}, fferr.NewResourceExecutionError(store.ProviderType.String(), feature, variant, fferr.FEATURE_VARIANT, resp.Error())
	}
	table := &redisOnlineIndex{client: store.client, key: key, valueType: vectorType}
	return table, nil
}

// TODO: Implement index deletion
func (store *redisOnlineStore) DeleteIndex(feature, variant string) error {
	return nil
}

func (store *redisOnlineStore) createIndexCmd(key redisIndexKey, vectorType types.VectorType) (rueidis.Completed, error) {
	serializedKey, err := key.serialize("")
	if err != nil {
		return rueidis.Completed{}, err
	}
	requiredParams := []string{
		"TYPE", "FLOAT32",
		"DIM", strconv.FormatUint(uint64(vectorType.Dimension), 10),
		"DISTANCE_METRIC", "COSINE",
	}
	return store.client.B().
		FtCreate().
		Index(string(serializedKey)).
		Schema().
		FieldName(key.getVectorField()).
		Vector("HNSW", int64(len(requiredParams)), requiredParams...).
		Build(), nil
}

type redisOnlineTable struct {
	client    rueidis.Client
	key       redisTableKey
	valueType types.ValueType
}

func (table redisOnlineTable) Set(entity string, value interface{}) error {
	switch v := value.(type) {
	case nil:
		value = "nil"
	case string:
		value = v
	case int:
		value = strconv.Itoa(v)
	case int32:
		value = strconv.FormatInt(int64(v), 10)
	case int64:
		value = strconv.FormatInt(v, 10)
	case float32:
		value = strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		value = strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		// The previous Redis client stored booleans as 1 or 0; to maintain backwards compatibility
		// we do the same here, stringifying the value to satisfy the interface. See redis_test.go
		// lines 59-66 for more reasons why we do this.
		if v {
			value = "1"
		} else {
			value = "0"
		}
	case time.Time:
		value = v.Format(time.RFC3339)
	case []float32:
		value = rueidis.VectorString32(v)
	default:
		return fferr.NewDataTypeNotFoundErrorf(value, "unsupported data type")
	}
	cmd := table.client.B().
		Hset().
		Key(table.key.String()).
		FieldValue().
		FieldValue(entity, value.(string)).
		Build()
	res := table.client.Do(context.TODO(), cmd)
	if res.Error() != nil {
		wrapped := fferr.NewResourceExecutionError(pt.RedisOnline.String(), table.key.Feature, table.key.Variant, fferr.ENTITY, res.Error())
		wrapped.AddDetail("entity", entity)
		return wrapped
	}
	return nil
}

func (table redisOnlineTable) Get(entity string) (interface{}, error) {
	cmd := table.client.B().
		Hget().
		Key(table.key.String()).
		Field(entity).
		Build()
	resp := table.client.Do(context.TODO(), cmd)
	if resp.Error() != nil {
		return nil, fferr.NewEntityNotFoundError(table.key.Feature, table.key.Variant, entity, resp.Error())
	}
	var err error
	var result interface{}
	val, err := resp.ToString()
	if err != nil {
		return nil, fferr.NewResourceExecutionError(pt.RedisOnline.String(), table.key.Feature, table.key.Variant, fferr.ENTITY, err)
	}
	if table.valueType.IsVector() {
		return rueidis.ToVector32(val), nil
	}
	switch table.valueType {
	case types.NilType, types.String:
		result, err = val, nil
	case types.Int:
		result, err = strconv.Atoi(val)
	case types.Int32:
		if result, err = strconv.ParseInt(val, 10, 32); err == nil {
			result = int32(result.(int64))
		}
	case types.Int64:
		result, err = strconv.ParseInt(val, 10, 64)
	case types.Float32:
		if result, err = strconv.ParseFloat(val, 32); err == nil {
			result, err = float32(result.(float64)), nil
		}
	case types.Float64:
		result, err = strconv.ParseFloat(val, 64)
	case types.Bool:
		result, err = strconv.ParseBool(val)
	case types.Timestamp, types.Datetime: // Including `Datetime` here maintains compatibility with previously create timestamp tables
		// Maintains compatibility with go-redis implementation:
		// https://github.com/redis/go-redis/blob/v8.11.5/command.go#L939
		result, err = time.Parse(time.RFC3339Nano, val)
	default:
		result, err = val, nil
	}
	if err != nil {
		wrapped := fferr.NewInternalError(fmt.Errorf("could not cast value: %v to %s: %w", resp, table.valueType, err))
		wrapped.AddDetail("entity", entity)
		return nil, wrapped
	}
	return result, nil
}

type redisOnlineIndex struct {
	client    rueidis.Client
	key       redisIndexKey
	valueType types.ValueType
}

type redisIndexKey struct {
	Prefix, Feature, Variant, Entity string
}

func (k *redisIndexKey) serialize(entity string) ([]byte, error) {
	k.Entity = entity
	serialized, err := json.Marshal(k)
	if err != nil {
		wrapped := fferr.NewInternalError(err)
		wrapped.AddDetail("entity", entity)
		return nil, wrapped
	}
	return serialized, nil
}

func (k *redisIndexKey) deserialize(key []byte) error {
	err := json.Unmarshal(key, &k)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	return nil
}

func (k redisIndexKey) getVectorField() string {
	name_variant := fmt.Sprintf("%s_%s", k.Feature, k.Variant)
	// RawStdEncoding is necessary given the padding character is considered
	// a token character in RediSearch
	encoded := base64.RawStdEncoding.EncodeToString([]byte(name_variant))
	return fmt.Sprintf("vector_field_%s", encoded)
}

func (table redisOnlineIndex) Set(entity string, value interface{}) error {
	vector, ok := value.([]float32)
	if !ok {
		wrapped := fferr.NewDataTypeNotFoundErrorf(value, "value is not a vector")
		wrapped.AddDetail("entity", entity)
		return wrapped
	}
	serializedKey, err := table.key.serialize(entity)
	if err != nil {
		return err
	}
	cmd := table.client.B().
		Hset().
		Key(string(serializedKey)).
		FieldValue().
		FieldValue(table.key.getVectorField(), rueidis.VectorString32(vector)).
		Build()
	res := table.client.Do(context.TODO(), cmd)
	if res.Error() != nil {
		wrapped := fferr.NewResourceExecutionError(pt.RedisOnline.String(), table.key.Feature, table.key.Variant, fferr.ENTITY, res.Error())
		wrapped.AddDetail("entity", entity)
		return wrapped
	}
	return nil
}

func (table redisOnlineIndex) Get(entity string) (interface{}, error) {
	serializedKey, err := table.key.serialize(entity)
	if err != nil {
		return nil, err
	}
	cmd := table.client.B().
		Hget().
		Key(string(serializedKey)).
		Field(table.key.getVectorField()).
		Build()
	resp := table.client.Do(context.TODO(), cmd)
	if resp.Error() != nil {
		return nil, fferr.NewEntityNotFoundError(table.key.Feature, table.key.Variant, entity, resp.Error())
	}
	val, err := resp.ToString()
	if err != nil {
		return nil, fferr.NewResourceExecutionError(pt.RedisOnline.String(), table.key.Feature, table.key.Variant, fferr.ENTITY, err)
	}
	return rueidis.ToVector32(val), nil
}

func (table redisOnlineIndex) Nearest(feature, variant string, vector []float32, k int32) ([]string, error) {
	cmd, err := table.createNearestCmd(vector, k)
	if err != nil {
		return nil, err
	}
	_, docs, err := table.client.Do(context.Background(), cmd).AsFtSearch()
	if err != nil {
		return nil, fferr.NewResourceExecutionError(pt.RedisOnline.String(), feature, variant, fferr.FEATURE_VARIANT, err)
	}
	entities := make([]string, len(docs))
	for idx, doc := range docs {
		key := redisIndexKey{}
		err := key.deserialize([]byte(doc.Key))
		if err != nil {
			return nil, err
		}
		entities[idx] = key.Entity
	}
	return entities, nil
}

func (table redisOnlineIndex) createNearestCmd(vector []float32, k int32) (rueidis.Completed, error) {
	vectorField := table.key.getVectorField()
	serializedKey, err := table.key.serialize("")
	if err != nil {
		return rueidis.Completed{}, err
	}
	return table.client.B().
		FtSearch().
		Index(string(serializedKey)).
		Query(fmt.Sprintf("*=>[KNN $K @%s $BLOB]", vectorField)).
		Sortby(fmt.Sprintf("__%s_score", vectorField)).
		Params().
		Nargs(4).
		NameValue().
		NameValue("K", strconv.Itoa(int(k))).
		NameValue("BLOB", rueidis.VectorString32(vector)).
		Dialect(2).
		Build(), nil
}
