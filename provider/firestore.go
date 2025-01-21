// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package provider

import (
	"cloud.google.com/go/firestore"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/featureform/fferr"
	"github.com/featureform/logging"
	pl "github.com/featureform/provider/location"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	se "github.com/featureform/provider/serialization"
	vt "github.com/featureform/provider/types"
	"go.uber.org/zap"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"time"
)

const (
	// valueKey represents the key for the feature in a document.
	valueKey = "value"
)

// The serializer versions. If adding a new one make sure to add to the serializers map variable
const (
	// firestoreSerializeV0 serializes everything as strings, including numbers
	firestoreSerializeV0 se.SerializeVersion = iota
)

type firestoreSerializerV0 struct{}

func (ser firestoreSerializerV0) Version() se.SerializeVersion { return firestoreSerializeV0 }

func (ser firestoreSerializerV0) Serialize(_ vt.ValueType, value any) (interface{}, error) {
	// The client automatically handles this for us.
	return value, nil
}

func (ser firestoreSerializerV0) Deserialize(t vt.ValueType, value interface{}) (any, error) {
	// Firestore only has one integer and float type, each being converted into int64 and float64 respectively.
	// For conversions, see https://pkg.go.dev/cloud.google.com/go/firestore@v1.15.0#DocumentSnapshot.DataTo
	switch t {
	case vt.Int:
		if v, ok := value.(int64); ok {
			return int(v), nil
		} else {
			return nil, fmt.Errorf("expected int64 value but got %T", value)
		}
	case vt.Int8:
		if v, ok := value.(int64); ok {
			return int8(v), nil
		} else {
			return nil, fmt.Errorf("expected int64 value but got %T", value)
		}
	case vt.Int16:
		if v, ok := value.(int64); ok {
			return int16(v), nil
		} else {
			return nil, fmt.Errorf("expected int64 value but got %T", value)
		}
	case vt.Int32:
		if v, ok := value.(int64); ok {
			return int32(v), nil
		} else {
			return nil, fmt.Errorf("expected int64 value but got %T", value)
		}
	case vt.Int64:
		if v, ok := value.(int64); ok {
			return v, nil
		} else {
			return nil, fmt.Errorf("expected int64 value but got %T", value)
		}
	case vt.Float32:
		if v, ok := value.(float64); ok {
			return float32(v), nil
		} else {
			return nil, fmt.Errorf("expected float64 value but got %T", value)
		}
	case vt.Float64:
		if v, ok := value.(float64); ok {
			return v, nil
		} else {
			return nil, fmt.Errorf("expected float64 value but got %T", value)
		}
	case vt.NilType:
		if value == nil {
			return value, nil
		} else {
			return nil, fmt.Errorf("expected nil value but got %T", value)
		}
	case vt.Bool:
		if v, ok := value.(bool); ok {
			return v, nil
		} else {
			return nil, fmt.Errorf("expected bool value but got %T", value)
		}
	case vt.String:
		if v, ok := value.(string); ok {
			return v, nil
		} else {
			return nil, fmt.Errorf("expected string value but got %T", value)
		}
	case vt.Timestamp:
		if v, ok := value.(time.Time); ok {
			return v, nil
		} else {
			return nil, fmt.Errorf("expected timestamp value but got %T", value)
		}
	case vt.Datetime:
		if v, ok := value.(time.Time); ok {
			return v, nil
		} else {
			return nil, fmt.Errorf("expected datetime value but got %T", value)
		}
	default:
		err := fferr.NewInternalErrorf("Unsupported casting value %v of type %T into %v", value, value, t)
		err.AddDetail("version", ser.Version().String())
		return nil, err
	}
}

type firestoreOnlineStore struct {
	client     *firestore.Client
	collection *firestore.CollectionRef
	logger     *zap.SugaredLogger
	BaseProvider
}

type firestoreOnlineTable struct {
	client     *firestore.Client
	collection *firestore.CollectionRef
	key        firestoreTableKey
	valueType  vt.ValueType
	serializer se.Serializer[interface{}]
	logger     *zap.SugaredLogger
}

type firestoreTableKey struct {
	Collection, Feature, Variant string
}

func (t firestoreTableKey) String() string {
	return fmt.Sprintf("%s__%s__%s", t.Collection, t.Feature, t.Variant)
}

func (t firestoreTableKey) CollectionString() string {
	return t.Collection
}

func (t firestoreTableKey) FeatureString() string {
	return t.Feature
}

func (t firestoreTableKey) VariantString() string {
	return t.Variant
}

func firestoreOnlineStoreFactory(serialized pc.SerializedConfig) (Provider, error) {
	firestoreConfig := &pc.FirestoreConfig{}
	if err := firestoreConfig.Deserialize(serialized); err != nil {
		return nil, err
	}

	return NewFirestoreOnlineStore(firestoreConfig)
}

func NewFirestoreOnlineStore(options *pc.FirestoreConfig) (*firestoreOnlineStore, error) {
	credBytes, err := json.Marshal(options.Credentials)
	if err != nil {
		return nil, fferr.NewInternalError(err)
	}
	firestoreClient, err := firestore.NewClient(context.TODO(), options.ProjectID, option.WithCredentialsJSON(credBytes))
	if err != nil {
		return nil, fferr.NewExecutionError(pt.FirestoreOnline.String(), err)
	}

	firestoreCollection := firestoreClient.Collection(options.Collection)
	_, err = firestoreCollection.Doc(GetMetadataTable()).Set(context.TODO(), map[string]interface{}{}, firestore.MergeAll)
	if err != nil {
		return nil, fferr.NewExecutionError(pt.FirestoreOnline.String(), err)
	}

	logger := logging.NewLogger("firestore")

	return &firestoreOnlineStore{
		firestoreClient,
		firestoreCollection,
		logger.SugaredLogger,
		BaseProvider{
			ProviderType:   pt.FirestoreOnline,
			ProviderConfig: options.Serialize(),
		},
	}, nil
}

func (store *firestoreOnlineStore) AsOnlineStore() (OnlineStore, error) {
	return store, nil
}

func (store *firestoreOnlineStore) Close() error {
	if err := store.client.Close(); err != nil {
		return fferr.NewExecutionError(pt.FirestoreOnline.String(), err)
	}
	return nil
}

func GetMetadataTable() string {
	return "featureform_firestore_metadata"
}

func (store *firestoreOnlineStore) GetTable(feature, variant string) (OnlineStoreTable, error) {
	key := firestoreTableKey{store.collection.ID, feature, variant}
	tableKey := key.String()
	featureName := key.FeatureString()
	variantName := key.VariantString()

	variantTable := store.collection.Doc(featureName).Collection(variantName)

	metadata, err := store.collection.Doc(GetMetadataTable()).Get(context.TODO())
	if err != nil {
		wrapped := fferr.NewResourceExecutionError(pt.FirestoreOnline.String(), feature, variant, fferr.FEATURE_VARIANT, err)
		wrapped.AddDetail("table_name", GetMetadataTable())
		return nil, wrapped
	}
	valueType, err := metadata.DataAt(tableKey)
	if err != nil {
		wrapped := fferr.NewDatasetNotFoundError(feature, variant, err)
		wrapped.AddDetail("table_key", tableKey)
		return nil, wrapped
	}

	logger := store.logger.With("table", tableKey)
	return &firestoreOnlineTable{
		client:     store.client,
		collection: variantTable,
		key:        key,
		valueType:  vt.ScalarType(valueType.(string)),
		serializer: firestoreSerializerV0{},
		logger:     logger,
	}, nil
}

func (store *firestoreOnlineStore) CreateTable(feature, variant string, valueType vt.ValueType) (OnlineStoreTable, error) {
	table, _ := store.GetTable(feature, variant)
	if table != nil {
		return nil, fferr.NewDatasetAlreadyExistsError(feature, variant, nil)
	}

	logger := store.logger.With("feature", feature, "variant", variant)

	key := firestoreTableKey{store.collection.ID, feature, variant}
	tableKey := key.String()
	featureName := key.FeatureString()
	variantName := key.VariantString()

	metadataDoc := store.collection.Doc(GetMetadataTable())
	newMetadataField := map[string]interface{}{
		tableKey: valueType,
	}

	// We want to check if there is a pre-existing feature variant already registered, and error out
	// if it exists. Only then do we create a reference to a new table.
	// We do this in a transaction to avoid race conditions between the checks and insertions.
	err := store.client.RunTransaction(context.TODO(), func(ctx context.Context, tx *firestore.Transaction) error {
		metadata, err := tx.Get(metadataDoc)

		// If it returns an error, check if the document is not found. If that's the case, then create a new
		// document with the new entry.
		if err != nil {
			if status.Code(err) != codes.NotFound {
				logger.Error("Error grabbing metadata table")
				return err
			}

			return tx.Set(metadataDoc, newMetadataField)
		}

		// Else, the document already exists, so first check to see if a table entry already exists.
		// Check if the key already exists
		if _, exists := metadata.Data()[tableKey]; exists {
			logger.Error("Table already exists")
			return fferr.NewDatasetAlreadyExistsError(feature, variant, nil)
		}

		// Add the new key-value pair
		return tx.Set(metadataDoc, newMetadataField, firestore.MergeAll)
	})

	if err != nil {
		return nil, fferr.NewResourceExecutionError(pt.FirestoreOnline.String(), feature, variant, fferr.FEATURE_VARIANT, err)
	}

	variantTable := store.collection.Doc(featureName).Collection(variantName)

	return &firestoreOnlineTable{
		client:     store.client,
		collection: variantTable,
		key:        key,
		valueType:  valueType,
		serializer: firestoreSerializerV0{},
		logger:     logger,
	}, nil

}

func (store *firestoreOnlineStore) DeleteTable(feature, variant string) error {
	key := firestoreTableKey{store.collection.ID, feature, variant}
	tableName := key.String()
	_, err := store.collection.Doc(tableName).Delete(context.TODO())
	if err != nil {
		return fferr.NewResourceExecutionError(pt.FirestoreOnline.String(), feature, variant, fferr.FEATURE_VARIANT, err)
	}

	_, err = store.collection.Doc(GetMetadataTable()).Update(context.TODO(), []firestore.Update{
		{
			Path:  tableName,
			Value: firestore.Delete,
		},
	})

	if err != nil {
		return fferr.NewResourceExecutionError(pt.FirestoreOnline.String(), feature, variant, fferr.FEATURE_VARIANT, err)
	}

	return nil
}

func (store *firestoreOnlineStore) CheckHealth() (bool, error) {
	// We need to check whether we can reach Firestore. The simplest way of doing this is
	// by getting a random collection (by calling `Collections().Next()`). However, this
	// is still able to throw an iterator.Done error if there are no collections,
	// so we just check for that.
	_, err := store.client.Collections(context.TODO()).Next()
	if err != nil && !errors.Is(err, iterator.Done) {
		store.logger.Error("Health check failed, unable to connect to firestore")
		return false, fferr.NewExecutionError(pt.FirestoreOnline.String(), err)
	}
	store.logger.Info("Health check successful")
	return true, nil
}

func (store *firestoreOnlineStore) Delete(location pl.Location) error {
	return fferr.NewInternalErrorf("delete not implemented")
}

func (table firestoreOnlineTable) Set(entity string, value interface{}) error {
	// Set is just a special case of batch writing.
	err := table.BatchSet([]SetItem{{
		Entity: entity,
		Value:  value,
	}})

	if err != nil {
		wrapped := fferr.NewResourceExecutionError(pt.FirestoreOnline.String(), table.key.Feature, table.key.Variant, fferr.ENTITY, err)
		wrapped.AddDetail("entity", entity)
		return wrapped
	}
	return nil
}

func (table firestoreOnlineTable) Get(entity string) (interface{}, error) {
	dataSnap, err := table.collection.Doc(entity).Get(context.TODO())
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, fferr.NewEntityNotFoundError(table.key.Feature, table.key.Variant, entity, err)
		}
		wrapped := fferr.NewResourceExecutionError(pt.FirestoreOnline.String(), table.key.Feature, table.key.Variant, fferr.ENTITY, err)
		wrapped.AddDetail("entity", entity)
		return nil, wrapped
	}
	value, err := dataSnap.DataAt(valueKey)
	if err != nil {
		wrapped := fferr.NewResourceExecutionError(pt.FirestoreOnline.String(), table.key.Feature, table.key.Variant, fferr.ENTITY, err)
		wrapped.AddDetail("entity", entity)
		return nil, wrapped
	}

	return table.serializer.Deserialize(table.valueType, value)
}

// maxFirestoreBatchSize is the max amount of items that can be written to Firestore at once.
const maxFirestoreBatchSize = 20

func (table firestoreOnlineTable) MaxBatchSize() (int, error) { return maxFirestoreBatchSize, nil }

func (table firestoreOnlineTable) BatchSet(items []SetItem) error {
	if len(items) > maxFirestoreBatchSize {
		return fferr.NewInternalErrorf(
			"Cannot batch write %d items.\nMax: %d\n", len(items), maxFirestoreBatchSize)
	}

	bulkWriter := table.client.BulkWriter(context.TODO())

	for _, item := range items {
		serializedValue, err := table.serializer.Serialize(table.valueType, item.Value)
		if err != nil {
			table.logger.Error("Error serializing value", "entity", item.Entity, "value", item.Value, "err", err)
			return err
		}
		document := table.collection.Doc(item.Entity)
		_, err = bulkWriter.Create(document, map[string]interface{}{
			valueKey: serializedValue,
		})
		if err != nil {
			table.logger.Error("Error creating document", "entity", item.Entity, "value", item.Value, "err", err)
			return err
		}
	}

	bulkWriter.End()
	return nil
}
