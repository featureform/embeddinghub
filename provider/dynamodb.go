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
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"time"

	pl "github.com/featureform/provider/location"

	"github.com/araddon/dateparse"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/featureform/fferr"
	"github.com/featureform/logging"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	se "github.com/featureform/provider/serialization"
	vt "github.com/featureform/provider/types"
	sn "github.com/mrz1836/go-sanitize"
	"go.uber.org/zap"
)

const defaultMetadataTableName = "FeatureformMetadata"

func init() {
	if _, ok := serializers[dynamoSerializationVersion]; !ok {
		panic("Dynamo serializer not implemented")
	}
}

const (
	// Serialization version to use for new tables
	dynamoSerializationVersion = serializeV1
)

const (
	// Default timeout when waiting for dynamoDB tables to be ready
	defaultDynamoTableTimeout = 3 * time.Minute
	maxRetries                = 5
)

type dynamodbTableKey struct {
	Prefix, Feature, Variant string
}

func (t dynamodbTableKey) ToTableName() string {
	return formatDynamoTableName(t.Prefix, t.Feature, t.Variant)
}

func (t dynamodbTableKey) String() string {
	marshalled, err := json.Marshal(t)
	if err != nil {
		panic(err)
	}
	return string(marshalled)
}

type dynamodbOnlineStore struct {
	client *dynamodb.Client
	prefix string
	BaseProvider
	timeout            time.Duration
	logger             *zap.SugaredLogger
	accessKey          string
	secretKey          string
	region             string
	stronglyConsistent bool
	tags               []types.Tag
}

type dynamodbOnlineTable struct {
	client             *dynamodb.Client
	key                dynamodbTableKey
	valueType          vt.ValueType
	version            se.SerializeVersion
	stronglyConsistent bool
}

// dynamodbMetadataEntry is the format of each row in the Metadata table.
type dynamodbMetadataEntry struct {
	Tablename string `dynamodbav:"Tablename"`
	Valuetype string `dynamodbav:"ValueType"`
	Version   int    `dynamodbav:"SerializeVersion"`
}

// ToTableMetadata converts a dynamodb entry from the Metadata table to a struct
// with all its fields properly casted and type checked.
func (entry dynamodbMetadataEntry) ToTableMetadata() (*dynamodbTableMetadata, error) {
	version := se.SerializeVersion(entry.Version)
	if _, ok := serializers[version]; !ok {
		wrapped := fferr.NewInternalErrorf("serialization version not implemented")
		wrapped.AddDetail("dynamo_serialize_version", fmt.Sprintf("%d", entry.Version))
		wrapped.AddDetail("dynamo_metadata_entry_name", entry.Tablename)
		return nil, wrapped
	}
	t, err := vt.DeserializeType(entry.Valuetype)
	if err != nil {
		wrapped := fferr.NewInternalError(err)
		wrapped.AddDetail("type_string", entry.Valuetype)
		wrapped.AddDetail("dynamo_metadata_entry_name", entry.Tablename)
		return nil, wrapped
	}
	return &dynamodbTableMetadata{t, version}, nil
}

// dynamodbTableMetadata is created by taking an entry from the Metadata table and
// casting and validating its values.
type dynamodbTableMetadata struct {
	Valuetype vt.ValueType
	Version   se.SerializeVersion
}

func dynamodbOnlineStoreFactory(serialized pc.SerializedConfig) (Provider, error) {
	dynamodbConfig := &pc.DynamodbConfig{}
	if err := dynamodbConfig.Deserialize(serialized); err != nil {
		return nil, err
	}
	if dynamodbConfig.Prefix == "" {
		dynamodbConfig.Prefix = "Featureform_table__"
	}
	return NewDynamodbOnlineStore(dynamodbConfig)
}

// TODO(simba) make table name for metadata part of config
func NewDynamodbOnlineStore(options *pc.DynamodbConfig) (*dynamodbOnlineStore, error) {
	args := []func(*config.LoadOptions) error{
		config.WithRegion(options.Region),
		config.WithRetryer(func() aws.Retryer {
			return retry.AddWithMaxBackoffDelay(retry.NewStandard(func(o *retry.StandardOptions) {
				o.RateLimiter = ratelimit.None
			}), defaultDynamoTableTimeout)
		}),
	}
	accessKey, secretKey := "", ""
	// If the user is using a service account, we don't need to provide credentials
	// as the AWS SDK will use the IAM role of the K8s pod to authenticate.
	if staticCreds, ok := options.Credentials.(pc.AWSStaticCredentials); ok {
		accessKey = staticCreds.AccessKeyId
		secretKey = staticCreds.SecretKey
		creds := config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""))
		args = append(args, creds)
	}
	// If we are using a custom endpoint, such as when running localstack, we should point at it. We'd never set this when
	// directly accessing DynamoDB on AWS.
	if options.Endpoint != "" {
		args = append(args,
			config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(func(service, region string, opts ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL:           options.Endpoint,
					SigningRegion: options.Region,
				}, nil
			})))
	}
	cfg, err := config.LoadDefaultConfig(context.TODO(), args...)
	if err != nil {
		return nil, err
	}
	client := dynamodb.NewFromConfig(cfg)
	if err := waitForDynamoDB(client); err != nil {
		return nil, fferr.NewConnectionError("DynamoDB", err)
	}
	logger := logging.NewLogger("dynamodb")
	tags := toDynamoDBTags(options.Tags)
	if err := CreateMetadataTable(client, logger.SugaredLogger, tags); err != nil {
		return nil, err
	}
	return &dynamodbOnlineStore{client, options.Prefix, BaseProvider{
		ProviderType:   pt.DynamoDBOnline,
		ProviderConfig: options.Serialized(),
	}, defaultDynamoTableTimeout, logger.SugaredLogger,
		accessKey, secretKey, options.Region, options.StronglyConsistent, tags,
	}, nil
}

func (store *dynamodbOnlineStore) AsOnlineStore() (OnlineStore, error) {
	return store, nil
}

func (store *dynamodbOnlineStore) Close() error {
	// dynamoDB client does not implement an equivalent to Close
	return nil
}

// TODO(simba) make table name a param
func CreateMetadataTable(client *dynamodb.Client, logger *zap.SugaredLogger, tags []types.Tag) error {
	tableName := defaultMetadataTableName
	params := &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("Tablename"),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("Tablename"),
				KeyType:       types.KeyTypeHash,
			},
		},
		BillingMode: types.BillingModePayPerRequest,
		Tags:        tags,
	}
	describeMetadataTableParams := &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}
	_, err := client.DescribeTable(context.TODO(), describeMetadataTableParams)
	if err == nil {
		return nil
	}
	logger.Infow("Could not describe dynamo metadata table, attempting to create...", "Error", err)
	if _, err := client.CreateTable(context.TODO(), params); err != nil {
		return fferr.NewExecutionError(pt.DynamoDBOnline.String(), err)
	}
	if err := waitForDynamoTable(client, tableName, defaultDynamoTableTimeout); err != nil {
		return fferr.NewExecutionError(pt.DynamoDBOnline.String(), err)
	}
	return nil
}

func (store *dynamodbOnlineStore) updateMetadataTable(tablename string, valueType vt.ValueType, version se.SerializeVersion) error {
	input := &dynamodb.UpdateItemInput{
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":valtype": &types.AttributeValueMemberS{
				Value: vt.SerializeType(valueType),
			},
			":serializeVersion": &types.AttributeValueMemberN{
				Value: fmt.Sprintf("%d", version),
			},
		},
		TableName: aws.String(defaultMetadataTableName),
		Key: map[string]types.AttributeValue{
			"Tablename": &types.AttributeValueMemberS{
				Value: tablename,
			},
		},
		UpdateExpression: aws.String("set ValueType = :valtype, SerializeVersion = :serializeVersion"),
	}
	_, err := store.client.UpdateItem(context.TODO(), input)
	if err != nil {
		wrappedErr := fferr.NewExecutionError(pt.DynamoDBOnline.String(), err)
		wrappedErr.AddDetail("tablename", tablename)
		return wrappedErr
	}
	return nil
}

func (store *dynamodbOnlineStore) getFromMetadataTable(tablename string) (*dynamodbTableMetadata, error) {
	input := &dynamodb.GetItemInput{
		TableName: aws.String(defaultMetadataTableName),
		Key: map[string]types.AttributeValue{
			"Tablename": &types.AttributeValueMemberS{
				Value: tablename,
			},
		},
	}
	output_val, err := store.client.GetItem(context.TODO(), input)
	if len(output_val.Item) == 0 {
		return nil, fferr.NewDatasetNotFoundError("", "", fmt.Errorf("table %s not found", tablename))
	}
	if err != nil {
		wrappedErr := fferr.NewExecutionError(pt.DynamoDBOnline.String(), err)
		wrappedErr.AddDetail("tablename", tablename)
		return nil, wrappedErr
	}
	var entry dynamodbMetadataEntry
	if err = attributevalue.UnmarshalMap(output_val.Item, &entry); err != nil {
		wrappedErr := fferr.NewExecutionError(pt.DynamoDBOnline.String(), err)
		wrappedErr.AddDetail("tablename", tablename)
		return nil, wrappedErr
	}
	tableMeta, err := entry.ToTableMetadata()
	if err != nil {
		return nil, fferr.NewInternalError(err)
	}
	return tableMeta, nil
}

func formatDynamoTableName(prefix, feature, variant string) string {
	tablename := fmt.Sprintf("%s__%s__%s", sn.Custom(prefix, "[^a-zA-Z0-9_]"), sn.Custom(feature, "[^a-zA-Z0-9_]"), sn.Custom(variant, "[^a-zA-Z0-9_]"))
	return sn.Custom(tablename, "[^a-zA-Z0-9_.\\-]")
}

func (store *dynamodbOnlineStore) GetTable(feature, variant string) (OnlineStoreTable, error) {
	key := dynamodbTableKey{store.prefix, feature, variant}
	meta, err := store.getFromMetadataTable(formatDynamoTableName(store.prefix, feature, variant))
	if err != nil {
		return nil, fferr.NewDatasetNotFoundError(feature, variant, err)
	}
	table := &dynamodbOnlineTable{client: store.client, key: key, valueType: meta.Valuetype, version: meta.Version, stronglyConsistent: store.stronglyConsistent}
	return table, nil
}

func (store *dynamodbOnlineStore) FormatTableName(feature, variant string) string {
	return formatDynamoTableName(store.prefix, feature, variant)
}

func (store *dynamodbOnlineStore) CreateTable(feature, variant string, valueType vt.ValueType) (OnlineStoreTable, error) {
	key := dynamodbTableKey{store.prefix, feature, variant}
	tableName := formatDynamoTableName(store.prefix, feature, variant)
	if _, err := store.getFromMetadataTable(tableName); err == nil {
		wrapped := fferr.NewDatasetAlreadyExistsError(feature, variant, nil)
		wrapped.AddDetail("tablename", tableName)
		return nil, wrapped
	}
	params := &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String(feature),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		BillingMode: types.BillingModePayPerRequest,
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String(feature),
				KeyType:       types.KeyTypeHash,
			},
		},
		Tags: store.tags,
	}
	if _, err := store.client.CreateTable(context.TODO(), params); err != nil {
		return nil, fferr.NewResourceExecutionError(pt.DynamoDBOnline.String(), feature, variant, fferr.FEATURE_VARIANT, err)
	}
	if err := waitForDynamoTable(store.client, tableName, store.timeout); err != nil {
		return nil, fferr.NewResourceExecutionError(pt.DynamoDBOnline.String(), feature, variant, fferr.FEATURE_VARIANT, err)
	}
	if err := store.updateMetadataTable(tableName, valueType, dynamoSerializationVersion); err != nil {
		return nil, err
	}
	return &dynamodbOnlineTable{store.client, key, valueType, dynamoSerializationVersion, store.stronglyConsistent}, nil
}

func (store *dynamodbOnlineStore) DeleteTable(feature, variant string) error {
	params := &dynamodb.DeleteTableInput{
		TableName: aws.String(formatDynamoTableName(store.prefix, feature, variant)),
	}
	_, err := store.client.DeleteTable(context.TODO(), params)
	if err != nil {
		var notFoundErr *types.ResourceNotFoundException
		if errors.As(err, &notFoundErr) {
			return fferr.NewDatasetNotFoundError(feature, variant, err)
		} else {
			return fferr.NewExecutionError(pt.DynamoDBOnline.String(), err)
		}
	}

	return nil
}

func (store *dynamodbOnlineStore) CheckHealth() (bool, error) {
	store.logger.Info("Checking health of DynamoDB connnection ...")
	_, err := store.client.ListTables(context.TODO(), &dynamodb.ListTablesInput{Limit: aws.Int32(1)})
	if err != nil {
		store.logger.Errorw("DynamoDB health check failed", "err", err)
		return false, fferr.NewExecutionError(pt.DynamoDBOnline.String(), err)
	}
	store.logger.Info("DynamoDB health check succeeded")
	return true, nil
}

func (store dynamodbOnlineStore) Delete(location pl.Location) error {
	return fferr.NewInternalErrorf("delete not implemented as dynamodb doesn't support location")
}

// maxDynamoBatchSize is the max amount of items that can be written to Dynamo at once. It's a dynamo set limitation.
const maxDynamoBatchSize = 25

func (table dynamodbOnlineTable) BatchSet(items []SetItem) error {
	if len(items) > maxDynamoBatchSize {
		return fferr.NewInternalErrorf(
			"Cannot batch write %d items.\nMax: %d\n", len(items), maxDynamoBatchSize)
	}
	serialized := make([]map[string]types.AttributeValue, len(items))
	for i, item := range items {
		dynamoValue, err := serializers[table.version].Serialize(table.valueType, item.Value)
		if err != nil {
			return err
		}
		serialized[i] = map[string]types.AttributeValue{
			table.key.Feature: &types.AttributeValueMemberS{Value: item.Entity},
			"FeatureValue":    dynamoValue,
		}
	}
	reqs := make([]types.WriteRequest, len(serialized))
	for i, serItem := range serialized {
		reqs[i] = types.WriteRequest{PutRequest: &types.PutRequest{Item: serItem}}
	}
	batchInput := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			table.key.ToTableName(): reqs,
		},
	}

	if err := table.batchSetWithRetry(context.TODO(), batchInput); err != nil {
		return err
	}
	return nil
}

func (table dynamodbOnlineTable) batchSetWithRetry(ctx context.Context, input *dynamodb.BatchWriteItemInput) error {
	totalWaitedTime := time.Duration(0)
	for attempts := 0; attempts < maxRetries; attempts++ {
		output, err := table.client.BatchWriteItem(ctx, input)
		if err != nil {
			return fferr.NewExecutionError("DynamoDB", err)
		}
		if len(output.UnprocessedItems) == 0 {
			return nil
		}

		input.RequestItems = output.UnprocessedItems

		waitTime, newTotalWait := exponentialBackoff(attempts, totalWaitedTime)
		time.Sleep(waitTime)
		totalWaitedTime = newTotalWait
	}
	return fferr.NewExecutionError("DynamoDB", fmt.Errorf("failed to write all items after %d retries, unprocessed items: %d", maxRetries, len(input.RequestItems)))
}

func (table dynamodbOnlineTable) MaxBatchSize() (int, error) {
	return maxDynamoBatchSize, nil
}

func (table dynamodbOnlineTable) Set(entity string, value interface{}) error {
	dynamoValue, err := serializers[table.version].Serialize(table.valueType, value)
	if err != nil {
		wrap := fferr.NewInternalError(err)
		wrap.AddDetail("entity", entity)
		wrap.AddDetail("value", fmt.Sprintf("%v", value))
		return wrap
	}
	input := &dynamodb.UpdateItemInput{
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":val": dynamoValue,
		},
		TableName: aws.String(formatDynamoTableName(table.key.Prefix, table.key.Feature, table.key.Variant)),
		Key: map[string]types.AttributeValue{
			table.key.Feature: &types.AttributeValueMemberS{
				Value: entity,
			},
		},
		UpdateExpression: aws.String("set FeatureValue = :val"),
	}
	if _, err := table.client.UpdateItem(context.TODO(), input); err != nil {
		wrapped := fferr.NewResourceExecutionError(pt.DynamoDBOnline.String(), table.key.Feature, table.key.Variant, "FEATURE_VARIANT", fmt.Errorf("error setting entity: %w", err))
		wrapped.AddDetail("entity", entity)
		wrapped.AddDetail("value", fmt.Sprintf("%v", value))
		return wrapped
	}
	return nil
}

func (table dynamodbOnlineTable) Get(entity string) (interface{}, error) {
	input := &dynamodb.GetItemInput{
		TableName: aws.String(formatDynamoTableName(table.key.Prefix, table.key.Feature, table.key.Variant)),
		Key: map[string]types.AttributeValue{
			table.key.Feature: &types.AttributeValueMemberS{
				Value: entity,
			},
		},
		ConsistentRead: aws.Bool(table.stronglyConsistent),
	}
	output_val, err := table.client.GetItem(context.TODO(), input)
	if len(output_val.Item) == 0 {
		wrapped := fferr.NewEntityNotFoundError(table.key.Feature, table.key.Variant, entity, nil)
		wrapped.AddDetail("entity", entity)
		return nil, wrapped
	}
	if err != nil {
		return nil, err
	}
	item := output_val.Item
	value, ok := item["FeatureValue"]
	if !ok {
		wrapped := fferr.NewInternalErrorf("dynamoDB item does not have FeatureValue column")
		wrapped.AddDetail("entity", entity)
		return nil, wrapped
	}
	return serializers[table.version].Deserialize(table.valueType, value)
}

// waitForDynamoDB waits for DynamoDB to return a valid response with exponential backoff.
// We can't use waitForDynamoTable since we need to ignore most tcp and network errors and
// continue to retry.
func waitForDynamoDB(client *dynamodb.Client) error {
	totalWait := time.Duration(0)

	for attempts := 0; attempts < maxRetries; attempts++ {
		_, err := client.DescribeTable(context.TODO(), &dynamodb.DescribeTableInput{
			TableName: aws.String("FEATUREFORM-PING"), // Arbitrary name
		})

		if err != nil {
			var resourceNotFoundErr *types.ResourceNotFoundException
			if errors.As(err, &resourceNotFoundErr) {
				// The table doesn't exist, but DynamoDB responded, meaning it's ready.
				return nil
			}
		} else {
			// DescribeTable succeeded, indicating DynamoDB is ready and the table exists.
			return nil
		}

		waitTime, newTotalWait := exponentialBackoff(attempts, totalWait)
		time.Sleep(waitTime)
		totalWait = newTotalWait
	}

	return errors.New("DynamoDB is not ready after the maximum number of retries")
}

// exponentialBackoff handles the waiting with exponential backoff. TODO ditch for a resilience library
func exponentialBackoff(attempt int, totalWaitedTime time.Duration) (time.Duration, time.Duration) {
	// Using math.Pow to calculate the exponential increase
	timeToWaitBeforeRetry := time.Second * time.Duration(math.Pow(2, float64(attempt)))
	if totalWaitedTime+timeToWaitBeforeRetry > defaultDynamoTableTimeout {
		// If we're going to wait longer than the timeout, just wait the remaining time
		timeToWaitBeforeRetry = defaultDynamoTableTimeout - totalWaitedTime
	}
	return timeToWaitBeforeRetry, totalWaitedTime + timeToWaitBeforeRetry
}

// waitForDynamoDB waits for a DynamoDB table.
func waitForDynamoTable(client *dynamodb.Client, table string, maxWait time.Duration) error {
	waiter := dynamodb.NewTableExistsWaiter(client)
	waitParams := &dynamodb.DescribeTableInput{TableName: aws.String(table)}
	return waiter.Wait(context.TODO(), waitParams, maxWait)
}

// The serializer versions. If adding a new one make sure to add to the serializers map variable
const (
	// serializeV0 serializes everything as strings, including numbers
	serializeV0 se.SerializeVersion = iota
	// serializeV1 serializes everything into native dynamo types and handles lists as well
	serializeV1
)

// serializers is the map of all serializers. If a new version is added it should be added
// into this map as well.
var serializers = map[se.SerializeVersion]se.Serializer[types.AttributeValue]{
	serializeV0: serializerV0{},
	serializeV1: serializerV1{},
}

// serializerV0 serializes everything as strings, including numbers
type serializerV0 struct{}

func (ser serializerV0) Version() se.SerializeVersion {
	return serializeV0
}

func (ser serializerV0) Serialize(t vt.ValueType, value any) (types.AttributeValue, error) {
	if t.Scalar() == vt.Timestamp || t.Scalar() == vt.Datetime {
		return nil, fferr.NewTypeErrorf(t.String(), value, "Type not supported by Dynamo Serializer v0")
	}
	if value == nil {
		return &types.AttributeValueMemberNULL{
			Value: true,
		}, nil
	} else {
		return &types.AttributeValueMemberS{
			Value: fmt.Sprintf("%v", value),
		}, nil
	}
}

func (ser serializerV0) Deserialize(t vt.ValueType, value types.AttributeValue) (any, error) {
	if t.Scalar() == vt.Timestamp || t.Scalar() == vt.Datetime {
		return nil, fferr.NewInternalErrorf("Unable to deserialize %s", t)
	}
	if _, isNil := value.(*types.AttributeValueMemberNULL); isNil {
		return nil, nil
	}
	typed, ok := value.(*types.AttributeValueMemberS)
	if !ok {
		wrapped := fferr.NewInternalErrorf(
			"unable to deserialize dynamodb value into string, is %T", value)
		wrapped.AddDetail("version", ser.Version().String())
		return nil, wrapped
	}
	valString := typed.Value
	var result interface{}
	var err error
	switch t {
	case vt.NilType, vt.String:
		result, err = valString, nil
	case vt.Int:
		result, err = strconv.Atoi(valString)
	case vt.Int32:
		res64, perr := strconv.ParseInt(valString, 0, 32)
		err = perr
		result = int32(res64)
	case vt.Int64:
		result, err = strconv.ParseInt(valString, 0, 64)
	case vt.Float32:
		var result_float float64
		result_float, err = strconv.ParseFloat(valString, 32)
		result = float32(result_float)
	case vt.Float64:
		result, err = strconv.ParseFloat(valString, 64)
	case vt.Bool:
		result, err = strconv.ParseBool(valString)
	default:
		return nil, fferr.NewInternalErrorf("Unsupported type %s", t.String())
	}
	if err != nil {
		return nil, fferr.NewInternalError(err)
	}
	return result, nil
}

// serializerV1 serializes everything into native dynamo types and handles lists as well
type serializerV1 struct{}

func (ser serializerV1) Version() se.SerializeVersion {
	return serializeV1
}

func (ser serializerV1) Serialize(t vt.ValueType, value any) (types.AttributeValue, error) {
	// TODO support unsigned ints
	if value == nil {
		return &types.AttributeValueMemberNULL{Value: true}, nil
	}
	if !t.IsVector() {
		return ser.serializeScalar(t, value)
	} else {
		return ser.serializeVector(t, value)
	}
}

func (ser serializerV1) serializeVector(t vt.ValueType, value any) (types.AttributeValue, error) {
	vecT := t.(vt.VectorType)
	scalar := vecT.Scalar()

	list := reflect.ValueOf(value)
	if list.Kind() != reflect.Slice {
		wrapped := fferr.NewTypeError(vecT.String(), value, nil)
		wrapped.AddDetail("version", ser.Version().String())
		return nil, wrapped
	}
	length := list.Len()
	if int32(length) != vecT.Dimension {
		errMsg := "Type error. Wrong length.\nFound %d\nExpected %d"
		wrapped := fferr.NewTypeErrorf(vecT.String(), value, errMsg, vecT.Dimension, length)
		wrapped.AddDetail("version", ser.Version().String())
		return nil, wrapped
	}
	vals := make([]types.AttributeValue, length)
	for i := 0; i < length; i++ {
		elem := list.Index(i).Interface()
		val, err := ser.serializeScalar(scalar, elem)
		if err != nil {
			if typed, ok := err.(fferr.Error); ok {
				typed.AddDetail("list_element", strconv.Itoa(i))
			}
			return nil, err
		}
		vals[i] = val
	}
	return &types.AttributeValueMemberL{
		Value: vals,
	}, nil
}

func (ser serializerV1) serializeScalar(t vt.ValueType, value any) (types.AttributeValue, error) {
	if value == nil {
		return &types.AttributeValueMemberNULL{Value: true}, nil
	}
	// Dynamo teats all numerical types as strings, so we have to serialize.
	switch t {
	case vt.NilType:
		return &types.AttributeValueMemberNULL{Value: true}, nil
	case vt.Int:
		// This rounds via Go if needed
		intVal, err := se.CastNumberToInt(value)
		if err != nil {
			wrapped := fferr.NewTypeError(t.String(), value, err)
			wrapped.AddDetail("version", ser.Version().String())
			return nil, wrapped
		}
		intStr := strconv.FormatInt(int64(intVal), 10)
		return &types.AttributeValueMemberN{Value: intStr}, nil
	case vt.Int32:
		// This rounds via Go if needed
		intVal, err := se.CastNumberToInt32(value)
		if err != nil {
			wrapped := fferr.NewTypeError(t.String(), value, err)
			wrapped.AddDetail("version", ser.Version().String())
			return nil, wrapped
		}
		intStr := strconv.FormatInt(int64(intVal), 10)
		return &types.AttributeValueMemberN{Value: intStr}, nil
	case vt.Int64:
		intVal, err := se.CastNumberToInt64(value)
		if err != nil {
			wrapped := fferr.NewTypeError(t.String(), value, err)
			wrapped.AddDetail("version", ser.Version().String())
			return nil, wrapped
		}
		intStr := strconv.FormatInt(intVal, 10)
		return &types.AttributeValueMemberN{Value: intStr}, nil
	case vt.Float32:
		floatVal, err := se.CastNumberToFloat32(value)
		if err != nil {
			wrapped := fferr.NewTypeError(t.String(), value, err)
			wrapped.AddDetail("version", ser.Version().String())
			return nil, wrapped
		}
		floatStr := strconv.FormatFloat(float64(floatVal), 'e', -1, 32)
		return &types.AttributeValueMemberN{Value: floatStr}, nil
	case vt.Float64:
		floatVal, err := se.CastNumberToFloat64(value)
		if err != nil {
			wrapped := fferr.NewTypeError(t.String(), value, err)
			wrapped.AddDetail("version", ser.Version().String())
			return nil, wrapped
		}
		floatStr := strconv.FormatFloat(floatVal, 'e', -1, 64)
		return &types.AttributeValueMemberN{Value: floatStr}, nil
	case vt.Bool:
		casted, err := se.CastBool(value)
		if err != nil {
			wrapped := fferr.NewTypeError(t.String(), value, err)
			wrapped.AddDetail("version", ser.Version().String())
			return nil, wrapped
		}
		return &types.AttributeValueMemberBOOL{Value: casted}, nil
	case vt.String:
		casted, ok := value.(string)
		if !ok {
			wrapped := fferr.NewTypeError(t.String(), value, nil)
			wrapped.AddDetail("version", ser.Version().String())
			return nil, wrapped
		}
		return &types.AttributeValueMemberS{Value: casted}, nil
	case vt.Timestamp, vt.Datetime:
		ts, isTs := value.(time.Time)
		if isTs {
			intStr := strconv.FormatInt(ts.Unix(), 10)
			return &types.AttributeValueMemberN{Value: intStr}, nil
		}
		unixTime, unixTimeErr := se.CastNumberToInt64(value)
		isUnixTs := unixTimeErr == nil
		if isUnixTs {
			intStr := strconv.FormatInt(unixTime, 10)
			return &types.AttributeValueMemberN{Value: intStr}, nil
		}
		strForm, isString := value.(string)
		if !isString {
			wrapped := fferr.NewTypeError(t.String(), value, nil)
			wrapped.AddDetail("version", ser.Version().String())
			return nil, wrapped
		}
		// If timezone is ambiguous, this makes it UTC
		dt, err := dateparse.ParseIn(strForm, time.UTC)
		if err != nil {
			wrapped := fferr.NewTypeError(t.String(), value, err)
			wrapped.AddDetail("version", ser.Version().String())
			return nil, wrapped
		}
		intStr := strconv.FormatInt(dt.Unix(), 10)
		return &types.AttributeValueMemberN{Value: intStr}, nil
	default:
		wrapped := fferr.NewInternalErrorf("dynamo doesn't support type")
		wrapped.AddDetail("type", vt.SerializeType(t))
		return nil, wrapped
	}
}

func (ser serializerV1) Deserialize(t vt.ValueType, value types.AttributeValue) (any, error) {
	// TODO support unsigned ints

	// Dynamo teats all numerical types as strings, so we have to deserialize.
	version := ser.Version().String()
	_, ok := value.(*types.AttributeValueMemberNULL)
	if ok {
		return nil, nil
	}
	if !t.IsVector() {
		return deserializeScalar(t.Scalar(), value, version)
	}
	list, ok := value.(*types.AttributeValueMemberL)
	if !ok {
		wrapped := fferr.NewInternalErrorf("unable to deserialize dynamodb value into list, is %T", value)
		wrapped.AddDetail("version", ser.Version().String())
		return nil, wrapped
	}
	values := list.Value
	dims := t.(vt.VectorType).Dimension
	if len(values) != int(dims) {
		msg := "unable to deserialize dynamodb value into list, wrong size %d. Expected %d"
		wrapped := fferr.NewInternalErrorf(msg, len(values), dims)
		wrapped.AddDetail("version", ser.Version().String())
		return nil, wrapped
	}
	scalar := t.Scalar()
	switch scalar {
	case vt.Int:
		return deserializeList[int](scalar, values, version)
	case vt.Int32:
		return deserializeList[int32](scalar, values, version)
	case vt.Int64:
		return deserializeList[int64](scalar, values, version)
	case vt.Float32:
		return deserializeList[float32](scalar, values, version)
	case vt.Float64:
		return deserializeList[float64](scalar, values, version)
	case vt.Bool:
		return deserializeList[bool](scalar, values, version)
	case vt.String:
		return deserializeList[string](scalar, values, version)
	default:
		wrapped := fferr.NewInternalErrorf("dynamo doesn't support type")
		wrapped.AddDetail("type", vt.SerializeType(t))
		return nil, wrapped
	}
}

func deserializeList[T any](scalar vt.ScalarType, values []types.AttributeValue, version string) ([]T, error) {
	deserList := make([]T, len(values))
	for i, value := range values {
		deser, err := deserializeScalar(scalar, value, version)
		if err != nil {
			if typed, ok := err.(fferr.Error); ok {
				typed.AddDetail("list_element", strconv.Itoa(i))
			}
			return nil, err
		}
		casted, ok := deser.(T)
		if !ok {
			wrapped := fferr.NewInternalErrorf("Deserialize failed due to wrong generic")
			wrapped.AddDetail("found_type", fmt.Sprintf("%T", casted))
			wrapped.AddDetail("expected_type", scalar.String())
			wrapped.AddDetail("list_element", strconv.Itoa(i))
			wrapped.AddDetail("version", version)
			return nil, wrapped
		}
		deserList[i] = deser.(T)
	}
	return deserList, nil
}

func deserializeScalar(t vt.ScalarType, value types.AttributeValue, version string) (any, error) {
	// Dynamo teats all numerical types as strings, so we have to deserialize.
	switch t {
	case vt.Int:
		castedValue, ok := value.(*types.AttributeValueMemberN)
		if !ok {
			wrapped := fferr.NewInternalErrorf("unable to deserialize dynamodb value into numerical, is %T", value)
			wrapped.AddDetail("version", version)
			return nil, wrapped
		}
		val, err := strconv.ParseInt(castedValue.Value, 10, 0)
		return int(val), err
	case vt.Int32:
		castedValue, ok := value.(*types.AttributeValueMemberN)
		if !ok {
			wrapped := fferr.NewInternalErrorf("unable to deserialize dynamodb value into numerical, is %T", value)
			wrapped.AddDetail("version", version)
			return nil, wrapped
		}
		val, err := strconv.ParseInt(castedValue.Value, 10, 32)
		return int32(val), err
	case vt.Int64:
		castedValue, ok := value.(*types.AttributeValueMemberN)
		if !ok {
			wrapped := fferr.NewInternalErrorf("unable to deserialize dynamodb value into numerical, is %T", value)
			wrapped.AddDetail("version", version)
			return nil, wrapped
		}
		return strconv.ParseInt(castedValue.Value, 10, 64)
	case vt.Float32:
		castedValue, ok := value.(*types.AttributeValueMemberN)
		if !ok {
			wrapped := fferr.NewInternalErrorf("unable to deserialize dynamodb value into numerical, is %T", value)
			wrapped.AddDetail("version", version)
			return nil, wrapped
		}
		val, err := strconv.ParseFloat(castedValue.Value, 32)
		return float32(val), err
	case vt.Float64:
		castedValue, ok := value.(*types.AttributeValueMemberN)
		if !ok {
			wrapped := fferr.NewInternalErrorf("unable to deserialize dynamodb value into numerical, is %T", value)
			wrapped.AddDetail("version", version)
			return nil, wrapped
		}
		return strconv.ParseFloat(castedValue.Value, 64)
	case vt.Bool:
		castedValue, ok := value.(*types.AttributeValueMemberBOOL)
		if !ok {
			wrapped := fferr.NewInternalErrorf("unable to deserialize dynamodb value into bool, is %T", value)
			wrapped.AddDetail("version", version)
			return nil, wrapped
		}
		return castedValue.Value, nil
	case vt.String:
		castedValue, ok := value.(*types.AttributeValueMemberS)
		if !ok {
			wrapped := fferr.NewInternalErrorf("unable to deserialize dynamodb value into string, is %T", value)
			wrapped.AddDetail("version", version)
			return nil, wrapped
		}
		return castedValue.Value, nil
	case vt.Timestamp, vt.Datetime:
		castedValue, ok := value.(*types.AttributeValueMemberN)
		if !ok {
			wrapped := fferr.NewInternalErrorf("unable to deserialize dynamodb value into timestamp, is %T", value)
			wrapped.AddDetail("version", version)
			return nil, wrapped
		}
		val := castedValue.Value
		i64, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			msg := "unable to deserialize dynamodb value into timestamp, value: %v\nerr: %s"
			wrapped := fferr.NewInternalErrorf(msg, val, err)
			wrapped.AddDetail("version", version)
			return nil, wrapped
		}
		return time.Unix(i64, 0).UTC(), nil
	default:
		wrapped := fferr.NewInternalErrorf("Dynamo doesn't support type")
		wrapped.AddDetail("version", version)
		return nil, wrapped
	}
}

func toDynamoDBTags(tags map[string]string) []types.Tag {
	dynamoTags := make([]types.Tag, len(tags))
	i := 0
	for k, v := range tags {
		dynamoTags[i] = types.Tag{Key: aws.String(k), Value: aws.String(v)}
		i++
	}
	return dynamoTags
}
