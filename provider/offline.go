// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/mitchellh/mapstructure"
	"github.com/parquet-go/parquet-go"
	"golang.org/x/sync/syncmap"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"

	"github.com/featureform/fferr"
	"github.com/featureform/filestore"
	fs "github.com/featureform/filestore"
	"github.com/featureform/logging"
	"github.com/featureform/metadata"
	pl "github.com/featureform/provider/location"
	pc "github.com/featureform/provider/provider_config"
	ps "github.com/featureform/provider/provider_schema"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/provider/types"
)

// defaultRowsPerChunk is the number of rows in a chunk when using materializations.
const defaultRowsPerChunk int64 = 100_000

type OfflineResourceType int

const (
	NoType OfflineResourceType = iota
	Label
	Feature
	TrainingSet
	Primary
	Transformation
	FeatureMaterialization
)

var ProviderToMetadataResourceType = map[OfflineResourceType]metadata.ResourceType{
	Feature:        metadata.FEATURE_VARIANT,
	TrainingSet:    metadata.TRAINING_SET_VARIANT,
	Primary:        metadata.SOURCE_VARIANT,
	Transformation: metadata.SOURCE_VARIANT,
}

func (offlineType OfflineResourceType) String() string {
	typeMap := map[OfflineResourceType]string{
		Label:                  "Label",
		Feature:                "Feature",
		TrainingSet:            "TrainingSet",
		Primary:                "Primary",
		Transformation:         "Transformation",
		FeatureMaterialization: "Materialization",
	}
	return typeMap[offlineType]
}

type FeatureLabelColumnType string

const (
	Entity FeatureLabelColumnType = "entity"
	Value  FeatureLabelColumnType = "value"
	TS     FeatureLabelColumnType = "ts"
)

type ResourceID struct {
	Name, Variant string
	Type          OfflineResourceType
}

// TODO: deprecate
func (id *ResourceID) ToFilestorePath() string {
	return fmt.Sprintf("featureform/%s/%s/%s", id.Type, id.Name, id.Variant)
}

// TODO: add unit tests
func (id *ResourceID) FromFilestorePath(path string) error {
	featureformRootPathPart := "featureform/"
	idx := strings.Index(path, featureformRootPathPart)
	if idx == -1 {
		return fferr.NewInternalError(fmt.Errorf("expected \"featureform\" root path part in path %s", path))
	}
	resourceParts := strings.Split(path[idx+len(featureformRootPathPart):], "/")
	if len(resourceParts) < 3 {
		return fferr.NewInternalError(
			fmt.Errorf(
				"expected path %s to contain OfflineResourceType/Name/Variant",
				strings.Join(resourceParts, "/"),
			),
		)
	}
	switch resourceParts[0] {
	case "Label":
		id.Type = OfflineResourceType(1)
	case "Feature":
		id.Type = OfflineResourceType(2)
	case "TrainingSet":
		id.Type = OfflineResourceType(3)
	case "Primary":
		id.Type = OfflineResourceType(4)
	case "Transformation":
		id.Type = OfflineResourceType(5)
	case "Materialization":
		id.Type = OfflineResourceType(6)
	default:
		return fferr.NewInternalError(fmt.Errorf("unrecognized OfflineResourceType: %s", resourceParts[0]))
	}
	id.Name = resourceParts[1]
	id.Variant = resourceParts[2]
	return nil
}

func (id *ResourceID) check(expectedType OfflineResourceType, otherTypes ...OfflineResourceType) error {
	if id.Name == "" {
		return fferr.NewInvalidArgumentError(errors.New("ResourceID must have Name set"))
	}
	// If there is one expected type, we will default to it.
	if id.Type == NoType && len(otherTypes) == 0 {
		id.Type = expectedType
		return nil
	}
	possibleTypes := append(otherTypes, expectedType)
	for _, t := range possibleTypes {
		if id.Type == t {
			return nil
		}
	}
	return fferr.NewInvalidArgumentError(fmt.Errorf("unexpected ResourceID Type: %v", id.Type))
}

func GetOfflineStore(t pt.Type, c pc.SerializedConfig) (OfflineStore, error) {
	provider, err := Get(t, c)
	if err != nil {
		return nil, err
	}
	store, err := provider.AsOfflineStore()
	if err != nil {
		return nil, err
	}
	return store, nil
}

type LagFeatureDef struct {
	FeatureName    string
	FeatureVariant string
	LagName        string
	LagDelta       time.Duration
}

type TrainingSetDef struct {
	ID                 ResourceID
	Label              ResourceID
	LabelSourceMapping SourceMapping
	Features           []ResourceID
	// **NOTE** The ProviderType and ProviderConfig fields in FeatureSourceMappings correspond
	// the feature's source provider as the feature's provider will be the inference store.
	// See getFeatureSourceMapping in coordinator/tasks/trainingset.go for more details.
	FeatureSourceMappings   []SourceMapping
	LagFeatures             []LagFeatureDef
	ResourceSnowflakeConfig *metadata.ResourceSnowflakeConfig
}

type TrainingSetDefJSON struct {
	ID                      ResourceID                        `json:"ID"`
	Label                   ResourceID                        `json:"Label"`
	LabelSourceMapping      SourceMappingJSON                 `json:"LabelSourceMapping"`
	Features                []ResourceID                      `json:"Features"`
	FeatureSourceMappings   []SourceMappingJSON               `json:"FeatureSourceMappings"`
	LagFeatures             []LagFeatureDef                   `json:"LagFeatures"`
	ResourceSnowflakeConfig *metadata.ResourceSnowflakeConfig `json:"ResourceSnowflakeConfig,omitempty"`
}

func (def *TrainingSetDef) check() error {
	if err := def.ID.check(TrainingSet); err != nil {
		return err
	}
	if err := def.Label.check(Label); err != nil {
		return err
	}
	if len(def.Features) == 0 {
		return fferr.NewInvalidArgumentError(errors.New("training set must have at least one feature"))
	}
	for i := range def.Features {
		// We use features[i] to make sure that the Type value is updated to
		// Feature if it's unset.
		if err := def.Features[i].check(Feature); err != nil {
			return err
		}
	}
	return nil
}

type TransformationType string

const (
	NoTransformationType TransformationType = "NilTransformationType"
	SQLTransformation    TransformationType = "SQLTransformationType"
	DFTransformation     TransformationType = "DFTransformationType"
)

type SourceMapping struct {
	Template            string
	Source              string
	ProviderType        pt.Type
	ProviderConfig      pc.SerializedConfig
	TimestampColumnName string
	Location            pl.Location
	Columns             metadata.ResourceVariantColumns
}

type SourceMappingJSON struct {
	Template            string                          `json:"Template"`
	Source              string                          `json:"Source"`
	ProviderType        pt.Type                         `json:"ProviderType"`
	ProviderConfig      pc.SerializedConfig             `json:"ProviderConfig"`
	TimestampColumnName string                          `json:"TimestampColumnName"`
	Location            json.RawMessage                 `json:"Location,omitempty"`
	Columns             metadata.ResourceVariantColumns `json:"Columns"`
}

type TransformationConfig struct {
	Type             TransformationType
	TargetTableID    ResourceID
	Query            string
	Code             []byte
	SourceMapping    []SourceMapping
	Args             metadata.TransformationArgs
	ArgType          metadata.TransformationArgType
	MaxJobDuration   time.Duration
	LastRunTimestamp time.Time
	IsUpdate         bool
	SparkFlags       pc.SparkFlags
	// Make sure to update tempConfig in Unmarshal when adding fields
	OutputLocationType      pl.LocationType
	TableFormat             string
	ResourceSnowflakeConfig *metadata.ResourceSnowflakeConfig
}

func (m *TransformationConfig) MarshalJSON() ([]byte, error) {
	var argType metadata.TransformationArgType
	if m.Args != nil {
		argType = m.Args.Type()
	} else {
		argType = metadata.NoArgs
	}
	m.ArgType = argType

	// Prevents recursion in marshal
	type config TransformationConfig
	c := config(*m)
	marshal, err := json.Marshal(&c)
	if err != nil {
		return nil, fferr.NewInternalError(err)
	}
	return marshal, nil
}

func (m *TransformationConfig) UnmarshalJSON(data []byte) error {
	type tempConfig struct {
		Type             TransformationType
		TargetTableID    ResourceID
		Query            string
		Code             []byte
		SourceMapping    []SourceMapping
		Args             map[string]interface{}
		ArgType          metadata.TransformationArgType
		MaxJobDuration   time.Duration
		LastRunTimestamp time.Time
		IsUpdate         bool
		SparkFlags       pc.SparkFlags
	}

	var temp tempConfig
	err := json.Unmarshal(data, &temp)
	if err != nil {
		return fferr.NewInternalError(err)
	}

	m.Type = temp.Type
	m.TargetTableID = temp.TargetTableID
	m.Query = temp.Query
	m.Code = temp.Code
	m.SourceMapping = temp.SourceMapping
	m.MaxJobDuration = temp.MaxJobDuration
	m.LastRunTimestamp = temp.LastRunTimestamp
	m.IsUpdate = temp.IsUpdate
	m.SparkFlags = temp.SparkFlags

	err = m.decodeArgs(temp.ArgType, temp.Args)
	if err != nil {
		return err
	}

	return nil
}

func (m *TransformationConfig) decodeArgs(t metadata.TransformationArgType, argMap map[string]interface{}) error {
	var args metadata.TransformationArgs
	switch t {
	case metadata.K8sArgs:
		args = metadata.KubernetesArgs{}
	case metadata.NoArgs:
		m.Args = nil
		return nil
	default:
		return fferr.NewInvalidArgumentError(fmt.Errorf("invalid transformation arg type: %v", t))
	}
	err := mapstructure.Decode(argMap, &args)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	m.Args = args
	return nil
}

type TrainTestSplitDef struct {
	TrainingSetName    string
	TrainingSetVariant string
	TestSize           float32
	Shuffle            bool
	RandomState        int
}

type MaterializationOptions struct {
	Output                  filestore.FileType
	ShouldIncludeHeaders    bool
	MaxJobDuration          time.Duration
	JobName                 string
	ResourceSnowflakeConfig *metadata.ResourceSnowflakeConfig
	Schema                  ResourceSchema
	// If this is set, the provider is expected to copy
	// the materialized table directly to this online store
	// itself or fail with an error.
	DirectCopyTo OnlineStore
}

type MaterializationOptionType string

const (
	// DirectCopyDynamo means that the provider is capable of copying its
	// materialized table directly to DynamoDB.
	NullMaterializationOptionType MaterializationOptionType = ""
	DirectCopyDynamo              MaterializationOptionType = "DirectCopyDynamo"
)

func DirectCopyOptionType(store OnlineStore) MaterializationOptionType {
	switch store.(type) {
	case *dynamodbOnlineStore:
		return DirectCopyDynamo
	default:
		return NullMaterializationOptionType
	}
}

type TransformationOptionType string

const (
	// ResumableTransformation makes transformations run async and returns a parameter that can be used
	// to resume it in the future.
	ResumableTransformation TransformationOptionType = "ResumableTransformation"
)

type TransformationOptions []TransformationOption

func (opts TransformationOptions) GetByType(t TransformationOptionType) TransformationOption {
	for _, opt := range opts {
		if opt.Type() == t {
			return opt
		}
	}
	return nil
}

func (opts TransformationOptions) GetResumeOption(logger logging.Logger) (*ResumeOption, bool) {
	opt := opts.GetByType(ResumableTransformation)
	if opt == nil {
		logger.Debugw("ResumeOption not found")
		return nil, false
	}
	casted, ok := opt.(*ResumeOption)
	if !ok {
		logger.DPanicw(
			"Unknown transformation option with ResumableTransformation type",
			"option", opt,
		)
		return nil, false
	}
	return casted, true
}

type TransformationOption interface {
	Type() TransformationOptionType
}

type ResumeOption struct {
	// resumeID is used to resume a running transformation. It may have been set by the user in
	// which case this should become a resume operation. Must use mutex when checking.
	// Also wait for resumeIDChan to be closed before checking (or close manually if user sets it).
	resumeID types.ResumeID
	// used for accessing resumeID.
	resumeIDMtx sync.RWMutex
	// This channel should be closed when the associated async operation is complete.
	finishedChan chan struct{}
	// used when writing to the finished and err bools to make sure they aren't set twice.
	finishedMtx sync.Mutex
	// Use finishedMtx to access this bool.
	finished bool
	// The max amount of time that .Wait() will hang
	maxWait time.Duration
	// Make sure finishedChan is closed before checking this.
	err error
}

func (opt *ResumeOption) Type() TransformationOptionType {
	return ResumableTransformation
}

func (opt *ResumeOption) Wait() error {
	select {
	case <-opt.finishedChan:
	case <-time.After(opt.maxWait):
		return fmt.Errorf("Timed out after %s", opt.maxWait.String())
	}
	return opt.err
}

func (opt *ResumeOption) ResumeID() types.ResumeID {
	opt.resumeIDMtx.RLock()
	defer opt.resumeIDMtx.RUnlock()
	return opt.resumeID
}

func (opt *ResumeOption) IsResumeIDSet() bool {
	opt.resumeIDMtx.RLock()
	defer opt.resumeIDMtx.RUnlock()
	return opt.resumeID != types.NilResumeID
}

func (opt *ResumeOption) setResumeID(id types.ResumeID) error {
	if id == types.NilResumeID {
		return fferr.NewInternalErrorf("ResumeID cannot be an empty string")
	}
	opt.resumeIDMtx.Lock()
	defer opt.resumeIDMtx.Unlock()
	if opt.resumeID != types.NilResumeID {
		return fferr.NewInternalErrorf("Setting resume ID that's already set. Old value: %v New Value: %v", opt.resumeID, id)
	}
	opt.resumeID = id
	return nil
}

func (opt *ResumeOption) finishWithError(err error) error {
	opt.finishedMtx.Lock()
	defer opt.finishedMtx.Unlock()
	if opt.finished {
		return fferr.NewInternalErrorf("Finish called twice on option")
	}
	opt.finished = true
	opt.err = err
	close(opt.finishedChan)
	return nil
}

func ResumeOptionWithID(id types.ResumeID, maxWait time.Duration) (*ResumeOption, error) {
	opt := newResumeOption(maxWait)
	if err := opt.setResumeID(id); err != nil {
		return nil, err
	}
	return opt, nil

}

func RunAsyncWithResume(maxWait time.Duration) *ResumeOption {
	return newResumeOption(maxWait)
}

func newResumeOption(maxWait time.Duration) *ResumeOption {
	return &ResumeOption{
		finishedChan: make(chan struct{}),
		maxWait:      maxWait,
	}
}

type ResourceOptionType string

const (
	SnowflakeDynamicTableResource ResourceOptionType = "SnowflakeDynamicTableResource"
)

type ResourceOption interface {
	Type() ResourceOptionType
}

type ResourceSnowflakeConfigOption struct {
	Config    *metadata.SnowflakeDynamicTableConfig
	Warehouse string
}

func (opt *ResourceSnowflakeConfigOption) Type() ResourceOptionType {
	return SnowflakeDynamicTableResource
}

type OfflineStore interface {
	Provider
	OfflineStoreCore
	OfflineStoreDataset
	OfflineStoreMaterialization
	OfflineStoreTrainingSet
	OfflineStoreBatchFeature
}

type OfflineStoreCore interface {
	// TODO: move into Provider interface
	Close() error
	// ResourceLocation passes 'any' object because we currently don't have an interface for the Variant Objects
	// TODO: Create an interface for Variant Objects
	ResourceLocation(id ResourceID, resource any) (pl.Location, error)
	Provider
}

type OfflineStoreDataset interface {
	// CreatePrimaryTable is not used outside of the context of tests
	CreatePrimaryTable(id ResourceID, schema TableSchema) (PrimaryTable, error)
	RegisterPrimaryFromSourceTable(id ResourceID, tableLocation pl.Location) (PrimaryTable, error)
	GetPrimaryTable(id ResourceID, source metadata.SourceVariant) (PrimaryTable, error)
	SupportsTransformationOption(opt TransformationOptionType) (bool, error)
	CreateTransformation(config TransformationConfig, opts ...TransformationOption) error
	GetTransformationTable(id ResourceID) (TransformationTable, error)
	UpdateTransformation(config TransformationConfig, opts ...TransformationOption) error
}

type OfflineStoreMaterialization interface {
	CreateResourceTable(id ResourceID, schema TableSchema) (OfflineTable, error)
	GetResourceTable(id ResourceID) (OfflineTable, error)
	RegisterResourceFromSourceTable(id ResourceID, schema ResourceSchema, opts ...ResourceOption) (OfflineTable, error)
	CreateMaterialization(id ResourceID, opts MaterializationOptions) (Materialization, error)
	GetMaterialization(id MaterializationID) (Materialization, error)
	UpdateMaterialization(id ResourceID, opts MaterializationOptions) (Materialization, error)
	DeleteMaterialization(id MaterializationID) error
	SupportsMaterializationOption(opt MaterializationOptionType) (bool, error)
}

type OfflineStoreTrainingSet interface {
	CreateTrainingSet(TrainingSetDef) error
	UpdateTrainingSet(TrainingSetDef) error
	GetTrainingSet(id ResourceID) (TrainingSetIterator, error)
	CreateTrainTestSplit(TrainTestSplitDef) (func() error, error)
	GetTrainTestSplit(TrainTestSplitDef) (TrainingSetIterator, TrainingSetIterator, error)
}

type OfflineStoreBatchFeature interface {
	GetBatchFeatures(tables []ResourceID) (BatchFeatureIterator, error)
}

type MaterializationID string

func NewMaterializationID(id ResourceID) (MaterializationID, error) {
	if err := id.check(Feature); err != nil {
		return "", err
	}
	strID, err := ps.ResourceToMaterializationID(id.Type.String(), id.Name, id.Variant)
	if err != nil {
		return "", err
	}
	return MaterializationID(strID), nil
}

type TrainingSetIterator interface {
	Next() bool
	Features() []interface{}
	Label() interface{}
	Err() error
}

type GenericTableIterator interface {
	Next() bool
	Values() GenericRecord
	Columns() []string
	Err() error
	Close() error
}

type Materialization interface {
	ID() MaterializationID
	NumRows() (int64, error)
	IterateSegment(begin, end int64) (FeatureIterator, error)
	NumChunks() (int, error)
	IterateChunk(idx int) (FeatureIterator, error)
	Location() pl.Location
}

type Chunks interface {
	Size() int
	ChunkIterator(idx int) (FeatureIterator, error)
}

type FeatureIterator interface {
	Next() bool
	Value() ResourceRecord
	Err() error
	Close() error
}

type BatchFeatureIterator interface {
	Next() bool
	Entity() interface{}
	Features() GenericRecord
	Columns() []string
	Err() error
	Close() error
}

// Used to implement sort.Interface
type ResourceRecords []ResourceRecord

func (recs ResourceRecords) Swap(i, j int) {
	recs[i], recs[j] = recs[j], recs[i]
}

func (recs ResourceRecords) Less(i, j int) bool {
	return recs[j].TS.After(recs[i].TS)
}

func (recs ResourceRecords) Len() int {
	return len(recs)
}

type ResourceRecord struct {
	Entity string
	Value  interface{}
	// Defaults to 00:00 on 01-01-0001, technically if a user sets a time
	// in a BC year for some reason, our default time would not be the
	// earliest time in the feature store.
	TS time.Time
}

// This generic version of ResourceRecord is only used for converting
// ResourceRecord to a type that's interpretable by parquet-go. See
// BlobOfflineTable.writeRecordsToParquetBytes for more details.
// In addition to using generics to aid in parquet-go's encoding, int64
// is used for the timestamp due to a Spark issue relating to time.Time:
// org.apache.spark.sql.AnalysisException: Illegal Parquet type: INT64 (TIMESTAMP(NANOS,true))
type GenericResourceRecord[T any] struct {
	Entity string
	Value  T
	// TS     int64
	TS time.Time `parquet:"TS,timestamp"`
}

type GenericRecord []interface{}

func (rec ResourceRecord) check() error {
	if rec.Entity == "" {
		return fferr.NewInvalidArgumentError(fmt.Errorf("ResourceRecord must have Entity set"))
	}
	return nil
}

func (rec *ResourceRecord) SetEntity(entity interface{}) error {
	switch typedEntity := entity.(type) {
	case string:
		rec.Entity = typedEntity
	default:
		return fferr.NewInvalidArgumentError(fmt.Errorf("entity must be a string; received %T", entity))

	}
	return nil
}

func (rec ResourceRecord) Columns() []string {
	return []string{"Entity", "Value", "TS"}
}

// This interface represents the contract for implementations that
// write feature and label tables, which have a knowable schema.
type OfflineTable interface {
	Write(ResourceRecord) error
	WriteBatch([]ResourceRecord) error
	Location() pl.Location
}

// The "primary" in the name here might be misleading.
// This interface is meant to support generic tables,
// such as those created by transformations.
type PrimaryTable interface {
	Write(GenericRecord) error
	WriteBatch([]GenericRecord) error
	// TODO: Consider renaming this to GetSchema, which is more
	// descriptive and general purpose. The SourceTable string
	// could be used by callers that are only interested in the
	// absolute path to the source table (i.e. the "name" in our
	// current lexicon).
	GetName() string
	IterateSegment(n int64) (GenericTableIterator, error)
	NumRows() (int64, error)
}

type TransformationTable interface {
	PrimaryTable
}

// Dataset is a common interface for primary and transformation
// tables and means to unify the two interfaces into a common
// interface that can be used throughout the codebase.
type Dataset interface {
	Write(GenericRecord) error
	WriteBatch([]GenericRecord) error
	Location() pl.Location
	NumRows() (int64, error)
	IterateSegment(n int64) (GenericTableIterator, error)
	NumChunks() (int, error)
	IterateChunk(idx int) (GenericTableIterator, error)
}

type ResourceSchema struct {
	Entity      string
	Value       string
	TS          string
	SourceTable pl.Location
}

type ResourceSchemaJSON struct {
	Entity       string          `json:"Entity"`
	Value        string          `json:"Value"`
	TS           string          `json:"TS"`
	SourceTable  json.RawMessage `json:"SourceTable"`
	LocationType pl.LocationType `json:"LocationType"`
}

func (schema *ResourceSchema) Serialize() ([]byte, error) {
	var locationData string
	var err error
	if schema.SourceTable != nil {
		locationData, err = schema.SourceTable.Serialize()
		if err != nil {
			return nil, fferr.NewInternalErrorf("failed to serialize SourceTable: %v", err)
		}
	}

	data := ResourceSchemaJSON{
		Entity:       schema.Entity,
		Value:        schema.Value,
		TS:           schema.TS,
		SourceTable:  json.RawMessage(locationData),
		LocationType: schema.SourceTable.Type(),
	}

	return json.Marshal(data)
}

func (schema *ResourceSchema) Deserialize(config []byte) error {
	var data ResourceSchemaJSON
	err := json.Unmarshal(config, &data)
	if err != nil {
		return fferr.NewInternalErrorf("failed to deserialize ResourceSchema: %v", err)
	}

	schema.Entity = data.Entity
	schema.Value = data.Value
	schema.TS = data.TS

	var location pl.Location
	switch data.LocationType {
	case pl.SQLLocationType:
		location = &pl.SQLLocation{}
	case pl.FileStoreLocationType:
		location = &pl.FileStoreLocation{}
	case pl.CatalogLocationType:
		location = &pl.CatalogLocation{}
	default:
		return fferr.NewInternalErrorf("unknown location type: %s", data.LocationType)
	}

	err = location.Deserialize(data.SourceTable)
	if err != nil {
		return fferr.NewInternalErrorf("failed to deserialize SourceTable: %v", err)
	}
	schema.SourceTable = location

	return nil
}

type TableSchema struct {
	Columns []TableColumn
	// The complete URL that points to the location of the data file
	SourceTable string
}

func (r ResourceSchema) Validate() error {
	unsetFields := make([]string, 0)
	if r.Entity == "" {
		unsetFields = append(unsetFields, "Entity")
	}
	if r.Value == "" {
		unsetFields = append(unsetFields, "Value")
	}
	if r.SourceTable == nil || r.SourceTable.Location() == "" {
		unsetFields = append(unsetFields, "SourceTable")
	}
	if len(unsetFields) > 0 {
		return fferr.NewInvalidArgumentError(fmt.Errorf("missing required fields: %v", unsetFields))
	}
	return nil
}

type TableSchemaJSONWrapper struct {
	Columns     []TableColumnJSONWrapper
	SourceTable string
}

// This method converts the list of columns into a struct type that can be
// serialized by parquet-go. This is necessary because GenericRecord, which
// is of type []interface{}, does not hold the necessary metadata information
// to create a valid parquet-go schema.
func (schema *TableSchema) AsParquetSchema() *parquet.Schema {
	return parquet.SchemaOf(schema.AsReflectedStruct().Interface())
}

// This method converts the list of columns into a struct type that can be
// serialized by parquet-go. This is necessary because GenericRecord, which
// is of type []interface{}, does not hold the necessary metadata information
// to create a valid parquet-go schema.
func (schema *TableSchema) AsReflectedStruct() reflect.Value {
	fields := make([]reflect.StructField, len(schema.Columns))
	for i, col := range schema.Columns {
		caser := cases.Title(language.English)
		colType := col.Type()

		f := reflect.StructField{
			// We need to title case the column name to ensure the fields are public
			// in the struct we create.
			Name: caser.String(col.Name),
			Type: colType,
			// At a minimum, we need to set the parquet tag to the column name so that when
			// we read from the file, the field names match up with the column names as they
			// are defined; additionally, we set the optional tag to ensure that the field
			// is nullable in the parquet file.
			Tag: reflect.StructTag(fmt.Sprintf(`parquet:"%s,optional"`, col.Name)),
		}

		if col.IsVector() {
			f.Tag = reflect.StructTag(fmt.Sprintf(`parquet:"%s,optional,list"`, col.Name))
		}
		// This checks if the column type via reflection is Time, such as with time.Time.
		if colType.Name() == "Time" {
			f.Tag = reflect.StructTag(fmt.Sprintf(`parquet:"%s,optional,timestamp"`, col.Name))
		}

		fields[i] = f
	}
	structType := reflect.StructOf(fields)
	return reflect.New(structType)
}

func (schema *TableSchema) Serialize() ([]byte, error) {
	wrapper := &TableSchemaJSONWrapper{
		SourceTable: schema.SourceTable,
		Columns:     make([]TableColumnJSONWrapper, len(schema.Columns)),
	}
	for i, col := range schema.Columns {
		wrapper.Columns[i] = TableColumnJSONWrapper{
			Name:      col.Name,
			ValueType: types.ValueTypeJSONWrapper{col.ValueType},
		}
	}
	config, err := json.Marshal(wrapper)
	if err != nil {
		return nil, fferr.NewInternalError(err)
	}
	return config, nil
}

func (schema *TableSchema) Deserialize(config []byte) error {
	wrapper := &TableSchemaJSONWrapper{}
	err := json.Unmarshal(config, wrapper)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	schema.Columns = make([]TableColumn, len(wrapper.Columns))
	for i, col := range wrapper.Columns {
		schema.Columns[i] = TableColumn{
			Name:      col.Name,
			ValueType: col.ValueType.ValueType,
		}
	}
	schema.SourceTable = wrapper.SourceTable
	return nil
}

// ToParquetRecords turn a list of GenericRecords into a list of structs built via
// reflection. We use structs so that we can add struct tags like optional, timestamp,
// etc.
func (schema *TableSchema) ToParquetRecords(records []GenericRecord) ([]any, error) {
	parquetRecords := make([]any, len(records))
	caser := cases.Title(language.English)
	for i, record := range records {
		parquetRecord := schema.AsReflectedStruct()
		for j, value := range record {
			// if a value is nil, we skip it so that the zero value for the pointer
			// type is used instead, which will preserve the null value when the parquet
			// file is read back.
			if value == nil {
				continue
			}
			// To ensure the struct fields are public and accessible to other methods,
			// we need to title case them when setting them.
			colName := caser.String(schema.Columns[j].Name)
			parquetField := parquetRecord.Elem().FieldByName(colName)
			var reflectValue reflect.Value
			switch v := value.(type) {
			case int, int32, int64, float32, float64, string, bool:
				// Pointer types are used for all the scalar types to ensure they
				// can be nullable in the parquet file.
				ogVal := reflect.ValueOf(v)
				if ogVal.CanAddr() {
					reflectValue = ogVal.Addr()
				} else {
					reflectValue = reflect.New(reflect.TypeOf(v))
					reflectValue.Elem().Set(ogVal)
				}
			// Lists and timestamps
			default:
				reflectValue = reflect.ValueOf(v)
			}
			if !reflectValue.Type().AssignableTo(parquetField.Type()) {
				return nil, fferr.NewInternalErrorf(
					"writing invalid type to parquet record.\nFound %s\nexpected %s\n",
					reflectValue.Type().String(),
					parquetField.Type().String(),
				)
			}
			parquetField.Set(reflectValue)
		}
		parquetRecords[i] = parquetRecord.Interface()
	}
	return parquetRecords, nil
}

type TableColumnJSONWrapper struct {
	Name      string
	ValueType types.ValueTypeJSONWrapper
}

type TableColumn struct {
	Name string
	types.ValueType
}

type memoryOfflineStore struct {
	tables           syncmap.Map
	materializations syncmap.Map
	trainingSets     syncmap.Map
	BaseProvider
}

var memoryFactorySingleton Provider

func memoryOfflineStoreFactory(serializedConfig pc.SerializedConfig) (Provider, error) {
	// Add mutex
	if memoryFactorySingleton == nil {
		memoryFactorySingleton = NewMemoryOfflineStore()
		return memoryFactorySingleton, nil
	} else {
		return memoryFactorySingleton, nil
	}
}

func NewMemoryOfflineStore() *memoryOfflineStore {
	return &memoryOfflineStore{
		tables:           syncmap.Map{},
		materializations: syncmap.Map{},
		trainingSets:     syncmap.Map{},
		BaseProvider: BaseProvider{
			ProviderType:   pt.MemoryOffline,
			ProviderConfig: []byte{},
		},
	}
}

func (store *memoryOfflineStore) AsOfflineStore() (OfflineStore, error) {
	return store, nil
}

func (store *memoryOfflineStore) RegisterResourceFromSourceTable(id ResourceID, schema ResourceSchema, opts ...ResourceOption) (
	OfflineTable,
	error,
) {
	store.tables.Store(id, &memoryOfflineTable{})
	return &memoryOfflineTable{}, nil
}

func (store *memoryOfflineStore) RegisterPrimaryFromSourceTable(
	id ResourceID,
	tableLocation pl.Location,
) (PrimaryTable, error) {
	if id.Name == "make" && id.Variant == "panic" {
		panic("This is a panic")
	}
	store.tables.Store(id, &memoryPrimaryTable{})
	return &memoryPrimaryTable{}, nil
}

func (store *memoryOfflineStore) CreatePrimaryTable(id ResourceID, schema TableSchema) (PrimaryTable, error) {
	store.tables.Store(id, &memoryPrimaryTable{})
	return &memoryPrimaryTable{}, nil
}

type memoryPrimaryTable struct {
}

func (m *memoryPrimaryTable) Write(record GenericRecord) error {
	return nil
}

func (m *memoryPrimaryTable) WriteBatch(record []GenericRecord) error {
	return nil
}

func (m *memoryPrimaryTable) GetName() string {
	return "memoryTableName"
}

func (m *memoryPrimaryTable) IterateSegment(n int64) (GenericTableIterator, error) {
	return nil, nil
}

func (m *memoryPrimaryTable) NumRows() (int64, error) {
	return 0, nil
}

func (store *memoryOfflineStore) GetPrimaryTable(id ResourceID, source metadata.SourceVariant) (PrimaryTable, error) {
	table, has := store.tables.Load(id)
	if !has {
		return nil, fferr.NewDatasetNotFoundError(id.Name, id.Variant, nil)
	}
	memoryTable := table.(*memoryPrimaryTable)
	return memoryTable, nil
}

func (store *memoryOfflineStore) SupportsTransformationOption(opt TransformationOptionType) (bool, error) {
	return false, nil
}

func (store *memoryOfflineStore) CreateTransformation(config TransformationConfig, opts ...TransformationOption) error {
	if len(opts) > 0 {
		return fferr.NewInternalErrorf("Memory store does not support transformation options")
	}
	return fferr.NewInternalErrorf("CreateTransformation unsupported for this provider")
}

func (store *memoryOfflineStore) UpdateTransformation(config TransformationConfig, opts ...TransformationOption) error {
	return fferr.NewInternalError(fmt.Errorf("UpdateTransformation unsupported for this provider"))
}

func (store *memoryOfflineStore) GetTransformationTable(id ResourceID) (TransformationTable, error) {
	return nil, fferr.NewInternalError(fmt.Errorf("GetTransformationTable unsupported for this provider"))
}

func (store *memoryOfflineStore) CreateResourceTable(id ResourceID, schema TableSchema) (OfflineTable, error) {
	if err := id.check(Feature, Label); err != nil {
		return nil, err
	}
	if _, has := store.tables.Load(id); has {
		return nil, fferr.NewDatasetAlreadyExistsError(id.Name, id.Variant, nil)
	}
	table := newMemoryOfflineTable()
	store.tables.Store(id, table)
	return table, nil
}

func (store *memoryOfflineStore) GetResourceTable(id ResourceID) (OfflineTable, error) {
	return store.getMemoryResourceTable(id)
}

func (store *memoryOfflineStore) getMemoryResourceTable(id ResourceID) (*memoryOfflineTable, error) {
	table, has := store.tables.Load(id)
	if !has {
		return nil, fferr.NewDatasetNotFoundError(id.Name, id.Variant, nil)
	}
	memTable := table.(*memoryOfflineTable)
	return memTable, nil
}

func (store *memoryOfflineStore) ResourceLocation(id ResourceID, resource any) (pl.Location, error) {
	return nil, errors.New("ResourceLocation unsupported for this provider")
}

// Used to implement sort.Interface for sorting.
type materializedRecords []ResourceRecord

func (recs materializedRecords) Len() int {
	return len(recs)
}

func (recs materializedRecords) Less(i, j int) bool {
	return recs[i].Entity < recs[j].Entity
}

func (recs materializedRecords) Swap(i, j int) {
	recs[i], recs[j] = recs[j], recs[i]
}

func (store *memoryOfflineStore) GetBatchFeatures(tables []ResourceID) (BatchFeatureIterator, error) {
	return nil, nil
}

func (store *memoryOfflineStore) CreateMaterialization(id ResourceID, opts MaterializationOptions) (
	Materialization,
	error,
) {
	if id.Type != Feature {
		return nil, fferr.NewInvalidArgumentError(fmt.Errorf("only features can be materialized"))
	}
	table, err := store.getMemoryResourceTable(id)
	if err != nil {
		return nil, err
	}
	var matData materializedRecords
	table.entityMap.Range(
		func(key, value interface{}) bool {
			records := value.([]ResourceRecord)
			matRec := latestRecord(records)
			matData = append(matData, matRec)
			return true
		},
	)
	sort.Sort(matData)
	// Might be used for testing
	matId := MaterializationID(uuid.NewString())
	mat := &MemoryMaterialization{
		Id:           matId,
		Data:         matData,
		RowsPerChunk: defaultRowsPerChunk,
	}
	store.materializations.Store(matId, mat)
	return mat, nil
}

func (store *memoryOfflineStore) SupportsMaterializationOption(opt MaterializationOptionType) (bool, error) {
	return false, nil
}

func (store *memoryOfflineStore) GetMaterialization(id MaterializationID) (Materialization, error) {
	mat, has := store.materializations.Load(id)
	if !has {
		return nil, fferr.NewDatasetNotFoundError(string(id), "", nil)
	}
	return mat.(Materialization), nil
}

func (store *memoryOfflineStore) UpdateMaterialization(id ResourceID, opts MaterializationOptions) (
	Materialization,
	error,
) {
	return store.CreateMaterialization(id, MaterializationOptions{Output: fs.Parquet})
}

func (store *memoryOfflineStore) DeleteMaterialization(id MaterializationID) error {
	if _, has := store.materializations.Load(id); !has {
		return fferr.NewDatasetNotFoundError(string(id), "", nil)
	}
	store.materializations.Delete(id)
	return nil
}

func latestRecord(recs []ResourceRecord) ResourceRecord {
	latest := recs[0]
	for _, rec := range recs {
		if latest.TS.Before(rec.TS) {
			latest = rec
		}
	}
	return latest
}

func (store *memoryOfflineStore) CreateTrainingSet(def TrainingSetDef) error {
	if err := def.check(); err != nil {
		return err
	}
	label, err := store.getMemoryResourceTable(def.Label)
	if err != nil {
		return err
	}
	features := make([]*memoryOfflineTable, len(def.Features))
	for i, id := range def.Features {
		feature, err := store.getMemoryResourceTable(id)
		if err != nil {
			return err
		}
		features[i] = feature
	}
	labelRecs := label.records()
	trainingData := make(trainingRows, len(labelRecs))
	for i, rec := range labelRecs {
		featureVals := make([]interface{}, len(features))
		for i, feature := range features {
			featureVals[i] = feature.getLastValueBefore(rec.Entity, rec.TS)
		}
		labelVal := rec.Value
		trainingData[i] = trainingRow{
			Features: featureVals,
			Label:    labelVal,
		}
	}
	store.trainingSets.Store(def.ID, trainingData)
	return nil
}

func (store *memoryOfflineStore) UpdateTrainingSet(def TrainingSetDef) error {
	return store.CreateTrainingSet(def)
}

func (store *memoryOfflineStore) GetTrainingSet(id ResourceID) (TrainingSetIterator, error) {
	if err := id.check(TrainingSet); err != nil {
		return nil, err
	}
	data, has := store.trainingSets.Load(id)
	if !has {
		return nil, fferr.NewDatasetNotFoundError(id.Name, id.Variant, nil)
	}
	return data.(trainingRows).Iterator(), nil
}

func (store *memoryOfflineStore) CreateTrainTestSplit(def TrainTestSplitDef) (func() error, error) {
	// TODO properly implement this
	dropFunc := func() error {
		return nil
	}
	return dropFunc, nil
}

func (store *memoryOfflineStore) GetTrainTestSplit(def TrainTestSplitDef) (
	TrainingSetIterator,
	TrainingSetIterator,
	error,
) {
	// TODO properly implement this
	trainingSetResourceId := ResourceID{
		Name:    def.TrainingSetName,
		Variant: def.TrainingSetVariant,
	}
	trainingSet, err := store.GetTrainingSet(trainingSetResourceId)
	if err != nil {
		return nil, nil, err
	}
	return trainingSet, trainingSet, nil

}

func (store *memoryOfflineStore) Close() error {
	return nil
}

func (store *memoryOfflineStore) CheckHealth() (bool, error) {
	return false, fferr.NewInternalError(fmt.Errorf("provider health check not implemented"))
}

func (store *memoryOfflineStore) Delete(location pl.Location) error {
	return fferr.NewInternalErrorf("delete not implemented")
}

type trainingRows []trainingRow

func (rows trainingRows) Iterator() TrainingSetIterator {
	return newMemoryTrainingSetIterator(rows)
}

type trainingRow struct {
	Features []interface{}
	Label    interface{}
}

type memoryTrainingRowsIterator struct {
	data trainingRows
	idx  int
}

func newMemoryTrainingSetIterator(data trainingRows) TrainingSetIterator {
	return &memoryTrainingRowsIterator{
		data: data,
		idx:  -1,
	}
}

func (it *memoryTrainingRowsIterator) Next() bool {
	lastIdx := len(it.data) - 1
	if it.idx == lastIdx {
		return false
	}
	it.idx++
	return true
}

func (it *memoryTrainingRowsIterator) Err() error {
	return nil
}

func (it *memoryTrainingRowsIterator) Close() error {
	return nil
}

func (it *memoryTrainingRowsIterator) Features() []interface{} {
	return it.data[it.idx].Features
}

func (it *memoryTrainingRowsIterator) Label() interface{} {
	return it.data[it.idx].Label
}

type memoryOfflineTable struct {
	entityMap syncmap.Map
}

func newMemoryOfflineTable() *memoryOfflineTable {
	return &memoryOfflineTable{
		entityMap: syncmap.Map{},
	}
}

func (table *memoryOfflineTable) records() []ResourceRecord {
	allRecs := make([]ResourceRecord, 0)
	table.entityMap.Range(
		func(key, value interface{}) bool {
			allRecs = append(allRecs, value.([]ResourceRecord)...)
			return true
		},
	)
	return allRecs
}

func (table *memoryOfflineTable) getLastValueBefore(entity string, ts time.Time) interface{} {
	recs, has := table.entityMap.Load(entity)
	if !has {
		return nil
	}
	sortedRecs := ResourceRecords(recs.([]ResourceRecord))
	sort.Sort(sortedRecs)
	lastIdx := len(sortedRecs) - 1
	for i, rec := range sortedRecs {
		if rec.TS.After(ts) {
			// Entity was not yet set at timestamp, don't return a record.
			if i == 0 {
				return nil
			}
			// Use the record before this, since it would have been before TS.
			return sortedRecs[i-1].Value
		} else if i == lastIdx {
			// Every record happened before the TS, use the last record.
			return rec.Value
		}
	}
	// This line should never be able to be reached.
	panic("Unable to getLastValue before timestamp")
}

func (table *memoryOfflineTable) Write(rec ResourceRecord) error {
	rec = checkTimestamp(rec)
	if err := rec.check(); err != nil {
		return err
	}

	if records, has := table.entityMap.Load(rec.Entity); has {
		// Replace any record with the same timestamp/entity pair.
		recs := records.([]ResourceRecord)
		for i, existingRec := range recs {
			if existingRec.TS == rec.TS {
				recs[i] = rec
				return nil
			}
		}
		table.entityMap.Store(rec.Entity, append(recs, rec))
	} else {
		table.entityMap.Store(rec.Entity, []ResourceRecord{rec})
	}
	return nil
}

func (table *memoryOfflineTable) WriteBatch(recs []ResourceRecord) error {
	for _, rec := range recs {
		if err := table.Write(rec); err != nil {
			return err
		}
	}
	return nil
}

func (table *memoryOfflineTable) Location() pl.Location {
	return nil
}

// Used in runner/copy_test.go
type MemoryMaterialization struct {
	Id           MaterializationID
	Data         []ResourceRecord
	RowsPerChunk int64
}

func (mat *MemoryMaterialization) ID() MaterializationID {
	return mat.Id
}

func (mat *MemoryMaterialization) NumRows() (int64, error) {
	return int64(len(mat.Data)), nil
}

func (mat *MemoryMaterialization) IterateSegment(start, end int64) (FeatureIterator, error) {
	if end > int64(len(mat.Data)) {
		return nil, fmt.Errorf("Index out of bounds\nStart: %d\nEnd: %d\nLen: %d\n", start, end, len(mat.Data))
	}
	segment := mat.Data[start:end]
	return newMemoryFeatureIterator(segment), nil
}

func (mat *MemoryMaterialization) NumChunks() (int, error) {
	if mat.RowsPerChunk == 0 {
		mat.RowsPerChunk = defaultRowsPerChunk
	}
	return genericNumChunks(mat, mat.RowsPerChunk)
}

func (mat *MemoryMaterialization) IterateChunk(idx int) (FeatureIterator, error) {
	if mat.RowsPerChunk == 0 {
		mat.RowsPerChunk = defaultRowsPerChunk
	}
	return genericIterateChunk(mat, mat.RowsPerChunk, idx)
}

func (mat *MemoryMaterialization) Location() pl.Location {
	return nil
}

type memoryFeatureIterator struct {
	data []ResourceRecord
	idx  int64
}

func newMemoryFeatureIterator(recs []ResourceRecord) FeatureIterator {
	return &memoryFeatureIterator{
		data: recs,
		idx:  -1,
	}
}

func (iter *memoryFeatureIterator) Next() bool {
	if isLastIdx := iter.idx == int64(len(iter.data)-1); isLastIdx {
		return false
	}
	iter.idx++
	return true
}

func (iter *memoryFeatureIterator) Value() ResourceRecord {
	return iter.data[iter.idx]
}

func (iter *memoryFeatureIterator) Err() error {
	return nil
}

func (iter *memoryFeatureIterator) Close() error {
	return nil
}

// checkTimestamp checks the timestamp of a record.
// If the record has the default initialization value of 0001-01-01 00:00:00 +0000 UTC, it is changed
// to the start of unix epoch time, since snowflake cannot handle values before 1582
func checkTimestamp(rec ResourceRecord) ResourceRecord {
	checkRecord := ResourceRecord{}
	if rec.TS == checkRecord.TS {
		rec.TS = time.UnixMilli(0).UTC()
	}
	return rec
}

type sanitization func(string) string

func replaceSourceName(query string, mapping []SourceMapping, sanitize sanitization) (string, error) {
	replacements := make(
		[]string,
		len(mapping)*2,
	) // It's times 2 because each replacement will be a pair; (original, replacedValue)

	for _, m := range mapping {
		replacements = append(replacements, m.Template)
		replacements = append(replacements, sanitize(m.Source))
	}

	replacer := strings.NewReplacer(replacements...)
	replacedQuery := replacer.Replace(query)

	if strings.Contains(replacedQuery, "{{") {
		err := fferr.NewInternalError(fmt.Errorf("template replacement error"))
		err.AddDetail("query", replacedQuery)
		return "", err
	}

	return replacedQuery, nil
}

func genericNumChunks(mat Materialization, rowsPerChunk int64) (int, error) {
	_, numChunks, err := getNumRowsAndChunks(mat, rowsPerChunk)
	return int(numChunks), err
}

func getNumRowsAndChunks(mat Materialization, rowsPerChunk int64) (int64, int, error) {
	rows, err := mat.NumRows()
	if err != nil {
		return -1, -1, err
	}
	numChunks := rows / rowsPerChunk
	if rows%rowsPerChunk != 0 {
		numChunks++
	}
	return rows, int(numChunks), nil
}

func genericIterateChunk(mat Materialization, rowsPerChunk int64, idx int) (FeatureIterator, error) {
	rows, chunks, err := getNumRowsAndChunks(mat, rowsPerChunk)
	if err != nil {
		return nil, err
	}
	if idx > chunks {
		return nil, fferr.NewInternalErrorf("Chunk out of range\nIdx: %d\nTotal: %d", idx, chunks)
	}
	start := int64(idx) * rowsPerChunk
	end := (int64(idx) + 1) * rowsPerChunk
	if start >= rows {
		start = rows
	}
	if end > rows {
		end = rows
	}
	return mat.IterateSegment(start, end)
}
