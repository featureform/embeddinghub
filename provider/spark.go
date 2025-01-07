// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/featureform/config"
	"github.com/featureform/fferr"
	"github.com/featureform/filestore"
	"github.com/featureform/logging"
	"github.com/featureform/metadata"
	pl "github.com/featureform/provider/location"
	pc "github.com/featureform/provider/provider_config"
	ps "github.com/featureform/provider/provider_schema"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/provider/types"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"golang.org/x/exp/slices"
)

type JobType string

const (
	Materialize       JobType = "Materialization"
	Transform         JobType = "Transformation"
	CreateTrainingSet JobType = "Training Set"
	BatchFeatures     JobType = "Batch Features"
)

const MATERIALIZATION_ID_SEGMENTS = 3
const ENTITY_INDEX = 0
const VALUE_INDEX = 1
const TIMESTAMP_INDEX = 2
const SPARK_SUBMIT_PARAMS_BYTE_LIMIT = 10_240

type pysparkSourceInfo struct {
	Location     string  `json:"location"`
	LocationType string  `json:"locationType"`
	Provider     pt.Type `json:"provider"`
	// TableFormat is used for file sources
	TableFormat string `json:"tableFormat"`
	// FileType and IsDir are used for file sources
	FileType string `json:"fileType"`
	IsDir    bool   `json:"isDir"`
	// Database and Schema are used for Snowflake sources
	Database string `json:"database"`
	Schema   string `json:"schema"`

	// AwsAssumeRoleArn is used for S3/Glue sources that
	// require a specific role to access
	AwsAssumeRoleArn    string `json:"awsAssumeRoleArn"`
	TimestampColumnName string `json:"timestampColumnName"`

	// Deprecated
	// TODO remove
	// Old version of our pyspark job actually passed in strings
	// as opposed to JSON for source infos. If legacy string is
	// set than serialization will just return this string.
	LegacyString string `json:"-"`
}

// Legacy parts of our script, specifically materialization, don't use the
// JSON version of pyspark source info.
func wrapLegacyPysparkSourceInfos(paths []string) []pysparkSourceInfo {
	sources := make([]pysparkSourceInfo, len(paths))
	for i, path := range paths {
		sources[i] = pysparkSourceInfo{
			LegacyString: path,
		}
	}
	return sources
}

func (p *pysparkSourceInfo) Serialize() (string, error) {
	if p.LegacyString != "" {
		return p.LegacyString, nil
	}
	jsonBytes, err := json.Marshal(p)
	if err != nil {
		return "", fferr.NewInternalError(err)
	}
	return string(jsonBytes), nil
}

type SparkExecutorConfig interface {
	Serialize() ([]byte, error)
	Deserialize(config pc.SerializedConfig) error
	IsExecutorConfig() bool
}

type PythonOfflineQueries interface {
	materializationCreate(schema ResourceSchema) string
	trainingSetCreate(def TrainingSetDef, featureSchemas []ResourceSchema, labelSchema ResourceSchema) string
}

type defaultPythonOfflineQueries struct {
	Logger logging.Logger
}

func (q defaultPythonOfflineQueries) materializationCreate(schema ResourceSchema) (string, error) {
	logger := q.Logger.With("schema", schema)
	logger.Debug("Creating materialization query for schema")
	timestampColumn := schema.TS
	if schema.TS == "" {
		q.Logger.Debug("Creating materialization query without timestamp")
		path := config.GetMaterializeNoTimestampQueryPath()
		q.Logger.Debugw("Reading query template from path", "path", path)
		data, err := os.ReadFile(path)
		if err != nil {
			q.Logger.Errorw("Failed to read query template from path", "path", path)
			return "", err
		}
		query := fmt.Sprintf(string(data), schema.Entity, schema.Value, schema.Entity)
		q.Logger.Debugw("Created query without TS", "query", query)
		return query, nil
	}
	q.Logger.Debug("Creating materialization query with timestamp")
	path := config.GetMaterializeWithTimestampQueryPath()
	data, err := os.ReadFile(path)
	if err != nil {
		q.Logger.Errorw("Failed to read SQL format from path", "path", path, "err", err)
		return "", err
	}
	query := fmt.Sprintf(
		string(data),
		schema.Entity,
		schema.Value,
		timestampColumn,
		"source_0",
		timestampColumn,
		timestampColumn,
		"source_0",
		schema.Entity,
		schema.Entity,
	)
	q.Logger.Debugw("Created query with TS", "query", query)
	return query, nil
}

// Spark SQL _seems_ to have some issues with double quotes in column names based on troubleshooting
// the offline tests. Given this, we will use backticks to quote column names in the queries.
func createQuotedIdentifier(id ResourceID) string {
	return fmt.Sprintf("`%s__%s__%s`", id.Type, id.Name, id.Variant)
}

func (q defaultPythonOfflineQueries) trainingSetCreate(
	def TrainingSetDef,
	featureSchemas []ResourceSchema,
	labelSchema ResourceSchema,
) string {
	columns := make([]string, 0)
	joinQueries := make([]string, 0)
	feature_timestamps := make([]string, 0)
	for i, feature := range def.Features {
		featureColumnName := createQuotedIdentifier(feature)
		columns = append(columns, featureColumnName)
		var featureWindowQuery string
		// if no timestamp column, set to default generated by resource registration
		if featureSchemas[i].TS == "" {
			featureWindowQuery = fmt.Sprintf(
				"SELECT * FROM (SELECT %s as t%d_entity, %s as %s, CAST(0 AS TIMESTAMP) as t%d_ts FROM source_%d) ORDER BY t%d_ts ASC",
				featureSchemas[i].Entity,
				i+1,
				featureSchemas[i].Value,
				featureColumnName,
				i+1,
				i+1,
				i+1,
			)
		} else {
			featureWindowQuery = fmt.Sprintf(
				"SELECT * FROM (SELECT %s as t%d_entity, %s as %s, %s as t%d_ts FROM source_%d) ORDER BY t%d_ts ASC",
				featureSchemas[i].Entity,
				i+1,
				featureSchemas[i].Value,
				featureColumnName,
				featureSchemas[i].TS,
				i+1,
				i+1,
				i+1,
			)
		}
		featureJoinQuery := fmt.Sprintf(
			"LEFT OUTER JOIN (%s) t%d ON (t%d_entity = entity AND t%d_ts <= label_ts)",
			featureWindowQuery,
			i+1,
			i+1,
			i+1,
		)
		joinQueries = append(joinQueries, featureJoinQuery)
		feature_timestamps = append(feature_timestamps, fmt.Sprintf("t%d_ts", i+1))
	}
	for i, lagFeature := range def.LagFeatures {
		lagFeaturesOffset := len(def.Features)
		idx := slices.IndexFunc(
			def.Features, func(id ResourceID) bool {
				return id.Name == lagFeature.FeatureName && id.Variant == lagFeature.FeatureVariant
			},
		)
		lagSource := fmt.Sprintf("source_%d", idx+1)
		lagColumnName := sanitize(lagFeature.LagName)
		if lagFeature.LagName == "" {
			lagColumnName = fmt.Sprintf(
				"`%s_%s_lag_%s`",
				lagFeature.FeatureName,
				lagFeature.FeatureVariant,
				lagFeature.LagDelta,
			)
		}
		columns = append(columns, lagColumnName)
		timeDeltaSeconds := lagFeature.LagDelta.Seconds() //parquet stores time as microseconds
		curIdx := lagFeaturesOffset + i + 1
		var lagWindowQuery string
		if featureSchemas[idx].TS == "" {
			lagWindowQuery = fmt.Sprintf(
				"SELECT * FROM (SELECT %s as t%d_entity, %s as %s, CAST(0 AS TIMESTAMP) as t%d_ts FROM %s) ORDER BY t%d_ts ASC",
				featureSchemas[idx].Entity,
				curIdx,
				featureSchemas[idx].Value,
				lagColumnName,
				curIdx,
				lagSource,
				curIdx,
			)
		} else {
			lagWindowQuery = fmt.Sprintf(
				"SELECT * FROM (SELECT %s as t%d_entity, %s as %s, %s as t%d_ts FROM %s) ORDER BY t%d_ts ASC",
				featureSchemas[idx].Entity,
				curIdx,
				featureSchemas[idx].Value,
				lagColumnName,
				featureSchemas[idx].TS,
				curIdx,
				lagSource,
				curIdx,
			)
		}
		lagJoinQuery := fmt.Sprintf(
			"LEFT OUTER JOIN (%s) t%d ON (t%d_entity = entity AND (t%d_ts + INTERVAL %f SECOND) <= label_ts)",
			lagWindowQuery,
			curIdx,
			curIdx,
			curIdx,
			timeDeltaSeconds,
		)
		joinQueries = append(joinQueries, lagJoinQuery)
		feature_timestamps = append(feature_timestamps, fmt.Sprintf("t%d_ts", curIdx))
	}
	columnStr := strings.Join(columns, ", ")
	joinQueryString := strings.Join(joinQueries, " ")
	var labelWindowQuery string
	if labelSchema.TS == "" {
		labelWindowQuery = fmt.Sprintf(
			"SELECT %s AS entity, %s AS value, CAST(0 AS TIMESTAMP) AS label_ts FROM source_0",
			labelSchema.Entity,
			labelSchema.Value,
		)
	} else {
		labelWindowQuery = fmt.Sprintf(
			"SELECT %s AS entity, %s AS value, %s AS label_ts FROM source_0",
			labelSchema.Entity,
			labelSchema.Value,
			labelSchema.TS,
		)
	}
	labelPartitionQuery := fmt.Sprintf(
		"(SELECT * FROM (SELECT entity, value, label_ts FROM (%s) t ) t0)",
		labelWindowQuery,
	)
	labelJoinQuery := fmt.Sprintf("%s %s", labelPartitionQuery, joinQueryString)

	timeStamps := strings.Join(feature_timestamps, ", ")
	timeStampsDesc := strings.Join(feature_timestamps, " DESC,")
	fullQuery := fmt.Sprintf(
		"SELECT %s, value AS %s, entity, label_ts, %s, ROW_NUMBER() over (PARTITION BY entity, value, label_ts ORDER BY label_ts DESC, %s DESC) as row_number FROM (%s) tt",
		columnStr,
		createQuotedIdentifier(def.Label),
		timeStamps,
		timeStampsDesc,
		labelJoinQuery,
	)
	finalQuery := fmt.Sprintf(
		"SELECT %s, %s FROM (SELECT * FROM (SELECT *, row_number FROM (%s) WHERE row_number=1 ))  ORDER BY label_ts",
		columnStr,
		createQuotedIdentifier(def.Label),
		fullQuery,
	)
	return finalQuery
}

type SparkOfflineStore struct {
	Executor   SparkExecutor
	Store      SparkFileStore
	GlueConfig *pc.GlueConfig
	Logger     logging.Logger
	query      *defaultPythonOfflineQueries
	BaseProvider
}

func (store *SparkOfflineStore) AsOfflineStore() (OfflineStore, error) {
	return store, nil
}

func (store *SparkOfflineStore) GetBatchFeatures(ids []ResourceID) (BatchFeatureIterator, error) {
	if len(ids) == 0 {
		return &FileStoreBatchServing{store: store.Store, iter: nil}, fferr.NewInternalError(fmt.Errorf("no feature ids provided"))
	}
	// Convert all IDs to materialization IDs
	materializationIDs := make([]ResourceID, len(ids))
	batchDir := ""
	for i, id := range ids {
		materializationIDs[i] = ResourceID{Name: id.Name, Variant: id.Variant, Type: FeatureMaterialization}
		batchDir += fmt.Sprintf("%s-%s", id.Name, id.Variant)
		if i != len(ids)-1 {
			batchDir += "-"
		}
	}
	// Convert materialization ID to file paths
	materializationPaths, err := store.createFilePathsFromIDs(materializationIDs)
	if err != nil {
		return nil, err
	}

	sources := wrapLegacyPysparkSourceInfos(materializationPaths)

	// Create a query that selects all features from the table
	query := createJoinQuery(len(ids))

	// Create output file path
	batchDirUUID := uuid.NewSHA1(uuid.NameSpaceDNS, []byte(batchDir))
	outputPath, err := store.Store.CreateFilePath(fmt.Sprintf("featureform/BatchFeatures/%s", batchDirUUID), true)
	if err != nil {
		return nil, err
	}

	// Submit arguments for a spark job
	sparkArgs, err := sparkScriptCommandDef{
		DeployMode:     getSparkDeployModeFromEnv(),
		TFType:         SQLTransformation,
		OutputLocation: pl.NewFileLocation(outputPath),
		Code:           query,
		SourceList:     sources,
		JobType:        BatchFeatures,
		Store:          store.Store,
		Mappings:       make([]SourceMapping, 0),
	}.PrepareCommand(store.Logger)

	if err != nil {
		store.Logger.Errorw("Problem creating spark submit arguments", "error", err, "args", sparkArgs)
		return nil, err
	}
	// Run the spark job
	if err := store.Executor.RunSparkJob(sparkArgs, store.Store, SparkJobOptions{MaxJobDuration: time.Hour * 48}, nil); err != nil {
		store.Logger.Errorw("Error running Spark job", "error", err)
		return nil, err
	}
	// Create a batch iterator that iterates through the dir
	outputFiles, err := store.Store.List(outputPath, filestore.Parquet)
	if err != nil {
		return nil, err
	}
	groups, err := filestore.NewFilePathGroup(outputFiles, filestore.DateTimeDirectoryGrouping)
	if err != nil {
		return nil, err
	}
	newest, err := groups.GetFirst()
	if err != nil {
		return nil, err
	}
	iterator, err := store.Store.Serve(newest)
	if err != nil {
		return nil, err
	}
	store.Logger.Debug("Successfully created batch iterator")
	return &FileStoreBatchServing{store: store.Store, iter: iterator, numFeatures: len(ids)}, nil
}

func (store *SparkOfflineStore) createFilePathsFromIDs(materializationIDs []ResourceID) ([]string, error) {
	materializationPaths := make([]string, len(materializationIDs))
	for i, id := range materializationIDs {
		path, err := store.Store.CreateFilePath(id.ToFilestorePath(), true)
		if err != nil {
			return nil, err
		}
		sourceFiles, err := store.Store.List(path, filestore.Parquet)
		if err != nil {
			return nil, err
		}
		groups, err := filestore.NewFilePathGroup(sourceFiles, filestore.DateTimeDirectoryGrouping)
		if err != nil {
			return nil, err
		}
		newest, err := groups.GetFirst()
		if err != nil {
			return nil, err
		}
		matDir, err := store.Store.CreateFilePath(newest[0].KeyPrefix(), true)
		if err != nil {
			return nil, err
		}
		source := pysparkSourceInfo{
			Location:     matDir.ToURI(),
			LocationType: string(pl.FileStoreLocationType),
			Provider:     pt.Type(store.Store.Type()),
		}
		jsonSource, err := source.Serialize()
		if err != nil {
			return nil, err
		}
		materializationPaths[i] = jsonSource
	}
	return materializationPaths, nil
}

func createJoinQuery(numFeatures int) string {
	query := ""
	asEntity := ""
	withFeatures := ""
	joinTables := ""
	featureColumns := ""

	for i := 0; i < numFeatures; i++ {
		if i > 0 {
			joinTables += "FULL OUTER JOIN "
		}
		withFeatures += fmt.Sprintf(", source_%d.value AS feature%d, source_%d.ts AS TS%d ", i, i+1, i, i+1)
		featureColumns += fmt.Sprintf(", feature%d", i+1)
		joinTables += fmt.Sprintf("source_%d ", i)
		if i == 1 {
			joinTables += fmt.Sprintf("ON %s = source_%d.entity ", asEntity, i)
			asEntity += ", "
		}
		if i > 1 {
			joinTables += fmt.Sprintf("ON COALESCE(%s) = source_%d.entity ", asEntity, i)
			asEntity += ", "
		}
		asEntity += fmt.Sprintf("source_%d.entity", i)
	}
	if numFeatures == 1 {
		query = fmt.Sprintf("SELECT %s AS entity %s FROM source_0", asEntity, withFeatures)
	} else {
		query = fmt.Sprintf("SELECT COALESCE(%s) AS entity %s FROM %s", asEntity, withFeatures, joinTables)
	}
	return query
}

func (store *SparkOfflineStore) Close() error {
	return nil
}

// For Spark, the CheckHealth method must confirm 3 things:
// 1. The Spark executor is able to run a Spark job
// 2. The Spark job is able to read/write to the configured blob store
// 3. Backend business logic is able to read/write to the configured blob store
// To achieve this check, we'll perform the following steps:
// 1. Write to <blob-store>/featureform/HealthCheck/health_check.csv
// 2. Run a Spark job that reads from <blob-store>/featureform/HealthCheck/health_check.csv and
// writes to <blob-store>/featureform/HealthCheck/health_check_out.csv
func (store *SparkOfflineStore) CheckHealth() (bool, error) {
	logger := store.Logger.With("running-health-check", "true")
	if config.ShouldSkipSparkHealthCheck() {
		logger.Debug("Skipping health check")
		return true, nil
	}
	logger.Info("Running spark offline store health check")
	healthCheckPath, err := store.Store.CreateFilePath("featureform/HealthCheck/health_check.csv", false)
	if err != nil {
		wrapped := fferr.NewInternalError(err)
		wrapped.AddDetails("store_type", store.Type(), "action", "file_path_creation")
		logger.Errorw("Failed to create file path", "error", wrapped)
		return false, wrapped
	}
	csvBytes, err := store.getHealthCheckCSVBytes()
	if err != nil {
		errMsg := fmt.Sprintf(
			"failed to create mock CSV data for health check file: %v", err,
		)
		logger.Error(errMsg)
		return false, fferr.NewInternalErrorf(errMsg)
	}
	if err := store.Store.Write(healthCheckPath, csvBytes); err != nil {
		wrapped := fferr.NewConnectionError(store.Type().String(), err)
		wrapped.AddDetail("action", "write")
		logger.Errorw("Failed to write to health check path", "err", wrapped)
		return false, wrapped
	}
	healthCheckOutPath, err := store.Store.CreateFilePath("featureform/HealthCheck/health_check_out", true)
	if err != nil {
		wrapped := fferr.NewInternalError(err)
		wrapped.AddDetails("store_type", store.Type(), "action", "file_path_creation")
		logger.Errorw("Failed to create health check filepath", "err", wrapped)
		return false, wrapped
	}
	source := pysparkSourceInfo{
		Location:     healthCheckPath.ToURI(),
		LocationType: string(pl.FileStoreLocationType),
		Provider:     store.Type(),
	}

	// We hardcode client to get the best error message from health check.
	args, err := sparkScriptCommandDef{
		DeployMode:     types.SparkClientDeployMode,
		TFType:         SQLTransformation,
		OutputLocation: pl.NewFileLocation(healthCheckOutPath),
		Code:           "SELECT * FROM source_0",
		SourceList:     []pysparkSourceInfo{source},
		JobType:        Transform,
		Store:          store.Store,
		Mappings:       make([]SourceMapping, 0),
	}.PrepareCommand(logger)
	if err != nil {
		logger.Errorw("Failed to prepare spark submit command", "error", err)
		return false, err
	}
	opts := SparkJobOptions{
		MaxJobDuration: 30 * time.Minute,
		JobName:        "featureform-health-check",
	}
	if err := store.Executor.RunSparkJob(args, store.Store, opts, nil); err != nil {
		wrapped := fferr.NewConnectionError(store.Type().String(), err)
		wrapped.AddDetail("action", "job_submission")
		logger.Errorw("Spark health check failed", "error", wrapped)
		return false, wrapped
	}
	logger.Info("Spark health check job succeeded")

	if store.UsesCatalog() {
		logger.Info("Running aws glue health check")
		glueS3Filestore, isGlueS3Filestore := store.Store.(*SparkGlueS3FileStore)
		if !isGlueS3Filestore {
			return false, fferr.NewInternalErrorf("filestore is not SparkGlueS3FileStore; received %T", store.Store)
		}
		db, err := glueS3Filestore.GlueClient.GetDatabase(context.Background(), &glue.GetDatabaseInput{Name: &store.GlueConfig.Database})
		if err != nil {
			return false, fferr.NewProviderConfigError(store.Type().String(), err)
		}
		if db.Database.LocationUri == nil {
			return false, fferr.NewProviderConfigError(store.Type().String(), fmt.Errorf("database location is required or doesn't exist; please, check the Glue database configuration and reapply the provider"))
		}
		fp := filestore.S3Filepath{}
		if err := fp.ParseDirPath(*db.Database.LocationUri); err != nil {
			return false, fferr.NewProviderConfigError(store.Type().String(), err)
		}
	}

	return true, nil
}

func (store *SparkOfflineStore) getHealthCheckCSVBytes() ([]byte, error) {
	buf := new(bytes.Buffer)
	w := csv.NewWriter(buf)
	records := [][]string{
		{"entity", "value", "ts"},
		{"entity1", "value1", "2020-01-01T00:00:00Z"},
		{"entity2", "value3", "2020-01-02T00:00:00Z"},
		{"entity3", "value3", "2020-01-03T00:00:00Z"},
	}
	if err := w.WriteAll(records); err != nil {
		return nil, fferr.NewInternalError(err)
	}
	return buf.Bytes(), nil
}

func sparkOfflineStoreFactory(config pc.SerializedConfig) (Provider, error) {
	sc := pc.SparkConfig{}
	logger := logging.NewLogger("spark")
	if err := sc.Deserialize(config); err != nil {
		logger.Errorw("Invalid config to initialize spark offline store", "error", err)
		return nil, err
	}
	logger.Infow("Creating Spark executor:", "type", sc.ExecutorType)
	exec, err := NewSparkExecutor(sc.ExecutorType, sc.ExecutorConfig, logger)
	if err != nil {
		logger.Errorw("Failure initializing Spark executor", "type", sc.ExecutorType, "error", err)
		return nil, err
	}

	// TODO get rid of this once catalog is a first class citizen on the spark store
	// But for now we use a GlueS3 store type if there is a glue config
	var storeType = sc.StoreType
	if sc.UsesCatalog() {
		storeType = filestore.Glue
	}

	logger.Infow("Creating Spark store:", "type", storeType)
	serializedFilestoreConfig, err := sc.StoreConfig.Serialize()
	if err != nil {
		return nil, err
	}

	store, err := CreateSparkFileStore(storeType, sc.GlueConfig, serializedFilestoreConfig)
	if err != nil {
		logger.Errorw("Failure initializing blob store", "type", storeType, "error", err)
		return nil, err
	}
	logger.Info("Uploading Spark script to store")

	logger.Debugf("Store type: %s", storeType)
	if err := exec.InitializeExecutor(store); err != nil {
		logger.Errorw("Failure initializing executor", "error", err)
		return nil, err
	}
	logger.Info("Created Spark Offline Store")
	queries := defaultPythonOfflineQueries{
		Logger: logger,
	}

	sparkOfflineStore := SparkOfflineStore{
		Executor:   exec,
		Store:      store,
		GlueConfig: sc.GlueConfig,
		Logger:     logger,
		query:      &queries,
		BaseProvider: BaseProvider{
			ProviderType:   pt.SparkOffline,
			ProviderConfig: config,
		},
	}
	return &sparkOfflineStore, nil
}

type SparkJobOptions struct {
	MaxJobDuration time.Duration
	JobName        string
}

type SparkArgsOptions struct{}

func newBaseExecutor() (baseExecutor, error) {
	configFiles, err := config.CreateSparkScriptConfig()
	if err != nil {
		return baseExecutor{}, err
	}
	return baseExecutor{
		files: configFiles,
	}, nil
}

type baseExecutor struct {
	files config.SparkFileConfigs
}

type SparkExecutor interface {
	InitializeExecutor(store SparkFileStoreV2) error
	RunSparkJob(cmd *sparkCommand, store SparkFileStoreV2, opts SparkJobOptions, tfOpts TransformationOptions) error
	SupportsTransformationOption(opt TransformationOptionType) (bool, error)
}

func NewSparkExecutor(
	execType pc.SparkExecutorType,
	config pc.SparkExecutorConfig,
	logger logging.Logger,
) (SparkExecutor, error) {
	switch execType {
	case pc.EMR:
		emrConfig, ok := config.(*pc.EMRConfig)
		if !ok {
			return nil, fferr.NewInternalError(fmt.Errorf("cannot convert config into 'EMRConfig'"))
		}
		return NewEMRExecutor(*emrConfig, logger)
	case pc.Databricks:
		databricksConfig, ok := config.(*pc.DatabricksConfig)
		if !ok {
			return nil, fferr.NewInternalError(fmt.Errorf("cannot convert config into 'DatabricksConfig'"))
		}
		return NewDatabricksExecutor(*databricksConfig, logger)
	case pc.SparkGeneric:
		sparkGenericConfig, ok := config.(*pc.SparkGenericConfig)
		if !ok {
			return nil, fferr.NewInternalError(fmt.Errorf("cannot convert config into 'SparkGenericConfig'"))
		}
		return NewSparkGenericExecutor(*sparkGenericConfig, logger)
	default:
		return nil, fferr.NewInvalidArgumentErrorf("the executor type ('%s') is not supported", execType)
	}
}

func (spark *SparkOfflineStore) RegisterPrimaryFromSourceTable(id ResourceID, tableLocation pl.Location) (PrimaryTable, error) {
	switch lt := tableLocation.(type) {
	case *pl.SQLLocation:
		return nil, fferr.NewInternalErrorf("SQLLocation not supported for primary table registration")
	case *pl.FileStoreLocation:
		return blobRegisterPrimary(id, *lt, spark.Logger.SugaredLogger, spark.Store)
	case *pl.CatalogLocation:
		return spark.registerPrimaryCatalogTable(id, *lt, spark.Logger, spark.Store)
	default:
		return nil, fferr.NewInternalErrorf("unsupported location type for primary table registration")
	}
}

func (spark *SparkOfflineStore) registerPrimaryCatalogTable(id ResourceID, catalogLocation pl.CatalogLocation, logger logging.Logger, store FileStore) (PrimaryTable, error) {
	return nil, nil
}

func (spark *SparkOfflineStore) RegisterResourceFromSourceTable(id ResourceID, schema ResourceSchema, opts ...ResourceOption) (OfflineTable, error) {
	if len(opts) > 0 {
		spark.Logger.Errorf("Spark Offline Store does not currently support resource options; received %v for resource %v", opts, id)
	}
	return blobRegisterResourceFromSourceTable(id, schema, spark.Logger.SugaredLogger, spark.Store)
}

func (spark *SparkOfflineStore) SupportsTransformationOption(opt TransformationOptionType) (bool, error) {
	spark.Logger.Debugw("Checking if Spark supports option", "type", opt)
	if supports, err := spark.Executor.SupportsTransformationOption(opt); err != nil {
		return false, err

	} else if supports {
		return true, nil
	}
	return false, nil
}

func (spark *SparkOfflineStore) CreateTransformation(config TransformationConfig, opts ...TransformationOption) error {
	return spark.transformation(config, false, opts)
}

func (spark *SparkOfflineStore) transformation(config TransformationConfig, isUpdate bool, opts TransformationOptions) error {
	if config.Type == SQLTransformation {
		return spark.sqlTransformation(config, isUpdate, opts)
	} else if config.Type == DFTransformation {
		return spark.dfTransformation(config, isUpdate, opts)
	} else {
		spark.Logger.Errorw("Unsupported transformation type", config.Type)
		return fferr.NewInvalidArgumentError(fmt.Errorf("the transformation type '%v' is not supported", config.Type))
	}
}

type pysparkOutputTable struct {
	Type      string                 `json:"type"` // filestore, catalog
	Filestore *pysparkFilestoreTable `json:"filestore"`
	Catalog   *pysparkCatalogTable   `json:"catalog"`
}

type pysparkFilestoreTable struct {
	Path   string `json:"path"`
	Format string `json:"format"`
}

type pysparkCatalogTable struct {
	Database  string `json:"database"`
	Table     string `json:"table"`
	Warehouse string `json:"warehouse"`
	Region    string `json:"region"`
}

func (spark *SparkOfflineStore) sqlTransformation(config TransformationConfig, isUpdate bool, tfOpts TransformationOptions) error {
	logger := spark.Logger.With(
		"transform-config", config,
		"isUpdate", isUpdate,
		"transform-options", tfOpts,
	)
	logger.Debug("Running SQL transformation")
	updatedQuery, sources, err := spark.prepareQueryForSpark(config.Query, config.SourceMapping)
	if err != nil {
		logger.Errorw("Could not generate updated query for spark transformation", "error", err)
		return err
	}
	logger = logger.With("update-query", updatedQuery, "sources", sources)
	logger.Debug("Updated query and sources")
	outputLocation, err := spark.outputLocation(config.TargetTableID)
	if err != nil {
		logger.Errorw("Could not generate output location for spark transformation", "error", err)
		return err
	}

	transformationExists, err := spark.Store.Exists(outputLocation)
	if err != nil {
		logger.Errorw("Could not check if transformation exists", "error", err)
		return err
	}

	if !isUpdate && transformationExists {
		logger.Errorw("Creation when transformation already exists", "target", config.TargetTableID, "location", outputLocation.Location())
		return fferr.NewDatasetAlreadyExistsError(config.TargetTableID.Name, config.TargetTableID.Variant, fmt.Errorf(outputLocation.Location()))
	} else if isUpdate && !transformationExists {
		logger.Errorw("Update job attempted when transformation does not exist", "target", config.TargetTableID, "location", outputLocation.Location())
		return fferr.NewDatasetNotFoundError(config.TargetTableID.Name, config.TargetTableID.Variant, fmt.Errorf(outputLocation.Location()))
	}

	logger.Debugw("Running SQL transformation")
	sparkArgs, err := sparkScriptCommandDef{
		DeployMode:     getSparkDeployModeFromEnv(),
		TFType:         SQLTransformation,
		OutputLocation: outputLocation,
		Code:           updatedQuery,
		SourceList:     sources,
		JobType:        Transform,
		Store:          spark.Store,
		Mappings:       config.SourceMapping,
	}.PrepareCommand(logger)
	if err != nil {
		logger.Errorw("Problem creating spark submit arguments", "error", err, "args", sparkArgs)
		return err
	}

	opts := SparkJobOptions{
		MaxJobDuration: config.MaxJobDuration,
		JobName: fmt.Sprintf(
			"featureform-sql-transformation--%s--%s",
			config.TargetTableID.Name,
			config.TargetTableID.Variant,
		),
	}
	logger.Debugw("Running spark job", "args", sparkArgs, "options", opts)
	if err := spark.Executor.RunSparkJob(sparkArgs, spark.Store, opts, tfOpts); err != nil {
		logger.Errorw("spark submit job for transformation failed to run", "target", config.TargetTableID, "error", err)
		return err
	}
	logger.Debugw("Successfully ran SQL transformation")
	return nil
}

func (spark *SparkOfflineStore) dfTransformation(config TransformationConfig, isUpdate bool, tfOpts TransformationOptions) error {
	logger := spark.Logger.With(
		"type",
		config.Type,
		"name",
		config.TargetTableID.Name,
		"variant",
		config.TargetTableID.Variant,
	)
	logger.Info("Creating DF transformation")

	picklePath := ps.ResourceToPicklePath(
		config.TargetTableID.Name,
		config.TargetTableID.Variant,
	)
	logger = logger.With("df-pickle-path", picklePath)
	pickledTransformationPath, err := spark.Store.CreateFilePath(
		picklePath, false,
	)
	if err != nil {
		logger.Errorw("Unable to create file path for pickle path", "err", err)
		return err
	}

	pickleExists, err := spark.Store.Exists(pl.NewFileLocation(pickledTransformationPath))
	if err != nil {
		logger.Errorw("Unable to check if pickle exists", "err", err)
		return err
	}

	// If the transformation is not an update, the pickle file should not exist yet
	datasetAlreadyExists := pickleExists && !isUpdate
	// If the transformation is an update, as it will be for scheduled transformation, the pickle file must exist
	datasetNotFound := !pickleExists && isUpdate

	if datasetAlreadyExists {
		logger.Error("Transformation already exists")
		return fferr.NewDatasetAlreadyExistsError(
			config.TargetTableID.Name,
			config.TargetTableID.Variant,
			fmt.Errorf(pickledTransformationPath.ToURI()),
		)
	}

	if datasetNotFound {
		logger.Errorw(
			"Transformation doesn't exists at destination but is being updated",
		)
		return fferr.NewDatasetNotFoundError(
			config.TargetTableID.Name,
			config.TargetTableID.Variant,
			fmt.Errorf(pickledTransformationPath.ToURI()),
		)
	}

	// It's important to set the scheme to s3:// here because the runner script uses boto3 to read the file, and it expects an s3:// path
	if err := pickledTransformationPath.SetScheme(filestore.S3Prefix); err != nil {
		logger.Errorw("Unable to set scheme to s3", "err", err)
		return err
	}

	if err := spark.Store.Write(pickledTransformationPath, config.Code); err != nil {
		logger.Errorw("Unable to write pickle file", "err", err)
		return err
	}

	logger.Info("Successfully wrote transformation pickle file")
	pysparkSourceInfos, err := createSourceInfo(config.SourceMapping, logger)
	if err != nil {
		logger.Errorw("Unable to create source info", "err", err)
		return err
	}

	outputLocation, err := spark.outputLocation(config.TargetTableID)
	if err != nil {
		logger.Errorw("Could not generate output location for spark transformation", "error", err)
		return err
	}
	logger.With("output-location", outputLocation.Location())

	sparkArgs, err := sparkScriptCommandDef{
		DeployMode:     getSparkDeployModeFromEnv(),
		TFType:         DFTransformation,
		OutputLocation: outputLocation,
		Code:           pickledTransformationPath.Key(),
		SourceList:     pysparkSourceInfos,
		JobType:        Transform,
		Store:          spark.Store,
		Mappings:       config.SourceMapping,
	}.PrepareCommand(logger)
	if err != nil {
		logger.Errorw("error getting spark dataframe arguments", err)
		return err
	}

	opts := SparkJobOptions{
		MaxJobDuration: config.MaxJobDuration,
		JobName: fmt.Sprintf(
			"featureform-df-transformation--%s--%s",
			config.TargetTableID.Name,
			config.TargetTableID.Variant,
		),
	}
	logger.Debugw("Running DF transformation", "args", sparkArgs, "options", opts)
	if err := spark.Executor.RunSparkJob(sparkArgs, spark.Store, opts, tfOpts); err != nil {
		logger.Errorw("error running Spark dataframe job", "error", err)
		return err
	}
	logger.Infow("Successfully ran transformation")
	return nil
}

func (spark *SparkOfflineStore) outputLocation(targetTableID ResourceID) (pl.Location, error) {
	if !spark.UsesCatalog() {
		key := ps.ResourceToDirectoryPath(targetTableID.Type.String(), targetTableID.Name, targetTableID.Variant)
		fp, err := spark.Store.CreateFilePath(key, true)
		if err != nil {
			return nil, err
		}
		return pl.NewFileLocation(fp), nil
	}
	_, isEMR := spark.Executor.(*EMRExecutor)
	if !isEMR {
		return nil, fferr.NewInternalErrorf("AWS Glue is only supported on EMR")
	}
	tableName, err := ps.ResourceToCatalogTableName(targetTableID.Type.String(), targetTableID.Name, targetTableID.Variant)
	if err != nil {
		return nil, err
	}
	return pl.NewCatalogLocation(spark.GlueConfig.Database, tableName, string(spark.GlueConfig.TableFormat)), nil
}

func createSourceInfo(mapping []SourceMapping, logger logging.Logger) ([]pysparkSourceInfo, error) {
	sources := make([]pysparkSourceInfo, 0)

	for _, m := range mapping {
		logger.Debugw("Source mapping in createSourceInfo", "mapping", m)
		var source pysparkSourceInfo

		switch m.ProviderType {
		case pt.SparkOffline:
			logger.Debugw("Processing SparkOffline provider", "source_location", m.Location.Location(), "location_type", fmt.Sprintf("%T", m.Location))

			var sparkConfig pc.SparkConfig
			if err := sparkConfig.Deserialize(m.ProviderConfig); err != nil {
				return nil, err
			}

			switch lt := m.Location.(type) {
			case *pl.FileStoreLocation:
				source = pysparkSourceInfo{
					Location:     lt.Location(),
					LocationType: string(lt.Type()),
				}
			case *pl.CatalogLocation:
				source = pysparkSourceInfo{
					Location:     lt.Location(),
					LocationType: string(lt.Type()),
					TableFormat:  lt.TableFormat(),
				}
			default:
				return nil, fferr.NewInternalErrorf("unsupported location type for query replacement: %T", m.Location)
			}

			source.Provider = pt.SparkOffline
			source.TimestampColumnName = m.TimestampColumnName

			if sparkConfig.UsesCatalog() && sparkConfig.GlueConfig.AssumeRoleArn != "" {
				source.AwsAssumeRoleArn = sparkConfig.GlueConfig.AssumeRoleArn
			}

		case pt.SnowflakeOffline:
			var config pc.SnowflakeConfig
			if err := config.Deserialize(m.ProviderConfig); err != nil {
				logger.Errorw("Error deserializing Snowflake config", "error", err)
				return nil, err
			}

			source = pysparkSourceInfo{
				Location:            m.Source,
				LocationType:        string(m.Location.Type()),
				Provider:            pt.SnowflakeOffline,
				Database:            config.Database,
				Schema:              config.Schema,
				TimestampColumnName: m.TimestampColumnName,
			}

		default:
			logger.Errorw("Unsupported source type", "source_type", m.ProviderType)
			return nil, fferr.NewInternalErrorf("unsupported source type: %s", m.ProviderType.String())
		}

		// Append the source struct directly to the sources slice
		sources = append(sources, source)
		logger.Debugw("Appended source", "source", source)
	}

	return sources, nil
}

func (spark *SparkOfflineStore) prepareQueryForSpark(query string, mapping []SourceMapping) (string, []pysparkSourceInfo, error) {
	spark.Logger.Debugw("Updating query", "query", query, "mapping", mapping)
	sources := make([]pysparkSourceInfo, len(mapping))
	replacements := make(
		[]string,
		len(mapping)*2,
	) // It's times 2 because each replacement will be a pair; (original, replacedValue)

	for i, m := range mapping {
		replacements = append(replacements, m.Template)
		spark.Logger.Debugw("Source mapping in prepareQueryForSpark", "template", m.Template, "index", i)
		replacements = append(replacements, fmt.Sprintf("source_%v", i))
		var source pysparkSourceInfo

		switch m.ProviderType {
		case pt.SparkOffline:
			spark.Logger.Debugw("Source mapping in prepareQueryForSpark", "source_location", m.Location.Location(), "location_type", fmt.Sprintf("%T", m.Location))

			sparkConfig := pc.SparkConfig{}
			if err := sparkConfig.Deserialize(m.ProviderConfig); err != nil {
				return "", sources, err
			}

			switch lt := m.Location.(type) {
			case *pl.FileStoreLocation:
				source = pysparkSourceInfo{
					Location:     lt.Location(),
					LocationType: string(lt.Type()),
				}
			case *pl.CatalogLocation:
				source = pysparkSourceInfo{
					Location:     lt.Location(),
					LocationType: string(lt.Type()),
					TableFormat:  lt.TableFormat(),
				}
			default:
				return "", nil, fferr.NewInternalErrorf("unsupported location type for query replacement: %T", m.Location)
			}
			source.Provider = pt.SparkOffline
			source.TimestampColumnName = m.TimestampColumnName

			if sparkConfig.UsesCatalog() && sparkConfig.GlueConfig.AssumeRoleArn != "" {
				source.AwsAssumeRoleArn = sparkConfig.GlueConfig.AssumeRoleArn
			}
		case pt.SnowflakeOffline:
			config := pc.SnowflakeConfig{}
			if err := config.Deserialize(m.ProviderConfig); err != nil {
				spark.Logger.Errorw("Error deserializing snowflake sparkConfig", "error", err)
				return "", nil, err
			}
			sqlLocation, ok := m.Location.(*pl.SQLLocation)
			if !ok {
				return "", nil, fferr.NewInternalErrorf("location for SnowflakeOffline source mapping is not a SQLLocation: %T", m.Location)
			}
			database := config.Database
			if sqlLocation.GetDatabase() != "" {
				database = sqlLocation.GetDatabase()
			}
			schema := config.Schema
			if sqlLocation.GetSchema() != "" {
				schema = sqlLocation.GetSchema()
			}
			source = pysparkSourceInfo{
				Location:            sqlLocation.GetTable(),
				LocationType:        string(m.Location.Type()),
				Provider:            pt.SnowflakeOffline,
				Database:            database,
				Schema:              schema,
				TimestampColumnName: m.TimestampColumnName,
			}
			spark.Logger.Debugw("Source mapping in prepareQueryForSpark", "source", source)
		default:
			spark.Logger.Errorw("Unsupported source type", "source_type", m.ProviderType)
			return "", nil, fferr.NewInternalErrorf("unsupported source type: %s", m.ProviderType.String())
		}
		sources[i] = source
	}

	replacer := strings.NewReplacer(replacements...)
	updatedQuery := replacer.Replace(query)

	if strings.Contains(updatedQuery, "{{") {
		spark.Logger.Errorw("Template replace failed", "query", updatedQuery, "mapping", mapping)
		err := fferr.NewInternalErrorf("template replacement error")
		err.AddDetail("Query", updatedQuery)
		return "", nil, err
	}
	return updatedQuery, sources, nil
}

func (spark *SparkOfflineStore) ResourceLocation(id ResourceID, resource any) (pl.Location, error) {
	if spark.UsesCatalog() {
		table, err := ps.ResourceToCatalogTableName(id.Type.String(), id.Name, id.Variant)
		if err != nil {
			return nil, err
		}
		return pl.NewCatalogLocation(spark.GlueConfig.Database, table, string(spark.GlueConfig.TableFormat)), nil
	}

	path, err := spark.Store.CreateFilePath(id.ToFilestorePath(), true)
	if err != nil {
		return nil, errors.Wrap(err, "could not create dir path")
	}

	newestFile, err := spark.Store.NewestFileOfType(path, filestore.Parquet)
	if err != nil {
		return nil, errors.Wrap(err, "could not get newest file")
	}

	newestFileDirPathDateTime, err := spark.Store.CreateFilePath(newestFile.KeyPrefix(), true)
	if err != nil {
		return nil, fmt.Errorf("could not create directory path for spark newestFile: %v", err)
	}
	return pl.NewFileLocation(newestFileDirPathDateTime), nil
}

// TODO: Currently, GetTransformationTable is only used in the context of serving source data as an iterator,
// and given we currently cannot serve catalog tables in this way, there's no need to implement support for
// catalog locations here. However, eventually, we'll need to address this gap in implementation.
func (spark *SparkOfflineStore) GetTransformationTable(id ResourceID) (TransformationTable, error) {
	transformationPath, err := spark.Store.CreateFilePath(id.ToFilestorePath(), true)
	if err != nil {
		return nil, err
	}
	spark.Logger.Debugw("Retrieved transformation source", "id", id, "filePath", transformationPath.ToURI())
	return &FileStorePrimaryTable{spark.Store, transformationPath, TableSchema{}, true, id}, nil
}

func (spark *SparkOfflineStore) UpdateTransformation(config TransformationConfig, opts ...TransformationOption) error {
	return spark.transformation(config, true, opts)
}

// TODO: add a comment akin to the one explaining the logic for CreateResourceTable
// **NOTE:** Unlike the pathway for registering a primary table from a data source that previously existed in the filestore, this
// method controls the location of the data source that will be written to once the primary table (i.e. a file that simply holds the
// fully qualified URL pointing to the source file), so it's important to consider what pattern we adopt here.
func (spark *SparkOfflineStore) CreatePrimaryTable(id ResourceID, schema TableSchema) (PrimaryTable, error) {
	if err := id.check(Primary); err != nil {
		return nil, err
	}
	primaryTableFilepath, err := spark.Store.CreateFilePath(id.ToFilestorePath(), false)
	if err != nil {
		return nil, err
	}
	if exists, err := spark.Store.Exists(pl.NewFileLocation(primaryTableFilepath)); err != nil {
		return nil, err
	} else if exists {
		return nil, fferr.NewDatasetAlreadyExistsError(id.Name, id.Variant, fmt.Errorf(primaryTableFilepath.ToURI()))
	}
	// Create a URL in the same directory as the primary table that follows the naming convention <VARIANT>_src.parquet
	schema.SourceTable = fmt.Sprintf(
		"%s/%s/src.parquet",
		primaryTableFilepath.ToURI(),
		time.Now().Format("2006-01-02-15-04-05-999999"),
	)
	data, err := schema.Serialize()
	if err != nil {
		return nil, err
	}
	err = spark.Store.Write(primaryTableFilepath, data)
	if err != nil {
		return nil, err
	}
	return &FileStorePrimaryTable{spark.Store, primaryTableFilepath, schema, false, id}, nil
}

func (spark *SparkOfflineStore) GetPrimaryTable(id ResourceID, source metadata.SourceVariant) (PrimaryTable, error) {
	return fileStoreGetPrimary(id, spark.Store, spark.Logger.SugaredLogger)
}

// Unlike a resource table created from a source table, which is effectively a pointer in the filestore to the source table
// with the names of the entity, value and timestamp columns, the resource table created by this method is the data itself.
// This requires a means of differentiating between the two types of resource tables such that we know when/how to read one
// versus the other.
//
// Currently, a resource table created from a source table is located at /featureform/Feature/<NAME DIR>/<VARIANT FILE>, where
// <VARIANT FILE>:
// * has no file extension
// * is the serialization JSON representation of the struct ResourceSchema (i.e. {"Entity":"entity","Value":"value","TS":"ts","SourceTable":"abfss://..."})
//
// One option is the keep with the above pattern by populating "SourceTable" with the path to a source table contained in a subdirectory of
// the resource directory in the pattern Spark uses (i.e. /featureform/Feature/<NAME DIR>/<VARIANT DIR>/<DATETIME DIR>/src.parquet).
func (spark *SparkOfflineStore) CreateResourceTable(id ResourceID, schema TableSchema) (OfflineTable, error) {
	if err := id.check(Feature, Label); err != nil {
		return nil, err
	}
	resourceTableFilepath, err := spark.Store.CreateFilePath(id.ToFilestorePath(), false)
	if err != nil {
		return nil, err
	}
	if exists, err := spark.Store.Exists(pl.NewFileLocation(resourceTableFilepath)); err != nil {
		return nil, err
	} else if exists {
		return nil, fferr.NewDatasetAlreadyExistsError(id.Name, id.Variant, fmt.Errorf(resourceTableFilepath.ToURI()))
	}
	path := fmt.Sprintf("%s/%s/src.parquet", resourceTableFilepath.ToURI(), time.Now().Format("2006-01-02-15-04-05-999999"))
	fp, err := filestore.NewEmptyFilepath(spark.Store.FilestoreType())
	if err != nil {
		return nil, err
	}
	if err := fp.ParseFilePath(path); err != nil {
		return nil, err
	}
	fpLocation := pl.NewFileLocation(fp)
	table := BlobOfflineTable{
		store: spark.Store,
		schema: ResourceSchema{
			// Create a URI in the same directory as the resource table that follows the naming convention <VARIANT>_src.parquet
			SourceTable: fpLocation,
		},
	}
	for _, col := range schema.Columns {
		switch col.Name {
		case string(Entity):
			table.schema.Entity = col.Name
		case string(Value):
			table.schema.Value = col.Name
		case string(TS):
			table.schema.TS = col.Name
		default:
			// TODO: verify the assumption that col.Name should be:
			// * Entity ("entity")
			// * Value ("value")
			// * TS ("ts")
			// makes sense in the context of the schema
			return nil, fmt.Errorf("unexpected column name: %s", col.Name)
		}
	}
	data, err := table.schema.Serialize()
	if err != nil {
		return nil, err
	}
	err = spark.Store.Write(resourceTableFilepath, data)
	if err != nil {
		return nil, err
	}
	return &table, nil
}

func (spark *SparkOfflineStore) GetResourceTable(id ResourceID) (OfflineTable, error) {
	return fileStoreGetResourceTable(id, spark.Store, spark.Logger.SugaredLogger)
}

func blobSparkMaterialization(
	id ResourceID,
	spark *SparkOfflineStore,
	isUpdate bool,
	opts MaterializationOptions,
) (Materialization, error) {
	if err := id.check(Feature); err != nil {
		spark.Logger.Errorw("Attempted to create a materialization of a non feature resource", "type", id.Type)
		return nil, err
	}
	resourceTable, err := spark.GetResourceTable(id)
	if err != nil {
		spark.Logger.Errorw("Attempted to fetch resource table of non registered resource", "error", err)
		return nil, err
	}
	sparkResourceTable, ok := resourceTable.(*BlobOfflineTable)
	if !ok {
		spark.Logger.Errorw("Could not convert resource table to blob offline table", "id", id)
		return nil, fferr.NewInternalErrorf("could not convert offline table with id %v to sparkResourceTable", id)
	}
	var tableFormat string
	if sparkResourceTable.schema.SourceTable.Type() == pl.CatalogLocationType {
		tableFormat = string(sparkResourceTable.schema.SourceTable.(*pl.CatalogLocation).TableFormat())
	}
	// get destination path for the materialization
	materializationID := ResourceID{Name: id.Name, Variant: id.Variant, Type: FeatureMaterialization}
	destinationPath, err := spark.Store.CreateFilePath(materializationID.ToFilestorePath(), true)
	if err != nil {
		return nil, err
	}
	materializationExists, err := spark.Store.Exists(pl.NewFileLocation(destinationPath))
	if err != nil {
		return nil, err
	}
	if materializationExists && !isUpdate {
		spark.Logger.Errorw("Attempted to create a materialization that already exists", "id", id)
		return nil, fferr.NewDatasetAlreadyExistsError(id.Name, id.Variant, fmt.Errorf(destinationPath.ToURI()))
	} else if !materializationExists && isUpdate {
		spark.Logger.Errorw("Attempted to update a materialization that doesn't exists", "id", id)
		return nil, fferr.NewDatasetNotFoundError(id.Name, id.Variant, fmt.Errorf(destinationPath.ToURI()))
	}
	materializationQuery, err := spark.query.materializationCreate(sparkResourceTable.schema)
	if err != nil {
		return nil, err
	}
	sourcePySpark := pysparkSourceInfo{
		Location:     sparkResourceTable.schema.SourceTable.Location(),
		LocationType: string(sparkResourceTable.schema.SourceTable.Type()),
		TableFormat:  tableFormat,
		Provider:     spark.Type(),
	}
	sparkArgs, err := sparkScriptCommandDef{
		DeployMode:     getSparkDeployModeFromEnv(),
		TFType:         SQLTransformation,
		OutputLocation: pl.NewFileLocation(destinationPath),
		Code:           materializationQuery,
		SourceList:     []pysparkSourceInfo{sourcePySpark},
		JobType:        Materialize,
		Store:          spark.Store,
		Mappings:       make([]SourceMapping, 0),
	}.PrepareCommand(spark.Logger)
	if err != nil {
		spark.Logger.Errorw("Problem creating spark submit arguments", "error", err, "args", sparkArgs)
		return nil, err
	}
	sparkArgs.AddConfigs(
		sparkLegacyOutputFormatFlag{
			FileType: opts.Output,
		},
		sparkLegacyIncludeHeadersFlag{
			ShouldInclude: opts.ShouldIncludeHeaders,
		},
	)
	if isUpdate {
		spark.Logger.Debugw("Updating materialization", "id", id)
	} else {
		spark.Logger.Debugw("Creating materialization", "id", id)
	}
	sparkOpts := SparkJobOptions{
		MaxJobDuration: opts.MaxJobDuration,
		JobName:        opts.JobName,
	}
	spark.Logger.Debugw("Running spark job", "args", sparkArgs, "options", sparkOpts)
	if err := spark.Executor.RunSparkJob(sparkArgs, spark.Store, sparkOpts, nil); err != nil {
		spark.Logger.Errorw("Spark submit job failed to run", "error", err)
		return nil, err
	}
	exists, err := spark.Store.Exists(pl.NewFileLocation(destinationPath))
	if err != nil {
		spark.Logger.Errorf("could not check if materialization file exists: %v", err)
		return nil, err
	}
	if !exists {
		spark.Logger.Errorf("materialization not found in directory: %s", destinationPath.ToURI())
		return nil, fferr.NewDatasetNotFoundError(
			materializationID.Name,
			materializationID.Variant,
			fmt.Errorf("materialization not found in directory: %s", destinationPath.ToURI()),
		)
	}
	spark.Logger.Debugw("Successfully created materialization", "id", id)
	return &FileStoreMaterialization{materializationID, spark.Store}, nil
}

func (spark *SparkOfflineStore) CreateMaterialization(id ResourceID, opts MaterializationOptions) (
	Materialization,
	error,
) {
	if opts.DirectCopyTo != nil {
		// This returns nil for Materialization.
		return nil, spark.directCopyMaterialize(id, opts)
	}
	return blobSparkMaterialization(id, spark, false, opts)
}

func (spark *SparkOfflineStore) directCopyMaterialize(id ResourceID, opts MaterializationOptions) error {
	online := opts.DirectCopyTo
	logger := spark.Logger.With("resource_id", id, "online_store_type", fmt.Sprintf("%T", online))
	logger.Debugf("Running direct copy materialization")
	if err := id.check(Feature); err != nil {
		logger.Error("Attempted to create a materialization of a non feature resource")
		return err
	}
	dynamo, ok := online.(*dynamodbOnlineStore)
	if !ok {
		errStr := fmt.Sprintf("Cannot direct copy from Spark to %T", online)
		logger.Error(errStr)
		return fferr.NewInternalErrorf(errStr)
	}
	schema, err := spark.getResourceSchema(id)
	if err != nil {
		errStr := fmt.Sprintf("Failed to get resource schema for %v: %s", id, err)
		logger.Error(errStr)
		return fferr.NewInternalErrorf(errStr)
	}
	sourceTable := schema.SourceTable
	tableFormat := ""
	if sourceTable.Type() == pl.CatalogLocationType {
		tableFormat = string(sourceTable.(*pl.CatalogLocation).TableFormat())
	}
	sourceList := []pysparkSourceInfo{
		pysparkSourceInfo{
			Location:     sourceTable.Location(),
			LocationType: string(sourceTable.Type()),
			TableFormat:  tableFormat,
			Provider:     spark.Type(),
		},
	}
	sparkArgs, err := sparkScriptCommandDef{
		DeployMode:     getSparkDeployModeFromEnv(),
		TFType:         SQLTransformation,
		OutputLocation: pl.NilLocation{},
		Code:           "",
		SourceList:     sourceList,
		JobType:        Materialize,
		Store:          spark.Store,
		Mappings:       make([]SourceMapping, 0),
	}.PrepareCommand(logger)
	if err != nil {
		logger.Errorw("Problem creating spark submit arguments", "error", err, "args", sparkArgs)
		return err
	}
	sparkArgs.AddConfigs(
		sparkDirectCopyFlags{
			Creds: sparkDynamoFlags{
				Region:    dynamo.region,
				AccessKey: dynamo.accessKey,
				SecretKey: dynamo.secretKey,
			},
			Target:          directCopyDynamo,
			TableName:       dynamo.FormatTableName(id.Name, id.Variant),
			FeatureName:     id.Name,
			FeatureVariant:  id.Variant,
			EntityColumn:    schema.Entity,
			ValueColumn:     schema.Value,
			TimestampColumn: schema.TS,
		},
	)
	sparkOpts := SparkJobOptions{
		MaxJobDuration: opts.MaxJobDuration,
		JobName:        opts.JobName,
	}
	logger.Debugw("Running spark job", "args", sparkArgs, "options", sparkOpts)
	if err := spark.Executor.RunSparkJob(sparkArgs, spark.Store, sparkOpts, nil); err != nil {
		logger.Errorw("Spark submit job failed to run", "error", err)
		return err
	}
	logger.Debugw("Successfully created materialization", "id", id)
	return nil
}

func (spark *SparkOfflineStore) SupportsMaterializationOption(opt MaterializationOptionType) (bool, error) {
	spark.Logger.Debugw("Checking if Spark supports option", "type", opt)
	switch opt {
	case DirectCopyDynamo:
		return true, nil
	default:
		return false, nil
	}
}

func (spark *SparkOfflineStore) GetMaterialization(id MaterializationID) (Materialization, error) {
	return fileStoreGetMaterialization(id, spark.Store, spark.Logger.SugaredLogger)
}

func (spark *SparkOfflineStore) UpdateMaterialization(id ResourceID, opts MaterializationOptions) (
	Materialization,
	error,
) {
	return blobSparkMaterialization(id, spark, true, opts)
}

func (spark *SparkOfflineStore) DeleteMaterialization(id MaterializationID) error {
	return fileStoreDeleteMaterialization(id, spark.Store, spark.Logger.SugaredLogger)
}

func (spark *SparkOfflineStore) getResourceSchema(id ResourceID) (ResourceSchema, error) {
	if err := id.check(Feature, Label); err != nil {
		return ResourceSchema{}, err
	}
	spark.Logger.Debugw("Getting resource schema", "id", id)
	table, err := spark.GetResourceTable(id)
	if err != nil {
		spark.Logger.Errorw("Resource not registered in spark store", "id", id, "error", err)
		return ResourceSchema{}, err
	}
	sparkResourceTable, ok := table.(*BlobOfflineTable)
	if !ok {
		spark.Logger.Errorw("could not convert offline table to sparkResourceTable", "id", id)
		return ResourceSchema{}, fferr.NewInternalError(
			fmt.Errorf(
				"could not convert offline table with id %v to sparkResourceTable",
				id,
			),
		)
	}
	spark.Logger.Debugw("Successfully retrieved resource schema", "id", id, "schema", sparkResourceTable.schema)
	return sparkResourceTable.schema, nil
}

func sparkTrainingSet(def TrainingSetDef, spark *SparkOfflineStore, isUpdate bool) error {
	if err := def.check(); err != nil {
		spark.Logger.Errorw("Training set definition not valid", "definition", def, "error", err)
		return err
	}
	logger := spark.Logger.With("id", def.ID)
	sourcePaths := make([]pysparkSourceInfo, 0)
	featureSchemas := make([]ResourceSchema, 0)
	filePath := def.ID.ToFilestorePath()
	logger = logger.With("path", filePath)
	destinationPath, err := spark.Store.CreateFilePath(filePath, true)
	if err != nil {
		logger.Errorw("Failed to create destination path")
		return err
	}
	trainingSetExists, err := spark.Store.Exists(pl.NewFileLocation(destinationPath))
	if err != nil {
		logger.Errorw("Unable to check if path exists")
		return err
	}
	if trainingSetExists && !isUpdate {
		logger.Errorw("Training set already exists")
		return fferr.NewDatasetAlreadyExistsError(def.ID.Name, def.ID.Variant, fmt.Errorf(destinationPath.ToURI()))
	} else if !trainingSetExists && isUpdate {
		logger.Errorw("Training set does not exist")
		return fferr.NewDatasetNotFoundError(def.ID.Name, def.ID.Variant, fmt.Errorf(destinationPath.ToURI()))
	}
	var labelSchema ResourceSchema
	var labelPySparkSource pysparkSourceInfo
	logger.Debugw("Label provider", "provider", def.LabelSourceMapping.ProviderType)
	switch def.LabelSourceMapping.ProviderType {
	case pt.SparkOffline:
		labelSchema, err = spark.getResourceSchema(def.Label)
		if err != nil {
			logger.Errorw("Could not get schema of label in spark store", "label", def.Label, "error", err)
			return err
		}
		var tableFormat string
		if labelSchema.SourceTable.Type() == pl.CatalogLocationType {
			tableFormat = labelSchema.SourceTable.(*pl.CatalogLocation).TableFormat()
		}
		labelPySparkSource = pysparkSourceInfo{
			Location:     labelSchema.SourceTable.Location(),
			LocationType: string(labelSchema.SourceTable.Type()),
			Provider:     def.LabelSourceMapping.ProviderType,
			TableFormat:  tableFormat,
		}
	case pt.SnowflakeOffline:
		config := pc.SnowflakeConfig{}
		if err := config.Deserialize(def.LabelSourceMapping.ProviderConfig); err != nil {
			logger.Errorw("Error deserializing snowflake config", "error", err)
			return err
		}
		labelPySparkSource = pysparkSourceInfo{
			Location:     def.LabelSourceMapping.Source,
			LocationType: string(pl.SQLLocationType),
			Provider:     def.LabelSourceMapping.ProviderType,
			Database:     config.Database,
			Schema:       config.Schema,
		}
		labelSchema = ResourceSchema{
			Entity: "entity",
			Value:  "value",
			TS:     "ts",
		}
	default:
		logger.Errorw("Unsupported label provider", "provider", def.LabelSourceMapping.ProviderType)
		return fferr.NewInternalErrorf("unsupported label provider: %s", def.LabelSourceMapping.ProviderType.String())
	}
	sourcePaths = append(sourcePaths, labelPySparkSource)
	for idx, feature := range def.Features {
		var featureSchema ResourceSchema
		var featureSourceLocation pl.Location

		switch def.FeatureSourceMappings[idx].ProviderType {
		case pt.SparkOffline:
			featureSchema, err = spark.getResourceSchema(feature)
			if err != nil {
				logger.Errorw("Could not get schema of feature in spark store", "feature", feature, "error", err)
				return err
			}
			featureSourceLocation = featureSchema.SourceTable
		case pt.SnowflakeOffline:
			featureSourceLocation = pl.NewSQLLocation(def.FeatureSourceMappings[idx].Source)
			featureSchema = ResourceSchema{
				Entity: "entity",
				Value:  "value",
				TS:     "ts",
			}
		default:
			logger.Errorw("Unsupported feature provider", "provider", def.FeatureSourceMappings[idx].ProviderType)
			return fferr.NewInternalErrorf(
				"unsupported feature provider: %s",
				def.FeatureSourceMappings[idx].ProviderType.String(),
			)
		}
		var tableFormat string
		if featureSourceLocation.Type() == pl.CatalogLocationType {
			tableFormat = featureSourceLocation.(*pl.CatalogLocation).TableFormat()
		}
		featurePySparkSource := pysparkSourceInfo{
			Location:     featureSourceLocation.Location(),
			LocationType: string(featureSourceLocation.Type()),
			Provider:     spark.Type(),
			TableFormat:  tableFormat,
		}
		sourcePaths = append(sourcePaths, featurePySparkSource)
		featureSchemas = append(featureSchemas, featureSchema)
	}
	trainingSetQuery := spark.query.trainingSetCreate(def, featureSchemas, labelSchema)
	sourceMappings := append(def.FeatureSourceMappings, def.LabelSourceMapping)
	sparkArgs, err := sparkScriptCommandDef{
		DeployMode:     getSparkDeployModeFromEnv(),
		TFType:         SQLTransformation,
		OutputLocation: pl.NewFileLocation(destinationPath),
		Code:           trainingSetQuery,
		SourceList:     sourcePaths,
		JobType:        CreateTrainingSet,
		Store:          spark.Store,
		Mappings:       sourceMappings,
	}.PrepareCommand(logger)
	if err != nil {
		logger.Errorw("Problem creating spark submit arguments", "error", err, "args", sparkArgs)
		return err
	}
	logger.Debugw("Creating training set", "definition", def)
	opts := SparkJobOptions{
		MaxJobDuration: time.Hour * 48,
		JobName:        fmt.Sprintf("featureform-training-set--%s--%s", def.ID.Name, def.ID.Variant),
	}
	if err := spark.Executor.RunSparkJob(sparkArgs, spark.Store, opts, nil); err != nil {
		logger.Errorw("Spark submit training set job failed to run", "definition", def.ID, "error", err)
		return err
	}
	trainingSetExists, err = spark.Store.Exists(pl.NewFileLocation(destinationPath))
	if err != nil {
		logger.Errorw("Unable to check if training set exists after running job", "err", err)
		return err
	}
	if !trainingSetExists {
		spark.Logger.Errorw("Training set doesn't exist after running job")
		return fferr.NewDatasetNotFoundError(def.ID.Name, def.ID.Variant, fmt.Errorf(destinationPath.ToURI()))
	}
	spark.Logger.Infow(
		"Successfully created training set",
		"definition",
		def,
		"location",
		destinationPath.ToURI(),
	)
	return nil
}

func (spark *SparkOfflineStore) CreateTrainingSet(def TrainingSetDef) error {
	return sparkTrainingSet(def, spark, false)

}

func (spark *SparkOfflineStore) UpdateTrainingSet(def TrainingSetDef) error {
	return sparkTrainingSet(def, spark, true)
}

func (spark *SparkOfflineStore) GetTrainingSet(id ResourceID) (TrainingSetIterator, error) {
	return fileStoreGetTrainingSet(id, spark.Store, spark.Logger.SugaredLogger)
}

func (spark *SparkOfflineStore) CreateTrainTestSplit(def TrainTestSplitDef) (func() error, error) {
	return nil, fmt.Errorf("not Implemented")
}

func (spark *SparkOfflineStore) GetTrainTestSplit(def TrainTestSplitDef) (
	TrainingSetIterator,
	TrainingSetIterator,
	error,
) {
	return nil, nil, fmt.Errorf("not Implemented")
}

func (spark *SparkOfflineStore) UsesCatalog() bool {
	return spark.GlueConfig != nil
}

func sanitizeSparkSQL(name string) string {
	return name
}
