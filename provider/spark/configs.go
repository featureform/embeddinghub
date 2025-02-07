// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package spark

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/featureform/config"
	"github.com/featureform/filestore"
	"github.com/featureform/helpers/stringset"
	"github.com/featureform/logging"
	"github.com/featureform/logging/redacted"
	pl "github.com/featureform/provider/location"
	pc "github.com/featureform/provider/provider_config"
	"github.com/featureform/provider/types"

	"cloud.google.com/go/dataproc/v2/apiv1/dataprocpb"
	dbjobs "github.com/databricks/databricks-sdk-go/service/jobs"
)

const RedactedString = "<FF_REDACTED>"

type Command struct {
	Script     filestore.Filepath
	ScriptArgs []string
	Configs    Configs
}

func (cmd *Command) AddConfigs(cfgs ...Config) {
	cmd.Configs = append(cmd.Configs, cfgs...)
}

func (cmd *Command) Compile() []string {
	return cmd.Configs.CompileCommand(cmd.Script, cmd.ScriptArgs...)
}

func (cmd *Command) Redacted() *Command {
	return &Command{
		Script:     cmd.Script,
		ScriptArgs: cmd.ScriptArgs,
		Configs:    cmd.Configs.Redacted(),
	}
}

func (cmd *Command) CompileDataprocServerless(projectID, region string) *dataprocpb.CreateBatchRequest {
	list := cmd.Configs.ToSparkFlagsList()
	nativeFlags, scriptFlags := list.SeparateNativeFlags()
	scriptArgs := append(cmd.ScriptArgs, scriptFlags.SparkStringFlags()...)
	batch := &dataprocpb.Batch{
		BatchConfig: &dataprocpb.Batch_PysparkBatch{
			PysparkBatch: &dataprocpb.PySparkBatch{
				MainPythonFileUri: cmd.Script.ToURI(),
				Args:              scriptArgs,
			},
		},
	}
	nativeFlags.ApplyToDataprocServerless(batch)
	return &dataprocpb.CreateBatchRequest{
		Parent: fmt.Sprintf("projects/%s/locations/%s", projectID, region),
		Batch:  batch,
	}
}

func (cmd *Command) CompileDatabricks() dbjobs.Task {
	list := cmd.Configs.ToSparkFlagsList()
	nativeFlags, scriptFlags := list.SeparateNativeFlags()
	scriptArgs := append(cmd.ScriptArgs, scriptFlags.SparkStringFlags()...)
	var pyFile string
	if config.ShouldUseDBFS() {
		// In this case, we assume the script has been copied to dbfs already
		// TODO(simba) move this formatting elsewhere
		pyFile = fmt.Sprintf("dbfs:/tmp/%s", cmd.Script.Key())
	} else {
		pyFile = cmd.Script.ToURI()
	}
	task := dbjobs.Task{
		SparkPythonTask: &dbjobs.SparkPythonTask{
			PythonFile: pyFile,
			Parameters: scriptArgs,
		},
	}
	nativeFlags.ApplyToDatabricks(&task)
	return task
}

// CompileScriptOnly returns the script location as a string followed by
// all the arguments in the script for use in providers like Databricks.
func (cmd *Command) CompileScriptOnly() (string, []string) {
	list := cmd.Configs.ToSparkFlagsList()
	_, scriptFlags := list.SeparateNativeFlags()
	args := append(cmd.ScriptArgs, scriptFlags.SparkStringFlags()...)
	return cmd.Script.ToURI(), args
}

type Configs []Config

func (cfgs Configs) ToSparkFlagsList() FlagsList {
	list := make(FlagsList, len(cfgs))
	for i, cfg := range cfgs {
		list[i] = cfg.SparkFlags()
	}
	return list
}

func (cfgs Configs) CompileCommand(scriptLoc filestore.Filepath, args ...string) []string {
	cmd := []string{
		"spark-submit",
	}
	list := cfgs.ToSparkFlagsList()
	native, script := list.SeparateNativeFlags()
	cmd = append(cmd, native.SparkStringFlags()...)
	cmd = append(cmd, scriptLoc.ToURI())
	cmd = append(cmd, args...)
	cmd = append(cmd, script.SparkStringFlags()...)
	return cmd
}

// Redact replaces all sensitive strings with RedactedString
func (cfgs Configs) Redacted() Configs {
	redacted := make(Configs, len(cfgs))
	for i, cfg := range cfgs {
		redacted[i] = cfg.Redacted()
	}
	return redacted
}

type Config interface {
	SparkFlags() Flags
	Redacted() Config
}

type FlagsList []Flags

func (flagsList FlagsList) SeparateNativeFlags() (NativeFlags, ScriptFlags) {
	allNative := make(NativeFlags, 0)
	allScript := make(ScriptFlags, 0)
	for _, flags := range flagsList {
		native, script := flags.SeparateNativeFlags()
		allNative = append(allNative, native...)
		allScript = append(allScript, script...)
	}
	return allNative, allScript
}

func (flagsList FlagsList) SparkStringFlags() []string {
	args := make([]string, 0)
	for _, flags := range flagsList {
		args = append(args, flags.SparkStringFlags()...)
	}
	return args
}

type NativeFlags []NativeFlagStringer

func (flags NativeFlags) SparkStringFlags() []string {
	casted := make(Flags, len(flags))
	for i, flag := range flags {
		casted[i] = flag
	}
	return casted.SparkStringFlags()
}

func (flags NativeFlags) ApplyToDataprocServerless(batch *dataprocpb.Batch) {
	for _, flag := range flags {
		flag.ApplyToDataprocServerless(batch)
	}
}

func (flags NativeFlags) ApplyToDatabricks(job *dbjobs.Task) {
	for _, flag := range flags {
		flag.ApplyToDatabricks(job)
	}
}

type ScriptFlags Flags

func (flags ScriptFlags) SparkStringFlags() []string {
	return Flags(flags).SparkStringFlags()
}

type Flags []FlagStringer

func (flags Flags) SeparateNativeFlags() (NativeFlags, ScriptFlags) {
	native := make(NativeFlags, 0)
	script := make(ScriptFlags, 0)
	for _, flag := range flags {
		if flag.IsSparkSubmitNative() {
			native = append(native, flag.(NativeFlagStringer))
		} else {
			script = append(script, flag)
		}
	}
	return native, script
}

func (flags Flags) SparkStringFlags() []string {
	combinedFlags := flags.combinedFlags()
	args := make([]string, 0, len(combinedFlags)*2)
	for _, flag := range combinedFlags {
		args = append(args, flag.SparkStringFlags()...)
	}
	return args
}

func (flags Flags) combinedFlags() Flags {
	combined := make(Flags, 0)
	for i := 0; i < len(flags); i++ {
		flag := flags[i]
		if flag == nil {
			continue
		}
		for j := i + 1; j < len(flags); j++ {
			other := flags[j]
			if combo := flag.TryCombine(other); combo != nil {
				flag = combo
				flags[j] = nil
			}
		}
		combined = append(combined, flag)
	}
	return combined
}

type FlagStringer interface {
	SparkStringFlags() []string
	// Used to distinguish between script flags and spark-submit flags.
	// spark-submit <native flags> file.py <script flags>
	IsSparkSubmitNative() bool
	// If two flag stringers have the same combine key, they can be
	// combined. This is important in flags like --packages that
	// need to be a comma separated list.
	TryCombine(FlagStringer) FlagStringer
}

type NativeFlagStringer interface {
	// Apply a flag to Dataproc serverless
	ApplyToDataprocServerless(*dataprocpb.Batch)
	// Apply a flag to Databricks
	ApplyToDatabricks(*dbjobs.Task)
	FlagStringer
}

// These are flags that are NOT native to spark-submit and are parsed
// directly by our offline runner. They go after the script name.
// spark-submit <native flags> file.py <script flags>
type ScriptFlag struct {
	Key   string
	Value string
}

func (flag ScriptFlag) SparkStringFlags() []string {
	return []string{
		fmt.Sprintf("--%s", flag.Key),
		flag.Value,
	}
}

func (flag ScriptFlag) IsSparkSubmitNative() bool {
	return false
}

func (flag ScriptFlag) TryCombine(other FlagStringer) FlagStringer {
	return nil
}

// NativeConfigFlag are passed via --conf to the spark submit
type NativeConfigFlag struct {
	Key   string
	Value string
}

func (config NativeConfigFlag) SparkStringFlags() []string {
	// TODO make sure key/value don't contain =
	return []string{
		"--conf",
		fmt.Sprintf("%s=%s", config.Key, config.Value),
	}
}

func (flag NativeConfigFlag) IsSparkSubmitNative() bool {
	return true
}

func (flag NativeConfigFlag) TryCombine(other FlagStringer) FlagStringer {
	return nil
}

func (flag NativeConfigFlag) ApplyToDataprocServerless(batch *dataprocpb.Batch) {
	if batch.RuntimeConfig == nil {
		batch.RuntimeConfig = &dataprocpb.RuntimeConfig{
			Properties: map[string]string{},
		}
	}
	if batch.RuntimeConfig.Properties == nil {
		batch.RuntimeConfig.Properties = map[string]string{}
	}
	batch.RuntimeConfig.Properties[flag.Key] = flag.Value
}

func (flag NativeConfigFlag) ApplyToDatabricks(settings *dbjobs.Task) {
	logging.GlobalLogger.Warnw("Ignoring native conf flags to databricks", "key", flag.Key)
}

// ConfigFlag should be set in Spark via spark.config in
// our PySpark scripts.
type ConfigFlag struct {
	Key   string
	Value string
}

func (config ConfigFlag) SparkStringFlags() []string {
	// TODO make sure key/value don't contain =
	return []string{
		"--spark_config",
		fmt.Sprintf("\"%s=%s\"", config.Key, config.Value),
	}
}

func (flag ConfigFlag) IsSparkSubmitNative() bool {
	return false
}

func (flag ConfigFlag) TryCombine(other FlagStringer) FlagStringer {
	return nil
}

// CredFlag are set via --credential flags and are handled directly
// by the script. Typically in a non-Spark specific way.
type CredFlag struct {
	Key   string
	Value string
}

func (flag CredFlag) SparkStringFlags() []string {
	// TODO make sure key/value don't contain =
	return []string{
		"--credential",
		fmt.Sprintf("\"%s=%s\"", flag.Key, flag.Value),
	}
}

func (flag CredFlag) IsSparkSubmitNative() bool {
	return false
}

func (flag CredFlag) TryCombine(other FlagStringer) FlagStringer {
	return nil
}

type PackagesFlag struct {
	Packages []string
}

func (flag PackagesFlag) SparkStringFlags() []string {
	return []string{
		"--packages",
		strings.Join(flag.Packages, ","),
	}
}

func (flag PackagesFlag) ApplyToDataprocServerless(batch *dataprocpb.Batch) {
	if batch.RuntimeConfig == nil {
		batch.RuntimeConfig = &dataprocpb.RuntimeConfig{
			Properties: map[string]string{},
		}
	}
	if batch.RuntimeConfig.Properties == nil {
		batch.RuntimeConfig.Properties = map[string]string{}
	}
	batch.RuntimeConfig.Properties["spark.jars.packages"] = strings.Join(flag.Packages, ",")
}

func (flag PackagesFlag) ApplyToDatabricks(settings *dbjobs.Task) {
	logging.GlobalLogger.Warnw("Ignoring packages in databricks", "packages", flag.Packages)
}

func (flag PackagesFlag) IsSparkSubmitNative() bool {
	return true
}

func (this PackagesFlag) TryCombine(other FlagStringer) FlagStringer {
	// All packages should be with the same --packages flag and comma separated
	that, ok := other.(PackagesFlag)
	if !ok {
		return nil
	}
	set := stringset.NewOrdered(this.Packages...)
	duplicates := set.AddAndGetDuplicates(that.Packages...)
	list := set.ToList()
	if len(duplicates) > 0 {
		logging.GlobalLogger.Warnw(
			"Ignoring duplicate packag.",
			"duplicates", duplicates, "full_list", list,
		)
	}
	return PackagesFlag{
		Packages: list,
	}
}

type SourcesFlag struct {
	Sources []SourceInfo
}

func (flag SourcesFlag) SparkStringFlags() []string {
	serializedSources := make([]string, len(flag.Sources))
	for i, source := range flag.Sources {
		serialized, err := source.Serialize()
		if err != nil {
			// TODO better error handling
			logging.GlobalLogger.Warnw(
				"Failed to serialized source, but will continue.",
				"source", source, "error", err,
			)
		}
		serializedSources[i] = serialized
	}
	flags := []string{"--sources"}
	return append(flags, serializedSources...)
}

func (flag SourcesFlag) IsSparkSubmitNative() bool {
	return false
}

func (this SourcesFlag) TryCombine(other FlagStringer) FlagStringer {
	// All source should be under the same --source flag
	that, ok := other.(SourcesFlag)
	if !ok {
		return nil
	}
	joined := make([]SourceInfo, 0, len(this.Sources)+len(that.Sources))
	joined = append(joined, this.Sources...)
	joined = append(joined, that.Sources...)
	return SourcesFlag{
		Sources: joined,
	}
}

func (flag SourcesFlag) SparkFlags() Flags {
	return Flags{
		flag,
	}
}

func (flag SourcesFlag) Redacted() Config {
	return flag
}

type IncludePyScript struct {
	Path filestore.Filepath
}

func (flag IncludePyScript) SparkStringFlags() []string {
	return []string{"--py-files", flag.Path.ToURI()}
}

func (flag IncludePyScript) IsSparkSubmitNative() bool {
	return true
}

func (flag IncludePyScript) TryCombine(other FlagStringer) FlagStringer {
	return nil
}

func (flag IncludePyScript) ApplyToDataprocServerless(batch *dataprocpb.Batch) {
	batchConfig, ok := batch.BatchConfig.(*dataprocpb.Batch_PysparkBatch)
	if !ok {
		logging.GlobalLogger.DPanicw(
			"Unable to cast batch config to pyspark batch", "config", batch.BatchConfig,
		)
	}
	pyspark := batchConfig.PysparkBatch
	pyspark.PythonFileUris = append(pyspark.PythonFileUris, flag.Path.ToURI())
}

func (flag IncludePyScript) ApplyToDatabricks(settings *dbjobs.Task) {
	logging.GlobalLogger.Warnw("Unable to include pyscript to databricks", "script", flag.Path.ToURI())
}

func (flag IncludePyScript) SparkFlags() Flags {
	return Flags{flag}
}

func (flag IncludePyScript) Redacted() Config {
	return flag
}

// sqlSubmitParamsURI points at a file containing --sql_query and --sources
// to get around character limits in spark submit APIs.
type SqlSubmitParamsURIFlag struct {
	URI filestore.Filepath
}

func (flag SqlSubmitParamsURIFlag) SparkFlags() Flags {
	return Flags{
		ScriptFlag{
			"submit_params_uri",
			flag.URI.Key(),
		},
	}
}

func (flag SqlSubmitParamsURIFlag) Redacted() Config {
	return flag
}

type SqlQueryFlag struct {
	CleanQuery string
	Sources    []SourceInfo
}

func (flag SqlQueryFlag) SparkFlags() Flags {
	return Flags{
		ScriptFlag{
			"sql_query",
			flag.CleanQuery,
		},
		SourcesFlag{
			Sources: flag.Sources,
		},
	}
}

func (flag SqlQueryFlag) Redacted() Config {
	return flag
}

type DataframeQueryFlag struct {
	Code    string
	Sources []SourceInfo
}

func (flag DataframeQueryFlag) SparkFlags() Flags {
	return Flags{
		ScriptFlag{
			"code",
			flag.Code,
		},
		SourcesFlag{
			Sources: flag.Sources,
		},
	}
}

func (flag DataframeQueryFlag) Redacted() Config {
	return flag
}

type DeployFlag struct {
	Mode types.SparkDeployMode
}

func (flag DeployFlag) SparkFlags() Flags {
	return Flags{flag}
}

func (flag DeployFlag) Redacted() Config {
	return flag
}

func (flag DeployFlag) SparkStringFlags() []string {
	return []string{"--deploy-mode", flag.Mode.SparkArg()}
}

func (flag DeployFlag) IsSparkSubmitNative() bool {
	return true
}

func (flag DeployFlag) TryCombine(other FlagStringer) FlagStringer {
	return nil
}

func (flag DeployFlag) ApplyToDataprocServerless(batch *dataprocpb.Batch) {
	logging.GlobalLogger.Debugw(
		"Ignoring spark deploy mode for GCP",
		"deploy-mode", flag.Mode.SparkArg(),
	)
}

func (flag DeployFlag) ApplyToDatabricks(settings *dbjobs.Task) {
	logging.GlobalLogger.Warnw("Unable to include deploy flag in databricks", "deploy-mode", flag.Mode.SparkArg())
}

type BigQueryFlags struct {
	Config *pc.BigQueryConfig
}

func (args BigQueryFlags) SparkFlags() Flags {
	if args.Config == nil {
		logging.GlobalLogger.Debug(
			"Not setting spark bigquery flags, bigquery config not set",
		)
		return Flags{}
	}
	bqCredBytes, err := json.Marshal(args.Config.Credentials)
	if err != nil {
		logging.GlobalLogger.Errorw(
			"Failed to marshal bigquery credentials",
		)
		return Flags{}
	}
	flags := Flags{
		CredFlag{
			Key:   "bqProjectId",
			Value: args.Config.ProjectId,
		},
		CredFlag{
			Key:   "bqDatasetId",
			Value: args.Config.DatasetId,
		},
		CredFlag{
			Key:   "bqCreds",
			Value: base64.StdEncoding.EncodeToString(bqCredBytes),
		},
	}
	return flags
}

func (args BigQueryFlags) Redacted() Config {
	return BigQueryFlags{
		Config: args.Config.Redacted(),
	}
}

type SnowflakeFlags struct {
	Config *pc.SnowflakeConfig
}

func (args SnowflakeFlags) SparkFlags() Flags {
	if args.Config == nil {
		logging.GlobalLogger.Debug(
			"Not setting spark snowflake flags, snowflake config not set",
		)
		return Flags{}
	}
	flags := Flags{
		CredFlag{
			Key:   "sfURL",
			Value: args.Config.GetBaseURL(),
		},
		CredFlag{
			Key:   "sfUser",
			Value: args.Config.Username,
		},
		CredFlag{
			Key:   "sfPassword",
			Value: args.Config.Password,
		},
		CredFlag{
			Key:   "sfWarehouse",
			Value: args.Config.Warehouse,
		},
		PackagesFlag{
			Packages: []string{
				"net.snowflake:snowflake-jdbc:3.13.22",
				"net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4",
			},
		},
	}
	return flags
}

func (args SnowflakeFlags) Redacted() Config {
	return SnowflakeFlags{
		Config: args.Config.Redacted(),
	}
}

// This is based on very legacy values and aren't tested
type AzureFlags struct {
	AccountName      string
	AccountKey       string
	ConnectionString string
	ContainerName    string
}

func (args AzureFlags) SparkFlags() Flags {
	return Flags{
		ConfigFlag{
			Key:   fmt.Sprintf("fs.azure.account.key.%s.dfs.core.windows.net", args.AccountName),
			Value: args.AccountKey,
		},
		CredFlag{
			Key:   "azure_connection_string",
			Value: args.ConnectionString,
		},
		CredFlag{
			Key:   "azure_container_name",
			Value: args.ContainerName,
		},
		PackagesFlag{
			Packages: []string{"org.apache.hadoop:hadoop-azure-3.2.0"},
		},
	}
}

func (args AzureFlags) Redacted() Config {
	return AzureFlags{
		AccountName:      args.AccountName,
		AccountKey:       redacted.String,
		ContainerName:    args.ContainerName,
		ConnectionString: redacted.String,
	}
}

// This is based on very legacy values and aren't tested
type GCSFlags struct {
	ProjectID string
	Bucket    string
	JSONCreds []byte
}

func (args GCSFlags) SparkFlags() Flags {
	return Flags{
		ConfigFlag{
			Key:   "fs.gs.impl",
			Value: "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
		},
		ConfigFlag{
			Key:   "fs.AbstractFileSystem.gs.impl",
			Value: "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
		},
		ConfigFlag{
			Key:   "fs.gs.auth.service.account.enable",
			Value: "true",
		},
		ConfigFlag{
			Key:   "fs.gs.auth.type",
			Value: "SERVICE_ACCOUNT_JSON_KEYFILE",
		},
		CredFlag{
			Key:   "gcp_project_id",
			Value: args.ProjectID,
		},
		CredFlag{
			Key:   "gcp_bucket_name",
			Value: args.Bucket,
		},
		CredFlag{
			Key:   "gcp_credentials",
			Value: base64.StdEncoding.EncodeToString(args.JSONCreds),
		},
		PackagesFlag{
			Packages: []string{"com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.0"},
		},
	}
}

func (args GCSFlags) Redacted() Config {
	return GCSFlags{
		ProjectID: args.ProjectID,
		Bucket:    args.Bucket,
		JSONCreds: []byte(redacted.String),
	}
}

type S3Flags struct {
	// AccessKey and SecretKey are optional, if they aren't set we default
	// to assuming a role.
	AccessKey, SecretKey string
	// Region defaults to us-east-1 if not set.
	Region string
	Bucket string
}

func (args S3Flags) SparkFlags() Flags {
	// TODO better arg checking and handling
	if args.Region == "" {
		args.Region = "us-east-1"
	}
	flags := Flags{
		ConfigFlag{
			Key:   "spark.hadoop.fs.s3.impl",
			Value: "org.apache.hadoop.fs.s3a.S3AFileSystem",
		},
		CredFlag{
			Key:   "aws_bucket_name",
			Value: args.Bucket,
		},
		CredFlag{
			Key:   "aws_region",
			Value: args.Region,
		},
		ConfigFlag{
			Key:   "fs.s3a.endpoint",
			Value: fmt.Sprintf("s3.%s.amazonaws.com", args.Region),
		},
		ScriptFlag{
			Key:   "store_type",
			Value: "s3",
		},
	}
	if args.AccessKey != "" && args.SecretKey != "" {
		flags = append(flags,
			ConfigFlag{
				Key:   "fs.s3a.aws.credentials.provider",
				Value: "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
			},
			ConfigFlag{
				Key:   "fs.s3a.access.key",
				Value: args.AccessKey,
			},
			ConfigFlag{
				Key:   "fs.s3a.secret.key",
				Value: args.SecretKey,
			},
			CredFlag{
				Key:   "aws_access_key_id",
				Value: args.AccessKey,
			},
			CredFlag{
				Key:   "aws_secret_access_key",
				Value: args.SecretKey,
			},
		)
	} else {
		flags = append(flags,
			CredFlag{
				Key:   "use_service_account",
				Value: "true",
			},
		)
	}
	return flags
}

func (args S3Flags) Redacted() Config {
	return S3Flags{
		AccessKey: redacted.String,
		SecretKey: redacted.String,
		Region:    args.Region,
		Bucket:    args.Bucket,
	}
}

type GlueFlags struct {
	FileStoreType   types.FileStoreType
	TableFormatType types.TableFormatType
	Region          string
	Warehouse       string
}

func (args GlueFlags) SparkFlags() Flags {
	if args.FileStoreType != types.S3Type {
		panic("TODO better fail")
	}
	switch args.TableFormatType {
	case types.IcebergType:
		return args.icebergFlags()
	case types.DeltaType:
		return args.deltaFlags()
	default:
		panic("TODO better fail")
	}
}

func (args GlueFlags) Redacted() Config {
	return args
}

func (args GlueFlags) icebergFlags() Flags {
	return Flags{
		ConfigFlag{
			Key:   "spark.sql.catalog.ff_catalog.io-impl",
			Value: "org.apache.iceberg.aws.s3.S3FileIO",
		},
		ConfigFlag{
			Key:   "spark.sql.catalog.ff_catalog.region",
			Value: args.Region,
		},
		ConfigFlag{
			Key:   "spark.sql.catalog.ff_catalog",
			Value: "org.apache.iceberg.spark.SparkCatalog",
		},
		ConfigFlag{
			Key:   "spark.sql.catalog.ff_catalog.catalog-impl",
			Value: "org.apache.iceberg.aws.glue.GlueCatalog",
		},
		ConfigFlag{
			Key:   "spark.sql.catalog.ff_catalog.warehouse",
			Value: args.Warehouse,
		},
	}
}

func (args GlueFlags) deltaFlags() Flags {
	return Flags{
		ConfigFlag{
			Key:   "spark.sql.catalog.spark_catalog",
			Value: "org.apache.spark.sql.delta.catalog.DeltaCatalog",
		},
		ConfigFlag{
			Key:   "spark.hadoop.hive.metastore.client.factory.class",
			Value: "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
		},
		ConfigFlag{
			Key:   "spark.sql.catalogImplementation",
			Value: "hive",
		},
	}
}

type IcebergFlags struct{}

func (args IcebergFlags) SparkFlags() Flags {
	return Flags{
		ConfigFlag{
			Key:   "spark.sql.extensions",
			Value: "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
		},
		PackagesFlag{
			Packages: []string{"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1"},
		},
	}
}

func (args IcebergFlags) Redacted() Config {
	return args
}

type DeltaFlags struct{}

func (args DeltaFlags) SparkFlags() Flags {
	return Flags{
		ConfigFlag{
			Key:   "spark.sql.extensions",
			Value: "io.delta.sql.DeltaSparkSessionExtension",
		},
	}
}

func (args DeltaFlags) Redacted() Config {
	return args
}

type KafkaFlags struct{}

func (args KafkaFlags) SparkFlags() Flags {
	return Flags{
		PackagesFlag{
			Packages: []string{
				"org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
				"software.amazon.msk:aws-msk-iam-auth:2.2.0",
			},
		},
	}
}

func (args KafkaFlags) Redacted() Config {
	return args
}

type DirectCopyFlags struct {
	Creds           Config
	Target          types.DirectCopyTarget
	TableName       string
	FeatureName     string
	FeatureVariant  string
	EntityColumn    string
	ValueColumn     string
	TimestampColumn string
}

func (args DirectCopyFlags) SparkFlags() Flags {
	credFlags := args.Creds.SparkFlags()
	copyFlags := Flags{
		ScriptFlag{
			Key:   "direct_copy_use_iceberg",
			Value: "true",
		},
		ScriptFlag{
			Key:   "direct_copy_target",
			Value: string(args.Target),
		},
		ScriptFlag{
			Key:   "direct_copy_table_name",
			Value: args.TableName,
		},
		ScriptFlag{
			Key:   "direct_copy_feature_name",
			Value: args.FeatureName,
		},
		ScriptFlag{
			Key:   "direct_copy_feature_variant",
			Value: args.FeatureVariant,
		},
		ScriptFlag{
			Key:   "direct_copy_entity_column",
			Value: args.EntityColumn,
		},
		ScriptFlag{
			Key:   "direct_copy_value_column",
			Value: args.ValueColumn,
		},
	}
	if args.TimestampColumn != "" {
		copyFlags = append(copyFlags, ScriptFlag{
			Key:   "direct_copy_timestamp_column",
			Value: args.TimestampColumn,
		})
	}
	return append(credFlags, copyFlags...)
}

func (args DirectCopyFlags) Redacted() Config {
	return DirectCopyFlags{
		Creds:           args.Creds.Redacted(),
		Target:          args.Target,
		TableName:       args.TableName,
		FeatureName:     args.FeatureName,
		FeatureVariant:  args.FeatureVariant,
		EntityColumn:    args.EntityColumn,
		ValueColumn:     args.ValueColumn,
		TimestampColumn: args.TimestampColumn,
	}
}

type DynamoFlags struct {
	Region    string
	AccessKey string
	SecretKey string
}

func (args DynamoFlags) SparkFlags() Flags {
	return Flags{
		CredFlag{
			Key:   "dynamo_aws_access_key_id",
			Value: args.AccessKey,
		},
		CredFlag{
			Key:   "dynamo_aws_secret_access_key",
			Value: args.SecretKey,
		},
		CredFlag{
			Key:   "dynamo_aws_region",
			Value: args.Region,
		},
	}
}

func (args DynamoFlags) Redacted() Config {
	return DynamoFlags{
		Region:    args.Region,
		AccessKey: redacted.String,
		SecretKey: redacted.String,
	}
}

type JobTypeFlag struct {
	Type types.Job
}

func (flag JobTypeFlag) SparkFlags() Flags {
	return Flags{
		ScriptFlag{
			Key:   "job_type",
			Value: string(flag.Type),
		},
	}
}

func (flag JobTypeFlag) Redacted() Config {
	return flag
}

type OutputFlag struct {
	Output pl.Location
}

func (flag OutputFlag) SparkFlags() Flags {
	if flag.Output == nil {
		return Flags{
			// Script expects an output always and needs to be JSON.
			ScriptFlag{
				Key:   "output",
				Value: "{}",
			},
		}
	}
	outputStr, err := flag.Output.Serialize()
	if err != nil {
		logging.GlobalLogger.Errorw(
			"Failed to serialize output for spark. Skipping flags.",
			"location", flag.Output,
			"error", err,
		)
		return Flags{
			// Script expects an output always and needs to be JSON.
			ScriptFlag{
				Key:   "output",
				Value: "{}",
			},
		}
	}
	return Flags{
		ScriptFlag{
			Key:   "output",
			Value: outputStr,
		},
	}
}

func (flag OutputFlag) Redacted() Config {
	return flag
}

// This is a legacy flag to keep the old version of
// materialization working.
type LegacyOutputFormatFlag struct {
	FileType filestore.FileType
}

func (flag LegacyOutputFormatFlag) SparkFlags() Flags {
	switch flag.FileType {
	case filestore.Parquet, filestore.CSV:
		return Flags{
			ScriptFlag{
				Key:   "output_format",
				Value: string(flag.FileType),
			},
		}
	case filestore.NilFileType:
		// Default to Parquet
		return Flags{
			ScriptFlag{
				Key:   "output_format",
				Value: string(filestore.Parquet),
			},
		}
	default:
		// Default to Parquet
		logging.GlobalLogger.Warnw(
			"Unsupported file type for output format flag. Default to Parquet.",
			"filetype", flag.FileType,
		)
		return Flags{
			ScriptFlag{
				Key:   "output_format",
				Value: string(filestore.Parquet),
			},
		}
	}
}

func (flag LegacyOutputFormatFlag) Redacted() Config {
	return flag
}

// This is a legacy flag to keep the old version of
// materialization working.
type LegacyIncludeHeadersFlag struct {
	ShouldInclude bool
}

func (flag LegacyIncludeHeadersFlag) SparkFlags() Flags {
	if flag.ShouldInclude {
		// Script defaults to include
		return Flags{}
	} else {
		return Flags{
			ScriptFlag{
				Key:   "headers",
				Value: "exclude",
			},
		}
	}
}

func (flag LegacyIncludeHeadersFlag) Redacted() Config {
	return flag
}

type MasterFlag struct {
	Master string
}

func (flag MasterFlag) SparkFlags() Flags {
	return Flags{flag}
}

func (flag MasterFlag) Redacted() Config {
	return flag
}

func (flag MasterFlag) SparkStringFlags() []string {
	return []string{"--master", flag.Master}
}

func (flag MasterFlag) IsSparkSubmitNative() bool {
	return true
}

func (flag MasterFlag) TryCombine(other FlagStringer) FlagStringer {
	return nil
}

func (flag MasterFlag) ApplyToDataprocServerless(batch *dataprocpb.Batch) {
	logging.GlobalLogger.Debugw(
		"Ignoring spark master flag for GCP",
		"master-flag", flag.Master,
	)
}

func (flag MasterFlag) ApplyToDatabricks(settings *dbjobs.Task) {
	logging.GlobalLogger.Warnw("Unable to include master flag in databricks", "master-flag", flag.Master)
}

type HighMemoryFlags struct{}

func (args HighMemoryFlags) SparkFlags() Flags {
	return Flags{
		NativeConfigFlag{
			Key:   "spark.executor.memory",
			Value: "2g",
		},
		NativeConfigFlag{
			Key:   "spark.driver.memory",
			Value: "2g",
		},
		NativeConfigFlag{
			Key:   "spark.executor.memoryOverhead",
			Value: "1g",
		},
	}
}

func (args HighMemoryFlags) Redacted() Config {
	return args
}
