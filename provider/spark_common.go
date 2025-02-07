// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/google/uuid"

	"github.com/featureform/config"
	"github.com/featureform/fferr"
	"github.com/featureform/filestore"
	"github.com/featureform/helpers"
	"github.com/featureform/logging"
	"github.com/featureform/logging/redacted"
	pl "github.com/featureform/provider/location"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/provider/spark"
	"github.com/featureform/provider/types"
)

func getSparkDeployModeFromEnv() types.SparkDeployMode {
	if helpers.GetEnvBool("USE_CLIENT_MODE", false) {
		return types.SparkClientDeployMode
	} else {
		return types.SparkClusterDeployMode
	}
}

func sparkPythonFileURI(store SparkFileStoreV2, logger logging.Logger) (filestore.Filepath, error) {
	logger.Debug("Getting python file URI")
	config, err := config.CreateSparkScriptConfig()
	if err != nil {
		logger.Errorw("Failed to get remote script path", "err", err)
		return nil, err
	}
	rawPath := config.RemoteScriptPath
	sparkScriptPath, err := store.CreateFilePath(rawPath, false)
	if err != nil {
		logger.Errorw("Failed to parse remote script path", "path", rawPath)
		return nil, err
	}
	// TODO (simba) make this a bool
	// Need to replace s3a:// with s3:// for the script name
	// to be correctly interpreted by Spark
	if sparkScriptPath.Scheme() == filestore.S3APrefix {
		if err := sparkScriptPath.SetScheme(filestore.S3Prefix); err != nil {
			logger.Errorw("Unable to change spark script scheme to s3 from s3a", "error", err)
			return nil, err
		}
	}
	return sparkScriptPath, nil
}

type sparkScriptCommandDef struct {
	// DeployMode specifies how the job should be deployed on Spark.
	DeployMode types.SparkDeployMode
	// TFType specificies the transformation type for transformations.
	TFType TransformationType
	// OutputLocation specifies where to write output to in certain transformations.
	OutputLocation pl.Location
	// Code should be either a path to the function pkl for dataframes or a SQL string.
	Code string
	// SourceList are read and mapped as inputs in the spark script.
	SourceList []spark.SourceInfo
	// JobType specifies which job mode to run the script against.
	JobType types.Job
	// Store is the filestore interface for Spark to read and write against.
	Store SparkFileStoreV2
	// Mappings provides SourceMappings for use alongside SourceList
	Mappings []SourceMapping
}

func (def sparkScriptCommandDef) Redacted() map[string]any {
	redactedMapping := make([]SourceMapping, len(def.Mappings))
	for i, m := range def.Mappings {
		redactedMapping[i] = SourceMapping{
			Template:            m.Template,
			Source:              m.Source,
			ProviderType:        m.ProviderType,
			ProviderConfig:      pc.SerializedConfig(redacted.String),
			TimestampColumnName: m.TimestampColumnName,
			Location:            m.Location,
			Columns:             m.Columns,
		}
	}
	return map[string]any{
		"DeployMode":     def.DeployMode,
		"TFType":         def.TFType,
		"OutputLocation": def.OutputLocation,
		"Code":           def.Code,
		"SourceList":     def.SourceList,
		"JobType":        def.JobType,
		"Mappings":       redactedMapping,
		"FileStoreType":  def.Store.FilestoreType(),
		"SparkStoreType": def.Store.Type(),
	}
}

func (def sparkScriptCommandDef) PrepareCommand(logger logging.Logger) (*spark.Command, error) {
	logger = logger.WithValues(def.Redacted())
	logger.Debug("SparkSubmitArgs")
	snowflakeConfig, err := getSnowflakeConfigFromSourceMapping(def.Mappings)
	if err != nil {
		logger.Errorw(
			"Could not get Snowflake config from source mapping",
			"error", err,
		)
		return nil, err
	}
	bqConfig, err := getBigQueryConfigFromSourceMapping(def.Mappings)
	if err != nil {
		logger.Errorw(
			"Could not get BigQuery config from source mapping",
			"error", err,
		)
		return nil, err
	}
	sparkScriptRemotePath, err := sparkPythonFileURI(def.Store, logger)
	if err != nil {
		logger.Errorw("Failed to get python file URI", "error", err)
		return nil, err
	}
	var scriptArg string
	switch def.TFType {
	case SQLTransformation:
		scriptArg = "sql"
	case DFTransformation:
		scriptArg = "df"
	default:
		errMsg := fmt.Sprintf("Unknown transformation type: %s", def.TFType)
		logger.Error(errMsg)
		return nil, fferr.NewInternalErrorf(errMsg)
	}
	cmd := &spark.Command{
		Script:     sparkScriptRemotePath,
		ScriptArgs: []string{scriptArg},
		Configs: sparkCoreConfigs(
			sparkCoreConfigsArgs{
				JobType:         def.JobType,
				Output:          def.OutputLocation,
				DeployMode:      def.DeployMode,
				SnowflakeConfig: snowflakeConfig,
				BigQueryConfig:  bqConfig,
				Store:           def.Store,
			},
		),
	}
	// In S3, we write the sql and sources to an extenral file to try to avoid going over the
	// maximum character limit
	if def.Store.FilestoreType() == filestore.S3 && def.TFType == SQLTransformation {
		logger.Debug("Writing submit params to file")
		paramsPath, err := writeSubmitParamsToFileStore(def.Code, def.SourceList, def.Store, logger)
		if err != nil {
			logger.Errorw("Failed to write submit params to file store", "err", err)
			return nil, err
		}
		logger = logger.With("spark-params-file", paramsPath.ToURI())
		logger.Debug("submit params to file")
		cmd.AddConfigs(spark.SqlSubmitParamsURIFlag{
			URI: paramsPath,
		})
	} else if def.TFType == SQLTransformation {
		cmd.AddConfigs(spark.SqlQueryFlag{
			CleanQuery: def.Code,
			Sources:    def.SourceList,
		})
	} else if def.TFType == DFTransformation {
		cmd.AddConfigs(spark.DataframeQueryFlag{
			Code:    def.Code,
			Sources: def.SourceList,
		})
	}
	// EMR's API enforces a 10K-character (i.e. bytes) limit on string values passed to HadoopJarStep, so to avoid a 400, we need
	// to check to ensure the args are below this limit. If they exceed this limit, it's most likely due to the query and/or the list
	// of sources, so we write these as a JSON file and read them from the PySpark runner script to side-step this constraint
	if exceedsSubmitParamsTotalByteLimit(cmd) {
		logger.Errorw(
			"Command exceeded",
			"filestore", def.Store.FilestoreType(),
			"command", cmd.Redacted(),
		)
		return nil, fferr.NewInternalErrorf(
			"Spark submit params exceeds max length that Spark allows.",
		)
	}
	logger.Debugw("Compiled spark command", "command", cmd.Redacted())
	return cmd, nil
}

type sparkCoreConfigsArgs struct {
	JobType         types.Job
	Output          pl.Location
	DeployMode      types.SparkDeployMode
	SnowflakeConfig *pc.SnowflakeConfig
	BigQueryConfig  *pc.BigQueryConfig
	Store           SparkFileStoreV2
}

func sparkCoreConfigs(args sparkCoreConfigsArgs) spark.Configs {
	configs := spark.Configs{
		spark.SnowflakeFlags{
			Config: args.SnowflakeConfig,
		},
		spark.BigQueryFlags{
			Config: args.BigQueryConfig,
		},
		spark.JobTypeFlag{
			Type: args.JobType,
		},
		spark.OutputFlag{
			Output: args.Output,
		},
		spark.DeployFlag{
			Mode: args.DeployMode,
		},
	}
	return append(configs, args.Store.SparkConfigs()...)
}

func readAndUploadFile(filePath filestore.Filepath, storePath filestore.Filepath, store SparkFileStoreV2) error {
	logger := logging.GlobalLogger.With(
		"fromPath", filePath.ToURI(),
		"toPath", storePath.ToURI(),
		"store", store.Type(),
	)
	fileExists, err := store.Exists(pl.NewFileLocation(storePath))
	if err != nil {
		logger.Errorw("Unable to check if file exists", "error", err)
		return err
	}
	if fileExists {
		logger.Infow("File already exists skipping copy")
		return nil
	}

	f, err := os.Open(filePath.Key())
	if err != nil {
		logger.Errorw("Unable open local file for copy", "error", err)
		return fferr.NewInternalError(err)
	}
	defer f.Close()

	fileStats, err := f.Stat()
	if err != nil {
		logger.Errorw("Failed to get local file size", "error", err)
		return fferr.NewInternalError(err)
	}

	pythonScriptBytes := make([]byte, fileStats.Size())
	if _, err = f.Read(pythonScriptBytes); err != nil {
		logger.Errorw("Failed to read local file for copy", "error", err)
		return fferr.NewInternalError(err)
	}
	if err := store.Write(storePath, pythonScriptBytes); err != nil {
		logger.Errorw("Failed to write to remote path", "error", err)
		return err
	}
	logger.Infow("Copied local file to remote filestore")
	return nil
}

func removeEscapeCharacters(values []string) []string {
	for i, v := range values {
		v = strings.Replace(v, "\\", "", -1)
		v = strings.Replace(v, "\"", "", -1)
		values[i] = v
	}
	return values
}

func exceedsSubmitParamsTotalByteLimit(cmd *spark.Command) bool {
	args := cmd.Compile()
	totalBytes := 0
	for _, str := range args {
		totalBytes += len(str)
	}
	spacesBetweenArgs := len(args) - 1
	totalBytes += spacesBetweenArgs
	return totalBytes >= SPARK_SUBMIT_PARAMS_BYTE_LIMIT
}

func writeSubmitParamsToFileStore(query string, sources []spark.SourceInfo, store SparkFileStoreV2, logger logging.Logger) (filestore.Filepath, error) {
	paramsFileId := uuid.New()
	paramsPath, err := store.CreateFilePath(
		fmt.Sprintf(
			"featureform/spark-submit-params/%s.json",
			paramsFileId.String(),
		), false,
	)
	if err != nil {
		return nil, err
	}
	serializedSources := make([]string, len(sources))
	for i, source := range sources {
		serialized, err := source.Serialize()
		if err != nil {
			logger.Errorw("Failed to serialize source", "source", source, "err", err)
			return nil, err
		}
		serializedSources[i] = serialized
	}
	paramsMap := map[string]interface{}{}
	paramsMap["sql_query"] = query
	paramsMap["sources"] = serializedSources

	data, err := json.Marshal(paramsMap)
	if err != nil {
		return nil, fferr.NewInternalError(err)
	}

	logger.Debugw("Writing spark submit params to filestore", "path", paramsPath, "data", string(data))
	if err := store.Write(paramsPath, data); err != nil {
		return nil, err
	}

	return paramsPath, nil
}

func getBigQueryConfigFromSourceMapping(mappings []SourceMapping) (*pc.BigQueryConfig, error) {
	var bqConfig *pc.BigQueryConfig
	for _, mapping := range mappings {
		if mapping.ProviderType == pt.BigQueryOffline {
			bqConfig = &pc.BigQueryConfig{}
			if err := bqConfig.Deserialize(mapping.ProviderConfig); err != nil {
				return nil, err
			}
			break
		}
	}
	return bqConfig, nil
}

func getSnowflakeConfigFromSourceMapping(mappings []SourceMapping) (*pc.SnowflakeConfig, error) {
	var snowflakeConfig *pc.SnowflakeConfig
	for _, mapping := range mappings {
		if mapping.ProviderType == pt.SnowflakeOffline {
			snowflakeConfig = &pc.SnowflakeConfig{}
			if err := snowflakeConfig.Deserialize(mapping.ProviderConfig); err != nil {
				return nil, err
			}
			break
		}
	}
	return snowflakeConfig, nil
}
