// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package tasks

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	db "github.com/jackc/pgx/v4"

	"github.com/featureform/fferr"
	"github.com/featureform/logging"
	"github.com/featureform/metadata"
	"github.com/featureform/provider"
	pl "github.com/featureform/provider/location"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	ptypes "github.com/featureform/provider/types"
	"github.com/featureform/runner"
	"github.com/featureform/scheduling"
)

type SourceTask struct {
	BaseTask
	// TODO, hack to pass context around quickly
	ctx context.Context
}

type tableMapping struct {
	name                string
	providerType        pt.Type
	providerConfig      pc.SerializedConfig
	timestampColumnName string
	location            pl.Location
}

func (t *SourceTask) Run() error {
	_, ctx, logger := t.logger.InitializeRequestID(context.TODO())
	t.ctx = ctx
	logger.Debugw("Running source task")
	nv, ok := t.taskDef.Target.(scheduling.NameVariant)
	if !ok {
		errMsg := fmt.Sprintf("cannot create a source from target type: %s", t.taskDef.TargetType)
		logger.Error(errMsg)
		return fferr.NewInternalErrorf(errMsg)
	}

	resID := metadata.ResourceID{Name: nv.Name, Variant: nv.Variant, Type: metadata.SOURCE_VARIANT}
	logger = logger.WithResource(logging.SourceVariant, resID.Name, resID.Variant)

	if t.isDelete {
		return t.handleDeletion(ctx, resID, logger)
	}

	t.logger.Infof("Running source task")
	err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Fetching Metadata...")
	if err != nil {
		logger.Warnw("Failed to add run log \"Fetching metadata\" Continuing.", "error", err)
	}

	source, err := t.metadata.GetSourceVariant(ctx, metadata.NameVariant{nv.Name, nv.Variant})
	if err != nil {
		logger.Errorw("Failed to get source variant", "error", err)
		return err
	}
	sourceStore, err := getStore(t.BaseTask, t.metadata, source)
	if err != nil {
		logger.Errorw("Failed to get store", "error", err)
		return err
	}
	logger = logger.With(
		"resource_id", resID,
		"is_primary", source.IsPrimaryData(),
		"definition", source.Definition(),
	)
	logger.Debug("Selecting source job type")
	switch {
	case source.IsSQLTransformation():
		logger.Info("Running SQL transformation job")
		return t.runSQLTransformationJob(source, resID, sourceStore)
	case source.IsDFTransformation():
		logger.Info("Running DF transformation job")
		return t.runDFTransformationJob(source, resID, sourceStore)
	case source.IsPrimaryData():
		logger.Info("Running primary table job")
		return t.runPrimaryTableJob(source, resID, sourceStore)
	default:
		logger.Error("Unknown source type")
		return fferr.NewInternalErrorf("source type not implemented")
	}
}

func (t *SourceTask) handleDeletion(ctx context.Context, resID metadata.ResourceID, logger logging.Logger) error {
	logger.Infow("Deleting source")
	sourceToDelete, stagedDeleteErr := t.metadata.GetStagedForDeletionSourceVariant(
		ctx,
		metadata.NameVariant{
			Name:    resID.Name,
			Variant: resID.Variant,
		})
	if stagedDeleteErr != nil {
		logger.Errorw("Failed to get staged for deletion source variant", "error", stagedDeleteErr)
		return stagedDeleteErr
	}

	if sourceToDelete.IsPrimaryData() {
		logger.Infow("Can't delete primary data table", "resource_id", resID)
	} else {
		tfLocation, tfLocationErr := sourceToDelete.GetTransformationLocation()
		if tfLocationErr != nil {
			logger.Errorw("Failed to get transformation location", "error", tfLocationErr)
			return tfLocationErr
		}

		logger.Debugw("Deleting source at location", "location", tfLocation, "error", tfLocationErr)
		sourceStore, err := getStore(t.BaseTask, t.metadata, sourceToDelete)
		if err != nil {
			logger.Errorw("Failed to get store", "error", err)
			return err
		}

		deleteErr := sourceStore.Delete(tfLocation)
		if deleteErr != nil {
			var notFoundErr *fferr.DatasetNotFoundError
			if errors.As(deleteErr, &notFoundErr) {
				logger.Infow("Table doesn't exist at location, continuing...", "location", tfLocation)
			} else {
				logger.Errorw("Failed to delete source", "error", deleteErr)
				return deleteErr
			}
		}
	}

	logger.Debugw("Deleting source metadata", "resource_id", resID)
	finalizeDeleteErr := t.metadata.FinalizeDelete(ctx, resID)
	if finalizeDeleteErr != nil {
		logger.Errorw("Failed to finalize delete", "error", finalizeDeleteErr)
		return finalizeDeleteErr
	}

	logger.Infow("Source deleted", "resource_id", resID)
	return nil
}

func (t *SourceTask) runSQLTransformationJob(
	transformSource *metadata.SourceVariant,
	resID metadata.ResourceID,
	offlineStore provider.OfflineStore,
) error {
	t.logger.Info("Running SQL transformation job on resource: ", resID)
	err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Fetching Queries...")
	if err != nil {
		return err
	}
	templateString := transformSource.SQLTransformationQuery()
	sources := transformSource.SQLTransformationSources() // sources contains all the sources including the incremental sources too
	t.logger.Debugw("SQL transform sources", "sources", sources)

	if err := t.metadata.Tasks.AddRunLog(
		t.taskDef.TaskId,
		t.taskDef.ID,
		"Waiting for dependent jobs to complete...",
	); err != nil {
		return err
	}

	if err := t.verifyCompletionOfSources(sources); err != nil {
		return err
	}

	if err := t.metadata.Tasks.AddRunLog(
		t.taskDef.TaskId,
		t.taskDef.ID,
		"Mapping Name Variants to Tables...",
	); err != nil {
		return err
	}

	sourceTableMapping, err := t.mapNameVariantsToTables(sources, t.logger)
	t.logger.Debugw("Source Table Mapping", "mapping", sourceTableMapping)
	if err != nil {
		return err
	}

	sourceMapping, err := getSourceMapping(templateString, sourceTableMapping)
	t.logger.Debugw("Source Mapping", "mapping", sourceMapping)
	if err != nil {
		return err
	}

	var query string
	query, err = templateReplace(templateString, sourceTableMapping, offlineStore, t.logger)
	if err != nil {
		return err
	}

	// Replaces unique Featureform variables in the query; i.e. FF_LAST_RUN_TIMESTAMP will be replaced with the current epoch time
	query = sqlVariableReplace(query, t)

	resourceSnowflakeConfig := &metadata.ResourceSnowflakeConfig{}
	if offlineStore.Type() == pt.SnowflakeOffline {
		tempConfig, err := transformSource.ResourceSnowflakeConfig()
		if err != nil {
			return err
		}
		resourceSnowflakeConfig = tempConfig
	}

	t.logger.Debugw("Created transformation query", "query", query)
	providerResourceID := provider.ResourceID{Name: resID.Name, Variant: resID.Variant, Type: provider.Transformation}
	transformationConfig := provider.TransformationConfig{
		Type:           provider.SQLTransformation,
		TargetTableID:  providerResourceID,
		Query:          query,
		SourceMapping:  sourceMapping,
		Args:           transformSource.TransformationArgs(),
		MaxJobDuration: transformSource.MaxJobDuration(),
		// StartTime begins when its PENDING and waiting for deps. That causes it to re-read data.
		// EndTime is the lesser evil.
		LastRunTimestamp:        t.lastSuccessfulTask.EndTime.UTC(),
		IsUpdate:                t.isUpdate,
		SparkFlags:              transformSource.SparkFlags(),
		ResourceSnowflakeConfig: resourceSnowflakeConfig,
	}
	t.logger.Debugw("Transformation Config", "config", transformationConfig)
	if err := t.runTransformationJob(transformationConfig, offlineStore); err != nil {
		return err
	}

	return nil
}

func (t *SourceTask) runDFTransformationJob(
	transformSource *metadata.SourceVariant,
	resID metadata.ResourceID,
	offlineStore provider.OfflineStore,
) error {
	t.logger.Info("Running DF transformation job on resource: ", resID)
	err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Fetching Queries...")
	if err != nil {
		return err
	}
	code := transformSource.DFTransformationQuery()
	sources := transformSource.DFTransformationSources()
	t.logger.Debugw("SQL transform sources", "sources", sources)

	err = t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Waiting for dependent jobs to complete...")
	if err != nil {
		return err
	}

	err = t.verifyCompletionOfSources(sources)
	if err != nil {
		return err
	}

	err = t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Mapping Name Variants to Tables...")
	if err != nil {
		return err
	}

	sourceMap, err := t.mapNameVariantsToTables(sources, t.logger)
	if err != nil {
		return err
	}

	sourceMapping, err := getOrderedSourceMappings(sources, sourceMap)
	if err != nil {
		return err
	}

	t.logger.Debugw("Created transformation query")
	providerResourceID := provider.ResourceID{Name: resID.Name, Variant: resID.Variant, Type: provider.Transformation}
	transformationConfig := provider.TransformationConfig{
		Type:           provider.DFTransformation,
		TargetTableID:  providerResourceID,
		Code:           code,
		SourceMapping:  sourceMapping,
		Args:           transformSource.TransformationArgs(),
		MaxJobDuration: transformSource.MaxJobDuration(),
		// StartTime begins when its PENDING and waiting for deps. That causes it to re-read data.
		// EndTime is the lesser evil.
		LastRunTimestamp: t.lastSuccessfulTask.EndTime.UTC(),
		IsUpdate:         t.isUpdate,
	}
	t.logger.Debugw("Transformation Config", "config", transformationConfig)

	if err := t.runTransformationJob(transformationConfig, offlineStore); err != nil {
		return err
	}

	return nil
}

func (t *SourceTask) runTransformationJob(transformationConfig provider.TransformationConfig, offlineStore provider.OfflineStore) error {
	t.logger.Debugw("Starting transformation")
	if err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Starting Transformation..."); err != nil {
		t.logger.Errorw("Unable to add run log", "error", err)
		// We can continue without the run log
	}
	type transformFnType func(provider.TransformationConfig, ...provider.TransformationOption) error
	var transformFn transformFnType
	if t.isUpdate {
		transformFn = offlineStore.UpdateTransformation
	} else {
		transformFn = offlineStore.CreateTransformation
	}

	var waiter interface {
		Wait() error
	}

	supportsAsyncOpt, err := offlineStore.SupportsTransformationOption(provider.ResumableTransformation)
	if err != nil {
		t.logger.Errorw("Unable to verify if offline store supports async options", "error", err)
		return err
	}

	var asyncOpt *provider.ResumeOption
	lastResumeID := t.taskDef.ResumeID
	isResuming := lastResumeID != ptypes.NilResumeID
	maxWait := transformationConfig.MaxJobDuration
	if isResuming {
		t.logger.Infow("Resuming transformation", "resume_id", lastResumeID)
		resumeOpt, err := provider.ResumeOptionWithID(lastResumeID, maxWait)
		if err != nil {
			return err
		}
		asyncOpt = resumeOpt
	} else {
		asyncOpt = provider.RunAsyncWithResume(maxWait)
	}
	if !supportsAsyncOpt && isResuming {
		// This is only possible if the provider used to support resumes and doesn't anymore
		t.logger.DPanicw("Unable to resume, re-running task", "resume_id", lastResumeID)
	}
	if supportsAsyncOpt {
		t.logger.Debugw("Running transformation with async option")
		if err := transformFn(transformationConfig, asyncOpt); err != nil {
			t.logger.Errorw("Transform failed with asyncOpt set", "error", err)
		}
		waiter = asyncOpt
	} else {
		t.logger.Debugw("Running transformation without async option")
		transformationWatcher := &runner.SyncWatcher{
			ResultSync:  &runner.ResultSync{},
			DoneChannel: make(chan interface{}),
		}
		go func() {
			if err := transformFn(transformationConfig); err != nil {
				t.logger.Errorw("Transform failed, ending watch", "error", err)
				transformationWatcher.EndWatch(err)
				return
			}
			transformationWatcher.EndWatch(nil)
		}()
		waiter = transformationWatcher
	}
	if err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Waiting for Transformation to complete..."); err != nil {
		t.logger.Errorw("Unable to add run log", "error", err)
		// We can continue without the run log
	}
	t.logger.Infow("Waiting For Transformation Completion")
	if err := waiter.Wait(); err != nil {
		t.logger.Errorw("Transformation failed")
		return err
	}
	t.logger.Infow("Transformation Complete")

	if err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Transformation Complete."); err != nil {
		t.logger.Errorw("Unable to add run log", "error", err)
		// We can continue without the run log
	}
	return nil
}

func (t *SourceTask) runPrimaryTableJob(
	source *metadata.SourceVariant,
	resID metadata.ResourceID,
	offlineStore provider.OfflineStore,
) error {
	t.logger.Info("Running primary table job on resource: ", resID)
	providerResourceID := provider.ResourceID{Name: resID.Name, Variant: resID.Variant, Type: provider.Primary}
	if !source.IsPrimaryData() {
		return fferr.NewInvalidArgumentErrorf("%s is not a primary table", source.Name())
	}
	location, err := source.GetPrimaryLocation()
	if err != nil {
		return err
	}
	if location == nil {
		return fferr.NewInvalidArgumentErrorf("source location is not set")
	}
	runErr := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Starting Registration...")
	if runErr != nil {
		return err
	}
	if _, err := offlineStore.RegisterPrimaryFromSourceTable(providerResourceID, location); err != nil {
		return err
	}
	err = t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Registration Complete.")
	if err != nil {
		return err
	}
	return nil
}

func (t *SourceTask) mapNameVariantsToTables(
	sources metadata.NameVariants,
	logger logging.Logger,
) (map[string]tableMapping, error) {
	sourceMap := make(map[string]tableMapping)
	for _, nameVariant := range sources {
		logger.Debugw("Mapping Name Variant to Table", "name_variant", nameVariant)
		source, err := t.metadata.GetSourceVariant(t.ctx, nameVariant)
		if err != nil {
			return nil, err
		}
		if source.Status() != scheduling.READY {
			return nil, fferr.NewResourceNotReadyError(source.Name(), source.Variant(), "SOURCE_VARIANT", nil)
		}
		providerResourceID := provider.ResourceID{Name: source.Name(), Variant: source.Variant()}
		sourceProvider, err := source.FetchProvider(t.metadata, t.ctx)
		if err != nil {
			return nil, err
		}

		var tableName string
		tblMapping := tableMapping{
			providerType:        pt.Type(sourceProvider.Type()),
			providerConfig:      sourceProvider.SerializedConfig(),
			timestampColumnName: source.PrimaryDataTimestampColumn(),
		}
		isSparkOrK8sOfflineStore := sourceProvider.Type() == "SPARK_OFFLINE" || sourceProvider.Type() == "K8S_OFFLINE"
		isBigQueryOrClickhouseOfflineStore := sourceProvider.Type() == "BIGQUERY_OFFLINE" || sourceProvider.Type() == "CLICKHOUSE_OFFLINE"
		isSnowflakeOfflineStore := sourceProvider.Type() == "SNOWFLAKE_OFFLINE"
		if isSparkOrK8sOfflineStore && source.IsTransformation() {
			logger.Debugw("Transformation Source on Spark", "source", source.Name(), "variant", source.Variant())
			// Spark & K8s use the transformation paths unlike majority of other offline stores
			providerResourceID.Type = provider.Transformation
			tableName, err = provider.GetTransformationTableName(providerResourceID)
			if err != nil {
				return nil, err
			}
			t.logger.Debugw("Transformation source", "source", source.Name(), "variant", source.Variant())
			transformationLocation, err := source.GetTransformationLocation()
			if err != nil {
				return nil, err
			}
			t.logger.Debugw("Transformation Location", "location", transformationLocation.Location(), "location_type", fmt.Sprintf("%T", transformationLocation))
			tblMapping.location = transformationLocation
		} else if (isSparkOrK8sOfflineStore || isBigQueryOrClickhouseOfflineStore) && source.IsPrimaryData() {
			logger.Debugw("Primary Data Source on Spark, BigQuery or ClickHouse", "source", source.Name(), "variant", source.Variant(), "provider_type", sourceProvider.Type())
			primaryLocation, err := source.GetPrimaryLocation()
			if err != nil {
				return nil, err
			}
			tblMapping.location = primaryLocation
			t.logger.Debugw("Primary Location", "location", primaryLocation.Location(), "location_type", fmt.Sprintf("%T", primaryLocation))
			// Spark and K8s, BigQuery, and Clickhouse use GetPrimaryTableName for primary table names for transformations
			// Spark and K8s use the primary table name to identify the metadata file location on filestore.
			// BigQuery and Clickhouse have not been updated to use viewless Primaries.
			providerResourceID.Type = provider.Primary
			tableName, err = provider.GetPrimaryTableName(providerResourceID)
			if err != nil {
				return nil, err
			}
		} else if isSnowflakeOfflineStore && source.IsTransformation() {
			logger.Debugw("Transformation Source on Snowflake", "source", source.Name(), "variant", source.Variant())
			// Snowflake is the only SQL provider that uses transformation table name for transformations
			// Snowflake used the primary table names as well but we decided to change it when working with Attentive
			location, err := source.GetTransformationLocation()
			if err != nil {
				return nil, err
			}
			tableName = location.Location()
			tblMapping.location = location
		} else if source.IsPrimaryData() {
			logger.Debugw("Primary Data Source", "source", source.Name(), "variant", source.Variant(), "provider", sourceProvider.Type())
			// All SQL providers use the direct Primary table instead of the view going forward except for BigQuery and Clickhouse
			// The primary table views are no longer used for the rest of the SQL providers so we have to utilize the original source table name
			location, err := source.GetPrimaryLocation()
			if err != nil {
				return nil, err
			}
			logger.Debugw("Primary Location", "location", location.Location(), "location_type", fmt.Sprintf("%T", location))
			sqlLocation, ok := location.(*pl.SQLLocation)
			if !ok {
				logger.Errorw("location should be of type SQLLocation", "location_type", fmt.Sprintf("%T", location), "location", location)
				return nil, fferr.NewInternalErrorf("location should be of type SQLLocation: %T", location)
			}
			// NOTE: It's critical we use TableLocation here instead of Location() because it's highly likely that primary tables will reside
			// in different databases and schemas. Given Featureform will write out all transformations, materializations and training sets
			// to the same database and schema that was registered with the provider, everything beyond the primary table can be assumed to be
			// located in the same database and schema.
			if isSnowflakeOfflineStore {
				obj := sqlLocation.TableLocation()
				tableName = provider.SanitizeSnowflakeIdentifier(obj)
			} else {
				tableName = sqlLocation.TableLocation().String()
			}
			tblMapping.location = location
		} else {
			logger.Errorw("source type not currently supported for table mapping", "source_type", sourceProvider.Type())
			return nil, fferr.NewInternalErrorf("source type not currently supported for table mapping: %s", sourceProvider.Type())
		}
		logger.Debugw("Table Name for Name Variant", "name", tableName)
		tblMapping.name = tableName
		sourceMap[nameVariant.ClientString()] = tblMapping
	}
	return sourceMap, nil
}

func (t *SourceTask) verifyCompletionOfSources(sources []metadata.NameVariant) error {
	allReady := false
	for !allReady {
		sourceVariants, err := t.metadata.GetSourceVariants(t.ctx, sources)
		if err != nil {
			return err
		}
		total := len(sourceVariants)
		totalReady := 0
		for _, sourceVariant := range sourceVariants {
			if sourceVariant.Status() == scheduling.READY {
				totalReady += 1
			}
			if sourceVariant.Status() == scheduling.FAILED {
				wrapped := fferr.NewResourceFailedError(
					sourceVariant.Name(),
					sourceVariant.Variant(),
					fferr.SOURCE_VARIANT,
					fmt.Errorf("required dataset is in a failed state"),
				)
				wrapped.AddDetail("resource_status", sourceVariant.Status().String())
				return wrapped
			}
		}
		allReady = total == totalReady
		time.Sleep(1 * time.Second)
	}
	return nil
}

func getSourceMapping(template string, replacements map[string]tableMapping) ([]provider.SourceMapping, error) {
	sourceMap := []provider.SourceMapping{}
	numEscapes := strings.Count(template, "{{")
	for i := 0; i < numEscapes; i++ {
		split := strings.SplitN(template, "{{", 2)
		afterSplit := strings.SplitN(split[1], "}}", 2)
		key := strings.TrimSpace(afterSplit[0])
		tableMapping, has := replacements[key]
		if !has {
			return nil, fferr.NewInvalidArgumentError(fmt.Errorf("value %s not found in replacements: %v", key, replacements))
		}
		sourceMap = append(
			sourceMap, provider.SourceMapping{
				Template:            sanitize(tableMapping.name),
				Source:              tableMapping.name,
				ProviderType:        tableMapping.providerType,
				ProviderConfig:      tableMapping.providerConfig,
				TimestampColumnName: tableMapping.timestampColumnName,
				Location:            tableMapping.location,
			},
		)
		template = afterSplit[1]
	}
	return sourceMap, nil
}

func getOrderedSourceMappings(
	sources []metadata.NameVariant,
	sourceMap map[string]tableMapping,
) ([]provider.SourceMapping, error) {
	sourceMapping := make([]provider.SourceMapping, len(sources))
	for i, nv := range sources {
		sourceKey := nv.ClientString()
		tableMapping, hasKey := sourceMap[sourceKey]
		if !hasKey {
			return nil, fferr.NewInternalError(fmt.Errorf("key %s not in source map", sourceKey))
		}
		sourceMapping[i] = provider.SourceMapping{
			Template:            sanitize(tableMapping.name),
			Source:              tableMapping.name,
			ProviderType:        tableMapping.providerType,
			ProviderConfig:      tableMapping.providerConfig,
			TimestampColumnName: tableMapping.timestampColumnName,
			Location:            tableMapping.location,
		}
	}
	return sourceMapping, nil
}

func templateReplace(template string, replacements map[string]tableMapping, offlineStore provider.OfflineStore, logger logging.Logger) (string, error) {
	logger.Debugw("Template Replace", "template", template, "replacements", replacements)
	formattedString := ""
	numEscapes := strings.Count(template, "{{")
	for i := 0; i < numEscapes; i++ {
		split := strings.SplitN(template, "{{", 2)
		afterSplit := strings.SplitN(split[1], "}}", 2)
		key := strings.TrimSpace(afterSplit[0])
		tableMapping, has := replacements[key]
		if !has {
			return "", fferr.NewInvalidArgumentError(fmt.Errorf("value %s not found in replacements: %v", key, replacements))
		}
		replacement, err := getReplacementString(offlineStore, tableMapping, logger)
		if err != nil {
			return "", err
		}
		formattedString += fmt.Sprintf("%s%s", split[0], replacement)
		template = afterSplit[1]
	}
	formattedString += template
	return formattedString, nil
}

func getReplacementString(offlineStore provider.OfflineStore, tableMapping tableMapping, logger logging.Logger) (string, error) {
	logger.Debugw("Getting Replacement String", "table_mapping", tableMapping, "offline_store_type", offlineStore.Type())
	switch offlineStore.Type() {
	case pt.BigQueryOffline:
		bqConfig := pc.BigQueryConfig{}
		if err := bqConfig.Deserialize(offlineStore.Config()); err != nil {
			return "", err
		}
		return fmt.Sprintf("`%s.%s.%s`", bqConfig.ProjectId, bqConfig.DatasetId, tableMapping.name), nil
	case pt.ClickHouseOffline:
		sqlLocation, isSqlLocation := tableMapping.location.(*pl.SQLLocation)
		if !isSqlLocation {
			return "", fferr.NewInvalidArgumentError(fmt.Errorf("expected SQLLocation for ClickHouse; got: %T", tableMapping.location))
		}
		return provider.SanitizeClickHouseIdentifier(sqlLocation.TableLocation().String()), nil
	case pt.SnowflakeOffline:
		sqlLocation, isSqlLocation := tableMapping.location.(*pl.SQLLocation)
		if !isSqlLocation {
			return "", fferr.NewInvalidArgumentError(fmt.Errorf("expected SQLLocation for Snowflake; got: %T", tableMapping.location))
		}
		return provider.SanitizeSnowflakeIdentifier(sqlLocation.TableLocation()), nil
	case pt.PostgresOffline, pt.RedshiftOffline:
		sqlLocation, isSqlLocation := tableMapping.location.(*pl.SQLLocation)
		if !isSqlLocation {
			return "", fferr.NewInvalidArgumentError(fmt.Errorf("expected SQLLocation for Postgres; got: %T", tableMapping.location))
		}
		return provider.SanitizeSqlLocation(sqlLocation.TableLocation()), nil
	case pt.SparkOffline:
		return sanitize(tableMapping.name), nil
	default:
		return "", fferr.NewInvalidArgumentError(fmt.Errorf("offline store type not supported: %s", offlineStore.Type()))
	}
}

type variableReplacement struct {
	variable    string
	replacement string
}

func sqlVariableReplace(query string, task *SourceTask) string {
	var unixTimeLastRun int64
	if task != nil && task.isUpdate {
		// StartTime begins when its PENDING and waiting for deps. That causes it to re-read data.
		// EndTime is the lesser evil.
		unixTimeLastRun = task.lastSuccessfulTask.EndTime.UTC().Unix()
	} else {
		unixTimeLastRun = 0
	}
	replacements := []variableReplacement{
		{"FF_LAST_RUN_TIMESTAMP", fmt.Sprintf("TO_TIMESTAMP(%d)", unixTimeLastRun)},
	}
	for _, replacement := range replacements {
		query = strings.ReplaceAll(query, replacement.variable, replacement.replacement)
	}
	return query
}

func sanitize(ident string) string {
	fmt.Println("Sanitizing: ", ident)
	parts := strings.Split(ident, ".")
	sanitized := make([]string, len(parts))

	for i, part := range parts {
		sanitized[i] = db.Identifier{part}.Sanitize()
	}

	return strings.Join(sanitized, ".")
}
