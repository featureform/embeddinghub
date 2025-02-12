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
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/featureform/config"
	"github.com/featureform/fferr"
	"github.com/featureform/filestore"
	"github.com/featureform/logging"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/provider/spark"

	re "github.com/avast/retry-go/v4"
	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/apierr"
	dbClient "github.com/databricks/databricks-sdk-go/client"
	dbConfig "github.com/databricks/databricks-sdk-go/config"
	"github.com/databricks/databricks-sdk-go/service/compute"
	dbfs "github.com/databricks/databricks-sdk-go/service/files"
	"github.com/databricks/databricks-sdk-go/service/jobs"
	"github.com/google/uuid"
)

type DatabricksResultState string

const (
	Success   DatabricksResultState = "SUCCESS"
	Failed    DatabricksResultState = "FAILED"
	Timeout   DatabricksResultState = "TIMEOUT"
	Cancelled DatabricksResultState = "CANCELLED"
)

func NewDatabricksExecutor(databricksConfig pc.DatabricksConfig, logger logging.Logger) (SparkExecutor, error) {
	client := databricks.Must(
		databricks.NewWorkspaceClient(&databricks.Config{
			Host:     databricksConfig.Host,
			Token:    databricksConfig.Token,
			Username: databricksConfig.Username,
			Password: databricksConfig.Password,
		}))

	if err := re.Do(
		func() error {
			// Creating a new workspace client doesn't actually test that the client is able to successfully connect and communicate with
			// the cluster given the provided credentials; to fail earlier in the process (i.e. _before_ submitting a job) we'll make a call
			// to Databricks's Clusters API to get information about the cluster with the provided ID.
			_, err := client.Clusters.Get(context.Background(), compute.GetClusterRequest{ClusterId: databricksConfig.Cluster})
			if err != nil {
				// The Databricks SDK uses Go's "net/url" under the hood for parsing the hostname; this _can_ result in error messages that
				// are not very helpful. For example, if the hostname is "_https://my-hostname" the error message will be:
				// parse '_https://my-hostname': first path segment in URL cannot contain colon
				// To direct users to a solution, we'll check for message prefix 'parse' and provide a more helpful error message that wraps
				// the original error message.
				if strings.Contains(err.Error(), "parse") {
					parsingError := strings.TrimPrefix(err.Error(), "parse ")
					return fferr.NewInternalError(fmt.Errorf("the hostname %s is invalid and resulted in a parsing error (%s); check that the hostname is correct before trying again", databricksConfig.Host, parsingError))
				}
			}
			return nil
		},
		re.DelayType(func(n uint, err error, config *re.Config) time.Duration {
			return re.BackOffDelay(n, err, config)
		}),
		re.Attempts(5),
	); err != nil {
		fmt.Printf("failed to get cluster information for %s due to error: %v\n", databricksConfig.Cluster, err)
		return nil, err
	}

	errorMessageClient, err := dbClient.New(&dbConfig.Config{
		Host:     databricksConfig.Host,
		Token:    databricksConfig.Token,
		Username: databricksConfig.Username,
		Password: databricksConfig.Password,
	})
	if err != nil {
		fmt.Println("could not create error message client: ", err)
		errorMessageClient = nil
	}

	base, err := newBaseExecutor()
	if err != nil {
		return nil, err
	}

	return &DatabricksExecutor{
		client:             client,
		cluster:            databricksConfig.Cluster,
		config:             databricksConfig,
		errorMessageClient: errorMessageClient,
		logger:             logger,
		baseExecutor:       base,
	}, nil
}

type DatabricksExecutor struct {
	client             *databricks.WorkspaceClient
	cluster            string
	config             pc.DatabricksConfig
	errorMessageClient *dbClient.DatabricksClient
	logger             logging.Logger
	baseExecutor
}

func (db *DatabricksExecutor) SupportsTransformationOption(opt TransformationOptionType) (bool, error) {
	return false, nil
}

func (db *DatabricksExecutor) RunSparkJob(cmd *spark.Command, store SparkFileStoreV2, opts SparkJobOptions, tfopts TransformationOptions) error {
	safeScript, safeArgs := cmd.Redacted().CompileScriptOnly()
	ctx := context.Background()
	id := uuid.New().String()
	task := cmd.CompileDatabricks()
	logger := db.logger.With("script", safeScript, "args", safeArgs, "store", store.Type(), "job_name", opts.JobName, "cluster_id", db.cluster, "id", id)
	task.TaskKey = fmt.Sprintf("featureform-task-%s", id)
	logger.Info("Running Spark job")
	task.ExistingClusterId = db.cluster
	jobToRun, err := db.client.Jobs.Create(ctx, jobs.CreateJob{
		Name:  fmt.Sprintf("%s-%s", opts.JobName, id),
		Tasks: []jobs.Task{task},
	})
	if err != nil {
		logger.Errorw("could not create job", "error", err)
		wrapped := fferr.NewExecutionError(pt.SparkOffline.String(), err)
		wrapped.AddDetails("job_name", fmt.Sprintf("%s-%s", opts.JobName, id), "job_id", fmt.Sprint(jobToRun.JobId), "executor_type", "Databricks", "store_type", store.Type())
		wrapped.AddFixSuggestion("Check the cluster logs for more information")
		return wrapped
	}

	if err := db.runSparkJobWithRetries(logger, jobToRun.JobId, opts.MaxJobDuration); err != nil {
		details := []any {
			"job_name", fmt.Sprintf("%s-%s", opts.JobName, id),
			"job_id", fmt.Sprint(jobToRun.JobId),
			"executor_type", "Databricks",
			"store_type", store.Type(),
		}
		errDetails := append(details, "error", err)
		logger.Errorw("job failed", errDetails...)
		wrapped := fferr.NewExecutionError(
			pt.SparkOffline.String(), fmt.Errorf("job failed: %v", err),
		)
		wrapped.AddDetails(details...)
		wrapped.AddFixSuggestion("Check the cluster logs for more information")
		return wrapped
	}
	return nil
}


func (db *DatabricksExecutor) runSparkJobWithRetries(
	logger logging.Logger, jobId int64, maxWait time.Duration,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), maxWait)
	defer cancel()

	handleErr := func(err error) error {
		errorMessage := err
		if db.errorMessageClient != nil {
			errorMessage, err = db.getErrorMessage(jobId)
			if err != nil {
				logger.Errorf(
					"the '%v' job failed, could not get error message: %v\n",
					jobId, err,
				)
			}
		}
		if !db.isEphemeralError(errorMessage) {
			// It we aren't sure if its ephemeral we shouldn't retry
			errorMessage = re.Unrecoverable(errorMessage)
		}
		return errorMessage
	}
	return re.Do(
		func() error {
			deadline, has := ctx.Deadline()
			if !has {
				errMsg := "Avoiding infinite loop, refusing to run databricks command without context deadline"
				logger.Error(errMsg)
				return re.Unrecoverable(fferr.NewInternalErrorf(errMsg))
			}
			_, err := db.client.Jobs.RunNow(ctx, jobs.RunNow{
				JobId: jobId,
			})
			if err != nil {
				logger.Errorw("job failed to start", "error", err)
				return handleErr(err)
			}
			timeoutLeft := time.Until(deadline)
			_, waitErr := db.client.Jobs.WaitGetRunJobTerminatedOrSkipped(ctx, jobId, timeoutLeft, nil) 
			if waitErr != nil {
				logger.Errorw("job failed", "error", waitErr)
				return handleErr(waitErr)
			}
			return nil
		},
		re.Context(ctx),                   // Stop retries when context times out
		re.Attempts(0),                    // Infinite retries (until context cancels)
		re.LastErrorOnly(true),            // Only return the last error
		re.DelayType(re.CombineDelay(      // Use exponential backoff + jitter
			re.BackOffDelay,               // Exponential backoff
			re.RandomDelay,                // Random jitter
		)),
		re.Delay(5*time.Second),           // Initial delay of 5 seconds
		re.MaxDelay(3*time.Minute),        // Cap max delay betweein iterations
	)
}

func (db *DatabricksExecutor) isEphemeralError(err error) bool {
	// This happens when the driver goes OOM sometimes
	dbrixErr, isDbrixErr := err.(*apierr.APIError)
	if !isDbrixErr {
		return false
	} else {
		db.logger.Debugw("Checking if DBrix err is ephemeral", "dbrix-err", dbrixErr)
	}
	if strings.Contains(err.Error(), "not reach driver of cluster") {
		return true
	}
	return false
}

func (db *DatabricksExecutor) InitializeExecutor(store SparkFileStoreV2) error {
	logger := db.logger.With("store", store.Type(), "executor_type", "Databricks")
	// We can't use CreateFilePath here because it calls Validate under the hood,
	// which will always fail given it's a local file without a valid scheme or bucket, for example.
	sparkLocalScriptPath := &filestore.LocalFilepath{}
	if err := sparkLocalScriptPath.SetKey(db.files.LocalScriptPath); err != nil {
		logger.Errorw("could not set local script path", "error", err)
		return err
	}
	if config.ShouldUseDBFS() {
		logger.Info("Copying run script to DBFS")
		if err := db.readAndUploadFileDBFS(sparkLocalScriptPath, db.files.RemoteScriptPath); err != nil {
			logger.Errorw("could not upload spark script", "error", err)
			return err
		}
	} else {
		logger.Debug("Not copying file to DBFS")
		sparkRemoteScriptPath, err := sparkPythonFileURI(store, logger)
		if err != nil {
			logger.Errorw("Failed to get remote script path during init", "err", err)
			return err
		}

		if err := readAndUploadFile(sparkLocalScriptPath, sparkRemoteScriptPath, store); err != nil {
			logger.Errorw("Failed to copy local file to remote", "err", err)
			return err
		}
	}
	return nil
}

func (db *DatabricksExecutor) getErrorMessage(jobId int64) (error, error) {
	ctx := context.Background()

	jobIdStr := fmt.Sprintf("%d", jobId)
	runRequest := jobs.ListRunsRequest{
		JobId: jobId,
	}

	runs, err := db.client.Jobs.ListRunsAll(ctx, runRequest)
	if err != nil {
		wrapped := fferr.NewExecutionError(pt.SparkOffline.String(), fmt.Errorf("could not get runs for job: %v", err))
		wrapped.AddDetail("job_id", jobIdStr)
		wrapped.AddDetail("executor_type", "Databricks")
		return nil, wrapped
	}

	if len(runs) == 0 {
		wrapped := fferr.NewInternalError(fmt.Errorf("no runs found for job"))
		wrapped.AddDetail("job_id", jobIdStr)
		wrapped.AddDetail("executor_type", "Databricks")
		return nil, wrapped
	}
	runID := runs[0].RunId
	run, err := db.client.Jobs.GetRun(ctx, jobs.GetRunRequest{IncludeHistory: true, RunId: runID})
	if err != nil {
		wrapped := fferr.NewExecutionError("Databricks", fmt.Errorf("could not get run for job: %v", err))
		wrapped.AddDetail("job_id", jobIdStr)
		wrapped.AddDetail("executor_type", "Databricks")
		return nil, wrapped
	}
	if len(run.Tasks) == 0 {
		innerErr := fmt.Errorf("no tasks found for job run %d", runID)
		noTasksErr := fferr.NewExecutionError("Databricks", innerErr)
		noTasksErr.AddDetail("job_id", jobIdStr)
		noTasksErr.AddDetail("executor_type", "Databricks")
		return nil, noTasksErr
	}
	task := run.Tasks[0]
	// use task.RunId to get output for the task
	output, err := db.client.Jobs.GetRunOutputByRunId(ctx, task.RunId)
	if err != nil {
		wrapped := fferr.NewExecutionError(pt.SparkOffline.String(), fmt.Errorf("could not get task output for job: %v", err))
		wrapped.AddDetail("job_id", jobIdStr)
		wrapped.AddDetail("task_id", fmt.Sprintf("%d", task.RunId))
		wrapped.AddDetail("executor_type", "Databricks")
		return nil, wrapped
	}
	return fferr.NewExecutionError("Databricks", errors.New(output.Error)), nil
}

func (db *DatabricksExecutor) readAndUploadFileDBFS(
	localPath filestore.Filepath, remotePathKey string,
) error {
	// TODO continue to move context further up the stack
	ctx := context.Background()
	// TODO move this formating out of here
	remotePath := fmt.Sprintf("dbfs:/tmp/%s", remotePathKey)
	logger := logging.GlobalLogger.With(
		"fromPath", localPath.ToURI(),
		"toPath", remotePath,
		"store", "DBFS-databricks",
	)
	_, getErr := db.client.Dbfs.GetStatus(ctx, dbfs.GetStatusRequest{
		Path: remotePath,
	})
	if getErr != nil {
		// Check if it's a "RESOURCE_DOES_NOT_EXIST" / 404 error
		var apiErr *apierr.APIError
		if errors.As(getErr, &apiErr) && apiErr.StatusCode != http.StatusNotFound {
			logger.Errorw("Failed to query DBFS for our script file", "err", getErr)
			return fferr.NewInternalError(getErr)
		}
		logger.Infow("File doesn't exist, creating now")
	}

	data, err := os.ReadFile(localPath.Key())
	if err != nil {
		logger.Errorw("Failed to read local file", "error", err)
		return fferr.NewInternalError(err)
	}

	encoded := base64.StdEncoding.EncodeToString(data)
	writeErr := db.client.Dbfs.Put(ctx, dbfs.Put{
		Path:      remotePath,
		Overwrite: true,
		Contents:  encoded,
	})
	if writeErr != nil {
		logger.Errorw("Failed to write to remote path", "error", writeErr)
		return fferr.NewInternalError(writeErr)
	}
	logger.Infow("Copied local file to remote filestore")
	return nil
}
