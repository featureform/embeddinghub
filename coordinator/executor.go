// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package coordinator

import (
	"fmt"
	"runtime/debug"
	"time"

	"golang.org/x/exp/slices"

	"github.com/featureform/coordinator/spawner"
	"github.com/featureform/coordinator/tasks"
	"github.com/featureform/fferr"
	"github.com/featureform/logging"
	"github.com/featureform/metadata"
	"github.com/featureform/scheduling"
	"github.com/google/uuid"
)

type ExecutorConfig struct {
	DependencyPollInterval time.Duration
}

type Executor struct {
	locker   *metadata.TaskLocker
	metadata *metadata.Client
	spawner  spawner.JobSpawner
	logger   logging.Logger
	config   ExecutorConfig
}

// We should only need to pass the runID here, but the way the data is stored doesn't allow that atm
// without searching through all tasks
func (e *Executor) RunTask(tid scheduling.TaskID, rid scheduling.TaskRunID) error {
	logger := e.logger.With("execution_id", uuid.NewString(), "task_id", tid, "run_id", rid)
	logger.Debug("Checking if task is lockable")
	unlockTask, err := e.locker.LockTask(tid, false)
	if _, ok := err.(*fferr.KeyAlreadyLockedError); ok {
		logger.Debug("Task already locked, aborting")
		return nil
	} else if err != nil {
		logger.Errorw("Unable to lock task", "err", err)
		return err
	}

	defer func() {
		if err := unlockTask(); err != nil {
			logger.Errorw("Failed to unlock task", "error", err)
		}
		logger.Debug("Unlocked task")
	}()

	logger.Debug("Checking if run is lockable")
	unlockRun, err := e.locker.LockRun(rid, false)
	if _, ok := err.(*fferr.KeyAlreadyLockedError); ok {
		logger.Debug("Run already locked, aborting")
		return nil
	} else if err != nil {
		logger.Errorw("Unable to lock run", "err", err)
		return err
	}
	logger.Info("Starting task run")

	defer func() {
		if err := unlockRun(); err != nil {
			logger.Errorw("Failed to unlock run", "error", err)
		}
		logger.Debug("Unlocked run")
	}()

	logger.Info("Fetching run metadata")
	run, err := e.metadata.Tasks.GetRun(tid, rid)
	if err != nil {
		logger.Errorw("Failed to get run", "error", err)
		return err
	}

	logger = logger.With("target", run.Target)
	logger.Debugw("Task run started", "run", run)

	// Stop attempting to run the run
	// In the future we should differentiate so we know if we need to cancel other runs
	// If the run is unlocked while running, we will assume theres no owner and rerun it. In the future
	// we should reattach to the runner that was running it previously
	if !slices.Contains([]scheduling.Status{scheduling.PENDING, scheduling.RUNNING}, run.Status) {
		logger.Info("Run is not pending or running, skipping...")
		return nil
	}

	logger.Info("Checking task dependencies")
	err = e.waitForPendingDependencies(run, logger)
	if _, ok := err.(*fferr.DependencyFailedError); ok {
		logger.Info("Dependency failed, cancelling run")
		err := e.metadata.Tasks.SetRunStatus(tid, rid, scheduling.CANCELLED, err)
		if err != nil {
			return err
		}
		return nil
	} else if err != nil {
		return err
	}

	//logger.Debug("Checking for cancel signal")
	//cancel, waitErr := e.metadata.Tasks.WatchForCancel(tid, rid)

	var lastSuccessfulRun scheduling.TaskRunMetadata

	isUpdate := false
	if run.LastSuccessful != nil {
		logger.Debugw("Fetching last successful run", "run", run.LastSuccessful)
		lastSuccessfulRun, err = e.metadata.Tasks.GetRun(tid, run.LastSuccessful)
		if err != nil {
			logger.Errorw("Failed to get last successful run", "error", err)
			return err
		}
		logger.Debugw("Last successful run found", "run", lastSuccessfulRun)
		isUpdate = true
	}

	task, err := e.getTaskRunner(run, lastSuccessfulRun, isUpdate, logger)
	if err != nil {
		return err
	}
	logger.Debugw("Setting run status to running", "runner", task)
	logger.Debug("Setting run status to running")
	if run.Status == scheduling.PENDING {
		logger.Debug("Current run is PENDING")
		if err := e.metadata.Tasks.SetRunStatus(tid, rid, scheduling.RUNNING, nil); err != nil {
			logger.Errorw("Failed to set run status to running", "error", err)
			return err
		}
	} else if run.Status == scheduling.RUNNING {
		logger.Infow("Rerunning previously attempted task")
		if err := e.metadata.Tasks.AddRunLog(tid, rid, "Previous run did not complete. Restarting..."); err != nil {
			logger.Errorw("Failed to add run log", "error", err)
			return err
		}
	} else {
		logger.Debugf("Cannot run task with status %s", run.Status.String())
	}
	logger.Info("Set run status to running")

	logger.Info("Starting Run")
	runErrChan := e.Run(task)

	// Disabling the cancel for now since we don't currently support it all the way and was running into panics
	select {
	//case <-cancel:
	//	logger.Info("Run Cancelled")
	//	return e.handleRunStatus(tid, rid, scheduling.CANCELLED, nil)
	//
	//case err := <-waitErr:
	//	logger.Errorf("Recieved error while watching for cancel: %s", err.Error())
	//	return err

	case err := <-runErrChan:
		if err != nil {
			logger.Errorf("Run Failed: %s", err.Error())
			if err := e.handleRunStatus(tid, rid, scheduling.FAILED, err); err != nil {
				logger.Error(err.Error())
			}
			return fferr.NewTaskRunFailedError(tid.String(), rid.String(), err)
		}
		logger.Info("Run Ready")
		if err := e.handleRunStatus(tid, rid, scheduling.READY, err); err != nil {
			logger.Error(err.Error())
		}
		return nil
	}
}

func (e *Executor) handleRunStatus(tid scheduling.TaskID, rid scheduling.TaskRunID, status scheduling.Status, err error) error {
	if err := e.metadata.Tasks.SetRunStatus(tid, rid, status, err); err != nil {
		return err
	}
	if err := e.metadata.Tasks.EndRun(tid, rid); err != nil {
		return err
	}
	return nil
}

func (e *Executor) Run(task tasks.Task) chan error {
	errChan := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				e.logger.Errorw(
					"an internal issue resulted in a panic",
					"panic_stack", string(debug.Stack()))
				// debug.Stack() will show the stacktrace that the panic occurred. Leave in for easier debugging
				errChan <- fmt.Errorf("an internal issue resulted in a panic: %v\n%s", r, string(debug.Stack()))
			}
		}()
		errChan <- task.Run()
	}()
	return errChan
}

func (e *Executor) getTaskRunner(runMetadata scheduling.TaskRunMetadata, lastSuccessfulRun scheduling.TaskRunMetadata, isUpdate bool, logger logging.Logger) (tasks.Task, error) {
	logger.Infow("getTaskRunner", "last task", lastSuccessfulRun)
	taskConfig := tasks.TaskConfig{
		DependencyPollInterval: e.config.DependencyPollInterval,
	}
	baseTask := tasks.NewBaseTask(e.metadata, runMetadata, lastSuccessfulRun, isUpdate, runMetadata.IsDelete, e.spawner, logger, taskConfig)
	e.logger.Infow("Base task created", "task", baseTask.Redacted())
	return tasks.Get(runMetadata.TargetType, baseTask)
}

func (e *Executor) waitForPendingDependencies(run scheduling.TaskRunMetadata, logger logging.Logger) error {
	allRuns, allTasks, err := e.collectAllRuns(run)
	if err != nil {
		return err
	}

	logger.Debugw("Dependencies", "runs", allRuns, "tasks", allTasks)

	logger.Infof("Found %d dependencies. Waiting for completion...", len(allRuns))
	// Create a channel to communicate errors back to the main goroutine.
	return e.checkAllRuns(allRuns, allTasks, logger)
}

func (e *Executor) checkAllRuns(allRuns []scheduling.TaskRunID, allTasks []scheduling.TaskID, logger logging.Logger) error {
	errCh := make(chan error, len(allRuns))
	defer close(errCh) // Close the channel when all operations are done.

	// Loop over all runs and start a goroutine for each task.
	for i, id := range allRuns {
		go func(i int, id scheduling.TaskRunID) { // Ensure variables are passed to avoid closure issues.
			if err := e.waitForRunCompletion(allTasks[i], id, logger); err != nil {
				errCh <- err // Send error to the main goroutine if it occurs.
				return
			}
			errCh <- nil // Send nil if no error.
		}(i, id)
	}

	// Wait for all goroutines to finish and check for errors.
	for range allRuns {
		if err := <-errCh; err != nil {
			return err // Return the first error encountered.
		}
	}
	return nil
}

// Helper function to collect all task run IDs from dependencies.
func (e *Executor) collectAllRuns(run scheduling.TaskRunMetadata) ([]scheduling.TaskRunID, []scheduling.TaskID, error) {
	var allRuns []scheduling.TaskRunID
	var allTasks []scheduling.TaskID
	for _, dep := range run.Dag.Dependencies(run.TaskId) {
		id, err := run.Dag.GetTaskRunID(dep)
		if err != nil {
			return nil, nil, err
		}
		allRuns = append(allRuns, id)
		allTasks = append(allTasks, run.TaskId)
	}
	return allRuns, allTasks, nil
}

func (e *Executor) waitForRunCompletion(tid scheduling.TaskID, rid scheduling.TaskRunID, logger logging.Logger) error {
	for {
		logger.Infow("Checking dependency status", "task_id", tid, "run_id", rid)
		run, err := e.metadata.Tasks.GetRun(tid, rid)
		if err != nil {
			return err
		}

		logger.Debugw("Dependency status", "status", run.Status, "run_id", rid, "task_id", tid)

		switch run.Status {
		case scheduling.READY:
			return nil
		case scheduling.PENDING:
			time.Sleep(10 * time.Second)
		default:
			err := run.Target.FailedError()
			logger.Error(err)
			return err
		}
	}
}
