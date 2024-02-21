package scheduling

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	sp "github.com/featureform/scheduling/storage_providers"
)

type Key interface {
	String() string
}

type TaskMetadataKey struct {
	taskID TaskID
}

func (tmk TaskMetadataKey) String() string {
	if tmk.taskID == 0 {
		return "/tasks/metadata/task_id="
	}
	return fmt.Sprintf("/tasks/metadata/task_id=%d", tmk.taskID)
}

type TaskRunKey struct {
	taskID TaskID
}

func (trk TaskRunKey) String() string {
	if trk.taskID == 0 {
		return "/tasks/runs/task_id="
	}
	return fmt.Sprintf("/tasks/runs/task_id=%d", trk.taskID)
}

type TaskRunMetadataKey struct {
	taskID TaskID
	runID  TaskRunID
	date   time.Time
}

func (trmk TaskRunMetadataKey) String() string {
	key := "/tasks/runs/metadata"

	// adds the date to the key if it's not the default value
	if trmk.date != (time.Time{}) {
		key += fmt.Sprintf("/%s", trmk.date.Format("2006/01/02"))

		// adds the task_id and run_id to the key if they're not the default value
		if trmk.taskID != 0 && trmk.runID != 0 {
			key += fmt.Sprintf("/task_id=%d/run_id=%d", trmk.taskID, trmk.runID)
		}
	}
	return key
}

func NewTaskManager(storage sp.StorageProvider) TaskManager {
	return TaskManager{storage: storage}
}

type TaskManager struct {
	storage sp.StorageProvider
}

type TaskMetadataList []TaskMetadata

func (tml *TaskMetadataList) ToJSON() string {
	return ""
}

// Task Methods
func (tm *TaskManager) CreateTask(name string, tType TaskType, target TaskTarget) (TaskMetadata, error) {
	keys, err := tm.storage.ListKeys(TaskMetadataKey{}.String())
	if err != nil {
		return TaskMetadata{}, fmt.Errorf("failed to fetch keys: %v", err)
	}

	// This logic could probably be somewhere else
	var latestID int
	if len(keys) == 0 {
		latestID = 0
	} else {
		latestID, err = getLatestID(keys)
		if err != nil {
			return TaskMetadata{}, err
		}
	}

	metadata := TaskMetadata{
		ID:          TaskID(latestID + 1),
		Name:        name,
		TaskType:    tType,
		Target:      target,
		TargetType:  target.Type(),
		DateCreated: time.Now().UTC(),
	}

	// I do this serialize and deserialize a lot in this file. Would be nice to have set and get helpers that deal with
	// all the converting instead
	serializedMetadata, err := metadata.Marshal()
	if err != nil {
		return TaskMetadata{}, fmt.Errorf("failed to marshal metadata: %v", err)
	}

	key := TaskMetadataKey{taskID: metadata.ID}
	lock, err := tm.storage.Lock(key.String())
	if err != nil {
		return TaskMetadata{}, fmt.Errorf("failed to lock task key: %v", err)
	}
	err = tm.storage.Set(key.String(), string(serializedMetadata), lock)
	if err != nil {
		return TaskMetadata{}, err
	}
	err = tm.storage.Unlock(key.String(), lock)
	if err != nil {
		return TaskMetadata{}, fmt.Errorf("failed to unlock task key: %v", err)
	}

	runs := TaskRuns{
		TaskID: metadata.ID,
		Runs:   []TaskRunSimple{},
	}
	serializedRuns, err := runs.Marshal()
	if err != nil {
		return TaskMetadata{}, err
	}

	taskRunKey := TaskRunKey{taskID: metadata.ID}
	lock, err = tm.storage.Lock(taskRunKey.String())
	if err != nil {
		return TaskMetadata{}, fmt.Errorf("failed to lock task run key: %v", err)
	}
	err = tm.storage.Set(taskRunKey.String(), string(serializedRuns), lock)
	if err != nil {
		return TaskMetadata{}, err
	}
	err = tm.storage.Unlock(taskRunKey.String(), lock)
	if err != nil {
		return TaskMetadata{}, fmt.Errorf("failed to unlock task run key: %v", err)
	}

	return metadata, nil
}

// Finds the highest increment in a list of strings formatted like "/tasks/metadata/task_id=0"
func getLatestID(taskPaths []string) (int, error) {
	highestIncrement := -1
	for _, path := range taskPaths {
		parts := strings.Split(path, "task_id=")
		if len(parts) < 2 {
			return -1, fmt.Errorf("invalid format for path: %s", path)
		}
		increment, err := strconv.Atoi(parts[1])
		if err != nil {
			return -1, fmt.Errorf("failed to convert task_id to integer: %s", err)
		}
		if increment > highestIncrement {
			highestIncrement = increment
		}
	}
	if highestIncrement == -1 {
		return -1, fmt.Errorf("no valid increments found")
	}
	return highestIncrement, nil
}

func (tm *TaskManager) GetTaskByID(id TaskID) (TaskMetadata, error) {
	key := TaskMetadataKey{taskID: id}.String()
	metadata, err := tm.storage.Get(key, false)
	if err != nil {
		return TaskMetadata{}, err
	}

	// Should enum 0 as EmptyList or something
	if len(metadata) == 0 {
		return TaskMetadata{}, fmt.Errorf("task not found for id: %s", string(id))
	}

	taskMetadata := TaskMetadata{}
	err = taskMetadata.Unmarshal([]byte(metadata[0]))
	if err != nil {
		return TaskMetadata{}, err
	}
	return taskMetadata, nil
}

func (tm *TaskManager) GetAllTasks() (TaskMetadataList, error) {
	metadata, err := tm.storage.Get(TaskMetadataKey{}.String(), true)
	if err != nil {
		return TaskMetadataList{}, err
	}

	// Want to move this logic out of this func
	tml := TaskMetadataList{}
	for _, m := range metadata {
		taskMetadata := TaskMetadata{}
		err = taskMetadata.Unmarshal([]byte(m))
		if err != nil {
			return TaskMetadataList{}, err
		}
		tml = append(tml, taskMetadata)
	}
	return tml, nil
}

// Need to implement this
type TaskRunList []TaskRunMetadata

func (trl *TaskRunList) ToJSON() string {
	return ""
}

func (trl *TaskRunList) FilterByStatus(status Status) {
	var newList TaskRunList
	for _, run := range *trl {
		if run.Status == status {
			newList = append(newList, run)
		}
	}
	*trl = newList
}

// Task Run Methods
func (tm *TaskManager) CreateTaskRun(name string, taskID TaskID, trigger Trigger) (TaskRunMetadata, error) {
	// ids will be generated by TM
	taskRunKey := TaskRunKey{taskID: taskID}
	key, err := tm.storage.Get(taskRunKey.String(), false)
	if err != nil {
		return TaskRunMetadata{}, fmt.Errorf("failed to fetch task: %v", err)
	}

	// Not sold on this naming for this struct. Maybe like RunHistory or something?
	runs := TaskRuns{}
	err = runs.Unmarshal([]byte(key[0]))
	if err != nil {
		return TaskRunMetadata{}, err
	}

	// This function could be a method of TaskRuns
	latestID, err := getHighestRunID(runs)
	if err != nil {
		return TaskRunMetadata{}, err
	}

	startTime := time.Now().UTC()

	metadata := TaskRunMetadata{
		ID:          TaskRunID(latestID + 1),
		TaskId:      taskID,
		Name:        name,
		Trigger:     trigger,
		TriggerType: trigger.Type(),
		Status:      Pending,
		StartTime:   startTime,
	}

	runs.Runs = append(runs.Runs, TaskRunSimple{RunID: metadata.ID, DateCreated: startTime})

	serializedRuns, err := runs.Marshal()
	if err != nil {
		return TaskRunMetadata{}, err
	}

	serializedMetadata, err := metadata.Marshal()
	if err != nil {
		return TaskRunMetadata{}, fmt.Errorf("failed to marshal metadata: %v", err)
	}

	lock, err := tm.storage.Lock(taskRunKey.String())
	if err != nil {
		return TaskRunMetadata{}, fmt.Errorf("failed to lock task run key: %v", err)
	}
	err = tm.storage.Set(taskRunKey.String(), string(serializedRuns), lock)
	if err != nil {
		return TaskRunMetadata{}, err
	}
	err = tm.storage.Unlock(taskRunKey.String(), lock)
	if err != nil {
		return TaskRunMetadata{}, fmt.Errorf("failed to unlock task run key: %v", err)
	}

	TaskRunMetadataKey := TaskRunMetadataKey{taskID: taskID, runID: metadata.ID, date: startTime}
	lock, err = tm.storage.Lock(TaskRunMetadataKey.String())
	if err != nil {
		return TaskRunMetadata{}, fmt.Errorf("failed to lock task run metadata key: %v", err)
	}
	err = tm.storage.Set(TaskRunMetadataKey.String(), string(serializedMetadata), lock)
	if err != nil {
		return TaskRunMetadata{}, err
	}
	err = tm.storage.Unlock(TaskRunMetadataKey.String(), lock)
	if err != nil {
		return TaskRunMetadata{}, fmt.Errorf("failed to unlock task run metadata key: %v", err)
	}

	return metadata, nil
}

func getHighestRunID(taskRuns TaskRuns) (TaskRunID, error) {
	if len(taskRuns.Runs) == 0 {
		return 0, nil
	}

	// Move this logic out
	highestRunID := taskRuns.Runs[0].RunID

	for _, run := range taskRuns.Runs[1:] {
		if run.RunID > highestRunID {
			highestRunID = run.RunID
		}
	}

	return highestRunID, nil
}

func (tm *TaskManager) GetRunByID(taskID TaskID, runID TaskRunID) (TaskRunMetadata, error) {
	taskRunKey := TaskRunKey{taskID: taskID}
	key, err := tm.storage.Get(taskRunKey.String(), false)
	if err != nil {
		return TaskRunMetadata{}, fmt.Errorf("failed to fetch task: %v", err)
	}

	runs := TaskRuns{}
	err = runs.Unmarshal([]byte(key[0]))
	if err != nil {
		return TaskRunMetadata{}, err
	}

	// Want to move this logic out
	found := false
	var runRecord TaskRunSimple
	for _, run := range runs.Runs {
		if run.RunID == runID {
			runRecord = run
			found = true
			break
		}
	}
	if !found {
		return TaskRunMetadata{}, fmt.Errorf("run not found")
	}

	date := runRecord.DateCreated
	taskRunMetadataKey := TaskRunMetadataKey{taskID: taskID, runID: runRecord.RunID, date: date}
	rec, err := tm.storage.Get(taskRunMetadataKey.String(), false)
	if err != nil {
		return TaskRunMetadata{}, err
	}

	taskRun := TaskRunMetadata{}
	err = taskRun.Unmarshal([]byte(rec[0]))
	if err != nil {
		return TaskRunMetadata{}, fmt.Errorf("failed to unmarshal run record: %v", err)
	}
	return taskRun, nil

}

func (tm *TaskManager) GetRunsByDate(start time.Time, end time.Time) (TaskRunList, error) {
	// the date range is inclusive
	var runs []TaskRunMetadata

	// iterate through each day in the date range including the end date
	for date := start; date.Before(end) || date.Equal(end); date = date.AddDate(0, 0, 1) {
		dayRuns, err := tm.getRunsForDay(date, start, end)
		if err != nil {
			return []TaskRunMetadata{}, err
		}
		runs = append(runs, dayRuns...)
	}

	return runs, nil
}

func (tm *TaskManager) getRunsForDay(date time.Time, start time.Time, end time.Time) ([]TaskRunMetadata, error) {
	key := TaskRunMetadataKey{date: date}
	recs, err := tm.storage.Get(key.String(), true)
	if err != nil {
		return []TaskRunMetadata{}, err
	}

	var runs []TaskRunMetadata
	for _, record := range recs {
		taskRun := TaskRunMetadata{}
		err = taskRun.Unmarshal([]byte(record))
		if err != nil {
			return []TaskRunMetadata{}, fmt.Errorf("failed to unmarshal run record: %v", err)
		}

		// if the task run started before the start time or after the end time, skip it
		if taskRun.StartTime.Before(start) || taskRun.StartTime.After(end) {
			continue
		}
		runs = append(runs, taskRun)
	}
	return runs, nil
}

func (tm *TaskManager) GetAllTaskRuns() (TaskRunList, error) {
	recs, err := tm.storage.Get(TaskRunMetadataKey{}.String(), true)
	if err != nil {
		return []TaskRunMetadata{}, err
	}

	var runs []TaskRunMetadata
	for _, record := range recs {
		taskRun := TaskRunMetadata{}
		err = taskRun.Unmarshal([]byte(record))
		if err != nil {
			return []TaskRunMetadata{}, fmt.Errorf("failed to unmarshal run record: %v", err)
		}
		runs = append(runs, taskRun)
	}
	return runs, nil
}

// Write Methods
func (t *TaskManager) SetRunStatus(runID TaskRunID, taskID TaskID, status Status, err error, lock sp.LockObject) error {
	if taskID <= 0 {
		return fmt.Errorf("invalid run id: %d", taskID)
	}
	metadata, e := t.GetRunByID(taskID, runID)
	if e != nil {
		return fmt.Errorf("failed to fetch run: %v", e)
	}
	if status == Failed && err == nil {
		return fmt.Errorf("error is required for failed status")
	}
	metadata.Status = status
	if err == nil {
		metadata.Error = ""
	} else {
		metadata.Error = err.Error()
	}

	serializedMetadata, e := metadata.Marshal()
	if e != nil {
		return fmt.Errorf("failed to marshal metadata: %v", e)
	}

	taskRunMetadataKey := TaskRunMetadataKey{taskID: taskID, runID: metadata.ID, date: metadata.StartTime}
	e = t.storage.Set(taskRunMetadataKey.String(), string(serializedMetadata), lock)
	return e
}

func (t *TaskManager) SetRunEndTime(runID TaskRunID, taskID TaskID, time time.Time, lock sp.LockObject) error {
	if taskID <= 0 {
		return fmt.Errorf("invalid run id: %d", taskID)
	}
	if time.IsZero() {
		return fmt.Errorf("invalid run end time: %v", time)
	}
	metadata, e := t.GetRunByID(taskID, runID)
	if e != nil {
		return fmt.Errorf("failed to fetch run: %v", e)
	}
	if metadata.StartTime.After(time) {
		return fmt.Errorf("end time cannot be before start time")
	}
	metadata.EndTime = time
	serializedMetadata, e := metadata.Marshal()
	if e != nil {
		return fmt.Errorf("failed to marshal metadata: %v", e)
	}

	taskRunMetadataKey := TaskRunMetadataKey{taskID: taskID, runID: metadata.ID, date: metadata.StartTime}
	e = t.storage.Set(taskRunMetadataKey.String(), string(serializedMetadata), lock)
	return e
}

func (t *TaskManager) AppendRunLog(id TaskRunID, log string) error {
	// we will need task id as well
	return fmt.Errorf("Not implemented")
}

// Locking
func (tm *TaskManager) LockTaskRun(taskID TaskID, runId TaskRunID) (sp.LockObject, error) {
	date := time.Now().UTC()
	key := TaskRunMetadataKey{taskID: taskID, runID: runId, date: date}.String()
	return tm.storage.Lock(key)
}

func (tm *TaskManager) UnlockTaskRun(taskID TaskID, runId TaskRunID, lock sp.LockObject) error {
	runDate := time.Now().UTC()
	key := TaskRunMetadataKey{taskID: taskID, runID: runId, date: runDate}.String()
	return tm.storage.Unlock(key, lock)
}
