package scheduling

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/featureform/locker"
	ss "github.com/featureform/storage_storer"
)

// type TaskMetadataManager interface {
// 	// Task Methods
// 	CreateTask(name string, tType TaskType, target TaskTarget) (TaskMetadata, error)
// 	GetTask(id TaskID) (TaskMetadata, error)
// 	UpdateTask(id TaskID, metadata TaskMetadata) error

// 	// Task Run Methods
// 	CreateTaskRun(run TaskRunMetadata) error
// 	GetTaskRun(taskId TaskID, runId TaskRunID) (TaskRunMetadata, error)
// 	UpdateTaskRun(taskId TaskID, runId TaskRunID, metadata TaskRunMetadata) error
// 	GetTaskRunLog(taskId TaskID, runId TaskRunID) (string, error)
// 	GetTaskRunError(taskId TaskID, runId TaskRunID) (error, error)

// 	GetAllRunsForTask(taskId TaskID) ([]TaskRunMetadata, error)
// }

type TaskMetadataList []TaskMetadata

func (tml *TaskMetadataList) ToJSON() string {
	return ""
}

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

type TaskMetadataManager struct {
	// storage ss.MetadataStorer
	storer ss.MetadataStorer
}

func NewMemoryTaskMetadataManager() TaskMetadataManager {
	memoryLocker := locker.MemoryLocker{
		LockedItems: sync.Map{},
		Mutex:       &sync.Mutex{},
	}

	memoryStorer := ss.MemoryStorerImplementation{
		Storage: make(map[string]string),
	}

	memoryMetadataStorer := ss.MetadataStorer{
		Locker: &memoryLocker,
		Storer: &memoryStorer,
	}
	return TaskMetadataManager{
		storer: memoryMetadataStorer,
	}
}

func (m *TaskMetadataManager) CreateTask(name string, tType TaskType, target TaskTarget) (TaskMetadata, error) {
	keys, err := m.storer.List(TaskMetadataKey{}.String())
	if err != nil {
		return TaskMetadata{}, fmt.Errorf("failed to fetch keys: %v", err)
	}

	// This logic could probably be somewhere else
	var latestID int
	if len(keys) == 0 {
		latestID = 0
	} else {
		latestID, err = getLatestId(keys)
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
	err = m.storer.Create(key.String(), string(serializedMetadata))
	if err != nil {
		return TaskMetadata{}, fmt.Errorf("failed to create task metadata: %v", err)
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
	err = m.storer.Create(taskRunKey.String(), string(serializedRuns))
	if err != nil {
		return TaskMetadata{}, fmt.Errorf("failed to create task run metadata: %v", err)
	}

	return metadata, nil
}

func (m *TaskMetadataManager) GetTaskByID(id TaskID) (TaskMetadata, error) {
	key := TaskMetadataKey{taskID: id}.String()
	metadata, err := m.storer.Get(key)
	if err != nil {
		return TaskMetadata{}, err
	}

	// Should enum 0 as EmptyList or something
	if len(metadata) == 0 {
		return TaskMetadata{}, fmt.Errorf("task not found for id: %s", string(id))
	}

	taskMetadata := TaskMetadata{}
	err = taskMetadata.Unmarshal([]byte(metadata))
	if err != nil {
		return TaskMetadata{}, err
	}
	return taskMetadata, nil
}

func (m *TaskMetadataManager) GetAllTasks() (TaskMetadataList, error) {
	metadata, err := m.storer.List(TaskMetadataKey{}.String())
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

func (m *TaskMetadataManager) CreateTaskRun(name string, taskID TaskID, trigger Trigger) (TaskRunMetadata, error) {
	// ids will be generated by TM
	taskRunKey := TaskRunKey{taskID: taskID}
	taskMetadata, err := m.storer.Get(taskRunKey.String())
	if err != nil {
		return TaskRunMetadata{}, fmt.Errorf("failed to fetch task: %v", err)
	}

	// Not sold on this naming for this struct. Maybe like RunHistory or something?
	runs := TaskRuns{}
	err = runs.Unmarshal([]byte(taskMetadata))
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
	err = m.storer.Create(taskRunKey.String(), string(serializedRuns))

	taskRunMetaKey := TaskRunMetadataKey{taskID: taskID, runID: metadata.ID, date: startTime}
	err = m.storer.Create(taskRunMetaKey.String(), string(serializedMetadata))

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

func (m *TaskMetadataManager) GetRunByID(taskID TaskID, runID TaskRunID) (TaskRunMetadata, error) {
	taskRunKey := TaskRunKey{taskID: taskID}
	taskRunMetadata, err := m.storer.Get(taskRunKey.String())
	if err != nil {
		return TaskRunMetadata{}, fmt.Errorf("failed to fetch task: %v", err)
	}

	runs := TaskRuns{}
	err = runs.Unmarshal([]byte(taskRunMetadata))
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
	rec, err := m.storer.Get(taskRunMetadataKey.String())
	if err != nil {
		return TaskRunMetadata{}, err
	}

	taskRun := TaskRunMetadata{}
	err = taskRun.Unmarshal([]byte(rec]))
	if err != nil {
		return TaskRunMetadata{}, fmt.Errorf("failed to unmarshal run record: %v", err)
	}
	return taskRun, nil
}

func (m *TaskMetadataManager) GetRunsByDate(start time.Time, end time.Time) (TaskRunList, error) {
	// the date range is inclusive
	var runs []TaskRunMetadata

	// iterate through each day in the date range including the end date
	for date := start; date.Before(end) || date.Equal(end); date = date.AddDate(0, 0, 1) {
		dayRuns, err := m.getRunsForDay(date, start, end)
		if err != nil {
			return []TaskRunMetadata{}, err
		}
		runs = append(runs, dayRuns...)
	}

	return runs, nil
}

func (m *TaskMetadataManager) getRunsForDay(date time.Time, start time.Time, end time.Time) ([]TaskRunMetadata, error) {
	// TODO: question, should this return an error if we can't marshal
	// a task run record? Or should it just skip that record and keep 
	// track of the error?
	key := TaskRunMetadataKey{date: date}
	recs, err := m.storer.List(key.String())
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

func (m *TaskMetadataManager) GetAllTaskRuns() (TaskRunList, error) {
	recs, err := m.storer.List(TaskRunMetadataKey{}.String())
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

func (m *TaskMetadataManager) SetRunStatus(runID TaskRunID, taskID TaskID, status Status, err error) error {
	if taskID <= 0 {
		return fmt.Errorf("invalid run id: %d", taskID)
	}

	updateStatus := func (runMetadata string) (string, error) {
		metadata := TaskRunMetadata{}
		err := metadata.Unmarshal([]byte(runMetadata))
		if err != nil {
			return "", fmt.Errorf("failed to unmarshal metadata: %v", err)
		}
		if status == Failed && err == nil {
			return "", fmt.Errorf("error is required for failed status")
		}
		metadata.Status = status
		if err == nil {
			metadata.Error = ""
		} else {
			metadata.Error = err.Error()
		}

		serializedMetadata, e := metadata.Marshal()
		if e != nil {
			return "", fmt.Errorf("failed to marshal metadata: %v", e)
		}

		return string(serializedMetadata), nil
	}

	taskRunMetadataKey := TaskRunMetadataKey{taskID: taskID, runID: metadata.ID, date: metadata.StartTime}
	e = m.storer.Update(taskRunMetadataKey.String(), updateStatus)
	return e
}

func (m *TaskMetadataManager) SetRunEndTime(runID TaskRunID, taskID TaskID, time time.Time) error {
	if taskID <= 0 {
		return fmt.Errorf("invalid run id: %d", taskID)
	}
	if time.IsZero() {
		return fmt.Errorf("invalid run end time: %v", time)
	}

	updateEndTime := func (runMetadata string) (string, error) {
		metadata := TaskRunMetadata{}
		err := metadata.Unmarshal([]byte(runMetadata))
		if err != nil {
			return "", fmt.Errorf("failed to unmarshal metadata: %v", err)
		}

		if metadata.StartTime.After(time) {
			return "", fmt.Errorf("end time cannot be before start time")
		}

		metadata.EndTime = time
		serializedMetadata, e := metadata.Marshal()
		if e != nil {
			return "", fmt.Errorf("failed to marshal metadata: %v", e)
		}

		return string(serializedMetadata), nil
	}

	taskRunMetadataKey := TaskRunMetadataKey{taskID: taskID, runID: metadata.ID, date: metadata.StartTime}
	e = m.storer.Update(taskRunMetadataKey.String(), updateEndTime)
	return e
}

func (m *TaskMetadataManager) AppendRunLog(runID TaskRunID, taskID TaskID, log string) error {
	if runID <= 0 {
		return fmt.Errorf("invalid run id: %d", taskID)
	}
	if taskID <= 0 {
		return fmt.Errorf("invalid task id: %d", taskID)
	}
	if log == "" {
		return fmt.Errorf("log cannot be empty")
	}

	updateLog := func (runMetadata string) (string, error) {
		metadata := TaskRunMetadata{}
		err := metadata.Unmarshal([]byte(runMetadata))
		if err != nil {
			return "", fmt.Errorf("failed to unmarshal metadata: %v", err)
		}

		metadata.Logs = append(metadata.Logs, log)

		serializedMetadata, e := metadata.Marshal()
		if e != nil {
			return "", fmt.Errorf("failed to marshal metadata: %v", e)
		}

		return string(serializedMetadata), nil
	}

	taskRunMetadataKey := TaskRunMetadataKey{taskID: taskID, runID: metadata.ID, date: metadata.StartTime}
	e = m.storer.Update(taskRunMetadataKey.String(), updateLog)
	return e
}

// Finds the highest increment in a list of strings formatted like "/tasks/metadata/task_id=0"
func getLatestId(taskMetadata map[string]string) (int, error) {
	highestIncrement := -1
	for path, _ := range taskMetadata {
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
