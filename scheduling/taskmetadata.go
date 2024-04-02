package scheduling

import (
	"fmt"
	"time"

	"github.com/featureform/metadata/proto"

	"github.com/featureform/fferr"
	"github.com/featureform/ffsync"
	"github.com/featureform/helpers"
	ss "github.com/featureform/storage"
)

const (
	EmptyList int = iota
)

type TaskMetadataList []TaskMetadata

type TaskRunList []TaskRunMetadata

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
	storage     ss.MetadataStorage
	idGenerator ffsync.OrderedIdGenerator
}

func NewMemoryTaskMetadataManager() (TaskMetadataManager, error) {
	memoryLocker, err := ffsync.NewMemoryLocker()
	if err != nil {
		return TaskMetadataManager{}, err
	}

	memoryStorage, err := ss.NewMemoryStorageImplementation()
	if err != nil {
		return TaskMetadataManager{}, err
	}

	memoryMetadataStorage := ss.MetadataStorage{
		Locker:  &memoryLocker,
		Storage: &memoryStorage,
	}

	idGenerator, err := ffsync.NewMemoryOrderedIdGenerator()
	if err != nil {
		return TaskMetadataManager{}, err
	}

	return TaskMetadataManager{
		storage:     memoryMetadataStorage,
		idGenerator: idGenerator,
	}, nil
}

func NewETCDTaskMetadataManager(config helpers.ETCDConfig) (TaskMetadataManager, error) {
	etcdLocker, err := ffsync.NewETCDLocker(config)
	if err != nil {
		return TaskMetadataManager{}, err
	}

	etcdStorage, err := ss.NewETCDStorageImplementation(config)
	if err != nil {
		return TaskMetadataManager{}, err
	}

	etcdMetadataStorage := ss.MetadataStorage{
		Locker:  etcdLocker,
		Storage: etcdStorage,
	}

	idGenerator, err := ffsync.NewETCDOrderedIdGenerator(config)
	if err != nil {
		return TaskMetadataManager{}, err
	}

	return TaskMetadataManager{
		storage:     etcdMetadataStorage,
		idGenerator: idGenerator,
	}, nil
}

func NewRDSTaskMetadataManager(config helpers.RDSConfig) (TaskMetadataManager, error) {
	rdsLocker, err := ffsync.NewRDSLocker(config)
	if err != nil {
		return TaskMetadataManager{}, err
	}

	rdsStorage, err := ss.NewRDSStorageImplementation(config, "ff_task_metadata")
	if err != nil {
		return TaskMetadataManager{}, err
	}

	rdsMetadataStorage := ss.MetadataStorage{
		Locker:  rdsLocker,
		Storage: rdsStorage,
	}

	idGenerator, err := ffsync.NewRDSOrderedIdGenerator(config)
	if err != nil {
		return TaskMetadataManager{}, err
	}

	return TaskMetadataManager{
		storage:     rdsMetadataStorage,
		idGenerator: idGenerator,
	}, nil
}

func (m *TaskMetadataManager) CreateTask(name string, tType TaskType, target TaskTarget) (TaskMetadata, error) {
	id, err := m.idGenerator.NextId("task")
	if err != nil {
		return TaskMetadata{}, err
	}

	metadata := TaskMetadata{
		ID:          TaskID(id),
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
		return TaskMetadata{}, err
	}

	key := TaskMetadataKey{taskID: metadata.ID}
	err = m.storage.Create(key.String(), string(serializedMetadata))
	if err != nil {
		return TaskMetadata{}, err
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
	err = m.storage.Create(taskRunKey.String(), string(serializedRuns))
	if err != nil {
		return TaskMetadata{}, err
	}

	return metadata, nil
}

func (m *TaskMetadataManager) GetTaskByID(id TaskID) (TaskMetadata, error) {
	key := TaskMetadataKey{taskID: id}.String()
	metadata, err := m.storage.Get(key)
	if err != nil {
		return TaskMetadata{}, err
	}

	if len(metadata) == EmptyList {
		return TaskMetadata{}, fferr.NewInternalError(fmt.Errorf("task not found for id: %s", id.String()))
	}

	taskMetadata := TaskMetadata{}
	err = taskMetadata.Unmarshal([]byte(metadata))
	if err != nil {
		return TaskMetadata{}, err
	}
	return taskMetadata, nil
}

func (m *TaskMetadataManager) GetAllTasks() (TaskMetadataList, error) {
	metadata, err := m.storage.List(TaskMetadataKey{}.String())
	if err != nil {
		return TaskMetadataList{}, err
	}

	return m.convertToTaskMetadataList(metadata)
}

func (m *TaskMetadataManager) convertToTaskMetadataList(metadata map[string]string) (TaskMetadataList, error) {
	tml := TaskMetadataList{}
	for _, meta := range metadata {
		taskMetadata := TaskMetadata{}
		err := taskMetadata.Unmarshal([]byte(meta))
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
	taskMetadata, err := m.storage.Get(taskRunKey.String())
	if err != nil {
		return TaskRunMetadata{}, err
	}

	// Not sold on this naming for this struct. Maybe like RunHistory or something?
	runs := TaskRuns{}
	err = runs.Unmarshal([]byte(taskMetadata))
	if err != nil {
		return TaskRunMetadata{}, err
	}

	id, err := m.idGenerator.NextId("task_run")
	if err != nil {
		return TaskRunMetadata{}, err
	}
	startTime := time.Now().UTC()

	metadata := TaskRunMetadata{
		ID:          TaskRunID(id),
		TaskId:      taskID,
		Name:        name,
		Trigger:     trigger,
		TriggerType: trigger.Type(),
		Status:      PENDING,
		StartTime:   startTime,
	}

	runs.Runs = append(runs.Runs, TaskRunSimple{RunID: metadata.ID, DateCreated: startTime})

	serializedRuns, err := runs.Marshal()
	if err != nil {
		return TaskRunMetadata{}, err
	}

	serializedMetadata, err := metadata.Marshal()
	if err != nil {
		return TaskRunMetadata{}, err
	}

	taskRunMetaKey := TaskRunMetadataKey{taskID: taskID, runID: metadata.ID, date: startTime}

	// this is used to store the metadata for the run as well as the list of runs for the task
	taskRunMetadata := map[string]string{
		taskRunKey.String():     string(serializedRuns),
		taskRunMetaKey.String(): string(serializedMetadata),
	}

	err = m.storage.MultiCreate(taskRunMetadata)
	if err != nil {
		return TaskRunMetadata{}, err
	}

	return metadata, nil
}

func (m *TaskMetadataManager) latestTaskRun(runs TaskRuns) (TaskRunID, error) {

	var latestTime time.Time
	var latestRunIdx int
	for i, run := range runs.Runs {
		if i == 0 {
			latestTime = run.DateCreated
			latestRunIdx = i
		} else if run.DateCreated.After(latestTime) {
			latestTime = run.DateCreated
			latestRunIdx = i
		}
	}
	return runs.Runs[latestRunIdx].RunID, nil
}

// GetLatestRun is not guaranteed to be completely accurate. This function should only
// be used for visual purposes on the Dashboard and CLI rather than internal business logic
func (m *TaskMetadataManager) GetLatestRun(taskID TaskID) (TaskRunMetadata, error) {
	runs, err := m.getTaskRunRecords(taskID)
	if err != nil {
		return TaskRunMetadata{}, err
	}

	if len(runs.Runs) == 0 {
		return TaskRunMetadata{}, fferr.NewNoRunsForTaskError(taskID.String())
	}

	latest, err := m.latestTaskRun(runs)
	if err != nil {
		return TaskRunMetadata{}, err
	}

	run, err := m.GetRunByID(taskID, latest)
	if err != nil {
		return TaskRunMetadata{}, err
	}
	return run, nil
}

func (m *TaskMetadataManager) GetTaskRunMetadata(taskID TaskID) (TaskRunList, error) {
	runs, err := m.getTaskRunRecords(taskID)
	if err != nil {
		return TaskRunList{}, err
	}
	runMetadata := TaskRunList{}
	for _, run := range runs.Runs {
		meta, err := m.GetRunByID(taskID, run.RunID)
		if err != nil {
			return TaskRunList{}, err
		}
		runMetadata = append(runMetadata, meta)
	}
	return runMetadata, nil
}

func (m *TaskMetadataManager) getTaskRunRecords(taskID TaskID) (TaskRuns, error) {
	taskRunKey := TaskRunKey{taskID: taskID}
	taskRunMetadata, err := m.storage.Get(taskRunKey.String())
	if err != nil {
		return TaskRuns{}, err
	}

	runs := TaskRuns{}
	err = runs.Unmarshal([]byte(taskRunMetadata))
	if err != nil {
		return TaskRuns{}, err
	}

	return runs, nil
}

func (m *TaskMetadataManager) GetRunByID(taskID TaskID, runID TaskRunID) (TaskRunMetadata, error) {
	taskRunKey := TaskRunKey{taskID: taskID}
	taskRunMetadata, err := m.storage.Get(taskRunKey.String())
	if err != nil {
		return TaskRunMetadata{}, err
	}

	runs := TaskRuns{}
	err = runs.Unmarshal([]byte(taskRunMetadata))
	if err != nil {
		return TaskRunMetadata{}, err
	}

	// Want to move this logic out
	found, runRecord := runs.ContainsRun(runID)
	if !found {
		err := fferr.NewKeyNotFoundError(taskRunKey.String(), fmt.Errorf("run not found"))
		return TaskRunMetadata{}, err
	}

	date := runRecord.DateCreated
	taskRunMetadataKey := TaskRunMetadataKey{taskID: taskID, runID: runRecord.RunID, date: date}
	rec, err := m.storage.Get(taskRunMetadataKey.String())
	if err != nil {
		return TaskRunMetadata{}, err
	}

	taskRun := TaskRunMetadata{}
	err = taskRun.Unmarshal([]byte(rec))
	if err != nil {
		return TaskRunMetadata{}, err
	}
	return taskRun, nil
}

func (m *TaskMetadataManager) GetRunsByDate(start time.Time, end time.Time) (TaskRunList, error) {
	/*
		Given a date range, return all runs that started within that range
		Currently, we are iterating through each day in the range and getting the runs for that day
		But in the feature, we can iterate by hour and minute as well. We just need to modify the for loop
		below to iterate by hour and minute and modify the getRunsForDay function to get runs for that hour or minute.
	*/

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
	key := TaskRunMetadataKey{date: date}
	recs, err := m.storage.List(key.TruncateToDay())
	if err != nil {
		return []TaskRunMetadata{}, err
	}

	var runs []TaskRunMetadata
	for _, record := range recs {
		taskRun := TaskRunMetadata{}
		err = taskRun.Unmarshal([]byte(record))
		if err != nil {
			return []TaskRunMetadata{}, err
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
	recs, err := m.storage.List(TaskRunMetadataKey{}.String())
	if err != nil {
		return []TaskRunMetadata{}, err
	}

	var runs []TaskRunMetadata
	for _, record := range recs {
		taskRun := TaskRunMetadata{}
		err = taskRun.Unmarshal([]byte(record))
		if err != nil {
			return []TaskRunMetadata{}, err
		}
		runs = append(runs, taskRun)
	}
	return runs, nil
}

func (m *TaskMetadataManager) SetRunStatus(runID TaskRunID, taskID TaskID, status *proto.ResourceStatus) error {
	metadata, e := m.GetRunByID(taskID, runID)
	if e != nil {
		return e
	}

	updateStatus := func(runMetadata string) (string, error) {
		metadata := TaskRunMetadata{}
		unmarshalErr := metadata.Unmarshal([]byte(runMetadata))
		if unmarshalErr != nil {
			e := fferr.NewInternalError(unmarshalErr)
			return "", e
		}
		if Status(status.Status) == FAILED && status.ErrorStatus == nil {
			e := fferr.NewInvalidArgumentError(fmt.Errorf("error is required for failed status"))
			return "", e
		}
		metadata.Status = Status(status.Status)
		if status.ErrorStatus == nil {
			metadata.Error = ""
		} else {
			metadata.Error = fferr.ToDashboardError(status)
		}

		serializedMetadata, marshalErr := metadata.Marshal()
		if marshalErr != nil {
			return "", marshalErr
		}

		return string(serializedMetadata), nil
	}

	taskRunMetadataKey := TaskRunMetadataKey{taskID: taskID, runID: metadata.ID, date: metadata.StartTime}
	e = m.storage.Update(taskRunMetadataKey.String(), updateStatus)
	return e
}

func (m *TaskMetadataManager) SetRunEndTime(runID TaskRunID, taskID TaskID, time time.Time) error {
	if time.IsZero() {
		errMessage := fmt.Errorf("end time cannot be zero")
		err := fferr.NewInvalidArgumentError(errMessage)
		return err
	}

	metadata, err := m.GetRunByID(taskID, runID)
	if err != nil {
		return err
	}

	updateEndTime := func(runMetadata string) (string, error) {
		metadata := TaskRunMetadata{}
		err := metadata.Unmarshal([]byte(runMetadata))
		if err != nil {
			return "", err
		}

		if metadata.StartTime.After(time) {
			err := fferr.NewInvalidArgumentError(fmt.Errorf("end time cannot be before start time"))
			return "", err
		}

		metadata.EndTime = time
		serializedMetadata, err := metadata.Marshal()
		if err != nil {
			return "", err
		}

		return string(serializedMetadata), nil
	}

	taskRunMetadataKey := TaskRunMetadataKey{taskID: taskID, runID: metadata.ID, date: metadata.StartTime}
	err = m.storage.Update(taskRunMetadataKey.String(), updateEndTime)
	return err
}

func (m *TaskMetadataManager) AppendRunLog(runID TaskRunID, taskID TaskID, log string) error {
	if log == "" {
		err := fferr.NewInvalidArgumentError(fmt.Errorf("log cannot be empty"))
		return err
	}

	metadata, err := m.GetRunByID(taskID, runID)
	if err != nil {
		return err
	}

	updateLog := func(runMetadata string) (string, error) {
		metadata := TaskRunMetadata{}
		err := metadata.Unmarshal([]byte(runMetadata))
		if err != nil {
			return "", err
		}

		metadata.Logs = append(metadata.Logs, log)

		serializedMetadata, err := metadata.Marshal()
		if err != nil {
			return "", err
		}

		return string(serializedMetadata), nil
	}

	taskRunMetadataKey := TaskRunMetadataKey{taskID: taskID, runID: metadata.ID, date: metadata.StartTime}
	err = m.storage.Update(taskRunMetadataKey.String(), updateLog)
	return err
}
