package scheduling

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/featureform/ffsync"
	"github.com/featureform/metadata/proto"
)

type MockOrderedID struct {
	Id uint64
}

func (id *MockOrderedID) Equals(other ffsync.OrderedId) bool {
	otherID, ok := other.(*MockOrderedID)
	if !ok {
		return false
	}
	return id.Id == otherID.Id
}

func (id *MockOrderedID) Less(other ffsync.OrderedId) bool {
	otherID, ok := other.(*MockOrderedID)
	if !ok {
		return false
	}
	return id.Id < otherID.Id
}

func (id *MockOrderedID) String() string {
	return fmt.Sprint(id.Id)
}

func (id *MockOrderedID) Value() interface{} {
	return id.Id
}

func (id *MockOrderedID) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprint(id.Id)), nil
}

func (id *MockOrderedID) UnmarshalJSON(data []byte) error {
	var tmp uint64
	if err := json.Unmarshal(data, &tmp); err != nil {
		return fmt.Errorf("failed to unmarshal MockOrderedID: %v", err)
	}

	id.Id = tmp
	return nil
}

func (id *MockOrderedID) FromString(idStr string) error {
	tmp, err := fmt.Sscan(idStr, &id.Id)
	if err != nil {
		return fmt.Errorf("failed to convert string to uint64: %v", err)
	}
	id.Id = uint64(tmp)
	return nil
}

func TestTaskMetadataManager(t *testing.T) {
	testFns := map[string]func(*testing.T, TaskMetadataManager){
		"CreateTask": testCreateTask,
	}

	memoryTaskMetadataManager, err := NewMemoryTaskMetadataManager()
	if err != nil {
		t.Fatalf("failed to create memory task metadata manager: %v", err)
	}

	for name, fn := range testFns {
		t.Run(name, func(t *testing.T) {
			fn(t, memoryTaskMetadataManager)
		})
	}
}

func testCreateTask(t *testing.T, manager TaskMetadataManager) {
	type taskInfo struct {
		Name       string
		Type       TaskType
		Target     TaskTarget
		ExpectedID TaskID
	}
	tests := []struct {
		Name        string
		Tasks       []taskInfo
		shouldError bool
	}{
		{
			"Single",
			[]taskInfo{
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}, TaskID(&MockOrderedID{Id: 1})},
			},
			false,
		},
		{
			"Multiple",
			[]taskInfo{
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}, TaskID(&MockOrderedID{Id: 1})},
				{"name2", ResourceCreation, NameVariant{"name", "variant", "type"}, TaskID(&MockOrderedID{Id: 2})},
				{"name3", ResourceCreation, NameVariant{"name", "variant", "type"}, TaskID(&MockOrderedID{Id: 3})},
			},
			false,
		},
	}

	fn := func(t *testing.T, tasks []taskInfo, shouldError bool) {
		manager, err := NewMemoryTaskMetadataManager() // TODO: will need to modify this to use any store and deletes tasks after job was done
		if err != nil {
			t.Fatalf("failed to create memory task metadata manager: %v", err)
		}

		for _, task := range tasks {
			taskDef, err := manager.CreateTask(task.Name, task.Type, task.Target)
			if err != nil && shouldError {
				continue
			} else if err != nil && !shouldError {
				t.Fatalf("failed to create task: %v", err)
			} else if err == nil && shouldError {
				t.Fatalf("expected error but did not receive one")
			}
			if task.ExpectedID.Equals(taskDef.ID) {
				t.Fatalf("Expected id: %d, got: %d", task.ExpectedID, taskDef.ID)
			}
		}
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			fn(t, tt.Tasks, tt.shouldError)
		})
	}
}

func TestTaskGetByID(t *testing.T) {
	type taskInfo struct {
		Name       string
		Type       TaskType
		Target     TaskTarget
		ExpectedID TaskID
	}
	type TestCase struct {
		Name        string
		Tasks       []taskInfo
		ID          TaskID
		shouldError bool
	}
	tests := []TestCase{
		{
			"Empty",
			[]taskInfo{},
			TaskID(&MockOrderedID{Id: 1}),
			true,
		},
		{
			"Single",
			[]taskInfo{
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}, TaskID(&MockOrderedID{Id: 1})},
			},
			TaskID(&MockOrderedID{Id: 1}),
			false,
		},
		{
			"Multiple",
			[]taskInfo{
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}, TaskID(&MockOrderedID{Id: 1})},
				{"name2", ResourceCreation, NameVariant{"name", "variant", "type"}, TaskID(&MockOrderedID{Id: 2})},
				{"name3", ResourceCreation, NameVariant{"name", "variant", "type"}, TaskID(&MockOrderedID{Id: 3})},
			},
			TaskID(&MockOrderedID{Id: 2}),
			false,
		},
		{
			"MultipleInsertInvalidLookup",
			[]taskInfo{
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}, &MockOrderedID{Id: 1}},
				{"name2", ResourceCreation, NameVariant{"name", "variant", "type"}, &MockOrderedID{Id: 2}},
				{"name3", ResourceCreation, NameVariant{"name", "variant", "type"}, &MockOrderedID{Id: 3}},
			},
			TaskID(&MockOrderedID{Id: 4}),
			true,
		},
	}

	fn := func(t *testing.T, test TestCase) {
		manager, err := NewMemoryTaskMetadataManager()
		if err != nil {
			t.Fatalf("failed to create memory task metadata manager: %v", err)
		}

		for _, task := range test.Tasks {
			_, err := manager.CreateTask(task.Name, task.Type, task.Target)
			if err != nil {
				t.Fatalf("failed to create task: %v", err)
			}
		}
		recievedDef, err := manager.GetTaskByID(test.ID)
		if err != nil && !test.shouldError {
			t.Fatalf("failed to fetch definiton: %v", err)
		} else if err == nil && test.shouldError {
			t.Fatalf("expected error and did not get one")
		} else if err != nil && test.shouldError {
			return
		}

		idx := test.ID.(*MockOrderedID).Id - 1 // Ids are 1 indexed
		if reflect.DeepEqual(recievedDef, test.Tasks[idx]) {
			t.Fatalf("Expected: %v got: %v", test.Tasks[idx], recievedDef)
		}

	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			fn(t, tt)
		})
	}
}

func TestTaskGetAll(t *testing.T) {
	id1 := ffsync.Uint64OrderedId(1)
	id2 := ffsync.Uint64OrderedId(2)
	id3 := ffsync.Uint64OrderedId(3)

	type taskInfo struct {
		Name       string
		Type       TaskType
		Target     TaskTarget
		ExpectedID TaskID
	}
	type TestCase struct {
		Name        string
		Tasks       []taskInfo
		ID          TaskID
		shouldError bool
	}
	tests := []TestCase{
		{
			"Empty",
			[]taskInfo{},
			TaskID(&id1),
			false,
		},
		{
			"Single",
			[]taskInfo{
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}, TaskID(&id2)},
			},
			TaskID(&id1),
			false,
		},
		{
			"Multiple",
			[]taskInfo{
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}, TaskID(&id1)},
				{"name2", ResourceCreation, NameVariant{"name", "variant", "type"}, TaskID(&id2)},
				{"name3", ResourceCreation, NameVariant{"name", "variant", "type"}, TaskID(&id3)},
			},
			TaskID(&id2),
			false,
		},
	}

	fn := func(t *testing.T, test TestCase) {
		manager, err := NewMemoryTaskMetadataManager()
		if err != nil {
			t.Fatalf("failed to create memory task metadata manager: %v", err)
		}

		var definitions []TaskMetadata
		for _, task := range test.Tasks {
			taskDef, err := manager.CreateTask(task.Name, task.Type, task.Target)
			if err != nil {
				t.Fatalf("failed to create task: %v", err)
			}
			definitions = append(definitions, taskDef)
		}
		recvTasks, err := manager.GetAllTasks()
		if err != nil && !test.shouldError {
			t.Fatalf("failed to fetch definiton: %v", err)
		} else if err == nil && test.shouldError {
			t.Fatalf("expected error and did not get one")
		} else if err != nil && test.shouldError {
			return
		}

		if len(recvTasks) != len(test.Tasks) {
			t.Fatalf("Expected %d tasks, got %d tasks", len(test.Tasks), len(recvTasks))
		}

		for i, def := range definitions {
			foundDef := false
			for _, recvTask := range recvTasks {
				if reflect.DeepEqual(def, recvTask) {
					foundDef = true
					break
				}
			}
			if !foundDef {
				t.Fatalf("Expected %v, got: %v", def, recvTasks[i])
			}
		}

	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			fn(t, tt)
		})
	}
}

func TestCreateTaskRun(t *testing.T) {
	id1 := ffsync.Uint64OrderedId(1)
	id2 := ffsync.Uint64OrderedId(2)
	id3 := ffsync.Uint64OrderedId(3)
	id4 := ffsync.Uint64OrderedId(4)

	type taskInfo struct {
		Name   string
		Type   TaskType
		Target TaskTarget
	}
	type runInfo struct {
		Name       string
		TaskID     TaskID
		Trigger    Trigger
		ExpectedID TaskRunID
	}
	type TestCase struct {
		Name        string
		Tasks       []taskInfo
		Runs        []runInfo
		shouldError bool
	}

	tests := []TestCase{
		{
			"Single",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{{"name", TaskID(&id1), OnApplyTrigger{"name"}, TaskRunID(&id1)}},
			false,
		},
		{
			"Multiple",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{
				{"name", TaskID(&id1), OnApplyTrigger{"name"}, TaskRunID(&id1)},
				{"name", TaskID(&id1), OnApplyTrigger{"name"}, TaskRunID(&id2)},
				{"name", TaskID(&id1), OnApplyTrigger{"name"}, TaskRunID(&id3)},
			},
			false,
		},
		{
			"InvalidTask",
			[]taskInfo{},
			[]runInfo{{"name", TaskID(&id1), OnApplyTrigger{"name"}, TaskRunID(&id1)}},
			true,
		},
		{
			"MultipleTasks",
			[]taskInfo{
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}},
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}},
			},
			[]runInfo{
				{"name", TaskID(&id1), OnApplyTrigger{"name"}, TaskRunID(&id1)},
				{"name", TaskID(&id1), OnApplyTrigger{"name"}, TaskRunID(&id2)},
				{"name", TaskID(&id2), OnApplyTrigger{"name"}, TaskRunID(&id3)},
				{"name", TaskID(&id2), OnApplyTrigger{"name"}, TaskRunID(&id4)},
			},
			false,
		},
	}

	fn := func(t *testing.T, test TestCase) {
		manager, err := NewMemoryTaskMetadataManager()
		if err != nil {
			t.Fatalf("failed to create memory task metadata manager: %v", err)
		}

		for _, task := range test.Tasks {
			_, err := manager.CreateTask(task.Name, task.Type, task.Target)
			if err != nil && !test.shouldError {
				t.Fatalf("failed to create task: %v", err)
			}
		}
		for _, run := range test.Runs {
			recvRun, err := manager.CreateTaskRun(run.Name, run.TaskID, run.Trigger)
			if err != nil && test.shouldError {
				continue
			} else if err != nil && !test.shouldError {
				t.Fatalf("failed to create task: %v", err)
			} else if err == nil && test.shouldError {
				t.Fatalf("expected error but did not receive one")
			}
			if run.ExpectedID.Value() != recvRun.ID.Value() {
				t.Fatalf("Expected id: %d, got: %d", run.ExpectedID.Value(), recvRun.ID.Value())
			}
		}
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			fn(t, tt)
		})
	}
}

func TestGetRunByID(t *testing.T) {
	id1 := ffsync.Uint64OrderedId(1)
	id2 := ffsync.Uint64OrderedId(2)
	id3 := ffsync.Uint64OrderedId(3)

	type taskInfo struct {
		Name   string
		Type   TaskType
		Target TaskTarget
	}
	type runInfo struct {
		Name    string
		TaskID  TaskID
		Trigger Trigger
	}
	type TestCase struct {
		Name        string
		Tasks       []taskInfo
		Runs        []runInfo
		FetchTask   TaskID
		FetchRun    TaskRunID
		shouldError bool
	}
	tests := []TestCase{
		{
			"Single",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{{"name", TaskID(&id1), OnApplyTrigger{"name"}}},
			TaskID(&id1),
			TaskRunID(&id1),
			false,
		},
		{
			"Multiple",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{
				{"name", TaskID(&id1), OnApplyTrigger{"name"}},
				{"name", TaskID(&id1), OnApplyTrigger{"name"}},
				{"name", TaskID(&id1), OnApplyTrigger{"name"}},
			},
			TaskID(&id1),
			TaskRunID(&id2),
			false,
		},
		{
			"MultipleTasks",
			[]taskInfo{
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}},
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}},
			},
			[]runInfo{
				{"name", TaskID(&id1), OnApplyTrigger{"name"}},
				{"name", TaskID(&id1), OnApplyTrigger{"name"}},
				{"name", TaskID(&id2), OnApplyTrigger{"name"}},
				{"name", TaskID(&id2), OnApplyTrigger{"name"}},
			},
			TaskID(&id2),
			TaskRunID(&id3),
			false,
		},
		{
			"Fetch NonExistent",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{{"name", TaskID(&MockOrderedID{Id: 1}), OnApplyTrigger{"name"}}},
			TaskID(&MockOrderedID{Id: 1}),
			TaskRunID(&MockOrderedID{Id: 2}),
			true,
		},
	}

	fn := func(t *testing.T, test TestCase) {
		manager, err := NewMemoryTaskMetadataManager()
		if err != nil {
			t.Fatalf("failed to create memory task metadata manager: %v", err)
		}

		for _, task := range test.Tasks {
			_, err := manager.CreateTask(task.Name, task.Type, task.Target)
			if err != nil && !test.shouldError {
				t.Fatalf("failed to create task: %v", err)
			}
		}

		var runDefs []TaskRunMetadata
		for _, run := range test.Runs {
			runDef, err := manager.CreateTaskRun(run.Name, run.TaskID, run.Trigger)
			if err != nil && !test.shouldError {
				t.Fatalf("failed to create task run: %v", err)
			}
			runDefs = append(runDefs, runDef)
		}
		recvRun, err := manager.GetRunByID(test.FetchTask, test.FetchRun)
		if err != nil && test.shouldError {
			return
		} else if err != nil && !test.shouldError {
			t.Fatalf("failed to get task by ID: %v", err)
		} else if err == nil && test.shouldError {
			t.Fatalf("expected error but did not receive one")
		}
		for _, runDef := range runDefs {
			if runDef.TaskId == test.FetchTask && runDef.ID == test.FetchRun {
				if !reflect.DeepEqual(runDef, recvRun) {
					t.Fatalf("Expcted %v, got: %v", runDef, recvRun)
				}
			}
		}
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			fn(t, tt)
		})
	}
}

func TestGetRunAll(t *testing.T) {
	id1 := ffsync.Uint64OrderedId(1)
	id2 := ffsync.Uint64OrderedId(2)

	type taskInfo struct {
		Name   string
		Type   TaskType
		Target TaskTarget
	}
	type runInfo struct {
		Name    string
		TaskID  TaskID
		Trigger Trigger
	}
	type TestCase struct {
		Name        string
		Tasks       []taskInfo
		Runs        []runInfo
		shouldError bool
	}
	tests := []TestCase{
		{
			"Single",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{{"name", TaskID(&id1), OnApplyTrigger{"name"}}},
			false,
		},
		{
			"Multiple",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{
				{"name", TaskID(&id1), OnApplyTrigger{"name"}},
				{"name", TaskID(&id1), OnApplyTrigger{"name"}},
				{"name", TaskID(&id1), OnApplyTrigger{"name"}},
			},
			false,
		},
		{
			"MultipleTasks",
			[]taskInfo{
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}},
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}},
			},
			[]runInfo{
				{"name", TaskID(&id1), OnApplyTrigger{"name"}},
				{"name", TaskID(&id1), OnApplyTrigger{"name"}},
				{"name", TaskID(&id2), OnApplyTrigger{"name"}},
				{"name", TaskID(&id2), OnApplyTrigger{"name"}},
			},
			false,
		},
		{
			"Empty",
			[]taskInfo{},
			[]runInfo{},
			false,
		},
	}

	fn := func(t *testing.T, test TestCase) {
		manager, err := NewMemoryTaskMetadataManager()
		if err != nil {
			t.Fatalf("failed to create memory task metadata manager: %v", err)
		}

		for _, task := range test.Tasks {
			_, err := manager.CreateTask(task.Name, task.Type, task.Target)
			if err != nil && !test.shouldError {
				t.Fatalf("failed to create task: %v", err)
			}
		}

		var runDefs TaskRunList
		for _, run := range test.Runs {
			runDef, err := manager.CreateTaskRun(run.Name, run.TaskID, run.Trigger)
			if err != nil && !test.shouldError {
				t.Fatalf("failed to create task run: %v", err)
			}
			runDefs = append(runDefs, runDef)
		}
		recvRuns, err := manager.GetAllTaskRuns()
		if err != nil && test.shouldError {
			return
		} else if err != nil && !test.shouldError {
			t.Fatalf("failed to get task by ID: %v", err)
		} else if err == nil && test.shouldError {
			t.Fatalf("expected error but did not receive one")
		}

		for _, runDef := range runDefs {
			foundDef := false
			for _, recvRun := range recvRuns {
				if reflect.DeepEqual(runDef, recvRun) {
					foundDef = true
					break
				}
			}
			if !foundDef {
				t.Fatalf("Expected %v, got: %v", runDef, recvRuns)
			}
		}
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			fn(t, tt)
		})
	}
}

func TestSetStatusByRunID(t *testing.T) {
	type taskInfo struct {
		Name   string
		Type   TaskType
		Target TaskTarget
	}
	type runInfo struct {
		Name    string
		TaskID  TaskID
		Trigger Trigger
	}
	type TestCase struct {
		Name        string
		Tasks       []taskInfo
		Runs        []runInfo
		ForTask     TaskID
		ForRun      TaskRunID
		SetStatus   *proto.ResourceStatus
		shouldError bool
	}
	tests := []TestCase{
		{
			"Single",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{{"name", TaskID(&MockOrderedID{Id: 1}), OnApplyTrigger{"name"}}},
			TaskID(&MockOrderedID{Id: 1}),
			TaskRunID(&MockOrderedID{Id: 1}),
			&proto.ResourceStatus{Status: proto.ResourceStatus_PENDING},
			false,
		},
		{
			"Multiple",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{
				{"name", TaskID(&MockOrderedID{Id: 1}), OnApplyTrigger{"name"}},
				{"name", TaskID(&MockOrderedID{Id: 1}), OnApplyTrigger{"name"}},
				{"name", TaskID(&MockOrderedID{Id: 1}), OnApplyTrigger{"name"}},
			},
			TaskID(&MockOrderedID{Id: 1}),
			TaskRunID(&MockOrderedID{Id: 2}),
			&proto.ResourceStatus{Status: proto.ResourceStatus_PENDING},
			false,
		},
		{
			"WrongID",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{
				{"name", TaskID(&MockOrderedID{Id: 1}), OnApplyTrigger{"name"}},
			},
			TaskID(&MockOrderedID{Id: 1}),
			TaskRunID(&MockOrderedID{Id: 2}),
			&proto.ResourceStatus{Status: proto.ResourceStatus_PENDING},
			true,
		},
		{
			"WrongRunID",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{
				{"name", TaskID(&MockOrderedID{Id: 1}), OnApplyTrigger{"name"}},
			},
			TaskID(&MockOrderedID{Id: 1}),
			TaskRunID(&MockOrderedID{Id: 2}),
			&proto.ResourceStatus{Status: proto.ResourceStatus_PENDING},
			true,
		},
		{
			"MultipleTasks",
			[]taskInfo{
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}},
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}},
			},
			[]runInfo{
				{"name", TaskID(&MockOrderedID{Id: 1}), OnApplyTrigger{"name"}},
				{"name", TaskID(&MockOrderedID{Id: 1}), OnApplyTrigger{"name"}},
				{"name", TaskID(&MockOrderedID{Id: 2}), OnApplyTrigger{"name"}},
				{"name", TaskID(&MockOrderedID{Id: 2}), OnApplyTrigger{"name"}},
			},
			TaskID(&MockOrderedID{Id: 2}),
			TaskRunID(&MockOrderedID{Id: 3}),
			&proto.ResourceStatus{Status: proto.ResourceStatus_PENDING},
			false,
		},
		{
			"FailedStatusWithoutError",
			[]taskInfo{
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}},
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}},
			},
			[]runInfo{
				{"name", TaskID(&MockOrderedID{Id: 1}), OnApplyTrigger{"name"}},
				{"name", TaskID(&MockOrderedID{Id: 2}), OnApplyTrigger{"name"}},
			},
			TaskID(&MockOrderedID{Id: 2}),
			TaskRunID(&MockOrderedID{Id: 2}),
			&proto.ResourceStatus{
				Status: proto.ResourceStatus_FAILED,
			},
			true,
		},
	}

	fn := func(t *testing.T, test TestCase) {
		manager, err := NewMemoryTaskMetadataManager()
		if err != nil {
			t.Fatalf("failed to create memory task metadata manager: %v", err)
		}

		for _, task := range test.Tasks {
			_, err := manager.CreateTask(task.Name, task.Type, task.Target)
			if err != nil && !test.shouldError {
				t.Fatalf("failed to create task: %v", err)
			}
		}

		for _, run := range test.Runs {
			_, err := manager.CreateTaskRun(run.Name, run.TaskID, run.Trigger)
			if err != nil && !test.shouldError {
				t.Fatalf("failed to create task run: %v", err)
			}
		}

		err = manager.SetRunStatus(test.ForRun, test.ForTask, test.SetStatus)
		if err != nil && test.shouldError {
			return
		} else if err != nil && !test.shouldError {
			t.Fatalf("failed to set status correctly: %v", err)
		} else if err == nil && test.shouldError {
			t.Fatalf("expected error but did not receive one")
		}

		recvRun, err := manager.GetRunByID(test.ForTask, test.ForRun)
		if err != nil {
			t.Fatalf("failed to get run by ID %d: %v", test.ForTask, err)
		}

		recvStatus := recvRun.Status
		if recvStatus.Proto() != test.SetStatus.Status {
			t.Fatalf("Expcted %v, got: %v", test.SetStatus, recvStatus)
		}
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			fn(t, tt)
		})
	}
}

func TestSetEndTimeByRunID(t *testing.T) {
	type taskInfo struct {
		Name   string
		Type   TaskType
		Target TaskTarget
	}
	type runInfo struct {
		Name    string
		TaskID  TaskID
		Trigger Trigger
	}
	type TestCase struct {
		Name        string
		Tasks       []taskInfo
		Runs        []runInfo
		ForTask     TaskID
		ForRun      TaskRunID
		SetTime     time.Time
		shouldError bool
	}

	tests := []TestCase{
		{
			"Single",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{{"name", TaskID(&MockOrderedID{Id: 1}), OnApplyTrigger{"name"}}},
			TaskID(&MockOrderedID{Id: 1}),
			TaskRunID(&MockOrderedID{Id: 1}),
			time.Now().Add(3 * time.Minute).Truncate(0).UTC(),
			false,
		},
		{
			"Multiple",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{
				{"name", TaskID(&MockOrderedID{Id: 1}), OnApplyTrigger{"name"}},
				{"name", TaskID(&MockOrderedID{Id: 1}), OnApplyTrigger{"name"}},
				{"name", TaskID(&MockOrderedID{Id: 1}), OnApplyTrigger{"name"}},
			},
			TaskID(&MockOrderedID{Id: 1}),
			TaskID(&MockOrderedID{Id: 2}),
			time.Now().Add(3 * time.Minute).Truncate(0).UTC(),
			false,
		},
		{
			"EmptyTime",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{
				{"name", TaskID(&MockOrderedID{Id: 1}), OnApplyTrigger{"name"}},
				{"name", TaskID(&MockOrderedID{Id: 1}), OnApplyTrigger{"name"}},
				{"name", TaskID(&MockOrderedID{Id: 1}), OnApplyTrigger{"name"}},
			},
			TaskID(&MockOrderedID{Id: 1}),
			TaskRunID(&MockOrderedID{Id: 2}),
			time.Time{},
			true,
		},
		{
			"WrongEndTime",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{
				{"name", TaskID(&MockOrderedID{Id: 1}), OnApplyTrigger{"name"}},
				{"name", TaskID(&MockOrderedID{Id: 1}), OnApplyTrigger{"name"}},
				{"name", TaskID(&MockOrderedID{Id: 1}), OnApplyTrigger{"name"}},
			},
			TaskID(&MockOrderedID{Id: 1}),
			TaskRunID(&MockOrderedID{Id: 2}),
			time.Unix(1, 0).Truncate(0).UTC(),
			true,
		},
		{
			"MultipleTasks",
			[]taskInfo{
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}},
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}},
			},
			[]runInfo{
				{"name", TaskID(&MockOrderedID{Id: 1}), OnApplyTrigger{"name"}},
				{"name", TaskID(&MockOrderedID{Id: 1}), OnApplyTrigger{"name"}},
				{"name", TaskID(&MockOrderedID{Id: 2}), OnApplyTrigger{"name"}},
				{"name", TaskID(&MockOrderedID{Id: 2}), OnApplyTrigger{"name"}},
			},
			TaskID(&MockOrderedID{Id: 2}),
			TaskRunID(&MockOrderedID{Id: 3}),
			time.Now().UTC().Add(3 * time.Minute).Truncate(0),
			false,
		},
	}

	fn := func(t *testing.T, test TestCase) {
		manager, err := NewMemoryTaskMetadataManager()
		if err != nil {
			t.Fatalf("failed to create memory task metadata manager: %v", err)
		}

		for _, task := range test.Tasks {
			_, err := manager.CreateTask(task.Name, task.Type, task.Target)
			if err != nil && !test.shouldError {
				t.Fatalf("failed to create task: %v", err)
			}
		}

		for _, run := range test.Runs {
			_, err := manager.CreateTaskRun(run.Name, run.TaskID, run.Trigger)
			if err != nil && !test.shouldError {
				t.Fatalf("failed to create task run: %v", err)
			}
		}

		err = manager.SetRunEndTime(test.ForRun, test.ForTask, test.SetTime)
		if err != nil && test.shouldError {
			return
		} else if err != nil && !test.shouldError {
			t.Fatalf("failed to set run end time correctly: %v", err)
		} else if err == nil && test.shouldError {
			t.Fatalf("expected error but did not receive one")
		}

		recvRun, err := manager.GetRunByID(test.ForTask, test.ForRun)
		if err != nil {
			t.Fatalf("failed to get run by ID %d: %v", test.ForTask, err)
		}

		if recvRun.EndTime != test.SetTime {
			t.Fatalf("expected %v, got: %v", test.SetTime, recvRun.EndTime)
		}
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			fn(t, tt)
		})
	}
}

func TestGetRunsByDate(t *testing.T) {
	id1 := ffsync.Uint64OrderedId(1)
	id2 := ffsync.Uint64OrderedId(2)
	id3 := ffsync.Uint64OrderedId(3)
	id4 := ffsync.Uint64OrderedId(4)

	type taskInfo struct {
		Name   string
		Type   TaskType
		Target TaskTarget
	}
	type runInfo struct {
		Name    string
		TaskID  TaskID
		Trigger Trigger
		Date    time.Time
	}
	type expectedRunInfo struct {
		TaskID TaskID
		RunID  TaskRunID
	}

	type TestCase struct {
		Name         string
		Tasks        []taskInfo
		Runs         []runInfo
		ExpectedRuns []expectedRunInfo
		ForTask      TaskID
		ForDate      time.Time
		shouldError  bool
	}

	tests := []TestCase{
		{
			"Empty",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{},
			[]expectedRunInfo{},
			TaskID(&id1),
			time.Now().UTC(),
			false,
		},
		{
			"Single",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{{"name", TaskID(&id1), OnApplyTrigger{"name"}, time.Now().UTC().Add(3 * time.Minute).Truncate(0).UTC()}},
			[]expectedRunInfo{{TaskID(&id1), TaskRunID(&id1)}},
			TaskID(&id1),
			time.Now().UTC(),
			false,
		},
		{
			"Multiple",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{
				{"name", TaskID(&id1), OnApplyTrigger{"name"}, time.Now().UTC().Add(1 * time.Minute).Truncate(0).UTC()},
				{"name", TaskID(&id1), OnApplyTrigger{"name"}, time.Now().UTC().Add(2 * time.Minute).Truncate(0).UTC()},
				{"name", TaskID(&id1), OnApplyTrigger{"name"}, time.Now().UTC().Add(3 * time.Minute).Truncate(0).UTC()},
			},
			[]expectedRunInfo{
				{TaskID(&id1), TaskRunID(&id1)},
				{TaskID(&id1), TaskRunID(&id2)},
				{TaskID(&id1), TaskRunID(&id3)},
			},
			TaskID(&id1),
			time.Now().UTC(),
			false,
		},
		{
			"MultipleTasks",
			[]taskInfo{
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}},
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}},
			},
			[]runInfo{
				{"name", TaskID(&id1), OnApplyTrigger{"name"}, time.Now().
					Add(1 * time.Minute).Truncate(0).UTC()},
				{"name", TaskID(&id1), OnApplyTrigger{"name"}, time.Now().
					Add(2 * time.Minute).Truncate(0).UTC()},
				{"name", TaskID(&id2), OnApplyTrigger{"name"}, time.Now().
					Add(3 * time.Minute).Truncate(0).UTC()},
				{"name", TaskID(&id2), OnApplyTrigger{"name"}, time.Now().
					Add(4 * time.Minute).Truncate(0).UTC()},
			},
			[]expectedRunInfo{
				{TaskID(&id1), TaskRunID(&id1)},
				{TaskID(&id1), TaskRunID(&id2)},
				{TaskID(&id2), TaskRunID(&id3)},
				{TaskID(&id2), TaskRunID(&id4)},
			},
			TaskID(&MockOrderedID{Id: 2}),
			time.Now().UTC(),
			false,
		},
	}

	fn := func(t *testing.T, test TestCase) {
		manager, err := NewMemoryTaskMetadataManager()
		if err != nil {
			t.Fatalf("failed to create memory task metadata manager: %v", err)
		}

		for _, task := range test.Tasks {
			_, err := manager.CreateTask(task.Name, task.Type, task.Target)
			if err != nil && !test.shouldError {
				t.Fatalf("failed to create task: %v", err)
			}
		}

		for _, run := range test.Runs {
			_, err := manager.CreateTaskRun(run.Name, run.TaskID, run.Trigger)
			if err != nil && !test.shouldError {
				t.Fatalf("failed to create task run: %v", err)
			}
		}

		recvRuns, err := manager.GetRunsByDate(test.ForDate, test.ForDate.Add(24*time.Hour))
		if err != nil && test.shouldError {
			return
		} else if err != nil && !test.shouldError {
			t.Fatalf("failed to get runs by date: %v", err)
		} else if err == nil && test.shouldError {
			t.Fatalf("expected error but did not receive one")
		}

		for _, run := range test.ExpectedRuns {
			foundRun := false
			for _, recvRun := range recvRuns {
				if recvRun.TaskId.Value() == run.TaskID.Value() && recvRun.ID.Value() == run.RunID.Value() {
					foundRun = true
					break
				}
			}
			if !foundRun {
				t.Fatalf("Expected Task %v, got: %v", run, recvRuns)
			}
		}
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			fn(t, tt)
		})
	}
}

func TestKeyPaths(t *testing.T) {
	type testCase struct {
		Name        string
		Key         Key
		ExpectedKey string
	}

	tests := []testCase{
		{
			Name:        "TaskMetadataKeyAll",
			Key:         TaskMetadataKey{},
			ExpectedKey: "/tasks/metadata/task_id=",
		},
		{
			Name:        "TaskMetadataKeyIndividual",
			Key:         TaskMetadataKey{taskID: TaskID(&MockOrderedID{Id: 1})},
			ExpectedKey: "/tasks/metadata/task_id=1",
		},
		{
			Name:        "TaskRunKeyAll",
			Key:         TaskRunKey{},
			ExpectedKey: "/tasks/runs/task_id=",
		},
		{
			Name:        "TaskRunKeyIndividual",
			Key:         TaskRunKey{taskID: TaskID(&MockOrderedID{Id: 1})},
			ExpectedKey: "/tasks/runs/task_id=1"},
		{
			Name:        "TaskRunMetadataKeyAll",
			Key:         TaskRunMetadataKey{},
			ExpectedKey: "/tasks/runs/metadata",
		},
		{
			Name: "TaskRunMetadataKeyIndividual",
			Key: TaskRunMetadataKey{
				taskID: TaskID(&MockOrderedID{Id: 1}),
				runID:  TaskRunID(&MockOrderedID{Id: 1}),
				date:   time.Date(2023, time.January, 20, 1, 1, 0, 0, time.UTC),
			},
			ExpectedKey: "/tasks/runs/metadata/2023/01/20/01/01/task_id=1/run_id=1",
		},
		{
			Name: "TaskRunMetadataKeyYearOnly",
			Key: TaskRunMetadataKey{
				date: time.Date(2023, time.January, 20, 2, 2, 0, 0, time.UTC),
			},
			ExpectedKey: "/tasks/runs/metadata/2023/01/20/02/02",
		},
	}

	fn := func(t *testing.T, test testCase) {
		if test.Key.String() != test.ExpectedKey {
			t.Fatalf("Expected: %s, got: %s", test.ExpectedKey, test.Key.String())
		}
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			fn(t, tt)
		})
	}
}
