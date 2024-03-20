package scheduling

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/featureform/ffsync"
	sp "github.com/featureform/scheduling/storage_providers"
)

type MockOrderedID struct {
	Id uint64
}

func (id MockOrderedID) Equals(other ffsync.OrderedId) bool {
	return id.Id == other.(MockOrderedID).Id
}

func (id MockOrderedID) Less(other ffsync.OrderedId) bool {
	return id.Id < other.(MockOrderedID).Id
}

func (id MockOrderedID) String() string {
	return fmt.Sprint(id.Id)
}

func TestTaskMetadataManager(t *testing.T) {
	testFns := map[string]func(*testing.T, TaskMetadataManager){
		"CreateTask": testCreateTask,
	}

	memoryTaskMetadataManager := NewMemoryTaskMetadataManager()

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
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}, TaskID(MockOrderedID{Id: 1})},
			},
			false,
		},
		{
			"Multiple",
			[]taskInfo{
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}, TaskID(MockOrderedID{Id: 1})},
				{"name2", ResourceCreation, NameVariant{"name", "variant", "type"}, TaskID(MockOrderedID{Id: 2})},
				{"name3", ResourceCreation, NameVariant{"name", "variant", "type"}, TaskID(MockOrderedID{Id: 3})},
			},
			false,
		},
	}

	fn := func(t *testing.T, tasks []taskInfo, shouldError bool) {
		manager := NewMemoryTaskMetadataManager() // TODO: will need to modify this to use any store and deletes tasks after job was done
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
			TaskID(MockOrderedID{Id: 1}),
			true,
		},
		{
			"Single",
			[]taskInfo{
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}, TaskID(MockOrderedID{Id: 1})},
			},
			TaskID(MockOrderedID{Id: 1}),
			false,
		},
		{
			"Multiple",
			[]taskInfo{
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}, TaskID(MockOrderedID{Id: 1})},
				{"name2", ResourceCreation, NameVariant{"name", "variant", "type"}, TaskID(MockOrderedID{Id: 2})},
				{"name3", ResourceCreation, NameVariant{"name", "variant", "type"}, TaskID(MockOrderedID{Id: 3})},
			},
			TaskID(MockOrderedID{Id: 2}),
			false,
		},
		{
			"MultipleInsertInvalidLookup",
			[]taskInfo{
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}, MockOrderedID{Id: 1}},
				{"name2", ResourceCreation, NameVariant{"name", "variant", "type"}, MockOrderedID{Id: 2}},
				{"name3", ResourceCreation, NameVariant{"name", "variant", "type"}, MockOrderedID{Id: 3}},
			},
			TaskID(MockOrderedID{Id: 4}),
			true,
		},
	}

	fn := func(t *testing.T, test TestCase) {
		manager := NewMemoryTaskMetadataManager()

		var definitions []TaskMetadata
		for _, task := range test.Tasks {
			taskDef, err := manager.CreateTask(task.Name, task.Type, task.Target)
			if err != nil {
				t.Fatalf("failed to create task: %v", err)
			}
			definitions = append(definitions, taskDef)
		}
		recievedDef, err := manager.GetTaskByID(test.ID)
		if err != nil && !test.shouldError {
			t.Fatalf("failed to fetch definiton: %v", err)
		} else if err == nil && test.shouldError {
			t.Fatalf("expected error and did not get one")
		} else if err != nil && test.shouldError {
			return
		}

		idx := test.ID.(MockOrderedID).Id
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
			TaskID(MockOrderedID{Id: 1}),
			false,
		},
		{
			"Single",
			[]taskInfo{
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}, TaskID(MockOrderedID{Id: 1})},
			},
			TaskID(MockOrderedID{Id: 1}),
			false,
		},
		{
			"Multiple",
			[]taskInfo{
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}, TaskID(MockOrderedID{Id: 1})},
				{"name2", ResourceCreation, NameVariant{"name", "variant", "type"}, TaskID(MockOrderedID{Id: 2})},
				{"name3", ResourceCreation, NameVariant{"name", "variant", "type"}, TaskID(MockOrderedID{Id: 3})},
			},
			TaskID(MockOrderedID{Id: 2}),
			false,
		},
	}

	fn := func(t *testing.T, test TestCase) {
		manager := NewMemoryTaskMetadataManager()
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
			[]runInfo{{"name", TaskID(MockOrderedID{Id: 1}), OneOffTrigger{"name"}, TaskRunID(MockOrderedID{Id: 1})}},
			false,
		},
		{
			"Multiple",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{
				{"name", TaskID(MockOrderedID{Id: 1}), OneOffTrigger{"name"}, TaskRunID(MockOrderedID{Id: 1})},
				{"name", TaskID(MockOrderedID{Id: 1}), OneOffTrigger{"name"}, TaskRunID(MockOrderedID{Id: 2})},
				{"name", TaskID(MockOrderedID{Id: 1}), OneOffTrigger{"name"}, TaskRunID(MockOrderedID{Id: 3})},
			},
			false,
		},
		{
			"InvalidTask",
			[]taskInfo{},
			[]runInfo{{"name", TaskID(MockOrderedID{Id: 1}), OneOffTrigger{"name"}, TaskRunID(MockOrderedID{Id: 1})}},
			true,
		},
		{
			"MultipleTasks",
			[]taskInfo{
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}},
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}},
			},
			[]runInfo{
				{"name", TaskID(MockOrderedID{Id: 1}), OneOffTrigger{"name"}, TaskRunID(MockOrderedID{Id: 1})},
				{"name", TaskID(MockOrderedID{Id: 1}), OneOffTrigger{"name"}, TaskRunID(MockOrderedID{Id: 2})},
				{"name", TaskID(MockOrderedID{Id: 2}), OneOffTrigger{"name"}, TaskRunID(MockOrderedID{Id: 1})},
				{"name", TaskID(MockOrderedID{Id: 2}), OneOffTrigger{"name"}, TaskRunID(MockOrderedID{Id: 2})},
			},
			false,
		},
	}

	fn := func(t *testing.T, test TestCase) {
		manager := NewMemoryTaskMetadataManager()
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
			if run.ExpectedID != recvRun.ID {
				t.Fatalf("Expected id: %d, got: %d", run.ExpectedID, recvRun.ID)
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
			[]runInfo{{"name", TaskID(MockOrderedID{Id: 1}), OneOffTrigger{"name"}}},
			TaskID(MockOrderedID{Id: 1}),
			TaskRunID(MockOrderedID{Id: 1}),
			false,
		},
		{
			"Multiple",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{
				{"name", TaskID(MockOrderedID{Id: 1}), OneOffTrigger{"name"}},
				{"name", TaskID(MockOrderedID{Id: 1}), OneOffTrigger{"name"}},
				{"name", TaskID(MockOrderedID{Id: 1}), OneOffTrigger{"name"}},
			},
			TaskID(MockOrderedID{Id: 1}),
			TaskRunID(MockOrderedID{Id: 2}),
			false,
		},
		{
			"MultipleTasks",
			[]taskInfo{
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}},
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}},
			},
			[]runInfo{
				{"name", TaskID(MockOrderedID{Id: 1}), OneOffTrigger{"name"}},
				{"name", TaskID(MockOrderedID{Id: 1}), OneOffTrigger{"name"}},
				{"name", TaskID(MockOrderedID{Id: 2}), OneOffTrigger{"name"}},
				{"name", TaskID(MockOrderedID{Id: 2}), OneOffTrigger{"name"}},
			},
			TaskID(MockOrderedID{Id: 2}),
			TaskRunID(MockOrderedID{Id: 1}),
			false,
		},
		{
			"Fetch NonExistent",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{{"name", TaskID(MockOrderedID{Id: 1}), OneOffTrigger{"name"}}},
			TaskID(MockOrderedID{Id: 1}),
			TaskRunID(MockOrderedID{Id: 2}),
			true,
		},
	}

	fn := func(t *testing.T, test TestCase) {
		storage := sp.NewMemoryStorageProvider()
		manager := NewTaskManager(storage)
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
			[]runInfo{{"name", TaskID(MockOrderedID{Id: 1}), OneOffTrigger{"name"}}},
			false,
		},
		{
			"Multiple",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{
				{"name", TaskID(MockOrderedID{Id: 1}), OneOffTrigger{"name"}},
				{"name", TaskID(MockOrderedID{Id: 1}), OneOffTrigger{"name"}},
				{"name", TaskID(MockOrderedID{Id: 1}), OneOffTrigger{"name"}},
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
				{"name", TaskID(MockOrderedID{Id: 1}), OneOffTrigger{"name"}},
				{"name", TaskID(MockOrderedID{Id: 1}), OneOffTrigger{"name"}},
				{"name", TaskID(MockOrderedID{Id: 2}), OneOffTrigger{"name"}},
				{"name", TaskID(MockOrderedID{Id: 2}), OneOffTrigger{"name"}},
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
		manager := NewMemoryTaskMetadataManager()
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
		// if !reflect.DeepEqual(recvRuns, runDefs) {
		// 	t.Fatalf("Expected \n%v, \ngot: \n%v", recvRuns, runDefs)
		// }
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
		SetStatus   Status
		SetError    error
		shouldError bool
	}
	tests := []TestCase{
		{
			"Single",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{{"name", TaskID(MockOrderedID{Id: 1}), OneOffTrigger{"name"}}},
			TaskID(MockOrderedID{Id: 1}),
			TaskRunID(MockOrderedID{Id: 1}),
			Success,
			nil,
			false,
		},
		{
			"Multiple",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{
				{"name", TaskID(MockOrderedID{Id: 1}), OneOffTrigger{"name"}},
				{"name", TaskID(MockOrderedID{Id: 1}), OneOffTrigger{"name"}},
				{"name", TaskID(MockOrderedID{Id: 1}), OneOffTrigger{"name"}},
			},
			TaskID(MockOrderedID{Id: 1}),
			TaskRunID(MockOrderedID{Id: 2}),
			Pending,
			nil,
			false,
		},
		{
			"WrongID",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{
				{"name", TaskID(MockOrderedID{Id: 1}), OneOffTrigger{"name"}},
			},
			TaskID(MockOrderedID{Id: 3}),
			TaskRunID(MockOrderedID{Id: 2}),
			Pending,
			nil,
			true,
		},
		{
			"WrongRunID",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{
				{"name", TaskID(MockOrderedID{Id: 1}), OneOffTrigger{"name"}},
			},
			TaskID(MockOrderedID{Id: 1}),
			TaskRunID(MockOrderedID{Id: 2}),
			Running,
			nil,
			true,
		},
		{
			"MultipleTasks",
			[]taskInfo{
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}},
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}},
			},
			[]runInfo{
				{"name", TaskID(MockOrderedID{Id: 1}), OneOffTrigger{"name"}},
				{"name", TaskID(MockOrderedID{Id: 1}), OneOffTrigger{"name"}},
				{"name", TaskID(MockOrderedID{Id: 2}), OneOffTrigger{"name"}},
				{"name", TaskID(MockOrderedID{Id: 2}), OneOffTrigger{"name"}},
			},
			TaskID(MockOrderedID{Id: 2}),
			TaskRunID(MockOrderedID{Id: 1}),
			Failed,
			fmt.Errorf("Failed to create task"),
			false,
		},
		{
			"FailedStatusWithoutError",
			[]taskInfo{
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}},
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}},
			},
			[]runInfo{
				{"name", TaskID(MockOrderedID{Id: 1}), OneOffTrigger{"name"}},
				{"name", TaskID(MockOrderedID{Id: 2}), OneOffTrigger{"name"}},
			},
			TaskID(MockOrderedID{Id: 2}),
			TaskRunID(MockOrderedID{Id: 1}),
			Failed,
			nil,
			true,
		},
	}

	fn := func(t *testing.T, test TestCase) {
		manager := NewMemoryTaskMetadataManager()
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

		err := manager.SetRunStatus(test.ForRun, test.ForTask, test.SetStatus, test.SetError)
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
		if recvStatus != test.SetStatus {
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
			[]runInfo{{"name", TaskID(MockOrderedID{Id: 1}), OneOffTrigger{"name"}}},
			TaskID(MockOrderedID{Id: 1}),
			TaskRunID(MockOrderedID{Id: 1}),
			time.Now().Add(3 * time.Minute).Truncate(0).UTC(),
			false,
		},
		{
			"Multiple",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{
				{"name", TaskID(MockOrderedID{Id: 1}), OneOffTrigger{"name"}},
				{"name", TaskID(MockOrderedID{Id: 1}), OneOffTrigger{"name"}},
				{"name", TaskID(MockOrderedID{Id: 1}), OneOffTrigger{"name"}},
			},
			TaskID(MockOrderedID{Id: 1}),
			TaskID(MockOrderedID{Id: 2}),
			time.Now().Add(3 * time.Minute).Truncate(0).UTC(),
			false,
		},
		{
			"EmptyTime",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{
				{"name", TaskID(MockOrderedID{Id: 1}), OneOffTrigger{"name"}},
				{"name", TaskID(MockOrderedID{Id: 1}), OneOffTrigger{"name"}},
				{"name", TaskID(MockOrderedID{Id: 1}), OneOffTrigger{"name"}},
			},
			TaskID(MockOrderedID{Id: 1}),
			TaskRunID(MockOrderedID{Id: 2}),
			time.Time{},
			true,
		},
		{
			"WrongEndTime",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{
				{"name", TaskID(MockOrderedID{Id: 1}), OneOffTrigger{"name"}},
				{"name", TaskID(MockOrderedID{Id: 1}), OneOffTrigger{"name"}},
				{"name", TaskID(MockOrderedID{Id: 1}), OneOffTrigger{"name"}},
			},
			TaskID(MockOrderedID{Id: 1}),
			TaskRunID(MockOrderedID{Id: 2}),
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
				{"name", TaskID(MockOrderedID{Id: 1}), OneOffTrigger{"name"}},
				{"name", TaskID(MockOrderedID{Id: 1}), OneOffTrigger{"name"}},
				{"name", TaskID(MockOrderedID{Id: 2}), OneOffTrigger{"name"}},
				{"name", TaskID(MockOrderedID{Id: 2}), OneOffTrigger{"name"}},
			},
			TaskID(MockOrderedID{Id: 2}),
			TaskRunID(MockOrderedID{Id: 1}),
			time.Now().UTC().Add(3 * time.Minute).Truncate(0),
			false,
		},
	}

	fn := func(t *testing.T, test TestCase) {
		manager := NewMemoryTaskMetadataManager()
		for _, task := range test.Tasks {
			_, err := manager.CreateTask(task.Name, task.Type, task.Target)
			if err != nil && !test.shouldError {
				t.Fatalf("failed to create task: %v", err)
			}
		}

		// var runDefs []TaskRunMetadata
		for _, run := range test.Runs {
			_, err := manager.CreateTaskRun(run.Name, run.TaskID, run.Trigger)
			if err != nil && !test.shouldError {
				t.Fatalf("failed to create task run: %v", err)
			}
			// runDefs = append(runDefs, runDef)
		}

		err := manager.SetRunEndTime(test.ForRun, test.ForTask, test.SetTime)
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
			Key:         TaskMetadataKey{taskID: TaskID(MockOrderedID{Id: 1})},
			ExpectedKey: "/tasks/metadata/task_id=1",
		},
		{
			Name:        "TaskRunKeyAll",
			Key:         TaskRunKey{},
			ExpectedKey: "/tasks/runs/task_id=",
		},
		{
			Name:        "TaskRunKeyIndividual",
			Key:         TaskRunKey{taskID: TaskID(MockOrderedID{Id: 1})},
			ExpectedKey: "/tasks/runs/task_id=1"},
		{
			Name:        "TaskRunMetadataKeyAll",
			Key:         TaskRunMetadataKey{},
			ExpectedKey: "/tasks/runs/metadata",
		},
		{
			Name: "TaskRunMetadataKeyIndividual",
			Key: TaskRunMetadataKey{
				taskID: TaskID(MockOrderedID{Id: 1}),
				runID:  TaskRunID(MockOrderedID{Id: 1}),
				date:   time.Date(2023, time.January, 20, 23, 0, 0, 0, time.UTC),
			},
			ExpectedKey: "/tasks/runs/metadata/2023/01/20/task_id=1/run_id=1",
		},
		{
			Name: "TaskRunMetadataKeyYearOnly",
			Key: TaskRunMetadataKey{
				date: time.Date(2023, time.January, 20, 23, 0, 0, 0, time.UTC),
			},
			ExpectedKey: "/tasks/runs/metadata/2023/01/20",
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
