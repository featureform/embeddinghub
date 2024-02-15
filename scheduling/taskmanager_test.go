package scheduling

import (
	"fmt"
	"reflect"
	"testing"
)

func TestInitialization(t *testing.T) {
	storage := MemoryStorageProvider{}
	NewTaskManager(&storage)
}

func TestCreateTask(t *testing.T) {
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
				{"name", ResourceCreation, NameVariant{"name", "variant"}, 1},
			},
			false,
		},
		{
			"Multiple",
			[]taskInfo{
				{"name", ResourceCreation, NameVariant{"name", "variant"}, 1},
				{"name2", ResourceCreation, NameVariant{"name", "variant"}, 2},
				{"name3", ResourceCreation, NameVariant{"name", "variant"}, 3},
			},
			false,
		},
	}

	fn := func(t *testing.T, tasks []taskInfo, shouldError bool) {
		storage := MemoryStorageProvider{}
		manager := NewTaskManager(&storage)
		for _, task := range tasks {
			taskDef, err := manager.CreateTask(task.Name, task.Type, task.Target)
			if err != nil && shouldError {
				continue
			} else if err != nil && !shouldError {
				t.Fatalf("failed to create task: %v", err)
			} else if err == nil && shouldError {
				t.Fatalf("expected error but did not receive one")
			}
			if task.ExpectedID != taskDef.ID {
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
			TaskID(1),
			true,
		},
		{
			"Single",
			[]taskInfo{
				{"name", ResourceCreation, NameVariant{"name", "variant"}, 1},
			},
			TaskID(1),
			false,
		},
		{
			"Multiple",
			[]taskInfo{
				{"name", ResourceCreation, NameVariant{"name", "variant"}, 1},
				{"name2", ResourceCreation, NameVariant{"name", "variant"}, 2},
				{"name3", ResourceCreation, NameVariant{"name", "variant"}, 3},
			},
			TaskID(2),
			false,
		},
		{
			"MultipleInsertInvalidLookup",
			[]taskInfo{
				{"name", ResourceCreation, NameVariant{"name", "variant"}, 1},
				{"name2", ResourceCreation, NameVariant{"name", "variant"}, 2},
				{"name3", ResourceCreation, NameVariant{"name", "variant"}, 3},
			},
			TaskID(4),
			true,
		},
	}

	fn := func(t *testing.T, test TestCase) {
		storage := MemoryStorageProvider{}
		manager := NewTaskManager(&storage)
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

		if reflect.DeepEqual(recievedDef, test.Tasks[test.ID-1]) {
			t.Fatalf("Expected: %v got: %v", test.Tasks[test.ID], recievedDef)
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
			TaskID(1),
			true,
		},
		{
			"Single",
			[]taskInfo{
				{"name", ResourceCreation, NameVariant{"name", "variant"}, 1},
			},
			TaskID(1),
			false,
		},
		{
			"Multiple",
			[]taskInfo{
				{"name", ResourceCreation, NameVariant{"name", "variant"}, 1},
				{"name2", ResourceCreation, NameVariant{"name", "variant"}, 2},
				{"name3", ResourceCreation, NameVariant{"name", "variant"}, 3},
			},
			TaskID(2),
			false,
		},
	}

	fn := func(t *testing.T, test TestCase) {
		storage := MemoryStorageProvider{}
		manager := NewTaskManager(&storage)
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
			if !reflect.DeepEqual(def, recvTasks[i]) {
				t.Fatalf("Expected: \n%v \ngot: \n%v", definitions, recvTasks)
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
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant"}}},
			[]runInfo{{"name", 1, OneOffTrigger{"name"}, 1}},
			false,
		},
		{
			"Multiple",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant"}}},
			[]runInfo{
				{"name", 1, OneOffTrigger{"name"}, 1},
				{"name", 1, OneOffTrigger{"name"}, 2},
				{"name", 1, OneOffTrigger{"name"}, 3},
			},
			false,
		},
		{
			"InvalidTask",
			[]taskInfo{},
			[]runInfo{{"name", 1, OneOffTrigger{"name"}, 1}},
			true,
		},
		{
			"MultipleTasks",
			[]taskInfo{
				{"name", ResourceCreation, NameVariant{"name", "variant"}},
				{"name", ResourceCreation, NameVariant{"name", "variant"}},
			},
			[]runInfo{
				{"name", 1, OneOffTrigger{"name"}, 1},
				{"name", 1, OneOffTrigger{"name"}, 2},
				{"name", 2, OneOffTrigger{"name"}, 1},
				{"name", 2, OneOffTrigger{"name"}, 2},
			},
			false,
		},
	}

	fn := func(t *testing.T, test TestCase) {
		storage := MemoryStorageProvider{}
		manager := NewTaskManager(&storage)
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
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant"}}},
			[]runInfo{{"name", 1, OneOffTrigger{"name"}}},
			1,
			1,
			false,
		},
		{
			"Multiple",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant"}}},
			[]runInfo{
				{"name", 1, OneOffTrigger{"name"}},
				{"name", 1, OneOffTrigger{"name"}},
				{"name", 1, OneOffTrigger{"name"}},
			},
			1,
			2,
			false,
		},
		{
			"MultipleTasks",
			[]taskInfo{
				{"name", ResourceCreation, NameVariant{"name", "variant"}},
				{"name", ResourceCreation, NameVariant{"name", "variant"}},
			},
			[]runInfo{
				{"name", 1, OneOffTrigger{"name"}},
				{"name", 1, OneOffTrigger{"name"}},
				{"name", 2, OneOffTrigger{"name"}},
				{"name", 2, OneOffTrigger{"name"}},
			},
			2,
			1,
			false,
		},
		{
			"Fetch NonExistent",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant"}}},
			[]runInfo{{"name", 1, OneOffTrigger{"name"}}},
			1,
			2,
			true,
		},
	}

	fn := func(t *testing.T, test TestCase) {
		storage := MemoryStorageProvider{}
		manager := NewTaskManager(&storage)
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
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant"}}},
			[]runInfo{{"name", 1, OneOffTrigger{"name"}}},
			false,
		},
		{
			"Multiple",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant"}}},
			[]runInfo{
				{"name", 1, OneOffTrigger{"name"}},
				{"name", 1, OneOffTrigger{"name"}},
				{"name", 1, OneOffTrigger{"name"}},
			},
			false,
		},
		{
			"MultipleTasks",
			[]taskInfo{
				{"name", ResourceCreation, NameVariant{"name", "variant"}},
				{"name", ResourceCreation, NameVariant{"name", "variant"}},
			},
			[]runInfo{
				{"name", 1, OneOffTrigger{"name"}},
				{"name", 1, OneOffTrigger{"name"}},
				{"name", 2, OneOffTrigger{"name"}},
				{"name", 2, OneOffTrigger{"name"}},
			},
			false,
		},
		{
			"Empty",
			[]taskInfo{},
			[]runInfo{},
			true,
		},
	}

	fn := func(t *testing.T, test TestCase) {
		storage := MemoryStorageProvider{}
		manager := NewTaskManager(&storage)
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
		if !reflect.DeepEqual(recvRuns, runDefs) {
			t.Fatalf("Expected \n%v, \ngot: \n%v", recvRuns, runDefs)
		}
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			fn(t, tt)
		})
	}
}

//func TestTaskRunList_FilterByStatus(t *testing.T) {
//	type args struct {
//		status Status
//	}
//	tests := []struct {
//		name string
//		given  TaskRunList
//		expected TaskRunList
//		args args
//	}{
//		{"Single", []TaskRunMetadata{{Name: }}},
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			tt.given.FilterByStatus(tt.args.status)
//		})
//	}
//}

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
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant"}}},
			[]runInfo{{"name", 1, OneOffTrigger{"name"}}},
			1,
			1,
			Success,
			nil,
			false,
		},
		{
			"Multiple",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant"}}},
			[]runInfo{
				{"name", 1, OneOffTrigger{"name"}},
				{"name", 1, OneOffTrigger{"name"}},
				{"name", 1, OneOffTrigger{"name"}},
			},
			1,
			2,
			Pending,
			nil,
			false,
		},
		{
			"WrongStatus",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant"}}},
			[]runInfo{
				{"name", 1, OneOffTrigger{"name"}},
				{"name", 1, OneOffTrigger{"name"}},
				{"name", 1, OneOffTrigger{"name"}},
			},
			1,
			2,
			"Random",
			nil,
			true,
		},
		{
			"MultipleTasks",
			[]taskInfo{
				{"name", ResourceCreation, NameVariant{"name", "variant"}},
				{"name", ResourceCreation, NameVariant{"name", "variant"}},
			},
			[]runInfo{
				{"name", 1, OneOffTrigger{"name"}},
				{"name", 1, OneOffTrigger{"name"}},
				{"name", 2, OneOffTrigger{"name"}},
				{"name", 2, OneOffTrigger{"name"}},
			},
			2,
			1,
			Failed,
			fmt.Errorf("Failed to create task"),
			false,
		},
		// {
		// 	"Fetch NonExistent",
		// 	[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant"}}},
		// 	[]runInfo{{"name", 1, OneOffTrigger{"name"}}},
		// 	1,
		// 	2,
		// 	true,
		// },
	}

	fn := func(t *testing.T, test TestCase) {
		storage := MemoryStorageProvider{}
		manager := NewTaskManager(&storage)
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
