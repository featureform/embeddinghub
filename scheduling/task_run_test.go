// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package scheduling

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/featureform/fferr"
	"github.com/featureform/ffsync"
	pb "github.com/featureform/metadata/proto"
	ptypes "github.com/featureform/provider/types"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestTriggerName(t *testing.T) {
	testCases := []struct {
		name     string
		trigger  Trigger
		expected string
	}{
		{
			name:     "OnApplyTriggerName",
			trigger:  OnApplyTrigger{TriggerName: "name1"},
			expected: "name1",
		},
		{
			name:     "OnApplyTriggerName2",
			trigger:  OnApplyTrigger{TriggerName: "name2"},
			expected: "name2",
		},
	}

	for _, currTest := range testCases {
		t.Run(currTest.name, func(t *testing.T) {
			if currTest.trigger.Name() != currTest.expected {
				t.Fatalf("Got trigger name: %v\n Expected:%v", currTest.trigger.Name(), currTest.expected)
			}
		})
	}

}

func TestEmptyVariables(t *testing.T) {
	id1 := ffsync.Uint64OrderedId(1)

	testCases := []struct {
		name   string
		task   TaskRunMetadata
		errMsg error
	}{
		{
			name: "NoName",
			task: TaskRunMetadata{
				ID:     TaskRunID(&id1),
				TaskId: TaskID(&id1),
				Name:   "",
				Trigger: OnApplyTrigger{
					TriggerName: "name1",
				},
				TriggerType: OnApplyTriggerType,
				Status:      PENDING,
				StartTime:   time.Now().Truncate(0).UTC(),
				EndTime:     time.Now().Truncate(0).UTC(),
				Logs:        nil,
				Error:       "No name present",
			},
			errMsg: fferr.NewInvalidArgumentError(fmt.Errorf("task run metadata is missing Name")),
		},
		{
			name: "NoStartTime",
			task: TaskRunMetadata{
				ID:     TaskRunID(&id1),
				TaskId: TaskID(&id1),
				Name:   "name2",
				Trigger: OnApplyTrigger{
					TriggerName: "name3",
				},
				TriggerType: OnApplyTriggerType,
				Status:      PENDING,
				EndTime:     time.Now().Truncate(0).UTC(),
				Logs:        nil,
				Error:       "No start time present",
			},
			errMsg: fferr.NewInvalidArgumentError(fmt.Errorf("task run metadata is missing StartTime")),
		},
	}

	for _, currTest := range testCases {
		t.Run(currTest.name, func(t *testing.T) {
			serializedTask, err := currTest.task.Marshal()
			if err != nil {
				return
			}

			deserializedTask := TaskRunMetadata{}
			err = deserializedTask.Unmarshal(serializedTask)
			if err == nil {
				t.Fatalf("Expected error for empty fields")
			}
			if err.Error() != currTest.errMsg.Error() {
				t.Fatalf("Expected error: %v\n Got: %v", currTest.errMsg, err)
			}
		})
	}

}

func TestSerializeTaskRunMetadata(t *testing.T) {
	id1 := ffsync.Uint64OrderedId(1)

	testCases := []struct {
		name        string
		task        TaskRunMetadata
		triggerType TriggerType
	}{
		{
			name: "WithOnApplyTrigger",
			task: TaskRunMetadata{
				ID:     TaskRunID(id1),
				TaskId: TaskID(id1),
				Name:   "oneoff_taskrun",
				Trigger: OnApplyTrigger{
					TriggerName: "name1",
				},
				TriggerType: OnApplyTriggerType,
				Target: NameVariant{
					Name:    "name",
					Variant: "variant",
				},
				TargetType: NameVariantTarget,
				Status:     PENDING,
				StartTime:  time.Now().Truncate(0).UTC(),
				EndTime:    time.Now().Truncate(0).UTC(),
				ResumeID:   ptypes.ResumeID("resume"),
				Logs:       nil,
				Error:      "",
			},
			triggerType: OnApplyTriggerType,
		},
		{
			name: "WithScheduleTrigger",
			task: TaskRunMetadata{
				ID:     TaskRunID(id1),
				TaskId: TaskID(id1),
				Name:   "dummy_taskrun",
				Trigger: ScheduleTrigger{
					TriggerName: "name2",
					Schedule:    "* * * * *",
				},
				TriggerType: ScheduleTriggerType,
				Target: Provider{
					Name: "name",
				},
				TargetType: ProviderTarget,
				Status:     FAILED,
				StartTime:  time.Now().Truncate(0).UTC(),
				EndTime:    time.Now().Truncate(0).UTC(),
				ResumeID:   ptypes.ResumeID("resume"),
				Logs:       nil,
				Error:      "",
			},
			triggerType: ScheduleTriggerType,
		},
	}

	for _, currTest := range testCases {
		t.Run(currTest.name, func(t *testing.T) {
			serializedTask, err := currTest.task.Marshal()
			if err != nil {
				t.Fatalf("failed to serialize task run metadata: %v", err)
			}

			deserializedTask := TaskRunMetadata{}
			if err := deserializedTask.Unmarshal(serializedTask); err != nil {
				t.Fatalf("failed to deserialize task run metadata: %v", err)
			}

			if !reflect.DeepEqual(deserializedTask, currTest.task) {
				t.Fatalf("Wrong struct values, \ngot: %#v\nExpected: %#v", deserializedTask, currTest.task)
			}
			if deserializedTask.Trigger.Type() != currTest.triggerType {
				t.Fatalf("Wrong trigger type, got: %#v\n Expected:%#v", deserializedTask.Trigger.Type(), currTest.triggerType)
			}
		})
	}
}

func TestIncorrectTaskRunMetadata(t *testing.T) {
	id := ffsync.Uint64OrderedId(1)

	testCases := []struct {
		name string
		task TaskRunMetadata
	}{
		{
			name: "OneOffDummyTrigger",
			task: TaskRunMetadata{
				ID:     TaskRunID(&id),
				TaskId: TaskID(&id),
				Name:   "OnApplyTrigger",
				Trigger: OnApplyTrigger{
					TriggerName: "name3",
				},
				TriggerType: ScheduleTriggerType,
				Status:      FAILED,
				StartTime:   time.Now().Truncate(0).UTC(),
				EndTime:     time.Now().Truncate(0).UTC(),
				Logs:        nil,
				Error:       "Mixed trigger present",
			},
		},

		{
			name: "NoTrigger",
			task: TaskRunMetadata{
				ID:        TaskRunID(&id),
				TaskId:    TaskID(&id),
				Name:      "no_trigger",
				Trigger:   nil,
				Status:    PENDING,
				StartTime: time.Now().Truncate(0).UTC(),
				EndTime:   time.Now().Truncate(0).UTC(),
				Logs:      nil,
				Error:     "No trigger present",
			},
		},
	}

	for _, currTest := range testCases {
		t.Run(currTest.name, func(t *testing.T) {
			serializedTask, err := currTest.task.Marshal()
			if err != nil {
				return
			}

			deserializedTask := TaskRunMetadata{}
			err = deserializedTask.Unmarshal(serializedTask)
			if err != nil {
				return
			}

			if reflect.DeepEqual(deserializedTask, currTest.task) {
				t.Fatalf("Expected trigger should be present and different from output trigger, \nexpected: %#v\n     Got: %#v", currTest.task, deserializedTask)
			}
		})
	}
}

func TestCorruptData(t *testing.T) {
	testCases := []struct {
		name      string
		inputfile []byte
		errMsg    string
	}{
		{
			name: "InvalidJson",
			inputfile: []byte(`{"id"1, "TaskIDs": 12, "name":"invalid_json_file",
			 "trigger": {"triggerName": "name4", "triggerType": "OnApplyTrigger", "dummyField":  false,},
			  "status": "FAILED", "startTime": "2021-08-26T15:04:05Z", "endTime": "2021-08-26T15:04:05Z",
			   "logs": nil, "error": "invalid json",
			}`),
			errMsg: "invalid character '1' after object key:value pair",
		},
		{
			name: "MissingName",
			inputfile: []byte(`{"id":1, "TaskIDs": 12,
			"trigger": {"triggerName": "name5", "triggerType": "OnApplyTrigger", "dummyField":  false,},
			 "status": "FAILED", "startTime": "2021-08-26T15:04:05Z", "endTime": "2021-08-26T15:04:05Z",
			  "logs": nil, "error": "invalid json",
		   }`),
			errMsg: "Missing field 'name'",
		},
		{
			name: "MissingTrigger",
			inputfile: []byte(`{"id":1, "TaskIDs": 12, "name":"invalid_json_file",
			 "status": "FAILED", "startTime": "2021-08-26T15:04:05Z", "endTime": "2021-08-26T15:04:05Z",
			  "logs": nil, "error": "invalid json",
		   }`),
			errMsg: "Missing field 'trigger'",
		},
		{
			name: "InvalidStatusType",
			inputfile: []byte(`{"id":1, "TaskIDs": 12, "name":"invalid_json_file",
			"trigger": {"triggerName": "name6", "triggerType": "OnApplyTrigger", "dummyField":  false,},
			 "status": "NOSTATUS", "startTime": "2021-08-26T15:04:05Z", "endTime": "2021-08-26T15:04:05Z",
			  "logs": nil, "error": "invalid json",
		   }`),
			errMsg: "No such status: 'NOSTATUS'",
		},
		{
			name: "InvalidTriggerType",
			inputfile: []byte(`{"id":1, "TaskIDs": 12, "name":"invalid_json_file",
			"trigger": {"triggerName": "name7", "triggerType": "wrongTrigger", "dummyField":  false,},
			 "status": "PENDING", "startTime": "2021-08-26T15:04:05Z", "endTime": "2021-08-26T15:04:05Z",
			  "logs": nil, "error": "invalid json",
		   }`),
			errMsg: "No such trigger type: 'wrongTrigger'",
		},
		{
			name: "InvalidTrigger",
			inputfile: []byte(`{"id":1, "TaskIDs": 12, "name":"invalid_json_file",
			"trigger": ["triggerName": "name8", "triggerType": "DummyTrigger", "dummyField":  false],
			 "status": "PENDING", "startTime": "2021-08-26T15:04:05Z", "endTime": "2021-08-26T15:04:05Z",
			  "logs": nil, "error": "invalid json",
		   }`),
			errMsg: "Wrong format of Trigger",
		},
	}

	for _, currTest := range testCases {
		t.Run(currTest.name, func(t *testing.T) {
			response := TaskRunMetadata{}
			err := response.Unmarshal(currTest.inputfile)
			if err == nil {
				t.Fatalf(currTest.errMsg)
			}
		})
	}
}

func TestStatus_validateTransition(t *testing.T) {
	type args struct {
		to Status
	}
	tests := []struct {
		name    string
		s       Status
		args    args
		wantErr bool
	}{
		{"NoStatusToNoStatus", NO_STATUS, args{NO_STATUS}, true},
		{"NoStatusToPending", NO_STATUS, args{PENDING}, false},
		{"NoStatusToCreated", NO_STATUS, args{CREATED}, false},
		{"NoStatusToReady", NO_STATUS, args{READY}, true},
		{"NoStatusToRunning", NO_STATUS, args{RUNNING}, true},
		{"NoStatusToFailed", NO_STATUS, args{FAILED}, true},
		{"NoStatusToCancelled", NO_STATUS, args{CANCELLED}, true},

		{"CreatedToNoStatus", CREATED, args{NO_STATUS}, true},
		{"CreatedToPending", CREATED, args{PENDING}, false},
		{"CreatedToCreated", CREATED, args{CREATED}, true},
		{"CreatedToReady", CREATED, args{READY}, true},
		{"CreatedToRunning", CREATED, args{RUNNING}, true},
		{"CreatedToFailed", CREATED, args{FAILED}, true},
		{"CreatedToCancelled", CREATED, args{CANCELLED}, true},

		{"PendingToNoStatus", PENDING, args{NO_STATUS}, true},
		{"PendingToPending", PENDING, args{PENDING}, true},
		{"PendingToCreated", PENDING, args{CREATED}, true},
		{"PendingToReady", PENDING, args{READY}, true},
		{"PendingToRunning", PENDING, args{RUNNING}, false},
		{"PendingToFailed", PENDING, args{FAILED}, true},
		{"PendingToCancelled", PENDING, args{CANCELLED}, false},

		{"RunningToNoStatus", RUNNING, args{NO_STATUS}, true},
		{"RunningToPending", RUNNING, args{PENDING}, true},
		{"RunningToCreated", RUNNING, args{CREATED}, true},
		{"RunningToReady", RUNNING, args{READY}, false},
		{"RunningToRunning", RUNNING, args{RUNNING}, true},
		{"RunningToFailed", RUNNING, args{FAILED}, false},
		{"RunningToCancelled", RUNNING, args{CANCELLED}, false},

		{"ReadyToNoStatus", READY, args{NO_STATUS}, true},
		{"ReadyToPending", READY, args{PENDING}, true},
		{"ReadyToCreated", READY, args{CREATED}, true},
		{"ReadyToReady", READY, args{READY}, true},
		{"ReadyToRunning", READY, args{RUNNING}, true},
		{"ReadyToFailed", READY, args{FAILED}, true},
		{"ReadyToCancelled", READY, args{CANCELLED}, true},

		{"FailedToNoStatus", FAILED, args{NO_STATUS}, true},
		{"FailedToPending", FAILED, args{PENDING}, true},
		{"FailedToCreated", FAILED, args{CREATED}, true},
		{"FailedToReady", FAILED, args{READY}, true},
		{"FailedToRunning", FAILED, args{RUNNING}, true},
		{"FailedToFailed", FAILED, args{FAILED}, true},
		{"FailedToCancelled", FAILED, args{CANCELLED}, true},

		{"CancelledToNoStatus", CANCELLED, args{NO_STATUS}, true},
		{"CancelledToPending", CANCELLED, args{PENDING}, true},
		{"CancelledToCreated", CANCELLED, args{CREATED}, true},
		{"CancelledToReady", CANCELLED, args{READY}, true},
		{"CancelledToRunning", CANCELLED, args{RUNNING}, true},
		{"CancelledToFailed", CANCELLED, args{FAILED}, true},
		{"CancelledToCancelled", CANCELLED, args{CANCELLED}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.s.validateTransition(tt.args.to); (err != nil) != tt.wantErr {
				t.Errorf("validateTransition() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_wrapTaskRunMetadataProto(t *testing.T) {
	id := ffsync.Uint64OrderedId(1)
	tests := []struct {
		name    string
		run     TaskRunMetadata
		wantErr bool
	}{
		{
			"On Apply",
			TaskRunMetadata{
				ID:     TaskRunID(id),
				TaskId: TaskID(id),
				Trigger: OnApplyTrigger{
					TriggerName: "trigger_name",
				},
				TriggerType: OnApplyTriggerType,
				Target: NameVariant{
					Name:         "name",
					Variant:      "variant",
					ResourceType: "SOURCE",
				},
				TargetType: NameVariantTarget,
				Status:     READY,
				StartTime:  time.Now().UTC(),
				EndTime:    time.Now().AddDate(0, 0, 1).UTC(),
				ResumeID:   "test",
				Logs:       []string{"log1", "log2"},
				Error:      "some string error",
				ErrorProto: &pb.ErrorStatus{
					Code:    1,
					Message: "Some error message",
					Details: []*anypb.Any{{Value: []byte("abcd")}},
				},
			},
			false,
		},
		{
			"Schedule",
			TaskRunMetadata{
				ID:     TaskRunID(id),
				TaskId: TaskID(id),
				Trigger: ScheduleTrigger{
					TriggerName: "trigger_name",
					Schedule:    "* * * * *",
				},
				TriggerType: ScheduleTriggerType,
				Target: NameVariant{
					Name:         "name",
					Variant:      "variant",
					ResourceType: "SOURCE",
				},
				TargetType: NameVariantTarget,
				Status:     READY,
				StartTime:  time.Now().UTC(),
				EndTime:    time.Now().AddDate(0, 0, 1).UTC(),
				Logs:       []string{"log1", "log2"},
				Error:      "some string error",
				ErrorProto: &pb.ErrorStatus{
					Code:    1,
					Message: "Some error message",
					Details: []*anypb.Any{{Value: []byte("abcd")}},
				},
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proto, err := tt.run.ToProto()
			if (err != nil) != tt.wantErr {
				t.Errorf("WrapTaskRunMetadataProto() error = %#v, wantErr %#v", err, tt.wantErr)
				return
			}
			got, err := TaskRunMetadataFromProto(proto)
			if (err != nil) != tt.wantErr {
				t.Errorf("wrapProtoTaskRunMetadata() error = %#v, wantErr %#v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(tt.run, got) {
				t.Errorf("wrapTaskRunMetadataProto() \ngiven = %#v,\n  got = %#v", tt.run, got)
			}
		})
	}
}
