// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package scheduling

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	mapset "github.com/deckarep/golang-set/v2"

	pb "github.com/featureform/metadata/proto"
	sch "github.com/featureform/scheduling/proto"
	tspb "google.golang.org/protobuf/types/known/timestamppb"

	"github.com/featureform/fferr"
	"github.com/featureform/ffsync"
	ptypes "github.com/featureform/provider/types"
)

const (
	taskRunIncompleteKeyPrefix = "/tasks/incomplete/runs/run_id="
	taskRunKeyPrefix           = "/tasks/runs/task_id="
	taskRunMetadataKeyPrefix   = "/tasks/runs/metadata"
)

type TaskRunKey struct {
	taskID TaskID
}

func (trk TaskRunKey) String() string {
	if trk.taskID == nil {
		return "/tasks/runs/task_id="
	}
	return fmt.Sprintf("%s%s", taskRunKeyPrefix, trk.taskID.String())
}

type TaskRunMetadataKey struct {
	taskID TaskID
	runID  TaskRunID
	date   time.Time
}

func (trmk TaskRunMetadataKey) String() string {
	// will return the taskRunMetadataKeyPrefix with date to the minute; ex. /tasks/runs/metadata/2021/01/01/15/04
	key := trmk.TruncateToMinute()

	// adds the task_id and run_id to the key if they're not null
	taskIdIsNotNil := trmk.taskID != nil
	runIdIsNotNil := trmk.runID != nil
	if taskIdIsNotNil && runIdIsNotNil {
		key += fmt.Sprintf("/task_id=%s/run_id=%s", trmk.taskID.String(), trmk.runID.String())
	}
	return key
}

func (trmk TaskRunMetadataKey) TruncateToDay() string {
	dayFormat := "2006/01/02"

	return trmk.pathWithDateFormat(dayFormat)
}

func (trmk TaskRunMetadataKey) TruncateToHour() string {
	hourFormat := "2006/01/02/15"

	return trmk.pathWithDateFormat(hourFormat)
}

func (trmk TaskRunMetadataKey) TruncateToMinute() string {
	minuteFormat := "2006/01/02/15/04"

	return trmk.pathWithDateFormat(minuteFormat)
}

func (trmk TaskRunMetadataKey) pathWithDateFormat(dateFormat string) string {
	key := taskRunMetadataKeyPrefix

	// adds the date to the key if it's not zero
	if !trmk.date.IsZero() {
		key += fmt.Sprintf("/%s", trmk.date.UTC().Format(dateFormat))
	}
	return key
}

type TaskRunID ffsync.OrderedId

func NewIntTaskRunID(id uint64) TaskRunID {
	tid := ffsync.Uint64OrderedId(id)
	return TaskRunID(&tid)
}

func ParseTaskRunID(id string) (TaskRunID, error) {
	num, err := strconv.ParseUint(id, 10, 64)
	if err != nil {
		return TaskRunID(ffsync.Uint64OrderedId(0)), fferr.NewInvalidArgumentError(fmt.Errorf("failed to parse task run id: %w", err))
	}
	return TaskRunID(ffsync.Uint64OrderedId(num)), nil
}

type Status int32

const (
	NO_STATUS Status = Status(pb.ResourceStatus_NO_STATUS)
	CREATED   Status = Status(pb.ResourceStatus_CREATED)
	PENDING   Status = Status(pb.ResourceStatus_PENDING)
	READY     Status = Status(pb.ResourceStatus_READY)
	FAILED    Status = Status(pb.ResourceStatus_FAILED)
	RUNNING   Status = Status(pb.ResourceStatus_RUNNING)
	CANCELLED Status = Status(pb.ResourceStatus_CANCELLED)
)

var validStatusTransitions = map[Status]mapset.Set[Status]{
	NO_STATUS: mapset.NewSet(CREATED, PENDING),
	CREATED:   mapset.NewSet(PENDING),
	PENDING:   mapset.NewSet(RUNNING, CANCELLED),
	RUNNING:   mapset.NewSet(READY, FAILED, CANCELLED),
	READY:     mapset.NewSet[Status](),
	FAILED:    mapset.NewSet[Status](),
	CANCELLED: mapset.NewSet[Status](),
}

func (s Status) String() string {
	return pb.ResourceStatus_Status_name[int32(s)]
}

func (s Status) Proto() pb.ResourceStatus_Status {
	return pb.ResourceStatus_Status(s)
}

func (s Status) validateTransition(to Status) error {
	if validTransitions, ok := validStatusTransitions[s]; !ok {
		return fferr.NewInternalErrorf("status %s does not exist", s)
	} else {
		if validTransitions.Contains(to) {
			return nil
		}
		return fferr.NewInvalidArgumentErrorf("invalid status transition from %s to %s", s, to)
	}
}

type TriggerType int32

const (
	OnApplyTriggerType  TriggerType = TriggerType(sch.TriggerType_ON_APPLY)
	ScheduleTriggerType TriggerType = TriggerType(sch.TriggerType_SCHEDULE)
)

func (tt TriggerType) String() string {
	return sch.TriggerType_name[int32(tt)]
}

func (tt TriggerType) Proto() sch.TriggerType {
	return sch.TriggerType(tt)
}

type Trigger interface {
	Type() TriggerType
	Name() string
}

type OnApplyTrigger struct {
	TriggerName string `json:"triggerName"`
}

func (t OnApplyTrigger) Type() TriggerType {
	return OnApplyTriggerType
}

func (t OnApplyTrigger) Name() string {
	return t.TriggerName
}

type ScheduleTrigger struct {
	TriggerName string `json:"triggerName"`
	Schedule    string `json:"schedule"`
}

func (t ScheduleTrigger) Type() TriggerType {
	return ScheduleTriggerType
}

func (t ScheduleTrigger) Name() string {
	return t.TriggerName
}

type TaskRunMetadata struct {
	ID             TaskRunID       `json:"runId"`
	TaskId         TaskID          `json:"taskId"`
	Name           string          `json:"name"`
	Trigger        Trigger         `json:"trigger"`
	TriggerType    TriggerType     `json:"triggerType"`
	Target         TaskTarget      `json:"target"`
	TargetType     TargetType      `json:"targetType"`
	Dependencies   []TaskRunID     `json:"dependencies"`
	Status         Status          `json:"status"`
	StartTime      time.Time       `json:"startTime"`
	EndTime        time.Time       `json:"endTime"`
	Logs           []string        `json:"logs"`
	Error          string          `json:"error"`
	Dag            TaskDAG         `json:"dag"`
	LastSuccessful TaskRunID       `json:"lastSuccessful"`
	IsDelete       bool            `json:"isDelete"`
	ResumeID       ptypes.ResumeID `json:"resumeID"`
	ErrorProto     *pb.ErrorStatus
}

func (t *TaskRunMetadata) Marshal() ([]byte, error) {
	bytes, err := json.Marshal(t)
	if err != nil {
		return nil, fferr.NewInternalError(fmt.Errorf("failed to marshal TaskRunMetadata: %w", err))
	}
	return bytes, nil
}

func (t *TaskRunMetadata) Unmarshal(data []byte) error {
	type tempConfig struct {
		ID             uint64          `json:"runId"`
		TaskId         uint64          `json:"taskId"`
		Name           string          `json:"name"`
		Trigger        json.RawMessage `json:"trigger"`
		TriggerType    TriggerType     `json:"triggerType"`
		Target         json.RawMessage `json:"target"`
		TargetType     TargetType      `json:"targetType"`
		Status         Status          `json:"status"`
		StartTime      time.Time       `json:"startTime"`
		EndTime        time.Time       `json:"endTime"`
		Logs           []string        `json:"logs"`
		Error          string          `json:"error"`
		ResumeID       string          `json:"resumeID"`
		ErrorProto     *pb.ErrorStatus
		LastSuccessful uint64 `json:"lastSuccessful"`
		IsDelete       bool   `json:"isDelete"`
	}

	var temp tempConfig
	if err := json.Unmarshal(data, &temp); err != nil {
		errMessage := fmt.Errorf("failed to deserialize task run metadata: %w", err)
		return fferr.NewInternalError(errMessage)
	}

	runId := ffsync.Uint64OrderedId(temp.ID)
	t.ID = TaskRunID(runId)

	taskId := ffsync.Uint64OrderedId(temp.TaskId)
	t.TaskId = TaskID(taskId)

	if temp.LastSuccessful == 0 {
		t.LastSuccessful = nil
	} else {
		t.LastSuccessful = ffsync.Uint64OrderedId(temp.LastSuccessful)
	}

	if temp.Name == "" {
		return fferr.NewInvalidArgumentError(fmt.Errorf("task run metadata is missing Name"))
	}

	if temp.StartTime.IsZero() {
		return fferr.NewInvalidArgumentError(fmt.Errorf("task run metadata is missing StartTime"))
	}
	t.Name = temp.Name
	t.Status = temp.Status
	t.StartTime = temp.StartTime
	t.TriggerType = temp.TriggerType
	t.TargetType = temp.TargetType
	t.ResumeID = ptypes.ResumeID(temp.ResumeID)
	t.EndTime = temp.EndTime
	t.Logs = temp.Logs
	t.Error = temp.Error
	t.IsDelete = temp.IsDelete

	triggerMap := make(map[string]interface{})
	if err := json.Unmarshal(temp.Trigger, &triggerMap); err != nil {
		errMessage := fmt.Errorf("failed to deserialize trigger data: %w", err)
		return fferr.NewInternalError(errMessage)
	}

	switch temp.TriggerType {
	case OnApplyTriggerType:
		var oneOffTrigger OnApplyTrigger
		if err := json.Unmarshal(temp.Trigger, &oneOffTrigger); err != nil {
			errMessage := fmt.Errorf("failed to deserialize One Off Trigger data: %w", err)
			return fferr.NewInternalError(errMessage)
		}
		t.Trigger = oneOffTrigger
	case ScheduleTriggerType:
		var scheduleTrigger ScheduleTrigger
		if err := json.Unmarshal(temp.Trigger, &scheduleTrigger); err != nil {
			errMessage := fmt.Errorf("failed to deserialize Schedule Trigger data: %w", err)
			return fferr.NewInternalError(errMessage)
		}
		t.Trigger = scheduleTrigger
	default:
		errMessage := fmt.Errorf("unknown trigger type: %s", temp.TriggerType)
		return fferr.NewInvalidArgumentError(errMessage)
	}

	targetMap := make(map[string]interface{})
	if err := json.Unmarshal(temp.Target, &targetMap); err != nil {
		errMessage := fmt.Errorf("failed to deserialize target data: %w", err)
		return fferr.NewInternalError(errMessage)
	}

	switch temp.TargetType {
	case NameVariantTarget:
		var nvTarget NameVariant
		if err := json.Unmarshal(temp.Target, &nvTarget); err != nil {
			errMessage := fmt.Errorf("failed to deserialize NameVariant target data: %w", err)
			return fferr.NewInternalError(errMessage)
		}
		t.Target = nvTarget
	case ProviderTarget:
		var providerTarget Provider
		if err := json.Unmarshal(temp.Target, &providerTarget); err != nil {
			errMessage := fmt.Errorf("failed to deserialize Schedule Trigger data: %w", err)
			return fferr.NewInternalError(errMessage)
		}
		t.Target = providerTarget
	default:
		errMessage := fmt.Errorf("unknown target type: %s", temp.Target)
		return fferr.NewInvalidArgumentError(errMessage)
	}

	return nil
}

func (run *TaskRunMetadata) ToProto() (*sch.TaskRunMetadata, error) {
	var lsid *sch.RunID
	if run.LastSuccessful == nil {
		lsid = &sch.RunID{Id: ""}
	} else {
		lsid = &sch.RunID{Id: run.LastSuccessful.String()}
	}
	taskRunMetadata := &sch.TaskRunMetadata{
		RunID:       &sch.RunID{Id: run.ID.String()},
		TaskID:      &sch.TaskID{Id: run.TaskId.String()},
		Name:        run.Name,
		TriggerType: run.TriggerType.Proto(),
		TargetType:  run.TargetType.Proto(),
		StartTime:   wrapTimestampProto(run.StartTime),
		EndTime:     wrapTimestampProto(run.EndTime),
		Logs:        run.Logs,
		Status: &pb.ResourceStatus{
			Status:       pb.ResourceStatus_Status(run.Status),
			ErrorMessage: run.Error,
			ErrorStatus:  run.ErrorProto,
		},
		ResumeID:       &sch.ResumeID{Id: run.ResumeID.String()},
		LastSuccessful: lsid,
		IsDelete:       run.IsDelete,
	}

	taskRunMetadata, err := setTriggerProto(taskRunMetadata, run.Trigger)
	if err != nil {
		return nil, err
	}

	taskRunMetadata, err = setTaskRunMetadataTargetProto(taskRunMetadata, run.Target)
	if err != nil {
		return nil, err
	}

	return taskRunMetadata, nil
}

func setTriggerProto(proto *sch.TaskRunMetadata, trigger Trigger) (*sch.TaskRunMetadata, error) {
	switch t := trigger.(type) {
	case OnApplyTrigger:
		proto.Trigger = getApplyTrigger(t)
	case ScheduleTrigger:
		proto.Trigger = getScheduleTrigger(t)
	default:
		return nil, fferr.NewUnimplementedErrorf("could not convert trigger to proto: type: %T", trigger)
	}
	return proto, nil
}

func wrapTimestampProto(ts time.Time) *tspb.Timestamp {
	return &tspb.Timestamp{
		Seconds: ts.Unix(),
		Nanos:   int32(ts.Nanosecond()),
	}
}

func setTaskRunMetadataTargetProto(proto *sch.TaskRunMetadata, target TaskTarget) (*sch.TaskRunMetadata, error) {
	switch t := target.(type) {
	case NameVariant:
		proto.Target = getTaskRunNameVariantTargetProto(t)
	case Provider:
		proto.Target = getTaskRunProviderTargetProto(t)
	default:
		return nil, fferr.NewUnimplementedErrorf("could not convert target to proto: type: %T", target)
	}
	return proto, nil
}

func getTaskRunNameVariantTargetProto(target NameVariant) *sch.TaskRunMetadata_NameVariant {
	return &sch.TaskRunMetadata_NameVariant{
		NameVariant: &sch.NameVariantTarget{
			ResourceID: &pb.ResourceID{
				Resource: &pb.NameVariant{
					Name:    target.Name,
					Variant: target.Variant,
				},
				ResourceType: pb.ResourceType(pb.ResourceType_value[target.ResourceType]),
			},
		},
	}
}

func getTaskRunProviderTargetProto(target Provider) *sch.TaskRunMetadata_Provider {
	return &sch.TaskRunMetadata_Provider{
		Provider: &sch.ProviderTarget{
			Name: target.Name,
		},
	}
}

func getApplyTrigger(trigger OnApplyTrigger) *sch.TaskRunMetadata_Apply {
	return &sch.TaskRunMetadata_Apply{
		Apply: &sch.OnApply{
			Name: trigger.Name(),
		},
	}
}

func getScheduleTrigger(trigger ScheduleTrigger) *sch.TaskRunMetadata_Schedule {
	return &sch.TaskRunMetadata_Schedule{
		Schedule: &sch.ScheduleTrigger{
			Name:     trigger.Name(),
			Schedule: trigger.Schedule,
		},
	}
}

func TaskRunMetadataFromProto(run *sch.TaskRunMetadata) (TaskRunMetadata, error) {
	rid, err := ParseTaskRunID(run.RunID.Id)
	if err != nil {
		return TaskRunMetadata{}, err
	}
	tid, err := ParseTaskID(run.TaskID.Id)
	if err != nil {
		return TaskRunMetadata{}, err
	}
	var lsid TaskRunID
	if run.LastSuccessful.Id != "" {
		lsid, err = ParseTaskRunID(run.LastSuccessful.Id)
		if err != nil {
			return TaskRunMetadata{}, err
		}
	} else {
		lsid = nil
	}

	t, err := convertProtoTriggerType(run.GetTrigger())
	if err != nil {
		return TaskRunMetadata{}, err
	}
	target, err := convertTaskRunProtoTarget(run.Target)
	if err != nil {
		return TaskRunMetadata{}, err
	}
	return TaskRunMetadata{
		ID:             rid,
		TaskId:         tid,
		Name:           run.Name,
		Trigger:        t,
		TriggerType:    TriggerType(run.TriggerType),
		Target:         target,
		TargetType:     TargetType(run.TargetType),
		Status:         Status(run.Status.Status),
		StartTime:      run.StartTime.AsTime(),
		EndTime:        run.EndTime.AsTime(),
		Logs:           run.Logs,
		Error:          run.Status.ErrorMessage,
		ErrorProto:     run.Status.ErrorStatus,
		ResumeID:       ptypes.ResumeID(run.GetResumeID().GetId()),
		LastSuccessful: lsid,
		IsDelete:       run.IsDelete,
	}, nil
}

func convertProtoTriggerType(trigger interface{}) (Trigger, error) {
	switch t := trigger.(type) {
	case *sch.TaskRunMetadata_Apply:
		return OnApplyTrigger{TriggerName: t.Apply.Name}, nil
	case *sch.TaskRunMetadata_Schedule:
		return ScheduleTrigger{TriggerName: t.Schedule.Name, Schedule: t.Schedule.Schedule}, nil
	default:
		return nil, fferr.NewUnimplementedErrorf("could not convert trigger type: %T", trigger)
	}
}

func convertTaskRunProtoTarget(target interface{}) (TaskTarget, error) {
	switch t := target.(type) {
	case *sch.TaskRunMetadata_NameVariant:
		return NameVariant{
			Name:         t.NameVariant.ResourceID.Resource.Name,
			Variant:      t.NameVariant.ResourceID.Resource.Variant,
			ResourceType: t.NameVariant.ResourceID.ResourceType.String(),
		}, nil
	case *sch.TaskRunMetadata_Provider:
		return Provider{
			Name: t.Provider.Name,
		}, nil
	default:
		return nil, fferr.NewUnimplementedErrorf("could not convert target proto type: %T", target)
	}
}
