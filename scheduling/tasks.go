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

	"slices"

	"time"

	"github.com/featureform/fferr"
	"github.com/featureform/ffsync"
	metapb "github.com/featureform/metadata/proto"
	schpb "github.com/featureform/scheduling/proto"
)

type Key interface {
	String() string
}

type TaskMetadataKey struct {
	taskID TaskID
}

func (tmk TaskMetadataKey) String() string {
	if tmk.taskID == nil {
		return "/tasks/metadata/task_id="
	}
	return fmt.Sprintf("/tasks/metadata/task_id=%s", tmk.taskID.String())
}

type TaskID ffsync.OrderedId

func NewIntTaskID(id uint64) TaskID {
	tid := ffsync.Uint64OrderedId(id)
	return tid
}

func ParseTaskID(id string) (TaskID, error) {
	num, err := strconv.ParseUint(id, 10, 64)
	if err != nil {
		return TaskID(ffsync.Uint64OrderedId(0)), fferr.NewInvalidArgumentError(fmt.Errorf("failed to parse task id: %w", err))
	}
	return TaskID(ffsync.Uint64OrderedId(num)), nil
}

type TaskType int32

const (
	ResourceCreation TaskType = TaskType(schpb.TaskType_RESOURCE_CREATION)
	ResourceDeletion TaskType = TaskType(schpb.TaskType_RESOURCE_DELETION)
	HealthCheck      TaskType = TaskType(schpb.TaskType_HEALTH_CHECK)
	Monitoring       TaskType = TaskType(schpb.TaskType_METRICS)
)

func (tt TaskType) String() string {
	return schpb.TaskType_name[int32(tt)]
}

func (tt TaskType) Proto() schpb.TaskType {
	return schpb.TaskType(tt)
}

type TargetType int32

const (
	ProviderTarget    TargetType = TargetType(schpb.TargetType_PROVIDER)
	NameVariantTarget TargetType = TargetType(schpb.TargetType_NAME_VARIANT)
)

func (tt TargetType) String() string {
	return schpb.TargetType_name[int32(tt)]
}

func (tt TargetType) Proto() schpb.TargetType {
	return schpb.TargetType(tt)
}

type Provider struct {
	Name string `json:"name"`
}

func (p Provider) Type() TargetType {
	return ProviderTarget
}

func (nv Provider) FailedError() error {
	err := fferr.NewDependencyFailedErrorf("dependent Provider task failed")
	err.AddDetail("Name", nv.Name)
	return err
}

type NameVariant struct {
	Name         string `json:"name"`
	Variant      string `json:"variant"`
	ResourceType string `json:"type"`
}

func (nv NameVariant) Type() TargetType {
	return NameVariantTarget
}

func (nv NameVariant) FailedError() error {
	err := fferr.NewDependencyFailedErrorf("dependent NameVariant task failed")
	err.AddDetail("Name", nv.Name)
	err.AddDetail("Variant", nv.Variant)
	err.AddDetail("ResourceType", nv.ResourceType)
	return err
}

type TaskTarget interface {
	Type() TargetType
	FailedError() error
}

type Type string

type TaskMetadata struct {
	ID          TaskID     `json:"id"`
	Name        string     `json:"name"`
	TaskType    TaskType   `json:"type"`
	Target      TaskTarget `json:"target"`
	TargetType  TargetType `json:"targetType"`
	DateCreated time.Time  `json:"dateCreated"`
}

func (t *TaskMetadata) Marshal() ([]byte, error) {
	bytes, err := json.Marshal(t)
	if err != nil {
		errMessage := fmt.Errorf("failed to serialize task metadata: %w", err)
		return nil, fferr.NewInternalError(errMessage)
	}
	return bytes, nil
}

func (t *TaskMetadata) Unmarshal(data []byte) error {
	type tempConfig struct {
		ID          uint64          `json:"id"`
		Name        string          `json:"name"`
		TaskType    TaskType        `json:"type"`
		Target      json.RawMessage `json:"target"`
		TargetType  TargetType      `json:"targetType"`
		DateCreated time.Time       `json:"dateCreated"`
	}

	var temp tempConfig
	if err := json.Unmarshal(data, &temp); err != nil {
		errMessage := fmt.Errorf("failed to deserialize task metadata: %w", err)
		return fferr.NewInternalError(errMessage)
	}

	id := ffsync.Uint64OrderedId(temp.ID)
	t.ID = TaskID(id)

	if temp.Name == "" {
		return fferr.NewInvalidArgumentError(fmt.Errorf("task metadata is missing name"))
	}
	t.Name = temp.Name

	if temp.TaskType.String() == "" {
		return fferr.NewInvalidArgumentError(fmt.Errorf("task metadata is missing TaskType"))
	}

	validTypes := []TaskType{ResourceCreation, HealthCheck, Monitoring, ResourceDeletion}
	if !slices.Contains(validTypes, temp.TaskType) {
		err := fferr.NewInvalidArgumentError(fmt.Errorf("task metadata has invalid TaskType"))
		err.AddDetail("TaskType", string(temp.TaskType))
		return err
	}
	t.TaskType = temp.TaskType

	if temp.DateCreated.IsZero() {
		return fferr.NewInvalidArgumentError(fmt.Errorf("task metadata is missing DateCreated"))
	}
	t.DateCreated = temp.DateCreated

	t.TargetType = temp.TargetType

	switch temp.TargetType {
	case ProviderTarget:
		var provider Provider
		if err := json.Unmarshal(temp.Target, &provider); err != nil {
			errMessage := fmt.Errorf("failed to deserialize Provider data: %w", err)
			return fferr.NewInternalError(errMessage)
		}
		t.Target = provider
	case NameVariantTarget:
		var nameVariant NameVariant
		if err := json.Unmarshal(temp.Target, &nameVariant); err != nil {
			errMessage := fmt.Errorf("failed to deserialize NameVariant data: %w", err)
			return fferr.NewInternalError(errMessage)
		}
		t.Target = nameVariant
	default:
		err := fferr.NewInvalidArgumentError(fmt.Errorf("unknown target type"))
		err.AddDetail("TargetType", string(temp.TargetType))
		return err
	}
	return nil
}

func (task *TaskMetadata) ToProto() (*schpb.TaskMetadata, error) {
	taskMetadata := &schpb.TaskMetadata{
		Id:         &schpb.TaskID{Id: task.ID.String()},
		Name:       task.Name,
		Type:       task.TaskType.Proto(),
		TargetType: task.TargetType.Proto(),
		Created:    wrapTimestampProto(task.DateCreated),
	}

	taskMetadata, err := setTaskMetadataTargetProto(taskMetadata, task.Target)
	if err != nil {
		return nil, err
	}

	return taskMetadata, nil
}

func WrapProtoTaskMetadata(task *schpb.TaskMetadata) (TaskMetadata, error) {
	tid, err := ParseTaskRunID(task.GetId().Id)
	if err != nil {
		return TaskMetadata{}, err
	}

	t, err := convertTaskProtoTarget(task.Target)
	if err != nil {
		return TaskMetadata{}, err
	}

	return TaskMetadata{
		ID:          tid,
		Name:        task.Name,
		TaskType:    TaskType(task.Type),
		Target:      t,
		TargetType:  TargetType(task.TargetType),
		DateCreated: task.Created.AsTime(),
	}, nil
}

func convertTaskProtoTarget(target interface{}) (TaskTarget, error) {
	switch t := target.(type) {
	case *schpb.TaskMetadata_NameVariant:
		return NameVariant{
			Name:         t.NameVariant.ResourceID.Resource.Name,
			Variant:      t.NameVariant.ResourceID.Resource.Variant,
			ResourceType: t.NameVariant.ResourceID.ResourceType.String(),
		}, nil
	case *schpb.TaskMetadata_Provider:
		return Provider{
			Name: t.Provider.Name,
		}, nil
	default:
		return nil, fferr.NewUnimplementedErrorf("could not convert target proto type: %T", target)
	}
}

func getTaskNameVariantTargetProto(target NameVariant) *schpb.TaskMetadata_NameVariant {
	return &schpb.TaskMetadata_NameVariant{
		NameVariant: &schpb.NameVariantTarget{
			ResourceID: &metapb.ResourceID{
				Resource: &metapb.NameVariant{
					Name:    target.Name,
					Variant: target.Variant,
				},
				ResourceType: metapb.ResourceType(metapb.ResourceType_value[target.ResourceType]),
			},
		},
	}
}

func getProviderTargetProto(target Provider) *schpb.TaskMetadata_Provider {
	return &schpb.TaskMetadata_Provider{
		Provider: &schpb.ProviderTarget{
			Name: target.Name,
		},
	}
}

func setTaskMetadataTargetProto(proto *schpb.TaskMetadata, target TaskTarget) (*schpb.TaskMetadata, error) {
	switch t := target.(type) {
	case NameVariant:
		proto.Target = getTaskNameVariantTargetProto(t)
	case Provider:
		proto.Target = getProviderTargetProto(t)
	default:
		return nil, fferr.NewUnimplementedErrorf("could not convert target to proto: type: %T", target)
	}
	return proto, nil
}
