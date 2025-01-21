// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package tasks

import (
	"context"
	"fmt"
	"time"

	"github.com/featureform/coordinator/spawner"
	"github.com/featureform/fferr"
	"github.com/featureform/logging"
	"github.com/featureform/metadata"
	"github.com/featureform/scheduling"
)

const Noop = "Noop"

func NewResourceCreationFactory(task BaseTask) (Task, error) {
	res, ok := task.taskDef.Target.(scheduling.NameVariant)
	if !ok {
		return nil, fferr.NewInternalErrorf("cannot create a task from target type: %s", task.taskDef.TargetType)
	}
	switch res.ResourceType {
	case metadata.FEATURE_VARIANT.String():
		return &FeatureTask{BaseTask: task}, nil
	case metadata.LABEL_VARIANT.String():
		return &LabelTask{BaseTask: task}, nil
	case metadata.SOURCE_VARIANT.String():
		return &SourceTask{BaseTask: task}, nil
	case metadata.TRAINING_SET_VARIANT.String():
		return &TrainingSetTask{BaseTask: task}, nil
	case Noop:
		return &NoopTask{BaseTask: task}, nil
	default:
		return nil, fferr.NewInternalErrorf("cannot create a task from target type: %s", res.ResourceType)
	}
}

type Task interface {
	Run() error
}

func init() {
	unregisteredFactories := map[scheduling.TargetType]Factory{
		scheduling.NameVariantTarget: NewResourceCreationFactory,
	}
	for name, factory := range unregisteredFactories {
		if err := RegisterFactory(name, factory); err != nil {
			panic(err)
		}
	}
}

type Factory func(task BaseTask) (Task, error)

var factories = make(map[scheduling.TargetType]Factory)

func RegisterFactory(t scheduling.TargetType, f Factory) error {
	if _, has := factories[t]; has {
		return fferr.NewInternalError(fmt.Errorf("%s Task factory already exists", t))
	}
	factories[t] = f
	return nil
}

func Get(t scheduling.TargetType, base BaseTask) (Task, error) {
	f, has := factories[t]
	if !has {
		return nil, fferr.NewInternalError(fmt.Errorf("no Task of type: %s", t))
	}
	return f(base)
}

type TaskConfig struct {
	DependencyPollInterval time.Duration
}

type BaseTask struct {
	metadata           *metadata.Client
	taskDef            scheduling.TaskRunMetadata
	lastSuccessfulTask scheduling.TaskRunMetadata
	isUpdate           bool
	isDelete           bool
	spawner            spawner.JobSpawner
	logger             logging.Logger
	config             TaskConfig
}

func NewBaseTask(
	metadata *metadata.Client,
	taskDef scheduling.TaskRunMetadata,
	lastSuccessfulTask scheduling.TaskRunMetadata,
	isUpdate bool,
	isDelete bool,
	spawner spawner.JobSpawner,
	logger logging.Logger,
	config TaskConfig,
) BaseTask {
	return BaseTask{
		metadata:           metadata,
		taskDef:            taskDef,
		lastSuccessfulTask: lastSuccessfulTask,
		isUpdate:           isUpdate,
		isDelete:           isDelete,
		spawner:            spawner,
		logger:             logger,
		config:             config,
	}
}

func (bt *BaseTask) Redacted() map[string]any {
	return map[string]any{
		"task-def":        bt.taskDef,
		"last-successful": bt.lastSuccessfulTask,
		"is-update":       bt.isUpdate,
		"spawner-type":    fmt.Sprintf("%T", bt.spawner),
		"task-config":     bt.config,
	}

}

func (bt *BaseTask) waitForRunCompletion(id []scheduling.TaskRunID) error {
	return nil
}

func (t *BaseTask) awaitPendingSource(sourceNameVariant metadata.NameVariant) (*metadata.SourceVariant, error) {
	sourceStatus := scheduling.PENDING
	for sourceStatus != scheduling.READY {
		source, err := t.metadata.GetSourceVariant(context.Background(), sourceNameVariant)
		if err != nil {
			return nil, err
		}
		var sourceType fferr.ResourceType
		if source.IsTransformation() {
			sourceType = fferr.TRANSFORMATION
		} else {
			sourceType = fferr.PRIMARY_DATASET
		}
		sourceStatus = source.Status()
		if sourceStatus == scheduling.FAILED {
			err := fferr.NewResourceFailedError(sourceNameVariant.Name, sourceNameVariant.Variant, sourceType, fmt.Errorf("required dataset is in a failed state"))
			return nil, err
		}
		if sourceStatus == scheduling.READY {
			return source, nil
		}
		time.Sleep(t.config.DependencyPollInterval)
	}
	return t.metadata.GetSourceVariant(context.Background(), sourceNameVariant)
}
