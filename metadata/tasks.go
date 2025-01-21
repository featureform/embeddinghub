// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package metadata

import (
	"context"
	"io"

	"github.com/featureform/ffsync"
	"github.com/featureform/logging"
	"github.com/featureform/metadata/proto"
	ptypes "github.com/featureform/provider/types"
	s "github.com/featureform/scheduling"
	schproto "github.com/featureform/scheduling/proto"
	"google.golang.org/grpc"
	grpcstatus "google.golang.org/grpc/status"
	tspb "google.golang.org/protobuf/types/known/timestamppb"
)

type Empty schproto.Empty

type TaskService interface {
	CreateRun(name string, id s.TaskID, trigger s.Trigger) (s.TaskRunID, error)
	SyncUnfinishedRuns() error
	GetTaskByID(id s.TaskID) (s.TaskMetadata, error)
	WatchForCancel(tid s.TaskID, id s.TaskRunID) (chan s.Status, chan error)
	GetAllRuns() (s.TaskRunList, error)
	GetUnfinishedRuns() (s.TaskRunList, error)
	GetRuns(tid s.TaskID) (s.TaskRunList, error)
	GetRun(tid s.TaskID, id s.TaskRunID) (s.TaskRunMetadata, error)
	GetLatestRun(id s.TaskID) (s.TaskRunMetadata, error)
	SetRunStatus(tid s.TaskID, runID s.TaskRunID, status s.Status, errMsg error) error
	SetRunResumeID(tid s.TaskID, runID s.TaskRunID, resumeID ptypes.ResumeID) error
	AddRunLog(taskID s.TaskID, runID s.TaskRunID, msg string) error
	EndRun(tid s.TaskID, runID s.TaskRunID) error
}

type Tasks struct {
	logger   logging.Logger
	conn     *grpc.ClientConn
	GrpcConn schproto.TasksClient
}

func (t *Tasks) CreateRun(name string, id s.TaskID, trigger s.Trigger) (s.TaskRunID, error) {
	rid, err := t.GrpcConn.CreateTaskRun(
		context.Background(),
		&schproto.CreateRunRequest{
			Name: name,
			TaskID: &schproto.TaskID{
				Id: id.String(),
			},
			Trigger: &schproto.CreateRunRequest_Apply{
				Apply: &schproto.OnApply{Name: "Apply"},
			},
		},
	)
	if err != nil {
		return nil, err
	}

	return s.ParseTaskRunID(rid.Id)
}

func (t *Tasks) SyncUnfinishedRuns() error {
	t.logger.Info("Syncing unfinished runs")
	_, err := t.GrpcConn.SyncUnfinishedRuns(context.Background(), &schproto.Empty{})
	return err
}

func (t *Tasks) GetTaskByID(id s.TaskID) (s.TaskMetadata, error) {
	t.logger.Debugw("Getting task by id", "id", id.String())
	task, err := t.GrpcConn.GetTaskByID(context.Background(), &schproto.TaskID{Id: id.String()})
	if err != nil {
		return s.TaskMetadata{}, err
	}
	t.logger.Debugw("Converting proto to TaskMetadata", "id", id.String())
	metadata, err := s.WrapProtoTaskMetadata(task)
	if err != nil {
		return s.TaskMetadata{}, err
	}
	return metadata, nil
}

func (t *Tasks) WatchForCancel(tid s.TaskID, rid s.TaskRunID) (chan s.Status, chan error) {
	t.logger.Debugw("Watching for cancel", "task_id", tid.String(), "run_id", rid.String())
	statusChannel := make(chan s.Status, 1)
	waitErr := make(chan error, 1)
	go func() {
		t.logger.Debugw("Starting cancel watch request", "task_id", tid.String(), "run_id", rid.String())
		status, err := t.GrpcConn.WatchForCancel(
			context.Background(),
			&schproto.TaskRunID{
				RunID:  &schproto.RunID{Id: rid.String()},
				TaskID: &schproto.TaskID{Id: tid.String()},
			})
		if err != nil {
			waitErr <- err
		}
		statusChannel <- s.Status(status.Status)
	}()
	return statusChannel, waitErr
}

type runStream interface {
	grpc.ClientStream
}

func (t *Tasks) genericParseRuns(client runStream) (s.TaskRunList, error) {
	runs := s.TaskRunList{}
	for {
		var msg schproto.TaskRunMetadata
		err := client.RecvMsg(&msg)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		wrappedRun, err := s.TaskRunMetadataFromProto(&msg)
		if err != nil {
			return s.TaskRunList{}, err
		}
		runs = append(runs, wrappedRun)

	}
	return runs, nil
}

func (t *Tasks) GetAllRuns() (s.TaskRunList, error) {
	client, err := t.GrpcConn.GetAllRuns(context.Background(), &schproto.Empty{})
	if err != nil {
		return s.TaskRunList{}, err
	}

	runs, err := t.genericParseRuns(client)
	if err != nil {
		return s.TaskRunList{}, err
	}
	return runs, nil
}

func (t *Tasks) GetRuns(id s.TaskID) (s.TaskRunList, error) {
	t.logger.Debugw("Getting all runs for task", "task_id", id.String())
	client, err := t.GrpcConn.GetRuns(context.Background(), &schproto.TaskID{Id: id.String()})
	if err != nil {
		return s.TaskRunList{}, err
	}

	runs, err := t.genericParseRuns(client)
	if err != nil {
		return s.TaskRunList{}, err
	}
	return runs, nil
}

func (t *Tasks) GetUnfinishedRuns() (s.TaskRunList, error) {
	t.logger.Debugw("Getting all incomplete runs")
	client, err := t.GrpcConn.GetUnfinishedRuns(context.Background(), &schproto.Empty{})
	if err != nil {
		return s.TaskRunList{}, err
	}

	runs, err := t.genericParseRuns(client)
	if err != nil {
		return s.TaskRunList{}, err
	}
	return runs, nil
}

func (t *Tasks) GetRun(tid s.TaskID, rid s.TaskRunID) (s.TaskRunMetadata, error) {
	t.logger.Debugw("Getting run", "task_id", tid.String(), "run_id", rid.String())
	meta, err := t.GrpcConn.GetRunMetadata(context.Background(), &schproto.TaskRunID{
		RunID:  &schproto.RunID{Id: rid.String()},
		TaskID: &schproto.TaskID{Id: tid.String()},
	})
	if err != nil {
		return s.TaskRunMetadata{}, err
	}
	parsed, err := s.TaskRunMetadataFromProto(meta)
	if err != nil {
		return s.TaskRunMetadata{}, err
	}
	return parsed, nil
}

func (t *Tasks) GetLatestRun(id s.TaskID) (s.TaskRunMetadata, error) {
	t.logger.Debugw("Getting latest run", "task_id", id.String())
	run, err := t.GrpcConn.GetLatestRun(context.Background(), &schproto.TaskID{Id: id.String()})
	if err != nil {
		return s.TaskRunMetadata{}, err
	}
	t.logger.Debugw("Converting proto to TaskRunMetadata", "task_id", id.String())
	wrappedRun, err := s.TaskRunMetadataFromProto(run)
	if err != nil {
		return s.TaskRunMetadata{}, err
	}
	return wrappedRun, nil
}

func (t *Tasks) SetRunStatus(tid s.TaskID, runID s.TaskRunID, status s.Status, errMsg error) error {
	t.logger.Debugw("Setting run status", "task_id", tid.String(), "run_id", runID.String(), "status", status.String())
	msg := ""
	if errMsg != nil {
		msg = errMsg.Error()
	}
	errorStatus, ok := grpcstatus.FromError(errMsg)
	errorProto := errorStatus.Proto()
	var errorStatusProto *proto.ErrorStatus
	if ok && errMsg != nil {
		errorStatusProto = &proto.ErrorStatus{Code: errorProto.Code, Message: errorProto.Message, Details: errorProto.Details}
	} else {
		errorStatusProto = nil
	}

	resourceStatus := proto.ResourceStatus{Status: proto.ResourceStatus_Status(status), ErrorMessage: msg, ErrorStatus: errorStatusProto}

	update := &schproto.StatusUpdate{
		RunID:  &schproto.RunID{Id: runID.String()},
		TaskID: &schproto.TaskID{Id: tid.String()},
		Status: &resourceStatus,
	}

	_, err := t.GrpcConn.SetRunStatus(context.Background(), update)
	if err != nil {
		return err
	}
	return nil
}

func (t *Tasks) SetRunResumeID(tid s.TaskID, runID s.TaskRunID, resumeID ptypes.ResumeID) error {
	logger := t.logger.WithValues(map[string]any{
		"task_id":   tid.String(),
		"run_id":    runID.String(),
		"resume_id": resumeID.String(),
	})
	logger.Debugw("Setting resumeID")
	update := &schproto.ResumeIDUpdate{
		RunID:    &schproto.RunID{Id: runID.String()},
		TaskID:   &schproto.TaskID{Id: tid.String()},
		ResumeID: &schproto.ResumeID{Id: resumeID.String()},
	}

	_, err := t.GrpcConn.SetRunResumeID(context.Background(), update)
	if err != nil {
		logger.Errorw("Failed to set resume ID", "error", err)
		return err
	}
	return nil
}

func (t *Tasks) AddRunLog(tid s.TaskID, runID s.TaskRunID, msg string) error {
	t.logger.Debugw("Adding run log", "task_id", tid.String(), "run_id", runID.String(), "msg", msg)
	log := &schproto.Log{RunID: &schproto.RunID{Id: runID.String()}, TaskID: &schproto.TaskID{Id: tid.String()}, Log: msg}
	_, err := t.GrpcConn.AddRunLog(context.Background(), log)
	if err != nil {
		return err
	}
	return nil
}

func (t *Tasks) EndRun(tid s.TaskID, runID s.TaskRunID) error {
	t.logger.Debugw("Ending run", "task_id", tid.String(), "run_id", runID.String())
	update := &schproto.RunEndTimeUpdate{
		TaskID: &schproto.TaskID{Id: tid.String()},
		End:    tspb.Now(),
		RunID:  &schproto.RunID{Id: runID.String()},
	}
	_, err := t.GrpcConn.SetRunEndTime(context.Background(), update)
	if err != nil {
		return err
	}
	return nil
}

type TaskLocker struct {
	Locker ffsync.Locker
}

func (tl *TaskLocker) LockTask(id s.TaskID, wait bool) (unlock func() error, err error) {
	runKey := ExecutorTaskLockPath(id)
	logger := logging.NewLogger("metadata.TaskLocker.LockTask").With("key", runKey, "wait", wait)
	logger.Debug("Locking Task Key")
	lock, err := tl.Locker.Lock(context.Background(), runKey, wait)
	if err != nil {
		return nil, err
	}
	return func() error {
		return tl.Unlock(lock)
	}, nil
}

func (tl *TaskLocker) LockRun(id s.TaskRunID, wait bool) (unlock func() error, err error) {
	runKey := ExecutorTaskRunLockPath(id)
	logger := logging.NewLogger("metadata.TaskLocker.LockRun").With("key", runKey, "wait", wait)
	logger.Debug("Locking Task Run Key")
	lock, err := tl.Locker.Lock(context.Background(), runKey, wait)
	if err != nil {
		return nil, err
	}
	return func() error {
		return tl.Unlock(lock)
	}, nil
}

func (tl *TaskLocker) Unlock(key ffsync.Key) error {
	return tl.Locker.Unlock(context.Background(), key)
}
