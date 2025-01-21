// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/featureform/fferr"
	"github.com/featureform/logging"
	pb "github.com/featureform/metadata/proto"
	"github.com/featureform/storage"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/encoding/protojson"
)

// Create Resource Lookup Using ETCD
type MemoryResourceLookup struct {
	Connection storage.MetadataStorage
}

// Wrapper around Resource/Job messages. Allows top level storage for info about saved value
type MemoryRow struct {
	ResourceType      ResourceType //Resource Type. For use when getting stored keys
	StorageType       StorageType  //Type of storage. Resource or Job
	Message           []byte       //Contents to be stored
	SerializedVersion int          //Checks if its serialized using JSON or proto
}

type MemoryRowTemp struct {
	ResourceType      ResourceType
	StorageType       StorageType //Type of storage. Resource or Job
	Message           []byte      //Contents to be stored
	SerializedVersion int         //Checks if its serialized using JSON or proto
}

// Serializes the entire ETCD Storage Object to be put into ETCD
func (lookup MemoryResourceLookup) serializeResource(res Resource) ([]byte, error) {
	p, err := protojson.Marshal(res.Proto())
	if err != nil {
		return nil, err
	}
	msg := EtcdRowTemp{
		ResourceType:      res.ID().Type,
		Message:           string(p),
		StorageType:       RESOURCE,
		SerializedVersion: 1,
	}

	serialMsg, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}
	return serialMsg, nil
}

// Deserializes object into ETCD Storage Object
func (lookup MemoryResourceLookup) deserialize(value []byte) (EtcdRow, error) {
	var tmp EtcdRowTemp
	if err := json.Unmarshal(value, &tmp); err != nil {
		return EtcdRow{}, errors.Wrap(err, fmt.Sprintf("failed to parse resource: %s", value))
	}
	msg := EtcdRow{
		ResourceType:      tmp.ResourceType,
		StorageType:       tmp.StorageType,
		Message:           tmp.Message,
		SerializedVersion: tmp.SerializedVersion,
	}
	return msg, nil
}

func (lookup MemoryResourceLookup) Lookup(ctx context.Context, id ResourceID, opts ...ResourceLookupOption) (Resource, error) {
	logger := logging.NewLogger("lookup")
	key := createKey(id)
	logger.Infow("Get", "key", key)

	options, err := NewResourceLookupOptions(opts...)
	if err != nil {
		logger.Errorw("Failed to create resource lookup options", "error", err)
		return nil, err
	}
	qOpts := options.generateQueryOpts()

	resp, err := lookup.Connection.Get(key, qOpts...)
	if err != nil || len(resp) == 0 {
		return nil, fferr.NewKeyNotFoundError(key, err)
	}
	logger.Infow("Deserialize", "key", key)
	msg, err := lookup.deserialize([]byte(resp))
	if err != nil {
		return nil, err
	}
	logger.Infow("Create empty resource", "key", key)
	resType, err := CreateEmptyResource(msg.ResourceType)
	if err != nil {
		return nil, err
	}
	logger.Infow("Parse resource", "key", key)
	resource, err := ParseResource(msg, resType)
	if err != nil {
		return nil, err
	}
	logger.Infow("Return", "key", key)
	return resource, nil
}

func (lookup MemoryResourceLookup) Delete(ctx context.Context, id ResourceID) error {
	logger := logging.NewLogger("lookup")
	key := createKey(id)
	logger.Infow("Delete", "key", key)
	_, err := lookup.Connection.Delete(key)
	if err != nil {
		return fferr.NewKeyNotFoundError(key, err)
	}
	return nil
}

func (lookup MemoryResourceLookup) GetCountWithPrefix(ctx context.Context, id string) (int, error) {
	get, err := lookup.Connection.List(id)
	if err != nil {
		return 0, err
	}
	return len(get), nil
}

func (lookup MemoryResourceLookup) Has(ctx context.Context, id ResourceID) (bool, error) {
	key := createKey(id)
	count, err := lookup.GetCountWithPrefix(ctx, key)
	if err != nil {
		return false, err
	}
	if count == 0 {
		return false, nil
	}
	return true, nil
}

func (lookup MemoryResourceLookup) HasJob(ctx context.Context, id ResourceID) (bool, error) {
	job_key := GetJobKey(id)
	count, err := lookup.GetCountWithPrefix(ctx, job_key)
	if err != nil {
		return false, err
	}
	if count == 0 {
		return false, nil
	}
	return true, nil
}

func (lookup MemoryResourceLookup) SetJob(ctx context.Context, id ResourceID, schedule string) error {
	if jobAlreadySet, _ := lookup.HasJob(ctx, id); jobAlreadySet {
		return fferr.NewInternalErrorf("job %v has already been created", id)
	}
	coordinatorJob := CoordinatorJob{
		Attempts: 0,
		Resource: id,
		Schedule: schedule,
	}
	serialized, err := coordinatorJob.Serialize()
	if err != nil {
		return err
	}
	jobKey := GetJobKey(id)
	if err := lookup.Connection.Create(jobKey, string(serialized)); err != nil {
		return err
	}

	return nil
}

func (lookup MemoryResourceLookup) SetSchedule(ctx context.Context, id ResourceID, schedule string) error {
	coordinatorScheduleJob := CoordinatorScheduleJob{
		Attempts: 0,
		Resource: id,
		Schedule: schedule,
	}
	serialized, err := coordinatorScheduleJob.Serialize()
	if err != nil {
		return err
	}
	jobKey := GetScheduleJobKey(id)
	if err := lookup.Connection.Create(jobKey, string(serialized)); err != nil {
		return err
	}
	return nil
}

func (lookup MemoryResourceLookup) Set(ctx context.Context, id ResourceID, res Resource) error {
	serRes, err := lookup.serializeResource(res)
	if err != nil {
		return err
	}
	key := createKey(id)
	if err := lookup.Connection.Create(key, string(serRes)); err != nil {
		return err
	}
	return nil
}

func (lookup MemoryResourceLookup) Submap(ctx context.Context, ids []ResourceID) (ResourceLookup, error) {
	resources := make(LocalResourceLookup, len(ids))

	for _, id := range ids {
		key := createKey(id)
		resp, err := lookup.Connection.Get(key)
		if err != nil {
			return nil, fferr.NewKeyNotFoundError(key, err)
		}
		etcdStore, err := lookup.deserialize([]byte(resp))
		if err != nil {
			return nil, err
		}

		resource, err := CreateEmptyResource(etcdStore.ResourceType)
		if err != nil {
			return nil, err
		}

		res, err := ParseResource(etcdStore, resource)
		if err != nil {
			return nil, err
		}
		resources[id] = res
	}
	return resources, nil
}

func (lookup MemoryResourceLookup) ListForType(ctx context.Context, t ResourceType) ([]Resource, error) {
	resources := make([]Resource, 0)
	resp, err := lookup.Connection.List(t.String())
	if err != nil {
		return nil, err
	}
	for _, v := range resp {
		etcdStore, err := lookup.deserialize([]byte(v))
		if err != nil {
			return nil, err
		}
		resource, err := CreateEmptyResource(etcdStore.ResourceType)
		if err != nil {
			return nil, err
		}
		parsedResource, err := ParseResource(etcdStore, resource)
		if err != nil {
			logging.GlobalLogger.Errorw("Failed to parse resource", "error", err)
			return nil, err
		}
		if parsedResource.ID().Type == t {
			resources = append(resources, parsedResource)
		}
	}
	return resources, nil
}

func (lookup MemoryResourceLookup) ListVariants(ctx context.Context, t ResourceType, name string, opts ...ResourceLookupOption) ([]Resource, error) {
	logger := logging.NewLogger("memmory_lookup.go:ListVariants")
	startTime := time.Now()
	resources := make([]Resource, 0)
	logger.Infow("list variants with prefix", "type", t, "name", name)

	options, err := NewResourceLookupOptions(opts...)
	if err != nil {
		logger.Errorw("Failed to create resource lookup options", "error", err)
		return nil, err
	}
	qOpts := options.generateQueryOpts()

	resp, err := lookup.Connection.List(variantLookupPrefix(t, name), qOpts...)
	logger.Infow("listed variants with prefix", "type", t, "name", name, "duration", time.Since(startTime))
	if err != nil {
		return nil, err
	}
	startParsingTime := time.Now()
	for _, v := range resp {
		etcdStore, err := lookup.deserialize([]byte(v))
		if err != nil {
			return nil, err
		}
		resource, err := CreateEmptyResource(etcdStore.ResourceType)
		if err != nil {
			return nil, err
		}
		resource, err = ParseResource(etcdStore, resource)
		if err != nil {
			return nil, err
		}
		id := resource.ID()
		if id.Type == t && id.Name == name {
			resources = append(resources, resource)
		}
	}
	logger.Infow("parsed variants with prefix", "type", t, "name", name, "duration", time.Since(startParsingTime))
	return resources, nil
}

func (lookup MemoryResourceLookup) List(ctx context.Context) ([]Resource, error) {
	resources := make([]Resource, 0)
	resp, err := lookup.Connection.List("")
	if err != nil {
		return nil, err
	}
	for _, v := range resp {
		etcdStore, err := lookup.deserialize([]byte(v))
		if err != nil {
			return nil, err
		}
		resource, err := CreateEmptyResource(etcdStore.ResourceType)
		if err != nil {
			return nil, err
		}
		resource, err = ParseResource(etcdStore, resource)
		if err != nil {
			return nil, err
		}
		resources = append(resources, resource)
	}
	return resources, nil
}

func (lookup *MemoryResourceLookup) SetStatus(ctx context.Context, id ResourceID, status *pb.ResourceStatus) error {
	res, err := lookup.Lookup(ctx, id)
	if err != nil {
		return err
	}
	if err := res.UpdateStatus(status); err != nil {
		return err
	}
	if err := lookup.Set(ctx, id, res); err != nil {
		return err
	}
	return nil
}
