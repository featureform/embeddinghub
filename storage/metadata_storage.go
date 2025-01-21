// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package storage

import (
	"context"

	"github.com/featureform/ffsync"
	"github.com/featureform/logging"
	"github.com/featureform/storage/query"
	"github.com/google/uuid"
)

type MetadataStorage struct {
	Locker          ffsync.Locker
	Storage         metadataStorageImplementation
	Logger          logging.Logger
	SkipListLocking bool
}

func (s *MetadataStorage) unlockWithLogger(ctx context.Context, Locker ffsync.Locker, key ffsync.Key, logger logging.Logger) {
	err := Locker.Unlock(ctx, key)
	if err != nil {
		logger.Errorw("Error unlocking key: ", err)
	}
}

func (s *MetadataStorage) Create(key string, value string) error {
	ctx := context.Background()
	reqID := uuid.NewString()
	ctx = context.WithValue(ctx, "request_id", reqID)

	logger := s.Logger.With("key", key, "request_id", reqID)
	logger.Debug("Creating key")
	lock, err := s.Locker.Lock(ctx, key, true)
	if err != nil {
		return err
	}

	defer s.unlockWithLogger(ctx, s.Locker, lock, logger)

	return s.Storage.Set(key, value)
}

func (s *MetadataStorage) MultiCreate(data map[string]string) error {
	ctx := context.Background()
	reqID := uuid.NewString()
	ctx = context.WithValue(ctx, "request_id", reqID)

	logger := s.Logger.With("keys", data, "request_id", reqID)
	logger.Debug("Creating multiple keys")
	// Lock all keys before setting any values
	for key := range data {
		lock, err := s.Locker.Lock(ctx, key, true)
		if err != nil {
			return err
		}

		defer s.unlockWithLogger(ctx, s.Locker, lock, logger)
	}

	// Set all values
	for key, value := range data {
		err := s.Storage.Set(key, value)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *MetadataStorage) Update(key string, updateFn func(string) (string, error)) error {
	ctx := context.Background()
	reqID := uuid.NewString()
	ctx = context.WithValue(ctx, "request_id", reqID)

	logger := s.Logger.With("key", key, "request_id", reqID)
	logger.Debug("Updating key")
	lock, err := s.Locker.Lock(ctx, key, true)
	if err != nil {
		return err
	}
	defer s.unlockWithLogger(ctx, s.Locker, lock, logger)

	currentValue, err := s.Storage.Get(key)
	if err != nil {
		return err
	}

	newValue, err := updateFn(currentValue)
	if err != nil {
		return err
	}

	return s.Storage.Set(key, newValue)
}

func (s *MetadataStorage) List(prefix string, opts ...query.Query) (map[string]string, error) {
	ctx := context.Background()
	reqID := uuid.NewString()
	ctx = context.WithValue(ctx, "request_id", reqID)

	logger := s.Logger.With("prefix", prefix, "request_id", reqID)
	logger.Debug("Listing keys")
	if !s.SkipListLocking {
		lock, err := s.Locker.Lock(ctx, prefix, true)
		if err != nil {
			return nil, err
		}
		defer s.unlockWithLogger(ctx, s.Locker, lock, logger)
	}

	return s.Storage.List(prefix, opts...)
}

func (s *MetadataStorage) ListColumn(prefix string, columns []query.Column, opts ...query.Query) ([]map[string]interface{}, error) {
	ctx := context.Background()
	reqID := uuid.NewString()
	ctx = context.WithValue(ctx, "request_id", reqID)

	logger := s.Logger.With("prefix", prefix, "request_id", reqID)
	logger.Debug("Listing records by columns")
	if !s.SkipListLocking {
		lock, err := s.Locker.Lock(ctx, prefix, true)
		if err != nil {
			return nil, err
		}
		defer s.unlockWithLogger(ctx, s.Locker, lock, logger)
	}

	return s.Storage.ListColumn(prefix, columns, opts...)
}

func (s *MetadataStorage) Count(prefix string, opts ...query.Query) (int, error) {
	ctx := context.Background()
	reqID := uuid.NewString()
	ctx = context.WithValue(ctx, "request_id", reqID)

	logger := s.Logger.With("prefix", prefix, "request_id", reqID)
	logger.Debug("Counting keys")
	if !s.SkipListLocking {
		lock, err := s.Locker.Lock(ctx, prefix, true)
		if err != nil {
			return 0, err
		}
		defer s.unlockWithLogger(ctx, s.Locker, lock, logger)
	}

	return s.Storage.Count(prefix, opts...)
}

func (s *MetadataStorage) Get(key string, opts ...query.Query) (string, error) {
	ctx := context.Background()
	reqID := uuid.NewString()
	ctx = context.WithValue(ctx, "request_id", reqID)

	logger := s.Logger.With("key", key, "request_id", reqID)
	logger.Debug("Get key")
	lock, err := s.Locker.Lock(ctx, key, true)
	if err != nil {
		return "", err
	}
	defer s.unlockWithLogger(ctx, s.Locker, lock, logger)

	val, err := s.Storage.Get(key, opts...)
	if err != nil {
		return "", err
	}
	logger.Debug("Retrieved key")
	return val, nil
}

func (s *MetadataStorage) Delete(key string) (string, error) {
	ctx := context.Background()
	reqID := uuid.NewString()
	ctx = context.WithValue(ctx, "request_id", reqID)

	logger := s.Logger.With("key", key, "request_id", reqID)
	logger.Debug("Delete key")
	lock, err := s.Locker.Lock(ctx, key, true)
	if err != nil {
		return "", err
	}
	defer s.unlockWithLogger(ctx, s.Locker, lock, logger)

	value, err := s.Storage.Delete(key)
	if err != nil {
		return "", err
	}
	return value, nil
}

func (s *MetadataStorage) Close() {
	s.Locker.Close()
	s.Storage.Close()
}

type MetadataStorageType string

const (
	MemoryMetadataStorage MetadataStorageType = "memory"
	ETCDMetadataStorage   MetadataStorageType = "etcd"
	PSQLMetadataStorage   MetadataStorageType = "psql"
)

type metadataStorageImplementation interface {
	// Set stores the value for the key and updates it if it already exists
	Set(key string, value string) error
	// Get returns the value for the key
	Get(key string, opts ...query.Query) (string, error)
	// List returns all the keys and values that match the query
	List(prefix string, opts ...query.Query) (map[string]string, error)
	// List Computed Columns
	ListColumn(prefix string, columns []query.Column, opts ...query.Query) ([]map[string]interface{}, error)
	// Delete removes the key and its value from the store
	Delete(key string) (string, error)
	// Count returns the number of items that match the query
	Count(prefix string, opts ...query.Query) (int, error)
	// Close closes the storage
	Close()
	// Type returns the type of the storage
	Type() MetadataStorageType
}
