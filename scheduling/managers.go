// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package scheduling

import (
	cfg "github.com/featureform/config"
	"github.com/featureform/fferr"
	"github.com/featureform/ffsync"
	"github.com/featureform/helpers"
	"github.com/featureform/helpers/notifications"
	"github.com/featureform/logging"
	ss "github.com/featureform/storage"
)

type TaskMetadataManagerType string

const (
	MemoryMetdataManager TaskMetadataManagerType = "memory"
	ETCDMetadataManager  TaskMetadataManagerType = "etcd"
	PSQLMetadataManager  TaskMetadataManagerType = "psql"
)

func NewTaskMetadataManagerFromEnv(t TaskMetadataManagerType) (TaskMetadataManager, error) {
	switch t {
	case MemoryMetdataManager:
		return NewMemoryTaskMetadataManager()
	case ETCDMetadataManager:
		return NewETCDTaskMetadataManagerFromEnv()
	case PSQLMetadataManager:
		return NewPSQLTaskMetadataManagerFromEnv()
	default:
		return TaskMetadataManager{}, fferr.NewInvalidArgumentErrorf("Metadata Manager must be one of type: memory, etcd, rds")
	}
}

func NewMemoryTaskMetadataManager() (TaskMetadataManager, error) {
	memoryLocker, err := ffsync.NewMemoryLocker()
	if err != nil {
		return TaskMetadataManager{}, err
	}

	memoryStorage, err := ss.NewMemoryStorageImplementation()
	if err != nil {
		return TaskMetadataManager{}, err
	}

	storage := ss.MetadataStorage{
		Locker:  &memoryLocker,
		Storage: &memoryStorage,
		Logger:  logging.NewLogger("memoryMetadataStorage"),
	}

	generator, err := ffsync.NewMemoryOrderedIdGenerator()
	if err != nil {
		return TaskMetadataManager{}, err
	}

	return TaskMetadataManager{
		Storage:     storage,
		idGenerator: generator,
	}, nil
}

func NewETCDTaskMetadataManagerFromEnv() (TaskMetadataManager, error) {
	config := helpers.ETCDConfig{
		Host:     helpers.GetEnv("ETCD_HOST", "localhost"),
		Port:     helpers.GetEnv("ETCD_PORT", "2379"),
		Username: helpers.GetEnv("ETCD_USERNAME", ""),
		Password: helpers.GetEnv("ETCD_PASSWORD", ""),
	}
	return NewETCDTaskMetadataManager(config)
}

func NewETCDTaskMetadataManager(config helpers.ETCDConfig) (TaskMetadataManager, error) {
	etcdLocker, err := ffsync.NewETCDLocker(config)
	if err != nil {
		return TaskMetadataManager{}, err
	}

	etcdStorage, err := ss.NewETCDStorageImplementation(config)
	if err != nil {
		return TaskMetadataManager{}, err
	}

	etcdMetadataStorage := ss.MetadataStorage{
		Locker:  etcdLocker,
		Storage: etcdStorage,
		Logger:  logging.NewLogger("etcdMetadataStorage"),
	}

	idGenerator, err := ffsync.NewETCDOrderedIdGenerator(config)
	if err != nil {
		return TaskMetadataManager{}, err
	}

	return TaskMetadataManager{
		Storage:     etcdMetadataStorage,
		idGenerator: idGenerator,
	}, nil
}

func NewPSQLTaskMetadataManagerFromEnv() (TaskMetadataManager, error) {
	config := helpers.PSQLConfig{
		Host:     helpers.GetEnv("PSQL_HOST", "localhost"),
		Port:     helpers.GetEnv("PSQL_PORT", "5432"),
		User:     helpers.GetEnv("PSQL_USER", "postgres"),
		Password: helpers.GetEnv("PSQL_PASSWORD", "password"),
		DBName:   helpers.GetEnv("PSQL_DB", "postgres"),
		SSLMode:  helpers.GetEnv("PSQL_SSLMODE", "disable"),
	}
	return NewPSQLTaskMetadataManager(config)
}

func NewPSQLTaskMetadataManager(config helpers.PSQLConfig) (TaskMetadataManager, error) {
	psqlLocker, err := ffsync.NewPSQLLocker(config)
	if err != nil {
		logger.Infow("failed to create PSQL locker", "error", err)
		return TaskMetadataManager{}, err
	}

	psqlStorage, err := ss.NewPSQLStorageImplementation(config, "ff_task_metadata")
	if err != nil {
		logger.Info("failed to create PSQL storage implementation", err)
		return TaskMetadataManager{}, err
	}

	psqlLogger := logging.NewLogger("psqlMetadataStorage")
	psqlMetadataStorage := ss.MetadataStorage{
		Locker:          psqlLocker,
		Storage:         psqlStorage,
		Logger:          psqlLogger,
		SkipListLocking: true,
	}

	idGenerator, err := ffsync.NewPSQLOrderedIdGenerator(config)
	if err != nil {
		return TaskMetadataManager{}, err
	}

	slackChannel := cfg.GetSlackChannelId()
	slackNotif := notifications.NewSlackNotifier(slackChannel, psqlLogger)

	return TaskMetadataManager{
		Storage:     psqlMetadataStorage,
		idGenerator: idGenerator,
		notifier:    slackNotif,
	}, nil
}
