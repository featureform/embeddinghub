// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package main

import (
	"fmt"
	"github.com/featureform/ffsync"
	help "github.com/featureform/helpers"
	"github.com/featureform/metadata"
	"github.com/featureform/scheduling"
	ss "github.com/featureform/storage"
	"go.uber.org/zap"
)

func main() {
	logger := zap.NewExample().Sugar()
	addr := help.GetEnv("METADATA_PORT", "8080")
	locker, err := ffsync.NewMemoryLocker()
	if err != nil {
		panic(err.Error())
	}
	mstorage, err := ss.NewMemoryStorageImplementation()
	if err != nil {
		panic(err.Error())
	}
	storage := ss.MetadataStorage{
		Locker:  &locker,
		Storage: &mstorage,
	}

	meta, err := scheduling.NewMemoryTaskMetadataManager()
	config := &metadata.Config{
		Logger:          logger,
		Address:         fmt.Sprintf(":%s", addr),
		StorageProvider: storage,
		TaskManager:     meta,
	}
	server, err := metadata.NewMetadataServer(config)
	if err != nil {
		logger.Panicw("Failed to create metadata server", "Err", err)
	}
	if err := server.Serve(); err != nil {
		logger.Errorw("Serve failed with error", "Err", err)
	}
}
