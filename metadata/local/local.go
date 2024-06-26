// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package main

import (
	"fmt"

	help "github.com/featureform/helpers"
	"github.com/featureform/logging"
	"github.com/featureform/metadata"
	"go.uber.org/zap"
)

func main() {
	sugaredLogger := zap.NewExample().Sugar()
	logger := logging.WrapZapLogger(sugaredLogger)
	addr := help.GetEnv("METADATA_PORT", "8080")
	config := &metadata.Config{
		Logger:          logger,
		Address:         fmt.Sprintf(":%s", addr),
		StorageProvider: metadata.LocalStorageProvider{},
	}
	server, err := metadata.NewMetadataServer(config)
	if err != nil {
		logger.Panicw("Failed to create metadata server", "Err", err)
	}
	if err := server.Serve(); err != nil {
		logger.Errorw("Serve failed with error", "Err", err)
	}
}
