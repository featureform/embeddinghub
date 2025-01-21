// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"time"

	"github.com/featureform/api"
	"github.com/featureform/coordinator"
	"github.com/featureform/coordinator/spawner"
	help "github.com/featureform/helpers"
	"github.com/featureform/logging"
	"github.com/featureform/metadata"
	dm "github.com/featureform/metadata/dashboard"
	"github.com/featureform/metrics"
	pb "github.com/featureform/proto"
	"github.com/featureform/scheduling"
	"github.com/featureform/serving"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func main() {
	/******************************************    Vars    ************************************************************/
	err := os.Setenv("SPARK_LOCAL_SCRIPT_PATH", "provider/scripts/spark/offline_store_spark_runner.py")
	if err != nil {
		log.Fatalf("err %v", err)
	}
	err = os.Setenv("MATERIALIZE_NO_TIMESTAMP_QUERY_PATH", "provider/queries/materialize_no_ts.sql")
	if err != nil {
		log.Fatalf("err %v", err)
	}
	err = os.Setenv("MATERIALIZE_WITH_TIMESTAMP_QUERY_PATH", "provider/queries/materialize_ts.sql")
	if err != nil {
		log.Fatalf("err %v", err)
	}
	apiPort := help.GetEnv("API_PORT", "7878")
	metadataHost := help.GetEnv("METADATA_HOST", "localhost")
	metadataPort := help.GetEnv("METADATA_PORT", "8080")
	metadataHTTPPort := help.GetEnv("METADATA_HTTP_PORT", "3001")
	servingHost := help.GetEnv("SERVING_HOST", "0.0.0.0")
	servingPort := help.GetEnv("SERVING_PORT", "8081")
	apiConn := fmt.Sprintf("0.0.0.0:%s", apiPort)
	metadataConn := fmt.Sprintf("%s:%s", metadataHost, metadataPort)
	servingConn := fmt.Sprintf("%s:%s", servingHost, servingPort)
	local := help.GetEnvBool("FEATUREFORM_LOCAL", true)
	managerType := help.GetEnv("FF_STATE_PROVIDER", "memory")

	manager, err := scheduling.NewTaskMetadataManagerFromEnv(scheduling.TaskMetadataManagerType(managerType))
	if err != nil {
		panic(err.Error())
	}

	// PPROF
	go func() {
		if err := http.ListenAndServe("localhost:6060", nil); err != nil {
			log.Printf("pprof server failed to start: %v", err)
		}
	}()

	logger := logging.NewLogger("api")

	/****************************************** API Server ************************************************************/

	go func() {
		err := api.StartHttpsServer(":8443")
		if err != nil && err != http.ErrServerClosed {
			panic(fmt.Sprintf("health check HTTP server failed: %+v", err))
		}
	}()

	/******************************************** Metadata ************************************************************/

	mLogger := logging.NewLogger("metadata")
	config := &metadata.Config{
		Logger:      mLogger,
		Address:     fmt.Sprintf(":%s", metadataPort),
		TaskManager: manager,
	}

	server, err := metadata.NewMetadataServer(config)
	if err != nil {
		logger.Panicw("Failed to create metadata server", "Err", err)
	}

	/******************************************** Coordinator ************************************************************/

	cLogger := logging.NewLogger("coordinator")
	defer cLogger.Sync()
	cLogger.Debug("Connected to ETCD")

	client, err := metadata.NewClient(metadataConn, cLogger)
	if err != nil {
		cLogger.Errorw("Failed to connect: %v", err)
		panic(err)
	}

	sconfig := coordinator.SchedulerConfig{
		TaskPollInterval:       1 * time.Second,
		TaskStatusSyncInterval: 1 * time.Minute,
		DependencyPollInterval: 1 * time.Second,
	}
	scheduler := coordinator.NewScheduler(client, cLogger, &spawner.MemoryJobSpawner{}, manager.Storage.Locker, sconfig)

	/**************************************** Dashboard Backend *******************************************************/
	dbLogger := logging.WrapZapLogger(zap.NewExample().Sugar())

	dbLogger.Infof("Serving metadata at: %s\n", metadataConn)

	metadataServer, err := dm.NewMetadataServer(dbLogger, client, manager.Storage)
	if err != nil {
		logger.Panicw("Failed to create server", "error", err)
	}

	metadataServingPort := fmt.Sprintf(":%s", metadataHTTPPort)
	dbLogger.Infof("Serving HTTP Metadata on port: %s\n", metadataServingPort)

	/**************************************** Serving *******************************************************/

	sLogger := logging.NewLogger("serving")

	sLogger.Infof("Serving feature serving at: %s\n", servingConn)

	lis, err := net.Listen("tcp", servingConn)
	if err != nil {
		sLogger.Panicw("Failed to listen on port", "Err", err)
	}
	metricsHandler := &metrics.NoOpMetricsHandler{}
	serv, err := serving.NewFeatureServer(client, metricsHandler, sLogger)
	if err != nil {
		sLogger.Panicw("Failed to create training server", "Err", err)
	}
	grpcServer := grpc.NewServer()

	pb.RegisterFeatureServer(grpcServer, serv)
	sLogger.Infow("Server starting", "Port", servingConn)

	/******************************************** Start Servers *******************************************************/

	go func() {
		serv, err := api.NewApiServer(logger, apiConn, metadataConn, servingConn)
		if err != nil {
			panic(err)
		}
		fmt.Println(serv.Serve())
	}()

	go func() {
		if err := server.Serve(); err != nil {
			logger.Errorw("Serve failed with error", "Err", err)
			if err != nil {
				panic(err)
			}
		}
	}()

	go func() {
		err = scheduler.Start()
		if err != nil {
			cLogger.Errorw(err.Error())
		}
	}()

	go func() {
		err := metadataServer.Start(metadataServingPort, local)
		if err != nil {
			panic(err)
		}
	}()

	go func() {
		serveErr := grpcServer.Serve(lis)
		if serveErr != nil {
			logger.Errorw("Serve failed with error", "Err", serveErr)
			panic(err)
		}
	}()

	for {
		time.Sleep(1 * time.Second)
	}
}
