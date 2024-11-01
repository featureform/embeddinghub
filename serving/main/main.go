// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package main

import (
	"fmt"
	"net"
	_ "net/http/pprof"

	help "github.com/featureform/helpers"
	"github.com/featureform/helpers/interceptors"
	"github.com/featureform/logging"
	"github.com/featureform/metadata"
	"github.com/featureform/metrics"
	pb "github.com/featureform/proto"
	"github.com/featureform/serving"
	"google.golang.org/grpc"
)

func main() {
	logger := logging.NewLogger("serving")

	host := help.GetEnv("SERVING_HOST", "0.0.0.0")
	logger.Infow("Using serving host", "host", host)
	port := help.GetEnv("SERVING_PORT", "8080")
	logger.Infow("Using serving port", "port", port)
	address := fmt.Sprintf("%s:%s", host, port)
	lis, err := net.Listen("tcp", address)
	if err != nil {
		logger.Panicw("Failed to listen on port", "Err", err)
	}

	promMetrics := metrics.NewMetrics("")
	metricsPort := help.GetEnv("METRICS_PORT", ":9090")
	logger.Infow("Using metrics port", "port", metricsPort)

	metadataHost := help.GetEnv("METADATA_HOST", "localhost")
	metadataPort := help.GetEnv("METADATA_PORT", "8080")
	metadataConn := fmt.Sprintf("%s:%s", metadataHost, metadataPort)
	logger.Infow("Using metadata conn", "conn_string", metadataConn)

	meta, err := metadata.NewClient(metadataConn, logger)
	if err != nil {
		logger.Panicw("Failed to connect to metadata", "Err", err)
	}

	serv, err := serving.NewFeatureServer(meta, promMetrics, logger)
	if err != nil {
		logger.Panicw("Failed to create training server", "Err", err)
	}
	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(interceptors.UnaryServerErrorInterceptor), grpc.StreamInterceptor(interceptors.StreamServerErrorInterceptor))

	pb.RegisterFeatureServer(grpcServer, serv)
	logger.Infow("Serving metrics", "Port", metricsPort)
	go promMetrics.ExposePort(metricsPort)
	logger.Infow("Server starting", "Addr", address)
	serveErr := grpcServer.Serve(lis)
	if serveErr != nil {
		logger.Errorw("Serve failed with error", "Err", serveErr)
	}

}
