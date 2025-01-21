// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/featureform/fferr"
	"github.com/featureform/helpers"
	"github.com/featureform/logging"
	"google.golang.org/protobuf/proto"

	pt "github.com/featureform/provider/provider_type"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"

	grpc_logrus "github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"

	"google.golang.org/grpc/reflection"
	grpc_status "google.golang.org/grpc/status"

	"google.golang.org/grpc/credentials/insecure"

	health "github.com/featureform/health"
	help "github.com/featureform/helpers"
	"github.com/featureform/metadata"
	pb "github.com/featureform/metadata/proto"
	srv "github.com/featureform/proto"
	"github.com/featureform/provider"
	"google.golang.org/grpc"
)

type ApiServer struct {
	Logger     logging.Logger
	address    string
	grpcServer *grpc.Server
	listener   net.Listener
	metadata   MetadataServer
	online     OnlineServer
}

type MetadataServer struct {
	address string
	Logger  logging.Logger
	meta    pb.MetadataClient
	client  *metadata.Client
	pb.UnimplementedApiServer
	health *health.Health
}

type OnlineServer struct {
	Logger  logging.Logger
	address string
	client  srv.FeatureClient
	srv.UnimplementedFeatureServer
}

func NewApiServer(logger logging.Logger, address string, metaAddr string, srvAddr string) (*ApiServer, error) {
	return &ApiServer{
		Logger:  logger,
		address: address,
		metadata: MetadataServer{
			address: metaAddr,
			Logger:  logger,
		},
		online: OnlineServer{
			Logger:  logger,
			address: srvAddr,
		},
	}, nil
}

// rpc CreateUser(User) returns (Empty);
// - Anyone can create
func (serv *MetadataServer) CreateUser(ctx context.Context, userRequest *pb.UserRequest) (*pb.Empty, error) {
	requestID, ctx, logger := serv.Logger.InitializeRequestID(ctx)
	logger = logger.WithResource(logging.User, userRequest.User.Name, logging.NoVariant)
	logger.Infow("Creating User")
	userRequest.RequestId = requestID

	serv.Logger.Infow("Creating User", "user", userRequest.User)
	out, err := serv.meta.CreateUser(ctx, userRequest)
	if err != nil {
		return nil, err
	}

	return out, nil
}

func (serv *MetadataServer) MarkForDeletion(ctx context.Context, req *pb.MarkForDeletionRequest) (*pb.MarkForDeletionResponse, error) {
	_, ctx, logger := serv.Logger.InitializeRequestID(ctx)
	logger = logger.WithResource(logging.ResourceTypeFromProto(req.ResourceId.ResourceType), req.ResourceId.Resource.Name, req.ResourceId.Resource.Variant)
	logger.Infow("Deleting Resource")

	out, err := serv.meta.MarkForDeletion(ctx, req)
	if err != nil {
		serv.Logger.Errorw("Failed to mark resource for deletion", "error", err)
		return nil, err
	}

	logger.Infow("Successfully marked resource for deletion")
	return out, nil
}

// rpc GetUsers(stream Name) returns (stream User);
// - Anyone can get
func (serv *MetadataServer) GetUsers(stream pb.Api_GetUsersServer) error {
	requestID, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Infow("Getting Users")
	for {
		nameRequest, err := stream.Recv()
		if err == io.EOF {
			logger.Debugw("End of stream reached. Stream request completed")
			return nil
		}
		if err != nil {
			logger.Errorw("Failed to read client request", "error", err)
			return err
		}
		loggerWithResource := logger.WithResource(logging.User, nameRequest.Name.Name, logging.NoVariant)
		loggerWithResource.Infow("Getting user from stream")
		nameRequest.RequestId = requestID

		proxyStream, err := serv.meta.GetUsers(ctx)
		if err != nil {
			loggerWithResource.Errorw("Failed to get users from server", "error", err)
			return err
		}
		sErr := proxyStream.Send(nameRequest)
		if sErr != nil {
			loggerWithResource.Errorw("Failed to send user to the server", "error", sErr)
			return sErr
		}
		res, err := proxyStream.Recv()
		if err != nil {
			loggerWithResource.Errorw("Failed to receive users from server", "error", err)
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			loggerWithResource.Errorw("Failed to send users to client", "error", sendErr)
			return sendErr
		}
	}
}

// rpc GetFeatures(stream Name) returns (stream Feature);
// - Anyone can get
func (serv *MetadataServer) GetFeatures(stream pb.Api_GetFeaturesServer) error {
	requestID, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Infow("Getting Features")
	for {
		nameRequest, err := stream.Recv()
		if err == io.EOF {
			logger.Debugw("End of stream reached. Stream request completed")
			return nil
		}
		if err != nil {
			logger.Errorw("Failed to read client request", "error", err)
			return err
		}
		loggerWithResource := logger.WithResource(logging.Feature, nameRequest.Name.Name, logging.NoVariant)
		loggerWithResource.Infow("Getting feature from stream")
		nameRequest.RequestId = requestID
		proxyStream, err := serv.meta.GetFeatures(ctx)
		if err != nil {
			loggerWithResource.Errorw("Failed to get features from server", "error", err)
			return err
		}
		sErr := proxyStream.Send(nameRequest)
		if sErr != nil {
			loggerWithResource.Errorw("Failed to send feature to the server", "error", sErr)
			return sErr
		}
		res, err := proxyStream.Recv()
		if err != nil {
			loggerWithResource.Errorw("Failed to receive features from server", "error", err)
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			loggerWithResource.Errorw("Failed to send features to client", "error", sendErr)
			return sendErr
		}
	}
}

// rpc GetFeatureVariants(stream NameVariant) returns (stream FeatureVariant);
// - Anyone can get
func (serv *MetadataServer) GetFeatureVariants(stream pb.Api_GetFeatureVariantsServer) error {
	requestID, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Infow("Getting Feature Variants")
	for {
		nameVariantRequest, err := stream.Recv()
		if err == io.EOF {
			logger.Debugw("End of stream reached. Stream request completed")
			return nil
		}
		if err != nil {
			logger.Errorw("Failed to read client request", "error", err)
			return err
		}
		loggerWithResource := logger.WithResource(logging.Feature, nameVariantRequest.NameVariant.Name, nameVariantRequest.NameVariant.Variant)
		loggerWithResource.Infow("Getting feature variant from stream")
		nameVariantRequest.RequestId = requestID

		proxyStream, err := serv.meta.GetFeatureVariants(ctx)
		if err != nil {
			loggerWithResource.Errorw("Failed to get feature variants from server", "error", err)
			return err
		}
		sErr := proxyStream.Send(nameVariantRequest)
		if sErr != nil {
			loggerWithResource.Errorw("Failed to send feature variant to the server", "error", sErr)
			return sErr
		}
		res, err := proxyStream.Recv()
		if err != nil {
			loggerWithResource.Errorw("Failed to receive feature variants from server", "error", err)
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			loggerWithResource.Errorw("Failed to send feature variants to client", "error", sendErr)
			return sendErr
		}
	}
}

func (serv *MetadataServer) GetLabels(stream pb.Api_GetLabelsServer) error {
	requestID, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Infow("Getting Labels")
	for {
		nameRequest, err := stream.Recv()
		if err == io.EOF {
			logger.Debugw("End of stream reached. Stream request completed")
			return nil
		}
		if err != nil {
			logger.Errorw("Failed to read client request", "error", err)
			return err
		}
		loggerWithResource := logger.WithResource(logging.Label, nameRequest.Name.Name, logging.NoVariant)
		loggerWithResource.Infow("Getting label from stream")
		nameRequest.RequestId = requestID
		proxyStream, err := serv.meta.GetLabels(ctx)
		if err != nil {
			loggerWithResource.Errorw("Failed to get labels from server", "error", err)
			return err
		}
		sErr := proxyStream.Send(nameRequest)
		if sErr != nil {
			loggerWithResource.Errorw("Failed to send labels to the server", "error", sErr)
			return sErr
		}
		res, err := proxyStream.Recv()
		if err != nil {
			loggerWithResource.Errorw("Failed to receive labels from server", "error", err)
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			loggerWithResource.Errorw("Failed to send labels to client", "error", sendErr)
			return sendErr
		}
	}
}

// rpc GetLabelVariants(stream NameVariant) returns (stream LabelVariant);
// - Anyone can get
func (serv *MetadataServer) GetLabelVariants(stream pb.Api_GetLabelVariantsServer) error {
	requestID, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Infow("Getting Label Variants")
	for {
		nameVariantRequest, err := stream.Recv()
		if err == io.EOF {
			logger.Debugw("End of stream reached. Stream request completed")
			return nil
		}
		if err != nil {
			logger.Errorw("Failed to read client request", "error", err)
			return err
		}
		loggerWithResource := logger.WithResource(logging.Label, nameVariantRequest.NameVariant.Name, nameVariantRequest.NameVariant.Variant)
		loggerWithResource.Infow("Getting label variant from stream")
		nameVariantRequest.RequestId = requestID
		proxyStream, err := serv.meta.GetLabelVariants(ctx)
		if err != nil {
			loggerWithResource.Errorw("Failed to get label variants from server", "error", err)
			return err
		}
		sErr := proxyStream.Send(nameVariantRequest)
		if sErr != nil {
			loggerWithResource.Errorw("Failed to send label variants to the server", "error", sErr)
			return sErr
		}
		res, err := proxyStream.Recv()
		if err != nil {
			loggerWithResource.Errorw("Failed to receive label variants from server", "error", err)
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			loggerWithResource.Errorw("Failed to send label variants to client", "error", sendErr)
			return sendErr
		}
	}
}

func (serv *MetadataServer) GetSources(stream pb.Api_GetSourcesServer) error {
	requestID, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Infow("Getting Sources")
	for {
		nameRequest, err := stream.Recv()
		if err == io.EOF {
			logger.Debugw("End of stream reached. Stream request completed")
			return nil
		}
		if err != nil {
			logger.Errorw("Failed to read client request", "error", err)
			return err
		}
		loggerWithResource := logger.WithResource(logging.Source, nameRequest.Name.Name, logging.NoVariant)
		loggerWithResource.Infow("Getting source from stream")
		nameRequest.RequestId = requestID
		proxyStream, err := serv.meta.GetSources(ctx)
		if err != nil {
			loggerWithResource.Errorw("Failed to get sources from server", "error", err)
			return err
		}
		sErr := proxyStream.Send(nameRequest)
		if sErr != nil {
			loggerWithResource.Errorw("Failed to send source to the server", "error", sErr)
			return sErr
		}
		res, err := proxyStream.Recv()
		if err != nil {
			loggerWithResource.Errorw("Failed to receive sources from server", "error", err)
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			loggerWithResource.Errorw("Failed to send sources to client", "error", sendErr)
			return sendErr
		}
	}
}

// rpc GetSourceVariants(stream NameVariant) returns (stream SourceVariant);
// - Anyone can get
func (serv *MetadataServer) GetSourceVariants(stream pb.Api_GetSourceVariantsServer) error {
	requestID, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Infow("Getting Source Variants")
	for {
		nameVariantRequest, err := stream.Recv()
		if err == io.EOF {
			logger.Debugw("End of stream reached. Stream request completed")
			return nil
		}
		if err != nil {
			logger.Errorw("Failed to read client request", "error", err)
			return err
		}
		loggerWithResource := logger.WithResource(logging.Source, nameVariantRequest.NameVariant.Name, nameVariantRequest.NameVariant.Variant)
		loggerWithResource.Infow("Getting source variant from stream")
		nameVariantRequest.RequestId = requestID
		proxyStream, err := serv.meta.GetSourceVariants(ctx)
		if err != nil {
			loggerWithResource.Errorw("Failed to get source variants from server", "error", err)
			return err
		}
		sErr := proxyStream.Send(nameVariantRequest)
		if sErr != nil {
			loggerWithResource.Errorw("Failed to send source to the server", "error", sErr)
			return sErr
		}
		res, err := proxyStream.Recv()
		if err != nil {
			loggerWithResource.Errorw("Failed to receive source variants from server", "error", err)
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			loggerWithResource.Errorw("Failed to send source variants to client", "error", sendErr)
			return sendErr
		}
	}
}

func (serv *MetadataServer) GetTrainingSets(stream pb.Api_GetTrainingSetsServer) error {
	requestID, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Infow("Getting Training Sets")
	for {
		nameRequest, err := stream.Recv()
		if err == io.EOF {
			logger.Debugw("End of stream reached. Stream request completed")
			return nil
		}
		if err != nil {
			logger.Errorw("Failed to read client request", "error", err)
			return err
		}
		loggerWithResource := logger.WithResource(logging.TrainingSet, nameRequest.Name.Name, logging.NoVariant)
		loggerWithResource.Infow("Getting training set from stream")
		nameRequest.RequestId = requestID

		proxyStream, err := serv.meta.GetTrainingSets(ctx)
		if err != nil {
			loggerWithResource.Errorw("Failed to get training sets from server", "error", err)
			return err
		}
		sErr := proxyStream.Send(nameRequest)
		if sErr != nil {
			loggerWithResource.Errorw("Failed to send training set to the server", "error", sErr)
			return sErr
		}
		res, err := proxyStream.Recv()
		if err != nil {
			loggerWithResource.Errorw("Failed to receive training sets from server", "error", err)
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			loggerWithResource.Errorw("Failed to send training sets to client", "error", sendErr)
			return sendErr
		}
	}
}

// rpc GetTrainingSetVariants(stream NameVariant) returns (stream TrainingSetVariant);
// - Anyone can get
func (serv *MetadataServer) GetTrainingSetVariants(stream pb.Api_GetTrainingSetVariantsServer) error {
	requestID, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Infow("Getting Training Set Variants")
	for {
		nameVariantRequest, err := stream.Recv()
		if err == io.EOF {
			logger.Debugw("End of stream reached. Stream request completed")
			return nil
		}
		if err != nil {
			logger.Errorw("Failed to read client request", "error", err)
			return err
		}
		loggerWithResource := logger.WithResource(logging.TrainingSet, nameVariantRequest.NameVariant.Name, nameVariantRequest.NameVariant.Variant)
		loggerWithResource.Infow("Getting training set variant from stream")
		nameVariantRequest.RequestId = requestID

		proxyStream, err := serv.meta.GetTrainingSetVariants(ctx)
		if err != nil {
			loggerWithResource.Errorw("Failed to get training set variants from server", "error", err)
			return err
		}
		sErr := proxyStream.Send(nameVariantRequest)
		if sErr != nil {
			loggerWithResource.Errorw("Failed to send training set to the server", "error", sErr)
			return sErr
		}
		res, err := proxyStream.Recv()
		if err != nil {
			loggerWithResource.Errorw("Failed to receive training set variants from server", "error", err)
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			loggerWithResource.Errorw("Failed to send training set variants to client", "error", sendErr)
			return sendErr
		}
	}
}

// rpc GetProviders(stream Name) returns (stream Provider);
func (serv *MetadataServer) GetProviders(stream pb.Api_GetProvidersServer) error {
	requestID, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Infow("Getting Providers")
	for {
		nameRequest, err := stream.Recv()
		if err == io.EOF {
			logger.Debugw("End of stream reached. Stream request completed")
			return nil
		}
		if err != nil {
			logger.Errorw("Failed to read client request", "error", err)
			return err
		}

		loggerWithResource := logger.WithResource(logging.Provider, nameRequest.Name.Name, logging.NoVariant)
		loggerWithResource.Infow("Getting provider from stream")
		nameRequest.RequestId = requestID

		proxyStream, err := serv.meta.GetProviders(ctx)
		if err != nil {
			loggerWithResource.Errorw("Failed to get providers from server", "error", err)
			return err
		}
		sErr := proxyStream.Send(nameRequest)
		if sErr != nil {
			loggerWithResource.Errorw("Failed to send provider to the server", "error", sErr)
			return sErr
		}
		res, err := proxyStream.Recv()
		if err != nil {
			loggerWithResource.Errorw("Failed to receive providers from server", "error", err)
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			loggerWithResource.Errorw("Failed to send providers to client", "error", sendErr)
			return sendErr
		}
	}
}

func (serv *MetadataServer) GetEntities(stream pb.Api_GetEntitiesServer) error {
	requestID, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Infow("Getting Entities")
	for {
		nameRequest, err := stream.Recv()
		if err == io.EOF {
			logger.Debugw("End of stream reached. Stream request completed")
			return nil
		}
		if err != nil {
			logger.Errorw("Failed to read client request", "error", err)
			return err
		}
		loggerWithResource := logger.WithResource(logging.Entity, nameRequest.Name.Name, logging.NoVariant)
		loggerWithResource.Infow("Getting entity from stream")
		nameRequest.RequestId = requestID
		proxyStream, err := serv.meta.GetEntities(ctx)
		if err != nil {
			loggerWithResource.Errorw("Failed to get entities from server", "error", err)
			return err
		}
		sErr := proxyStream.Send(nameRequest)
		if sErr != nil {
			loggerWithResource.Errorw("Failed to send entity to the server", "error", sErr)
			return sErr
		}
		res, err := proxyStream.Recv()
		if err != nil {
			loggerWithResource.Errorw("Failed to receive entities from server", "error", err)
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			loggerWithResource.Errorw("Failed to send entities to client", "error", sendErr)
			return sendErr
		}
	}
}

func (serv *MetadataServer) GetModels(stream pb.Api_GetModelsServer) error {
	requestID, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Infow("Getting Models")
	for {
		nameRequest, err := stream.Recv()
		if err == io.EOF {
			logger.Debugw("End of stream reached. Stream request completed")
			return nil
		}
		if err != nil {
			logger.Errorw("Failed to read client request", "error", err)
			return err
		}
		loggerWithResource := logger.WithResource(logging.Model, nameRequest.Name.Name, logging.NoVariant)
		loggerWithResource.Infow("Getting model from stream")
		nameRequest.RequestId = requestID
		proxyStream, err := serv.meta.GetModels(ctx)
		if err != nil {
			loggerWithResource.Errorw("Failed to get models from server", "error", err)
			return err
		}
		sErr := proxyStream.Send(nameRequest)
		if sErr != nil {
			loggerWithResource.Errorw("Failed to send model to the server", "error", sErr)
			return sErr
		}
		res, err := proxyStream.Recv()
		if err != nil {
			loggerWithResource.Errorw("Failed to receive models from server", "error", err)
			return err
		}
		sendErr := stream.Send(res)
		if sendErr != nil {
			loggerWithResource.Errorw("Failed to send models to client", "error", sendErr)
			return sendErr
		}
	}
}

func (serv *MetadataServer) GetEquivalent(ctx context.Context, req *pb.GetEquivalentRequest) (*pb.ResourceVariant, error) {
	ctx = logging.AttachRequestID(req.RequestId, ctx, serv.Logger)
	logger := logging.GetLoggerFromContext(ctx)

	preprocessSourceVariant(req)

	// Log start of the request
	logger.Info("Handling GetEquivalent call")
	resp, err := serv.meta.GetEquivalent(ctx, req)
	if err != nil {
		logger.Errorw("GetEquivalent failed", "error", err, "requestId", req.RequestId)
		return nil, err
	}

	return resp, nil
}

func preprocessSourceVariant(req *pb.GetEquivalentRequest) {
	sv := req.Variant.GetSourceVariant()
	if sv == nil {
		return
	}

	tf := sv.GetTransformation()
	if tf == nil {
		return
	}

	sqlTransformation := tf.GetSQLTransformation()
	if sqlTransformation == nil {
		return
	}

	sources := extractSourcesFromSQLTransformation(sqlTransformation.Query)
	sqlTransformation.Source = sources
	return
}

func (serv *MetadataServer) Run(ctx context.Context, req *pb.RunRequest) (*pb.Empty, error) {
	ctx = logging.AttachRequestID(req.RequestId, ctx, serv.Logger)
	logger := logging.GetLoggerFromContext(ctx)
	logger.Info("Handling Run call")
	resp, err := serv.meta.Run(ctx, req)
	if err != nil {
		logger.Errorw("Run failed", "error", err)
	}
	return resp, err
}

func (serv *MetadataServer) ListUsers(listRequest *pb.ListRequest, stream pb.Api_ListUsersServer) error {
	_, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Infow("Listing Users")
	proxyStream, err := serv.meta.ListUsers(ctx, listRequest)
	if err != nil {
		logger.Errorw("Failed to list users", "error", err)
		return err
	}
	for {
		res, err := proxyStream.Recv()
		if err == io.EOF {
			logger.Debugw("End of stream reached. Stream request completed")
			return nil
		}
		if err != nil {
			logger.Errorw("Failed to receive user from server", "error", err)
			return err
		}
		loggerWithResource := logger.WithResource(logging.User, res.Name, logging.NoVariant)
		loggerWithResource.Infow("Sending resource on stream")

		sendErr := stream.Send(res)
		if sendErr != nil {
			loggerWithResource.Errorw("Failed to send user to client", "error", sendErr)
			return sendErr
		}
	}
}

func (serv *MetadataServer) ListFeatures(listRequest *pb.ListRequest, stream pb.Api_ListFeaturesServer) error {
	_, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Infow("Listing Features")
	proxyStream, err := serv.meta.ListFeatures(ctx, listRequest)
	if err != nil {
		logger.Errorw("Failed to list features", "error", err)
		return err
	}
	for {
		res, err := proxyStream.Recv()
		if err == io.EOF {
			logger.Debugw("End of stream reached. Stream request completed")
			return nil
		}
		if err != nil {
			logger.Errorw("Failed to receive feature from server", "error", err)
			return err
		}
		loggerWithResource := logger.WithResource(logging.Feature, res.Name, res.DefaultVariant)
		loggerWithResource.Infow("Sending resource on stream")
		sendErr := stream.Send(res)
		if sendErr != nil {
			loggerWithResource.Errorw("Failed to send feature to client", "error", sendErr)
			return sendErr
		}
	}
}

func (serv *MetadataServer) ListLabels(listRequest *pb.ListRequest, stream pb.Api_ListLabelsServer) error {
	_, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Infow("Listing Labels")
	proxyStream, err := serv.meta.ListLabels(ctx, listRequest)
	if err != nil {
		logger.Errorw("Failed to list labels", "error", err)
		return err
	}
	for {
		res, err := proxyStream.Recv()
		if err == io.EOF {
			logger.Debugw("End of stream reached. Stream request completed")
			return nil
		}
		if err != nil {
			logger.Errorw("Failed to receive label from server", "error", err)
			return err
		}
		loggerWithResource := logger.WithResource(logging.Label, res.Name, res.DefaultVariant)
		loggerWithResource.Infow("Sending resource on stream")

		sendErr := stream.Send(res)
		if sendErr != nil {
			loggerWithResource.Errorw("Failed to send label to client", "error", err)
			return sendErr
		}
	}
}

func (serv *MetadataServer) ListSources(listRequest *pb.ListRequest, stream pb.Api_ListSourcesServer) error {
	_, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Infow("Listing Sources")
	proxyStream, err := serv.meta.ListSources(ctx, listRequest)
	if err != nil {
		logger.Errorw("Failed to list sources", "error", err)
		return err
	}
	for {
		res, err := proxyStream.Recv()
		if err == io.EOF {
			logger.Debugw("End of stream reached. Stream request completed")
			return nil
		}
		if err != nil {
			logger.Errorw("Failed to receive source from server", "error", err)
			return err
		}
		loggerWithResource := logger.WithResource(logging.Source, res.Name, res.DefaultVariant)
		loggerWithResource.Infow("Sending resource on stream")
		sendErr := stream.Send(res)
		if sendErr != nil {
			loggerWithResource.Errorw("Failed to send source to client", "error", err)
			return sendErr
		}
	}
}

func (serv *MetadataServer) ListTrainingSets(listRequest *pb.ListRequest, stream pb.Api_ListTrainingSetsServer) error {
	_, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Infow("Listing Training Sets")
	proxyStream, err := serv.meta.ListTrainingSets(ctx, listRequest)
	if err != nil {
		logger.Errorw("Failed to list training sets", "error", err)
		return err
	}
	for {
		res, err := proxyStream.Recv()
		if err == io.EOF {
			logger.Debugw("End of stream reached. Stream request completed")
			return nil
		}
		if err != nil {
			logger.Errorw("Failed to receive training set from server", "error", err)
			return err
		}
		loggerWithResource := logger.WithResource(logging.TrainingSet, res.Name, res.DefaultVariant)
		loggerWithResource.Infow("Sending resource on stream")

		sendErr := stream.Send(res)
		if sendErr != nil {
			loggerWithResource.Errorw("Failed to send training set to client", "error", sendErr)
			return sendErr
		}
	}
}

func (serv *MetadataServer) ListModels(listRequest *pb.ListRequest, stream pb.Api_ListModelsServer) error {
	_, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Infow("Listing Models")
	proxyStream, err := serv.meta.ListModels(ctx, listRequest)
	if err != nil {
		logger.Errorw("Failed to list models", "error", err)
		return err
	}
	for {
		res, err := proxyStream.Recv()
		if err == io.EOF {
			logger.Debugw("End of stream reached. Stream request completed")
			return nil
		}
		if err != nil {
			logger.Errorw("Failed to receive model from server", "error", err)
			return err
		}
		loggerWithResource := logger.WithResource(logging.Model, res.Name, logging.NoVariant)
		loggerWithResource.Infow("Sending resource on stream")

		sendErr := stream.Send(res)
		if sendErr != nil {
			loggerWithResource.Errorw("Failed to send model to client", "error", sendErr)
			return sendErr
		}
	}
}

func (serv *MetadataServer) ListEntities(listRequest *pb.ListRequest, stream pb.Api_ListEntitiesServer) error {
	_, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Infow("Listing Entities")
	proxyStream, err := serv.meta.ListEntities(ctx, listRequest)
	if err != nil {
		logger.Errorw("Failed to list entities", "error", err)
		return err
	}
	for {
		res, err := proxyStream.Recv()
		if err == io.EOF {
			logger.Debugw("End of stream reached. Stream request completed")
			return nil
		}
		if err != nil {
			logger.Errorw("Failed to receive entity from server", "error", err)
			return err
		}
		loggerWithResource := logger.WithResource(logging.Entity, res.Name, logging.NoVariant)
		loggerWithResource.Infow("Sending resource on stream")

		sendErr := stream.Send(res)
		if sendErr != nil {
			loggerWithResource.Errorw("Failed to send entity to client", "error", sendErr)
			return sendErr
		}
	}
}

func CensorProviderConfig(provider *pb.Provider) *pb.Provider {
	censoredProvider := proto.Clone(provider).(*pb.Provider)
	censoredProvider.SerializedConfig = []byte{}
	return censoredProvider
}

func (serv *MetadataServer) ListProviders(listRequest *pb.ListRequest, stream pb.Api_ListProvidersServer) error {
	_, ctx, logger := serv.Logger.InitializeRequestID(stream.Context())
	logger.Infow("Listing Providers")
	proxyStream, err := serv.meta.ListProviders(ctx, listRequest)
	if err != nil {
		logger.Errorw("Failed to list providers", "error", err)
		return err
	}
	for {
		res, err := proxyStream.Recv()
		if err == io.EOF {
			logger.Debugw("End of stream reached. Stream request completed")
			return nil
		}
		if err != nil {
			logger.Errorw("Failed to receive provider from server", "error", err)
			return err
		}
		loggerWithResource := logger.WithResource(logging.Provider, res.Name, logging.NoVariant).WithProvider(res.Type, res.Name)
		loggerWithResource.Infow("Sending resource on stream")

		sendErr := stream.Send(res)
		if sendErr != nil {
			loggerWithResource.Errorw("Failed to send provider to client", "error", sendErr)
			return sendErr
		}
	}
}

// rpc CreateProvider(Provider) returns (Empty);
// - Anyone can create
func (serv *MetadataServer) CreateProvider(ctx context.Context, providerRequest *pb.ProviderRequest) (*pb.Empty, error) {
	requestID, ctx, logger := serv.Logger.InitializeRequestID(ctx)
	logger = logger.WithResource("provider", providerRequest.Provider.Name, logging.NoVariant).WithProvider(providerRequest.Provider.Type, providerRequest.Provider.Name)
	provider := providerRequest.Provider
	logger.Infow("Creating Provider")
	providerRequest.RequestId = requestID

	_, err := serv.meta.CreateProvider(ctx, providerRequest)
	if err != nil {
		return nil, err
	}

	// The existence of a provider is part of the determination for checking provider health, hence why it
	// needs to happen prior to the call to CreateProvider, which is an upsert operation.
	shouldCheckProviderHealth, err := serv.shouldCheckProviderHealth(ctx, provider)
	if err != nil {
		logger.Errorw("Failed to check provider health", "error", err)
		return nil, err
	}
	_, err = serv.meta.CreateProvider(ctx, providerRequest)
	if err != nil && grpc_status.Code(err) != codes.AlreadyExists {
		logger.Errorw("Failed to create provider", "error", err)
		return nil, err
	}
	if !serv.health.IsSupportedProvider(pt.Type(provider.Type)) {
		logger.Infow("Provider type is currently not supported for health check", "type", provider.Type)
		return &pb.Empty{}, nil
	}
	if shouldCheckProviderHealth {
		logger.Infow("Checking provider health", "name", provider.Name)

		err := serv.checkProviderHealth(ctx, provider.Name)
		if err != nil {
			logger.Errorw("Failed to set provider status", "error", err, "health check error", err)
			return nil, err
		}
	}
	return &pb.Empty{}, err
}

func (serv *MetadataServer) shouldCheckProviderHealth(ctx context.Context, provider *pb.Provider) (bool, error) {
	var existingProvider *pb.Provider
	logger := logging.GetLoggerFromContext(ctx)
	for {
		stream, err := serv.meta.GetProviders(ctx)
		if err != nil {
			logger.Errorw("Failed to get providers from metadata server", "error", err)
			return false, err
		}
		if err := stream.Send(&pb.NameRequest{Name: &pb.Name{Name: provider.Name}, RequestId: logger.GetRequestID().String()}); err != nil {
			logger.Errorw("Failed to send provider to server", "error", err)
			return false, err
		}
		res, err := stream.Recv()
		if grpc_status.Code(err) == codes.NotFound {
			break
		}
		if err != nil {
			logger.Errorw("Failed to receive provider from server", "error", err)
			return false, err
		}
		if res.Name == provider.Name && res.Type == provider.Type {
			existingProvider = res
			break
		}
	}
	// We should check provider health if:
	// 1. The provider does not exist
	// 2. The provider has no status set
	// 3. The provider exists but the config has changed
	// 4. The provider exists but the previous health check failed
	doHealthCheck := false
	if existingProvider == nil || existingProvider.Status == nil {
		doHealthCheck = true
	} else {
		isSameConfig := bytes.Equal(existingProvider.SerializedConfig, provider.SerializedConfig)
		if !isSameConfig || existingProvider.Status.Status == pb.ResourceStatus_FAILED {
			doHealthCheck = true
		}
	}
	return doHealthCheck, nil
}

func (serv *MetadataServer) checkProviderHealth(ctx context.Context, providerName string) error {
	var status *pb.ResourceStatus
	logger := logging.GetLoggerFromContext(ctx)
	logger.Infow("Checking provider health")
	isHealthy, err := serv.health.CheckProvider(providerName)
	if err != nil || !isHealthy {
		logger.Errorw("Provider health check failed", "error", err)

		errorStatus, ok := grpc_status.FromError(err)
		if !ok {
			logger.Infow("Unknown codes", "error status", errorStatus, "error", err)
			return err
		}
		errorProto := errorStatus.Proto()
		var errorStatusProto *pb.ErrorStatus
		if ok {
			errorStatusProto = &pb.ErrorStatus{Code: errorProto.Code, Message: errorProto.Message, Details: errorProto.Details}
		} else {
			errorStatusProto = nil
		}

		status = &pb.ResourceStatus{
			Status:       pb.ResourceStatus_FAILED,
			ErrorMessage: err.Error(),
			ErrorStatus:  errorStatusProto,
		}
	} else {
		logger.Infow("Provider health check passed", "name", providerName)
		status = &pb.ResourceStatus{
			Status: pb.ResourceStatus_READY,
		}
	}
	statusReq := &pb.SetStatusRequest{
		ResourceId: &pb.ResourceID{
			Resource: &pb.NameVariant{
				Name: providerName,
			},
			ResourceType: pb.ResourceType_PROVIDER,
		},
		Status: status,
	}
	_, statusErr := serv.meta.SetResourceStatus(ctx, statusReq)
	return statusErr
}

// rpc CreateSourceVariant(SourceVariant) returns (Empty);
func (serv *MetadataServer) CreateSourceVariant(ctx context.Context, sourceRequest *pb.SourceVariantRequest) (*pb.Empty, error) {
	requestID, ctx, logger := serv.Logger.InitializeRequestID(ctx)
	logger = logger.WithResource(logging.Source, sourceRequest.SourceVariant.Name, sourceRequest.SourceVariant.Variant).WithProvider(logging.SkipProviderType, sourceRequest.SourceVariant.Provider)
	source := sourceRequest.SourceVariant
	logger.Infow("Creating Source Variant")
	sourceRequest.RequestId = requestID
	switch casted := source.Definition.(type) {

	case *pb.SourceVariant_Transformation:
		switch transformationType := casted.Transformation.Type.(type) {
		case *pb.Transformation_SQLTransformation:
			logger.Debugw("Retrieving the sources from SQL Transformation", "transformation type", transformationType)
			sources := extractSourcesFromSQLTransformation(transformationType.SQLTransformation.Query)
			logger.Debugw("Setting the source in the SQL Transformation", "sources", sources)
			source.Definition.(*pb.SourceVariant_Transformation).Transformation.Type.(*pb.Transformation_SQLTransformation).SQLTransformation.Source = sources
		case *pb.Transformation_DFTransformation:
		}
	}
	return serv.meta.CreateSourceVariant(ctx, sourceRequest)
}

func extractSourcesFromSQLTransformation(query string) []*pb.NameVariant {
	numEscapes := strings.Count(query, "{{")
	sources := make([]*pb.NameVariant, numEscapes)
	for i := 0; i < numEscapes; i++ {
		split := strings.SplitN(query, "{{", 2)
		afterSplit := strings.SplitN(split[1], "}}", 2)
		key := strings.TrimSpace(afterSplit[0])
		nameVariant := strings.SplitN(key, ".", 2)
		sources[i] = &pb.NameVariant{Name: nameVariant[0], Variant: nameVariant[1]}
		query = afterSplit[1]
	}
	return sources
}

func (serv *MetadataServer) CreateEntity(ctx context.Context, entityRequest *pb.EntityRequest) (*pb.Empty, error) {
	requestID, ctx, logger := serv.Logger.InitializeRequestID(ctx)
	logger = logger.WithResource(logging.Entity, entityRequest.Entity.Name, logging.NoVariant)
	logger.Infow("Creating Entity")
	entityRequest.RequestId = requestID

	return serv.meta.CreateEntity(ctx, entityRequest)
}

// rpc RequestScheduleChange(ScheduleChangeRequest) returns (Empty);
func (serv *MetadataServer) RequestScheduleChange(ctx context.Context, req *pb.ScheduleChangeRequest) (*pb.Empty, error) {
	_, ctx, logger := serv.Logger.InitializeRequestID(ctx)
	logger.Infow("Requesting Schedule Change", "resource", req.ResourceId, "new schedule", req.Schedule)
	return serv.meta.RequestScheduleChange(ctx, req)
}

// rpc CreateFeatureVariant(FeatureVariant) returns (Empty);
func (serv *MetadataServer) CreateFeatureVariant(ctx context.Context, featureRequest *pb.FeatureVariantRequest) (*pb.Empty, error) {
	requestID, ctx, logger := serv.Logger.InitializeRequestID(ctx)
	logger = logger.WithResource("feature_variant", featureRequest.FeatureVariant.Name, featureRequest.FeatureVariant.Variant).WithProvider(logging.SkipProviderType, featureRequest.FeatureVariant.Provider)
	logger.Infow("Creating Feature Variant")
	featureRequest.RequestId = requestID
	return serv.meta.CreateFeatureVariant(ctx, featureRequest)
}

// rpc CreateLabelVariant(LabelVariant) returns (Empty);
func (serv *MetadataServer) CreateLabelVariant(ctx context.Context, labelRequest *pb.LabelVariantRequest) (*pb.Empty, error) {
	requestID, ctx, logger := serv.Logger.InitializeRequestID(ctx)
	logger = logger.WithResource(logging.LabelVariant, labelRequest.LabelVariant.Name, labelRequest.LabelVariant.Variant).WithProvider(logging.SkipProviderType, labelRequest.LabelVariant.Provider)
	label := labelRequest.LabelVariant
	logger.Infow("Creating Label Variant")
	labelRequest.RequestId = requestID

	// Issue #1044
	// TODO: Provider is incorrectly set on the client for non-Stream labels so we're going to pull it from the source
	isStream := label.GetStream() != nil
	if !isStream {
		protoSource := label.Source
		logger.Debugw("Finding label source", "name", protoSource.Name, "variant", protoSource.Variant)
		source, err := serv.client.GetSourceVariant(ctx, metadata.NameVariant{protoSource.Name, protoSource.Variant})
		if err != nil {
			logger.Errorw("Could not create label source variant", "error", err)
			return nil, err
		}
		label.Provider = source.Provider()
	}

	resp, err := serv.meta.CreateLabelVariant(ctx, labelRequest)
	if err != nil {
		logger.Errorw("Could not create label variant", "response", resp, "error", err)
	}
	logger.Debugw("Created label variant", "response", resp)
	return resp, err
}

func (serv *MetadataServer) CreateTrainingSetVariant(ctx context.Context, trainRequest *pb.TrainingSetVariantRequest) (*pb.Empty, error) {
	requestID, ctx, logger := serv.Logger.InitializeRequestID(ctx)
	logger = logger.WithResource(logging.TrainingSetVariant, trainRequest.TrainingSetVariant.Name, trainRequest.TrainingSetVariant.Variant).WithProvider(logging.SkipProviderType, trainRequest.TrainingSetVariant.Provider)
	train := trainRequest.TrainingSetVariant
	logger.Infow("Creating Training Set Variant")
	trainRequest.RequestId = requestID

	protoLabel := train.Label
	label, err := serv.client.GetLabelVariant(ctx, metadata.NameVariant{Name: protoLabel.Name, Variant: protoLabel.Variant})
	if err != nil {
		return nil, err
	}

	// Prior to introducing disparate sources for Spark jobs (e.g. Snowflake tables as inputs), the assumption was the provider
	// for the training set was the same as the provider for the label. This is no longer a certainty, so if the provider is set
	// in the proto, we'll use that. Otherwise, we'll use the provider from the label.
	if train.Provider == "" {
		train.Provider = label.Provider()
	}
	train.Provider = label.Provider()
	return serv.meta.CreateTrainingSetVariant(ctx, trainRequest)
}

func (serv *MetadataServer) CreateModel(ctx context.Context, modelRequest *pb.ModelRequest) (*pb.Empty, error) {
	requestID, ctx, logger := serv.Logger.InitializeRequestID(ctx)
	logger = logger.WithResource(logging.Model, modelRequest.Model.Name, logging.NoVariant)
	logger.Infow("Creating Model")
	modelRequest.RequestId = requestID

	return serv.meta.CreateModel(ctx, modelRequest)
}

// rpc WriteFeatures(stream StreamingFeatureVariant) returns (Empty);
func (serv *MetadataServer) WriteFeatures(stream pb.Api_WriteFeaturesServer) error {
	ctx := stream.Context()
	for {
		fv, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&pb.Empty{})
		}
		if err != nil {
			return err
		}

		serv.Logger.Infow("Received streaming feature", "name", fv.Name, "variant", fv.Variant)
		// Get feature variant
		feature, err := serv.client.GetFeatureVariant(ctx, metadata.NameVariant{Name: fv.Name, Variant: fv.Variant})
		if err != nil {
			return err
		}
		// Get the offline table
		stream := feature.GetStream()
		if stream == nil {
			return fferr.NewInternalError(fmt.Errorf("feature %v does not have a stream", feature))
		}
		serv.Logger.Infow("Fetching offline provider for streaming feature", "name", fv.Name, "variant", fv.Variant)
		offlineProvider, err := stream.FetchProvider(serv.client, ctx)
		if err != nil {
			return err
		}
		p, err := provider.Get(pt.Type(offlineProvider.Type()), offlineProvider.SerializedConfig())
		if err != nil {
			return err
		}
		serv.Logger.Infow("Getting online store for streaming feature", "name", fv.Name, "variant", fv.Variant, "provider_type", p.Type())
		offlineStore, err := p.AsOfflineStore()
		if err != nil {
			return err
		}
		offlineTable, err := offlineStore.GetResourceTable(provider.ResourceID{
			Name: feature.Name(), Variant: feature.Variant(), Type: provider.Feature,
		})
		if err != nil {
			return err
		}
		// Get the online table
		serv.Logger.Infow("Fetching online provider for streaming feature", "name", fv.Name, "variant", fv.Variant)
		onlineProvider, err := feature.FetchProvider(serv.client, ctx)
		if err != nil {
			return err
		}
		p, err = provider.Get(pt.Type(onlineProvider.Type()), onlineProvider.SerializedConfig())
		if err != nil {
			return err
		}
		serv.Logger.Infow("Getting online store for streaming feature", "name", fv.Name, "variant", fv.Variant, "provider_type", p.Type())
		onlineStore, err := p.AsOnlineStore()
		if err != nil {
			return err
		}
		onlineTable, err := onlineStore.GetTable(feature.Name(), feature.Variant())
		if err != nil {
			return err
		}
		serv.Logger.Infow("Writing streaming feature to offline store", "name", fv.Name, "variant", fv.Variant)
		// Write the feature to the offline/online tables
		err = offlineTable.Write(provider.ResourceRecord{
			Entity: fv.Entity,
			Value:  fv.Value,
			TS:     fv.Ts.AsTime(),
		})
		if err != nil {
			return err
		}
		serv.Logger.Infow("Writing streaming feature to online store", "name", fv.Name, "variant", fv.Variant)
		// Currently, we always overwrite in the online table, but we will need to eventually write the most recent
		// value for each entity.
		err = onlineTable.Set(fv.Entity, fv.Value)
		if err != nil {
			return err
		}
	}
}

func (serv *OnlineServer) FeatureServe(ctx context.Context, req *srv.FeatureServeRequest) (*srv.FeatureRow, error) {
	_, ctx, logger := serv.Logger.InitializeRequestID(ctx)
	logger.Infow("Serving Features", "request", req.String())
	return serv.client.FeatureServe(ctx, req)
}

func (serv *OnlineServer) BatchFeatureServe(req *srv.BatchFeatureServeRequest, stream srv.Feature_BatchFeatureServeServer) error {
	_, ctx, logger := serv.Logger.InitializeRequestID(context.Background())
	logger.Infow("Serving Batch Features", "request", req.String())
	client, err := serv.client.BatchFeatureServe(ctx, req)
	if err != nil {
		logger.Errorw("Failed to serve batch features", "request", req.String(), "error", err)
		return fmt.Errorf("could not serve batch features: %v", err)
	}
	for {
		row, err := client.Recv()
		if err != nil {
			if err == io.EOF {
				logger.Debugw("End of stream reached. Stream request completed")
				return nil
			}
			logger.Errorw("Failed to receive row from client", "row", row, "error", err)
			return err
		}
		if err := stream.Send(row); err != nil {
			logger.Errorw("Failed to write to stream", "error", err)
			return err
		}
	}

}

func (serv *OnlineServer) TrainingData(req *srv.TrainingDataRequest, stream srv.Feature_TrainingDataServer) error {
	_, ctx, logger := serv.Logger.InitializeRequestID(context.Background())
	logger.Infow("Serving Training Data", "id", req.Id.String())
	client, err := serv.client.TrainingData(ctx, req)
	if err != nil {
		logger.Errorw("Failed to get Training Data client", "error", err)
		return err
	}
	for {
		row, err := client.Recv()
		if err != nil {
			if err == io.EOF {
				logger.Debugw("End of stream reached. Stream request completed")
				return nil
			}
			logger.Errorw("Failed to receive row from client", "row", row, "error", err)
			return err
		}
		if err := stream.Send(row); err != nil {
			logger.Errorw("Failed to write to stream", "error", err)
			return err
		}
	}
}

func (serv *OnlineServer) TrainTestSplit(stream srv.Feature_TrainTestSplitServer) error {
	_, ctx, logger := serv.Logger.InitializeRequestID(context.Background())
	logger.Infow("Starting Training Test Split Stream")
	clientStream, err := serv.client.TrainTestSplit(ctx)
	if err != nil {
		logger.Errorw("Failed to serve training test split", "error", err)
		return fmt.Errorf("could not serve training test split: %v", err)
	}

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			logger.Infow("Client has closed the stream")
			if err := clientStream.CloseSend(); err != nil {
				logger.Errorw("Failed to close send direction to downstream service", "error", err)
				return fferr.NewInternalError(err)
			}
			return nil
		}
		if err != nil {
			logger.Errorw("Error receiving from client stream", "error", err)
			return err
		}
		logger.Infow("Getting request from stream, Training data id", req.Id)
		if err := clientStream.Send(req); err != nil {
			logger.Errorw("Failed to send request to downstream service", "error", err)
			return fferr.NewInternalError(err)
		}

		resp, err := clientStream.Recv()
		if err == io.EOF {
			logger.Infow("Downstream service has closed the stream")
			return nil
		}
		if err != nil {
			logger.Errorw("Error receiving from downstream service", "error", err)
			return fferr.NewInternalError(err)
		}

		if err := stream.Send(resp); err != nil {
			logger.Errorw("Failed to send response to client", "error", err)
			return fferr.NewInternalError(err)
		}

	}
}

func (serv *OnlineServer) TrainingDataColumns(ctx context.Context, req *srv.TrainingDataColumnsRequest) (*srv.TrainingColumns, error) {
	_, ctx, logger := serv.Logger.InitializeRequestID(ctx)
	logger.Infow("Serving Training Set Columns", "id", req.Id.String())
	return serv.client.TrainingDataColumns(ctx, req)
}

func (serv *OnlineServer) SourceData(req *srv.SourceDataRequest, stream srv.Feature_SourceDataServer) error {
	_, ctx, logger := serv.Logger.InitializeRequestID(context.Background())
	logger.Infow("Serving Source Data", "id", req.Id.String())
	if req.Limit == 0 {
		err := fferr.NewInvalidArgumentError(fmt.Errorf("limit must be greater than 0"))
		logger.Errorw("Limit must be greater than 0", "invalid-argument", req.Limit, "error", err)
		return err
	}
	client, err := serv.client.SourceData(ctx, req)
	if err != nil {
		logger.Errorw("Failed to get Source Data client", "error", err)
		return err
	}
	for {
		row, err := client.Recv()
		if err != nil {
			if err == io.EOF {
				logger.Debugw("End of stream reached. Stream request completed")
				return nil
			}
			logger.Errorw("Failed to receive row from client", "row", row, "error", err)
			return err
		}
		if err := stream.Send(row); err != nil {
			logger.Errorw("failed to write to source data stream", "row", row, "error", err)
			return err
		}
	}
}

func (serv *OnlineServer) SourceColumns(ctx context.Context, req *srv.SourceColumnRequest) (*srv.SourceDataColumns, error) {
	_, ctx, logger := serv.Logger.InitializeRequestID(ctx)
	logger.Infow("Serving Source Columns", "id", req.Id.String())
	return serv.client.SourceColumns(ctx, req)
}

func (serv *OnlineServer) Nearest(ctx context.Context, req *srv.NearestRequest) (*srv.NearestResponse, error) {
	_, ctx, logger := serv.Logger.InitializeRequestID(ctx)
	logger.Infow("Serving Nearest", "id", req.Id.String())
	return serv.client.Nearest(ctx, req)
}

func (serv *OnlineServer) GetResourceLocation(ctx context.Context, req *srv.ResourceIdRequest) (*srv.ResourceLocation, error) {
	_, ctx, logger := serv.Logger.InitializeRequestID(ctx)
	logger.Infow("Serving Resource Location", "resource", req.String())
	loc, err := serv.client.GetResourceLocation(ctx, req)
	if err != nil {
		logger.Errorw("Failed to get resource location", "error", err)
	}
	return loc, err
}

func (serv *ApiServer) Serve() error {
	logger := logging.NewLogger("serve")
	logger.Infow("Starting server", "address", serv.address)
	if serv.grpcServer != nil {
		logger.Errorw("Server already running")
		return fferr.NewInternalError(fmt.Errorf("server already running"))
	}
	lis, err := net.Listen("tcp", serv.address)
	if err != nil {
		logger.Errorw("Failed to listen", "error", err)
		return fferr.NewInternalError(err)
	}
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		// grpc.WithUnaryInterceptor(fferr.UnaryClientInterceptor()),
		// grpc.WithStreamInterceptor(fferr.StreamClientInterceptor()),
	}
	metaConn, err := grpc.Dial(serv.metadata.address, opts...)
	if err != nil {
		logger.Errorw("Failed to dial metadata server", "error", err)
		return fferr.NewInternalError(err)
	}
	servConn, err := grpc.Dial(serv.online.address, opts...)
	if err != nil {
		logger.Errorw("Failed to dial serving server", "error", err)
		return fferr.NewInternalError(err)
	}
	serv.metadata.meta = pb.NewMetadataClient(metaConn)
	client, err := metadata.NewClient(serv.metadata.address, serv.Logger)
	if err != nil {
		logger.Errorw("Failed to create metadata client", "error", err)
		return err
	}
	serv.metadata.client = client
	serv.online.client = srv.NewFeatureClient(servConn)
	serv.metadata.health = health.NewHealth(client)
	logger.Infof("Created metadata client successfully.")
	return serv.ServeOnListener(lis)
}

func (serv *ApiServer) ServeOnListener(lis net.Listener) error {
	serv.listener = lis
	var (
		logrusLogger = logrus.New()
		customFunc   = func(code codes.Code) logrus.Level {
			if code == codes.OK {
				return logrus.DebugLevel
			}
			return logrus.DebugLevel
		}
	)
	logrusEntry := logrus.NewEntry(logrusLogger)
	lorgusOpts := []grpc_logrus.Option{
		grpc_logrus.WithLevels(customFunc),
	}
	grpc_logrus.ReplaceGrpcLogger(logrusEntry)
	minTimeStr := helpers.GetEnv("FEATUREFORM_KEEPALIVE_MINTIME", "1")
	minTime, err := strconv.ParseInt(minTimeStr, 0, 0)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	kaep := keepalive.EnforcementPolicy{
		MinTime: time.Duration(minTime) * time.Minute, // minimum amount of time a client should wait before sending a keepalive ping
	}
	kaTimeout := helpers.GetEnv("FEATUREFORM_KEEPALIVE_TIMEOUT", "5")
	timeout, err := strconv.ParseInt(kaTimeout, 0, 0)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	kasp := keepalive.ServerParameters{
		Timeout: time.Duration(timeout) * time.Minute, // time after which the connection is closed if no activity
	}

	grpc_logrus.ReplaceGrpcLogger(logrusEntry)
	minTimeStr = helpers.GetEnv("FEATUREFORM_KEEPALIVE_MINTIME", "1")
	minTime, err = strconv.ParseInt(minTimeStr, 0, 0)
	if err != nil {
		return fferr.NewInternalError(err)
	}

	kaep = keepalive.EnforcementPolicy{
		MinTime: (time.Duration(minTime) * time.Minute), // minimum amount of time a client should wait before sending a keepalive ping
	}
	kaTimeout = helpers.GetEnv("FEATUREFORM_KEEPALIVE_TIMEOUT", "5")
	timeout, err = strconv.ParseInt(kaTimeout, 0, 0)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	kasp = keepalive.ServerParameters{
		Timeout: time.Duration(timeout) * time.Minute, // time after which the connection is closed if no activity
	}

	opt := []grpc.ServerOption{
		grpc.StreamInterceptor(
			grpc_middleware.ChainStreamServer(
				grpc_logrus.StreamServerInterceptor(logrusEntry, lorgusOpts...),
			),
		),
		grpc.UnaryInterceptor(
			grpc_middleware.ChainUnaryServer(
				grpc_logrus.UnaryServerInterceptor(logrusEntry, lorgusOpts...),
			),
		),
		grpc.KeepaliveEnforcementPolicy(kaep),
		grpc.KeepaliveParams(kasp),
	}
	grpcServer := grpc.NewServer(opt...)
	reflection.Register(grpcServer)
	pb.RegisterApiServer(grpcServer, &serv.metadata)
	srv.RegisterFeatureServer(grpcServer, &serv.online)
	serv.grpcServer = grpcServer
	serv.Logger.Infow("Server starting", "Address", serv.listener.Addr().String())
	return grpcServer.Serve(lis)
}

func (serv *ApiServer) GracefulStop() error {
	if serv.grpcServer == nil {
		return fferr.NewInternalError(fmt.Errorf("server not running"))
	}
	serv.grpcServer.GracefulStop()
	serv.grpcServer = nil
	serv.listener = nil
	return nil
}

func handleHealthCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Strict-Transport-Security", "max-age=63072000; includeSubDomains")
	w.WriteHeader(http.StatusOK)

	_, err := io.WriteString(w, "OK")
	if err != nil {
		fmt.Printf("health check write response error: %+v", err)
	}

}

func handleIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Strict-Transport-Security", "max-age=63072000; includeSubDomains")
	w.Header().Set("Content-Type", "text/html")
	w.WriteHeader(http.StatusOK)

	_, err := io.WriteString(w, `<html><body>Welcome to featureform</body></html>`)
	if err != nil {
		fmt.Printf("index / write response error: %+v", err)
	}

}

func handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	imageVersion := help.GetEnv("IMAGE_VERSION", "0.0.0")
	currentTime := time.Now().UTC()
	status := map[string]string{
		"status":  "OK",
		"app":     "featureform",
		"version": imageVersion,
		"time":    currentTime.Format("1/2/06, 3:04:05 PM MST"),
	}

	jsonResponse, err := json.Marshal(status)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(jsonResponse)
}

func StartHttpsServer(port string) error {
	mux := &http.ServeMux{}

	mux.HandleFunc("/status", handleStatus)
	// handles possible trailing slash
	mux.HandleFunc("/status/", handleStatus)
	// Health check endpoint will handle all /_ah/* requests
	// e.g. /_ah/live, /_ah/ready and /_ah/lb
	// Create separate routes for specific health requests as needed.
	mux.HandleFunc("/_ah/", handleHealthCheck)
	mux.HandleFunc("/", handleIndex)
	// Add more routes as needed.

	// Set timeouts so that a slow or malicious client doesn't hold resources forever.
	httpsSrv := &http.Server{
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		IdleTimeout:  60 * time.Second,
		Handler:      mux,
		Addr:         port,
	}

	fmt.Printf("starting HTTP server on port %s", port)

	return httpsSrv.ListenAndServe()
}
