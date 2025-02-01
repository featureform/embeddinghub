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
	"time"

	pb "github.com/featureform/metadata/proto"
	pc "github.com/featureform/provider/provider_config"
	grpc "google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type mockSourceClient struct {
	grpc.ClientStream
	ctx  context.Context
	sent bool
}

func (x *mockSourceClient) Send(*pb.NameVariantRequest) error {
	return nil
}

func (x *mockSourceClient) Context() context.Context {
	return x.ctx
}

func (x *mockSourceClient) CloseSend() error {
	return nil
}

func (x *mockSourceClient) Recv() (sv *pb.SourceVariant, e error) {
	pbsv := &pb.SourceVariant{Name: "transactions",
		Variant:      "2024-09-06t21-07-40",
		Owner:        "anthony@featureform.com",
		Provider:     "postgres",
		Definition:   &pb.SourceVariant_PrimaryData{},
		Table:        "test.table",
		Status:       &pb.ResourceStatus{Status: pb.ResourceStatus_READY},
		Trainingsets: []*pb.NameVariant{},
		Features:     []*pb.NameVariant{},
		Labels:       []*pb.NameVariant{},
		Schedule:     "test.schedule",
		Tags:         &pb.Tags{},
		Properties:   &pb.Properties{},
	}
	if !x.sent {
		x.sent = true
		return pbsv, nil
	} else {
		return nil, io.EOF
	}
}

type mockProviderClient struct {
	grpc.ClientStream
	ctx  context.Context
	sent bool
}

func (x *mockProviderClient) Send(*pb.NameRequest) error {
	return nil
}

func (x *mockProviderClient) Context() context.Context {
	return x.ctx
}

func (x *mockProviderClient) CloseSend() error {
	return nil
}

var unitTestConfig = pc.UnitTestConfig{
	Username: "test.username",
	Password: "test.password",
}

func (x *mockProviderClient) Recv() (pv *pb.Provider, e error) {
	pbpv := &pb.Provider{
		Name:             "test.name",
		Description:      "test.description",
		Type:             "UNIT_TEST",
		Software:         "test.software",
		Sources:          []*pb.NameVariant{},
		Features:         []*pb.NameVariant{},
		Trainingsets:     []*pb.NameVariant{},
		Labels:           []*pb.NameVariant{},
		Tags:             &pb.Tags{},
		Properties:       &pb.Properties{},
		SerializedConfig: unitTestConfig.Serialize(),
	}
	if !x.sent {
		x.sent = true
		return pbpv, nil
	} else {
		return nil, io.EOF
	}
}

type mockFeatureClient struct {
	grpc.ClientStream
	ctx  context.Context
	sent bool
}

func (x *mockFeatureClient) Send(*pb.NameVariantRequest) error {
	return nil
}

func (x *mockFeatureClient) Context() context.Context {
	return x.ctx
}

func (x *mockFeatureClient) CloseSend() error {
	return nil
}

func (x *mockFeatureClient) Recv() (fv *pb.FeatureVariant, e error) {
	pbfv := &pb.FeatureVariant{Name: "avg_transactions",
		Variant:      "2024-08-21t18-16-06",
		Owner:        "anthony@featureform.com",
		Provider:     "latestv1test-redis",
		Entity:       "test.entity",
		Status:       &pb.ResourceStatus{Status: pb.ResourceStatus_FAILED},
		Source:       &pb.NameVariant{Name: "average_user_transaction", Variant: "2024-08-21t18-16-06"},
		Location:     &pb.FeatureVariant_Columns{Columns: &pb.Columns{Entity: "user_id", Value: "avg_transaction_amt"}},
		Trainingsets: []*pb.NameVariant{},
		Schedule:     "test.schedule",
		Tags:         &pb.Tags{Tag: []string{"testV1", "testV1-READY"}},
		Properties:   &pb.Properties{},
	}
	if !x.sent {
		x.sent = true
		return pbfv, nil
	} else {
		return nil, io.EOF
	}
}

type mockLabelClient struct {
	grpc.ClientStream
	ctx  context.Context
	sent bool
}

func (x *mockLabelClient) Send(*pb.NameVariantRequest) error {
	return nil
}

func (x *mockLabelClient) Context() context.Context {
	return x.ctx
}

func (x *mockLabelClient) CloseSend() error {
	return nil
}

func (x *mockLabelClient) Recv() (fv *pb.LabelVariant, e error) {
	pblv := &pb.LabelVariant{Name: "trans_label",
		Variant:      "2024-09-27t15-58-54",
		Owner:        "riddhi@featureform.com",
		Provider:     "postgres-quickstart",
		Source:       &pb.NameVariant{Name: "transaction", Variant: "variant_447335"},
		Location:     &pb.LabelVariant_Columns{Columns: &pb.Columns{Entity: "customerid", Value: "custlocation", Ts: "timestamp"}},
		Entity:       "test.entity",
		Trainingsets: []*pb.NameVariant{},
		Tags:         &pb.Tags{Tag: []string{"testV1", "testV1-READY"}},
		Properties:   &pb.Properties{},
	}
	if !x.sent {
		x.sent = true
		return pblv, nil
	} else {
		return nil, io.EOF
	}
}

type mockTrainingSetClient struct {
	grpc.ClientStream
	ctx  context.Context
	sent bool
}

func (x *mockTrainingSetClient) Send(*pb.NameVariantRequest) error {
	return nil
}

func (x *mockTrainingSetClient) Context() context.Context {
	return x.ctx
}

func (x *mockTrainingSetClient) CloseSend() error {
	return nil
}

func (x *mockTrainingSetClient) Recv() (fv *pb.TrainingSetVariant, e error) {
	pbtsv := &pb.TrainingSetVariant{Name: "my_training_set",
		Variant:    "2024-10-23t17-36-17",
		Owner:      "riddhi@featureform.com",
		Provider:   "postgres-quickstart",
		Created:    &timestamppb.Timestamp{Seconds: time.Now().Unix()},
		Label:      &pb.NameVariant{Name: "f7_label", Variant: "label_variant"},
		Status:     &pb.ResourceStatus{Status: pb.ResourceStatus_READY},
		Schedule:   "test.schedule",
		Tags:       &pb.Tags{Tag: []string{"dummyTag"}},
		Properties: &pb.Properties{},
	}
	if !x.sent {
		x.sent = true
		return pbtsv, nil
	} else {
		return nil, io.EOF
	}
}

type MetadataServerMock struct {
}

func (m MetadataServerMock) GetSourceVariants(ctx context.Context, opts ...grpc.CallOption) (pb.Metadata_GetSourceVariantsClient, error) {
	return &mockSourceClient{
		ctx:  context.Background(),
		sent: false,
	}, nil
}

func (MetadataServerMock) ListFeatures(ctx context.Context, in *pb.ListRequest, opts ...grpc.CallOption) (pb.Metadata_ListFeaturesClient, error) {
	return nil, nil
}

func (MetadataServerMock) CreateFeatureVariant(ctx context.Context, in *pb.FeatureVariantRequest, opts ...grpc.CallOption) (*pb.Empty, error) {
	return nil, nil
}
func (MetadataServerMock) GetFeatures(ctx context.Context, opts ...grpc.CallOption) (pb.Metadata_GetFeaturesClient, error) {
	return nil, nil
}

func (MetadataServerMock) GetFeatureVariants(ctx context.Context, opts ...grpc.CallOption) (pb.Metadata_GetFeatureVariantsClient, error) {
	return &mockFeatureClient{
		ctx:  context.Background(),
		sent: false,
	}, nil
}
func (MetadataServerMock) ListLabels(ctx context.Context, in *pb.ListRequest, opts ...grpc.CallOption) (pb.Metadata_ListLabelsClient, error) {
	return nil, nil
}
func (MetadataServerMock) CreateLabelVariant(ctx context.Context, in *pb.LabelVariantRequest, opts ...grpc.CallOption) (*pb.Empty, error) {
	return nil, nil
}
func (MetadataServerMock) GetLabels(ctx context.Context, opts ...grpc.CallOption) (pb.Metadata_GetLabelsClient, error) {
	return nil, nil
}
func (MetadataServerMock) GetLabelVariants(ctx context.Context, opts ...grpc.CallOption) (pb.Metadata_GetLabelVariantsClient, error) {
	return &mockLabelClient{
		ctx:  context.Background(),
		sent: false,
	}, nil
}
func (MetadataServerMock) ListTrainingSets(ctx context.Context, in *pb.ListRequest, opts ...grpc.CallOption) (pb.Metadata_ListTrainingSetsClient, error) {
	return nil, nil
}
func (MetadataServerMock) CreateTrainingSetVariant(ctx context.Context, in *pb.TrainingSetVariantRequest, opts ...grpc.CallOption) (*pb.Empty, error) {
	return nil, nil
}
func (MetadataServerMock) GetTrainingSets(ctx context.Context, opts ...grpc.CallOption) (pb.Metadata_GetTrainingSetsClient, error) {
	return nil, nil
}
func (MetadataServerMock) GetTrainingSetVariants(ctx context.Context, opts ...grpc.CallOption) (pb.Metadata_GetTrainingSetVariantsClient, error) {
	return &mockTrainingSetClient{
		ctx:  context.Background(),
		sent: false,
	}, nil
}
func (MetadataServerMock) ListSources(ctx context.Context, in *pb.ListRequest, opts ...grpc.CallOption) (pb.Metadata_ListSourcesClient, error) {
	return nil, nil
}
func (MetadataServerMock) CreateSourceVariant(ctx context.Context, in *pb.SourceVariantRequest, opts ...grpc.CallOption) (*pb.Empty, error) {
	return nil, nil
}
func (MetadataServerMock) GetSources(ctx context.Context, opts ...grpc.CallOption) (pb.Metadata_GetSourcesClient, error) {
	return nil, nil
}

func (MetadataServerMock) GetEquivalent(ctx context.Context, req *pb.GetEquivalentRequest, opts ...grpc.CallOption) (*pb.ResourceVariant, error) {
	return nil, nil
}

func (MetadataServerMock) GetProviders(ctx context.Context, opts ...grpc.CallOption) (pb.Metadata_GetProvidersClient, error) {
	return &mockProviderClient{
		ctx:  context.Background(),
		sent: false,
	}, nil
}

func (MetadataServerMock) ListUsers(ctx context.Context, in *pb.ListRequest, opts ...grpc.CallOption) (pb.Metadata_ListUsersClient, error) {
	return nil, nil
}
func (MetadataServerMock) CreateUser(ctx context.Context, in *pb.UserRequest, opts ...grpc.CallOption) (*pb.Empty, error) {
	return nil, nil
}
func (MetadataServerMock) GetUsers(ctx context.Context, opts ...grpc.CallOption) (pb.Metadata_GetUsersClient, error) {
	return nil, nil
}
func (MetadataServerMock) ListProviders(ctx context.Context, in *pb.ListRequest, opts ...grpc.CallOption) (pb.Metadata_ListProvidersClient, error) {
	return nil, nil
}
func (MetadataServerMock) CreateProvider(ctx context.Context, in *pb.ProviderRequest, opts ...grpc.CallOption) (*pb.Empty, error) {
	return nil, nil
}

func (MetadataServerMock) ListEntities(ctx context.Context, in *pb.ListRequest, opts ...grpc.CallOption) (pb.Metadata_ListEntitiesClient, error) {
	return nil, nil
}

func (MetadataServerMock) CreateEntity(ctx context.Context, in *pb.EntityRequest, opts ...grpc.CallOption) (*pb.Empty, error) {
	return nil, nil
}
func (MetadataServerMock) GetEntities(ctx context.Context, opts ...grpc.CallOption) (pb.Metadata_GetEntitiesClient, error) {
	return nil, nil
}
func (MetadataServerMock) ListModels(ctx context.Context, in *pb.ListRequest, opts ...grpc.CallOption) (pb.Metadata_ListModelsClient, error) {
	return nil, nil
}
func (MetadataServerMock) CreateModel(ctx context.Context, in *pb.ModelRequest, opts ...grpc.CallOption) (*pb.Empty, error) {
	return nil, nil
}
func (MetadataServerMock) GetModels(ctx context.Context, opts ...grpc.CallOption) (pb.Metadata_GetModelsClient, error) {
	return nil, nil
}
func (MetadataServerMock) SetResourceStatus(ctx context.Context, in *pb.SetStatusRequest, opts ...grpc.CallOption) (*pb.Empty, error) {
	return nil, nil
}
func (MetadataServerMock) RequestScheduleChange(ctx context.Context, in *pb.ScheduleChangeRequest, opts ...grpc.CallOption) (*pb.Empty, error) {
	return nil, nil
}

func (MetadataServerMock) Run(ctx context.Context, in *pb.RunRequest, opts ...grpc.CallOption) (*pb.Empty, error) {
	return nil, nil
}

func (m MetadataServerMock) MarkForDeletion(ctx context.Context, in *pb.MarkForDeletionRequest, opts ...grpc.CallOption) (*pb.MarkForDeletionResponse, error) {
	return &pb.MarkForDeletionResponse{}, nil
}

func (m MetadataServerMock) GetStagedForDeletionResource(ctx context.Context, in *pb.GetStagedForDeletionResourceRequest, opts ...grpc.CallOption) (*pb.GetStagedForDeletionResourceResponse, error) {
	return &pb.GetStagedForDeletionResourceResponse{}, nil
}

func (m MetadataServerMock) FinalizeDeletion(ctx context.Context, in *pb.FinalizeDeletionRequest, opts ...grpc.CallOption) (*pb.FinalizeDeletionResponse, error) {
	return &pb.FinalizeDeletionResponse{}, nil
}

func (m MetadataServerMock) PruneResource(ctx context.Context, in *pb.PruneResourceRequest, opts ...grpc.CallOption) (*pb.PruneResourceResponse, error) {
	return &pb.PruneResourceResponse{}, nil
}
