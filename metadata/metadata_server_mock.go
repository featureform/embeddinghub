package metadata

import (
	"context"
	"io"

	pb "github.com/featureform/metadata/proto"
	pc "github.com/featureform/provider/provider_config"
	grpc "google.golang.org/grpc"
)

type mockSourceClient struct {
	grpc.ClientStream
	ctx  context.Context
	sent bool
}

func (x *mockSourceClient) Send(*pb.NameVariant) error {
	return nil
}

func (x *mockSourceClient) Context() context.Context {
	return x.ctx
}

func (x *mockSourceClient) CloseSend() error {
	return nil
}

func (x *mockSourceClient) Recv() (sv *pb.SourceVariant, e error) {
	pbsv := &pb.SourceVariant{Name: "test.name",
		Variant:      "test.variant",
		Owner:        "test.owner",
		Provider:     "UNIT_TEST",
		Definition:   &pb.SourceVariant_PrimaryData{},
		Table:        "test.table",
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

func (x *mockProviderClient) Send(*pb.Name) error {
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

type MetadataServerMock struct {
}

func (m MetadataServerMock) GetSourceVariants(ctx context.Context, opts ...grpc.CallOption) (pb.Metadata_GetSourceVariantsClient, error) {
	return &mockSourceClient{
		ctx:  context.Background(),
		sent: false,
	}, nil
}

func (MetadataServerMock) ListFeatures(ctx context.Context, in *pb.Empty, opts ...grpc.CallOption) (pb.Metadata_ListFeaturesClient, error) {
	return nil, nil
}

func (MetadataServerMock) CreateFeatureVariant(ctx context.Context, in *pb.FeatureVariant, opts ...grpc.CallOption) (*pb.Empty, error) {
	return nil, nil
}
func (MetadataServerMock) GetFeatures(ctx context.Context, opts ...grpc.CallOption) (pb.Metadata_GetFeaturesClient, error) {
	return nil, nil
}

func (MetadataServerMock) GetFeatureVariants(ctx context.Context, opts ...grpc.CallOption) (pb.Metadata_GetFeatureVariantsClient, error) {
	return nil, nil
}
func (MetadataServerMock) ListLabels(ctx context.Context, in *pb.Empty, opts ...grpc.CallOption) (pb.Metadata_ListLabelsClient, error) {
	return nil, nil
}
func (MetadataServerMock) CreateLabelVariant(ctx context.Context, in *pb.LabelVariant, opts ...grpc.CallOption) (*pb.Empty, error) {
	return nil, nil
}
func (MetadataServerMock) GetLabels(ctx context.Context, opts ...grpc.CallOption) (pb.Metadata_GetLabelsClient, error) {
	return nil, nil
}
func (MetadataServerMock) GetLabelVariants(ctx context.Context, opts ...grpc.CallOption) (pb.Metadata_GetLabelVariantsClient, error) {
	return nil, nil
}
func (MetadataServerMock) ListTrainingSets(ctx context.Context, in *pb.Empty, opts ...grpc.CallOption) (pb.Metadata_ListTrainingSetsClient, error) {
	return nil, nil
}
func (MetadataServerMock) CreateTrainingSetVariant(ctx context.Context, in *pb.TrainingSetVariant, opts ...grpc.CallOption) (*pb.Empty, error) {
	return nil, nil
}
func (MetadataServerMock) GetTrainingSets(ctx context.Context, opts ...grpc.CallOption) (pb.Metadata_GetTrainingSetsClient, error) {
	return nil, nil
}
func (MetadataServerMock) GetTrainingSetVariants(ctx context.Context, opts ...grpc.CallOption) (pb.Metadata_GetTrainingSetVariantsClient, error) {
	return nil, nil
}
func (MetadataServerMock) ListSources(ctx context.Context, in *pb.Empty, opts ...grpc.CallOption) (pb.Metadata_ListSourcesClient, error) {
	return nil, nil
}
func (MetadataServerMock) CreateSourceVariant(ctx context.Context, in *pb.SourceVariant, opts ...grpc.CallOption) (*pb.Empty, error) {
	return nil, nil
}
func (MetadataServerMock) GetSources(ctx context.Context, opts ...grpc.CallOption) (pb.Metadata_GetSourcesClient, error) {
	return nil, nil
}

func (MetadataServerMock) GetEquivalent(ctx context.Context, req *pb.ResourceVariant, opts ...grpc.CallOption) (*pb.ResourceVariant, error) {
	return nil, nil
}

func (MetadataServerMock) GetProviders(ctx context.Context, opts ...grpc.CallOption) (pb.Metadata_GetProvidersClient, error) {
	return &mockProviderClient{
		ctx:  context.Background(),
		sent: false,
	}, nil
}

func (MetadataServerMock) ListUsers(ctx context.Context, in *pb.Empty, opts ...grpc.CallOption) (pb.Metadata_ListUsersClient, error) {
	return nil, nil
}
func (MetadataServerMock) CreateUser(ctx context.Context, in *pb.User, opts ...grpc.CallOption) (*pb.Empty, error) {
	return nil, nil
}
func (MetadataServerMock) GetUsers(ctx context.Context, opts ...grpc.CallOption) (pb.Metadata_GetUsersClient, error) {
	return nil, nil
}
func (MetadataServerMock) ListProviders(ctx context.Context, in *pb.Empty, opts ...grpc.CallOption) (pb.Metadata_ListProvidersClient, error) {
	return nil, nil
}
func (MetadataServerMock) CreateProvider(ctx context.Context, in *pb.Provider, opts ...grpc.CallOption) (*pb.Empty, error) {
	return nil, nil
}

func (MetadataServerMock) ListEntities(ctx context.Context, in *pb.Empty, opts ...grpc.CallOption) (pb.Metadata_ListEntitiesClient, error) {
	return nil, nil
}

func (MetadataServerMock) CreateEntity(ctx context.Context, in *pb.Entity, opts ...grpc.CallOption) (*pb.Empty, error) {
	return nil, nil
}
func (MetadataServerMock) GetEntities(ctx context.Context, opts ...grpc.CallOption) (pb.Metadata_GetEntitiesClient, error) {
	return nil, nil
}
func (MetadataServerMock) ListModels(ctx context.Context, in *pb.Empty, opts ...grpc.CallOption) (pb.Metadata_ListModelsClient, error) {
	return nil, nil
}
func (MetadataServerMock) CreateModel(ctx context.Context, in *pb.Model, opts ...grpc.CallOption) (*pb.Empty, error) {
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
