// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metadata

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"time"

	pb "github.com/featureform/metadata/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	tspb "google.golang.org/protobuf/types/known/timestamppb"
)

type NameVariant struct {
	Name    string
	Variant string
}

func (variant NameVariant) Serialize() *pb.NameVariant {
	return &pb.NameVariant{
		Name:    variant.Name,
		Variant: variant.Variant,
	}
}

func (variant NameVariant) ClientString() string {
	return fmt.Sprintf("%s.%s", variant.Name, variant.Variant)
}

func parseNameVariant(serialized *pb.NameVariant) NameVariant {
	return NameVariant{
		Name:    serialized.Name,
		Variant: serialized.Variant,
	}
}

type NameVariants []NameVariant

func (variants NameVariants) Serialize() []*pb.NameVariant {
	serialized := make([]*pb.NameVariant, len(variants))
	for i, variant := range variants {
		serialized[i] = variant.Serialize()
	}
	return serialized
}

func parseNameVariants(protos []*pb.NameVariant) NameVariants {
	parsed := make([]NameVariant, len(protos))
	for i, serialized := range protos {
		parsed[i] = parseNameVariant(serialized)
	}
	return parsed
}

func (variants NameVariants) Names() []string {
	names := make([]string, len(variants))
	for i, variant := range variants {
		names[i] = variant.Name
	}
	return names
}

type Tags []string

type Properties map[string]string

func (properties Properties) Serialize() *pb.Properties {
	serialized := &pb.Properties{
		Property: map[string]*pb.Property{},
	}

	for key, val := range properties {
		serialized.Property[key] = &pb.Property{Value: &pb.Property_StringValue{StringValue: val}}
	}

	return serialized
}

type Client struct {
	Logger   *zap.SugaredLogger
	conn     *grpc.ClientConn
	GrpcConn pb.MetadataClient
}

type ResourceDef interface {
	ResourceType() ResourceType
}

// accessible to the frontend as it does not directly change status in metadata
func (client *Client) RequestScheduleChange(ctx context.Context, resID ResourceID, schedule string) error {
	nameVariant := pb.NameVariant{Name: resID.Name, Variant: resID.Variant}
	resourceID := pb.ResourceID{Resource: &nameVariant, ResourceType: resID.Type.Serialized()}
	scheduleChangeRequest := pb.ScheduleChangeRequest{ResourceId: &resourceID, Schedule: schedule}
	_, err := client.GrpcConn.RequestScheduleChange(ctx, &scheduleChangeRequest)
	return err
}

func (client *Client) SetStatus(ctx context.Context, resID ResourceID, status ResourceStatus, errorMessage string) error {
	nameVariant := pb.NameVariant{Name: resID.Name, Variant: resID.Variant}
	resourceID := pb.ResourceID{Resource: &nameVariant, ResourceType: resID.Type.Serialized()}
	resourceStatus := pb.ResourceStatus{Status: pb.ResourceStatus_Status(status), ErrorMessage: errorMessage}
	statusRequest := pb.SetStatusRequest{ResourceId: &resourceID, Status: &resourceStatus}
	_, err := client.GrpcConn.SetResourceStatus(ctx, &statusRequest)
	return err
}

func (client *Client) CreateAll(ctx context.Context, defs []ResourceDef) error {
	for _, def := range defs {
		if err := client.Create(ctx, def); err != nil {
			return err
		}
	}
	return nil
}

func (client *Client) Create(ctx context.Context, def ResourceDef) error {
	switch casted := def.(type) {
	case FeatureDef:
		return client.CreateFeatureVariant(ctx, casted)
	case LabelDef:
		return client.CreateLabelVariant(ctx, casted)
	case TrainingSetDef:
		return client.CreateTrainingSetVariant(ctx, casted)
	case SourceDef:
		return client.CreateSourceVariant(ctx, casted)
	case UserDef:
		return client.CreateUser(ctx, casted)
	case ProviderDef:
		return client.CreateProvider(ctx, casted)
	case EntityDef:
		return client.CreateEntity(ctx, casted)
	case ModelDef:
		return client.CreateModel(ctx, casted)
	default:
		return fmt.Errorf("%T not implemented in Create", casted)
	}
}

func (client *Client) ListFeatures(ctx context.Context) ([]*Feature, error) {
	stream, err := client.GrpcConn.ListFeatures(ctx, &pb.Empty{})
	if err != nil {
		return nil, err
	}
	return client.parseFeatureStream(stream)
}

func (client *Client) GetFeature(ctx context.Context, feature string) (*Feature, error) {
	featureList, err := client.GetFeatures(ctx, []string{feature})
	if err != nil {
		return nil, err
	}
	return featureList[0], nil
}

func (client *Client) GetFeatures(ctx context.Context, features []string) ([]*Feature, error) {
	stream, err := client.GrpcConn.GetFeatures(ctx)
	if err != nil {
		return nil, err
	}
	go func() {
		for _, feature := range features {
			stream.Send(&pb.Name{Name: feature})
		}
		err := stream.CloseSend()
		if err != nil {
			client.Logger.Errorw("Failed to close send", "Err", err)
		}
	}()
	return client.parseFeatureStream(stream)
}

func (client *Client) GetFeatureVariants(ctx context.Context, ids []NameVariant) ([]*FeatureVariant, error) {
	stream, err := client.GrpcConn.GetFeatureVariants(ctx)
	if err != nil {
		return nil, err
	}
	go func() {
		for _, id := range ids {
			stream.Send(&pb.NameVariant{Name: id.Name, Variant: id.Variant})
		}
		err := stream.CloseSend()
		if err != nil {
			client.Logger.Errorw("Failed to close send", "Err", err)
		}
	}()
	return client.parseFeatureVariantStream(stream)
}

func (client *Client) GetFeatureVariant(ctx context.Context, id NameVariant) (*FeatureVariant, error) {
	variants, err := client.GetFeatureVariants(ctx, []NameVariant{id})
	if err != nil {
		return nil, err
	}
	return variants[0], nil
}

type FeaturePrimaryData interface {
	isFeaturePrimaryData() bool
}

type FeatureDef struct {
	Name        string
	Variant     string
	Source      NameVariant
	Type        string
	Entity      string
	Owner       string
	Description string
	Provider    string
	Schedule    string
	Location    interface{}
	Tags        Tags
	Properties  Properties
	Mode        ComputationMode
	IsOnDemand  bool
	IsEmbedding bool
}

type ResourceVariantColumns struct {
	Entity string
	Value  string
	TS     string
	Source string
}

func (c ResourceVariantColumns) SerializeFeatureColumns() *pb.FeatureVariant_Columns {
	return &pb.FeatureVariant_Columns{
		Columns: &pb.Columns{
			Entity: c.Entity,
			Value:  c.Value,
			Ts:     c.TS,
		},
	}
}

func (c ResourceVariantColumns) SerializeLabelColumns() *pb.LabelVariant_Columns {
	return &pb.LabelVariant_Columns{
		Columns: &pb.Columns{
			Entity: c.Entity,
			Value:  c.Value,
			Ts:     c.TS,
		},
	}
}

type PythonFunction struct {
	Query []byte
}

func (p PythonFunction) SerializePythonFunction() *pb.FeatureVariant_Function {
	return &pb.FeatureVariant_Function{
		Function: &pb.PythonFunction{
			Query: p.Query,
		},
	}
}

func (def FeatureDef) ResourceType() ResourceType {
	return FEATURE_VARIANT
}

func (def FeatureDef) Serialize() (*pb.FeatureVariant, error) {
	serialized := &pb.FeatureVariant{
		Name:        def.Name,
		Variant:     def.Variant,
		Source:      def.Source.Serialize(),
		Type:        def.Type,
		Entity:      def.Entity,
		Owner:       def.Owner,
		Description: def.Description,
		Status:      &pb.ResourceStatus{Status: pb.ResourceStatus_CREATED},
		Provider:    def.Provider,
		Schedule:    def.Schedule,
		Tags:        &pb.Tags{Tag: def.Tags},
		Properties:  def.Properties.Serialize(),
		Mode:        pb.ComputationMode(def.Mode),
		IsEmbedding: def.IsEmbedding,
	}
	switch x := def.Location.(type) {
	case ResourceVariantColumns:
		serialized.Location = def.Location.(ResourceVariantColumns).SerializeFeatureColumns()
	case PythonFunction:
		serialized.Location = def.Location.(PythonFunction).SerializePythonFunction()
	case nil:
		return nil, fmt.Errorf("FeatureDef Columns not set")
	default:
		return nil, fmt.Errorf("FeatureDef Columns has unexpected type %T", x)
	}
	return serialized, nil
}

func (client *Client) CreateFeatureVariant(ctx context.Context, def FeatureDef) error {
	serialized, err := def.Serialize()
	if err != nil {
		return err
	}
	_, err = client.GrpcConn.CreateFeatureVariant(ctx, serialized)
	return err
}

type featureStream interface {
	Recv() (*pb.Feature, error)
}

func (client *Client) parseFeatureStream(stream featureStream) ([]*Feature, error) {
	features := make([]*Feature, 0)
	for {
		serial, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		features = append(features, wrapProtoFeature(serial))
	}
	return features, nil
}

type featureVariantStream interface {
	Recv() (*pb.FeatureVariant, error)
}

func (client *Client) parseFeatureVariantStream(stream featureVariantStream) ([]*FeatureVariant, error) {
	features := make([]*FeatureVariant, 0)
	for {
		serial, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		features = append(features, wrapProtoFeatureVariant(serial))
	}
	return features, nil
}

func (client *Client) ListLabels(ctx context.Context) ([]*Label, error) {
	stream, err := client.GrpcConn.ListLabels(ctx, &pb.Empty{})
	if err != nil {
		return nil, err
	}
	return client.parseLabelStream(stream)
}

func (client *Client) GetLabel(ctx context.Context, label string) (*Label, error) {
	labelList, err := client.GetLabels(ctx, []string{label})
	if err != nil {
		return nil, err
	}
	return labelList[0], nil
}

func (client *Client) GetLabels(ctx context.Context, labels []string) ([]*Label, error) {
	stream, err := client.GrpcConn.GetLabels(ctx)
	if err != nil {
		return nil, err
	}
	go func() {
		for _, label := range labels {
			stream.Send(&pb.Name{Name: label})
		}
		err := stream.CloseSend()
		if err != nil {
			client.Logger.Errorw("Failed to close send", "Err", err)
		}
	}()
	return client.parseLabelStream(stream)
}

type LabelDef struct {
	Name        string
	Variant     string
	Description string
	Type        string
	Source      NameVariant
	Entity      string
	Owner       string
	Provider    string
	Location    interface{}
	Tags        Tags
	Properties  Properties
}

func (def LabelDef) ResourceType() ResourceType {
	return LABEL_VARIANT
}

func (def LabelDef) Serialize() (*pb.LabelVariant, error) {
	serialized := &pb.LabelVariant{
		Name:        def.Name,
		Variant:     def.Variant,
		Description: def.Description,
		Type:        def.Type,
		Source:      def.Source.Serialize(),
		Entity:      def.Entity,
		Owner:       def.Owner,
		Status:      &pb.ResourceStatus{Status: pb.ResourceStatus_NO_STATUS},
		Provider:    def.Provider,
		Tags:        &pb.Tags{Tag: def.Tags},
		Properties:  def.Properties.Serialize(),
	}
	switch x := def.Location.(type) {
	case ResourceVariantColumns:
		serialized.Location = def.Location.(ResourceVariantColumns).SerializeLabelColumns()
	case nil:
		return nil, fmt.Errorf("LabelDef Primary not set")
	default:
		return nil, fmt.Errorf("LabelDef Primary has unexpected type %T", x)
	}
	return serialized, nil
}

func (client *Client) CreateLabelVariant(ctx context.Context, def LabelDef) error {
	serialized, err := def.Serialize()
	if err != nil {
		return err
	}
	_, err = client.GrpcConn.CreateLabelVariant(ctx, serialized)
	return err
}

func (client *Client) GetLabelVariants(ctx context.Context, ids []NameVariant) ([]*LabelVariant, error) {
	stream, err := client.GrpcConn.GetLabelVariants(ctx)
	if err != nil {
		return nil, err
	}
	go func() {
		for _, id := range ids {
			stream.Send(&pb.NameVariant{Name: id.Name, Variant: id.Variant})
		}
		err := stream.CloseSend()
		if err != nil {
			client.Logger.Errorw("Failed to close send", "Err", err)
		}
	}()
	return client.parseLabelVariantStream(stream)
}

func (client *Client) GetLabelVariant(ctx context.Context, id NameVariant) (*LabelVariant, error) {
	variants, err := client.GetLabelVariants(ctx, []NameVariant{id})
	if err != nil {
		return nil, err
	}
	return variants[0], nil
}

type labelStream interface {
	Recv() (*pb.Label, error)
}

func (client *Client) parseLabelStream(stream labelStream) ([]*Label, error) {
	labels := make([]*Label, 0)
	for {
		serial, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		labels = append(labels, wrapProtoLabel(serial))
	}
	return labels, nil
}

type labelVariantStream interface {
	Recv() (*pb.LabelVariant, error)
}

func (client *Client) parseLabelVariantStream(stream labelVariantStream) ([]*LabelVariant, error) {
	features := make([]*LabelVariant, 0)
	for {
		serial, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		features = append(features, wrapProtoLabelVariant(serial))
	}
	return features, nil
}

func (client *Client) ListTrainingSets(ctx context.Context) ([]*TrainingSet, error) {
	stream, err := client.GrpcConn.ListTrainingSets(ctx, &pb.Empty{})
	if err != nil {
		return nil, err
	}
	return client.parseTrainingSetStream(stream)
}

func (client *Client) GetTrainingSet(ctx context.Context, trainingSet string) (*TrainingSet, error) {
	trainingSetList, err := client.GetTrainingSets(ctx, []string{trainingSet})
	if err != nil {
		return nil, err
	}
	return trainingSetList[0], nil
}

func (client *Client) GetTrainingSets(ctx context.Context, trainingSets []string) ([]*TrainingSet, error) {
	stream, err := client.GrpcConn.GetTrainingSets(ctx)
	if err != nil {
		return nil, err
	}
	go func() {
		for _, trainingSet := range trainingSets {
			stream.Send(&pb.Name{Name: trainingSet})
		}
		err := stream.CloseSend()
		if err != nil {
			client.Logger.Errorw("Failed to close send", "Err", err)
		}
	}()
	return client.parseTrainingSetStream(stream)
}

type TrainingSetDef struct {
	Name        string
	Variant     string
	Description string
	Owner       string
	Provider    string
	Schedule    string
	Label       NameVariant
	Features    NameVariants
	Tags        Tags
	Properties  Properties
}

func (def TrainingSetDef) ResourceType() ResourceType {
	return TRAINING_SET_VARIANT
}

func (def TrainingSetDef) Serialize() *pb.TrainingSetVariant {
	return &pb.TrainingSetVariant{
		Name:        def.Name,
		Variant:     def.Variant,
		Description: def.Description,
		Owner:       def.Owner,
		Provider:    def.Provider,
		Status:      &pb.ResourceStatus{Status: pb.ResourceStatus_CREATED},
		Label:       def.Label.Serialize(),
		Features:    def.Features.Serialize(),
		Schedule:    def.Schedule,
		Tags:        &pb.Tags{Tag: def.Tags},
		Properties:  def.Properties.Serialize(),
	}
}

func (client *Client) CreateTrainingSetVariant(ctx context.Context, def TrainingSetDef) error {
	serialized := def.Serialize()
	_, err := client.GrpcConn.CreateTrainingSetVariant(ctx, serialized)
	return err
}

func (client *Client) GetTrainingSetVariant(ctx context.Context, id NameVariant) (*TrainingSetVariant, error) {
	variants, err := client.GetTrainingSetVariants(ctx, []NameVariant{id})
	if err != nil {
		return nil, err
	}
	return variants[0], nil
}

func (client *Client) GetTrainingSetVariants(ctx context.Context, ids []NameVariant) ([]*TrainingSetVariant, error) {
	stream, err := client.GrpcConn.GetTrainingSetVariants(ctx)
	if err != nil {
		return nil, err
	}
	go func() {
		for _, id := range ids {
			stream.Send(&pb.NameVariant{Name: id.Name, Variant: id.Variant})
		}
		err := stream.CloseSend()
		if err != nil {
			client.Logger.Errorw("Failed to close send", "Err", err)
		}
	}()
	return client.parseTrainingSetVariantStream(stream)
}

type trainingSetStream interface {
	Recv() (*pb.TrainingSet, error)
}

func (client *Client) parseTrainingSetStream(stream trainingSetStream) ([]*TrainingSet, error) {
	trainingSets := make([]*TrainingSet, 0)
	for {
		serial, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		trainingSets = append(trainingSets, wrapProtoTrainingSet(serial))
	}
	return trainingSets, nil
}

type trainingSetVariantStream interface {
	Recv() (*pb.TrainingSetVariant, error)
}

func (client *Client) parseTrainingSetVariantStream(stream trainingSetVariantStream) ([]*TrainingSetVariant, error) {
	features := make([]*TrainingSetVariant, 0)
	for {
		serial, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		features = append(features, wrapProtoTrainingSetVariant(serial))
	}
	return features, nil
}

func (client *Client) ListSources(ctx context.Context) ([]*Source, error) {
	stream, err := client.GrpcConn.ListSources(ctx, &pb.Empty{})
	if err != nil {
		return nil, err
	}
	return client.parseSourceStream(stream)
}

func (client *Client) GetSource(ctx context.Context, source string) (*Source, error) {
	sourceList, err := client.GetSources(ctx, []string{source})
	if err != nil {
		return nil, err
	}
	return sourceList[0], nil
}

func (client *Client) GetSources(ctx context.Context, sources []string) ([]*Source, error) {
	stream, err := client.GrpcConn.GetSources(ctx)
	if err != nil {
		return nil, err
	}
	go func() {
		for _, source := range sources {
			stream.Send(&pb.Name{Name: source})
		}
		err := stream.CloseSend()
		if err != nil {
			client.Logger.Errorw("Failed to close send", "Err", err)
		}
	}()
	return client.parseSourceStream(stream)
}

type SourceDef struct {
	Name        string
	Variant     string
	Description string
	Owner       string
	Provider    string
	Schedule    string
	Definition  SourceType
	Tags        Tags
	Properties  Properties
}

type SourceType interface {
	isSourceType() bool
}

func (t TransformationSource) isSourceType() bool {
	return true
}
func (t PrimaryDataSource) isSourceType() bool {
	return true
}

func (t SQLTransformationType) IsTransformationType() bool {
	return true
}
func (t SQLTable) isPrimaryData() bool {
	return true
}

type TransformationSource struct {
	TransformationType TransformationType
}

type TransformationType interface {
	IsTransformationType() bool
}

type SQLTransformationType struct {
	Query   string
	Sources NameVariants
}

type PrimaryDataSource struct {
	Location PrimaryDataLocationType
}

type PrimaryDataLocationType interface {
	isPrimaryData() bool
}

type SQLTable struct {
	Name string
}

type TransformationSourceDef struct {
	Def interface{}
}

func (s TransformationSource) Serialize() (*pb.SourceVariant_Transformation, error) {
	var transformation *pb.Transformation
	switch x := s.TransformationType.(type) {
	case SQLTransformationType:
		transformation = &pb.Transformation{
			Type: &pb.Transformation_SQLTransformation{
				SQLTransformation: &pb.SQLTransformation{
					Query:  s.TransformationType.(SQLTransformationType).Query,
					Source: s.TransformationType.(SQLTransformationType).Sources.Serialize(),
				},
			},
		}
	case nil:
		return nil, fmt.Errorf("TransformationSource Type not set")
	default:
		return nil, fmt.Errorf("TransformationSource Type has unexpected type %T", x)
	}
	return &pb.SourceVariant_Transformation{
		Transformation: transformation,
	}, nil
}

func (s PrimaryDataSource) Serialize() (*pb.SourceVariant_PrimaryData, error) {
	var primaryData *pb.PrimaryData
	switch x := s.Location.(type) {
	case SQLTable:
		primaryData = &pb.PrimaryData{
			Location: &pb.PrimaryData_Table{
				Table: &pb.PrimarySQLTable{
					Name: s.Location.(SQLTable).Name,
				},
			},
		}
	case nil:
		return nil, fmt.Errorf("PrimaryDataSource Type not set")
	default:
		return nil, fmt.Errorf("PrimaryDataSource Type has unexpected type %T", x)
	}
	return &pb.SourceVariant_PrimaryData{
		PrimaryData: primaryData,
	}, nil
}

func (def SourceDef) ResourceType() ResourceType {
	return SOURCE_VARIANT
}

func (def SourceDef) Serialize() (*pb.SourceVariant, error) {
	serialized := &pb.SourceVariant{
		Name:        def.Name,
		Variant:     def.Variant,
		Description: def.Description,
		Owner:       def.Owner,
		Status:      &pb.ResourceStatus{Status: pb.ResourceStatus_CREATED},
		Provider:    def.Provider,
		Schedule:    def.Schedule,
		Tags:        &pb.Tags{Tag: def.Tags},
		Properties:  def.Properties.Serialize(),
	}
	var err error
	switch x := def.Definition.(type) {
	case TransformationSource:
		serialized.Definition, err = def.Definition.(TransformationSource).Serialize()
	case PrimaryDataSource:
		serialized.Definition, err = def.Definition.(PrimaryDataSource).Serialize()
	case nil:
		return nil, fmt.Errorf("SourceDef Definition not set")
	default:
		return nil, fmt.Errorf("SourceDef Definition has unexpected type %T", x)
	}
	if err != nil {
		return nil, err
	}
	return serialized, nil
}

func (client *Client) CreateSourceVariant(ctx context.Context, def SourceDef) error {
	serialized, err := def.Serialize()
	if err != nil {
		return err
	}
	_, err = client.GrpcConn.CreateSourceVariant(ctx, serialized)
	return err
}

func (client *Client) GetSourceVariants(ctx context.Context, ids []NameVariant) ([]*SourceVariant, error) {
	stream, err := client.GrpcConn.GetSourceVariants(ctx)
	if err != nil {
		return nil, fmt.Errorf("grpc connect: %w", err)
	}
	go func() {
		for _, id := range ids {
			err := stream.Send(&pb.NameVariant{Name: id.Name, Variant: id.Variant})
			if err != nil {
				client.Logger.Errorw("Failed to send source variant", "name", id.Name, "variant", id.Variant, "error", err)
			}
		}
		err := stream.CloseSend()
		if err != nil {
			client.Logger.Errorw("Failed to close send", "Err", err)
		}
	}()
	variants, err := client.parseSourceVariantStream(stream)
	if err != nil {
		client.Logger.Errorw("Failed to parse source variant stream", "ids", ids)
	}
	return variants, err
}

func (client *Client) GetSourceVariant(ctx context.Context, id NameVariant) (*SourceVariant, error) {
	variants, err := client.GetSourceVariants(ctx, []NameVariant{id})
	if err != nil {
		return nil, fmt.Errorf("get source variant: %w", err)
	}
	return variants[0], nil
}

type sourceStream interface {
	Recv() (*pb.Source, error)
}

func (client *Client) parseSourceStream(stream sourceStream) ([]*Source, error) {
	sources := make([]*Source, 0)
	for {
		serial, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		sources = append(sources, wrapProtoSource(serial))
	}
	return sources, nil
}

type sourceVariantStream interface {
	Recv() (*pb.SourceVariant, error)
}

func (client *Client) parseSourceVariantStream(stream sourceVariantStream) ([]*SourceVariant, error) {
	features := make([]*SourceVariant, 0)
	for {
		serial, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			client.Logger.Errorw("Error receiving parsed stream", "error", err)
			return nil, err
		}
		features = append(features, wrapProtoSourceVariant(serial))
	}
	return features, nil
}

func (client *Client) ListUsers(ctx context.Context) ([]*User, error) {
	stream, err := client.GrpcConn.ListUsers(ctx, &pb.Empty{})
	if err != nil {
		return nil, err
	}
	return client.parseUserStream(stream)
}

func (client *Client) GetUser(ctx context.Context, user string) (*User, error) {
	userList, err := client.GetUsers(ctx, []string{user})
	if err != nil {
		return nil, err
	}
	return userList[0], nil
}

func (client *Client) GetUsers(ctx context.Context, users []string) ([]*User, error) {
	stream, err := client.GrpcConn.GetUsers(ctx)
	if err != nil {
		return nil, err
	}
	go func() {
		for _, user := range users {
			stream.Send(&pb.Name{Name: user})
		}
		err := stream.CloseSend()
		if err != nil {
			client.Logger.Errorw("Failed to close send", "Err", err)
		}
	}()
	return client.parseUserStream(stream)
}

type UserDef struct {
	Name       string
	Tags       Tags
	Properties Properties
}

func (def UserDef) ResourceType() ResourceType {
	return USER
}

func (client *Client) CreateUser(ctx context.Context, def UserDef) error {
	serialized := &pb.User{
		Name:       def.Name,
		Tags:       &pb.Tags{Tag: def.Tags},
		Properties: def.Properties.Serialize(),
	}
	_, err := client.GrpcConn.CreateUser(ctx, serialized)
	return err
}

type userStream interface {
	Recv() (*pb.User, error)
}

func (client *Client) parseUserStream(stream userStream) ([]*User, error) {
	users := make([]*User, 0)
	for {
		serial, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		users = append(users, wrapProtoUser(serial))
	}
	return users, nil
}

func (client *Client) ListProviders(ctx context.Context) ([]*Provider, error) {
	stream, err := client.GrpcConn.ListProviders(ctx, &pb.Empty{})
	if err != nil {
		return nil, err
	}
	return client.parseProviderStream(stream)
}

func (client *Client) GetProvider(ctx context.Context, provider string) (*Provider, error) {
	providerList, err := client.GetProviders(ctx, []string{provider})
	if err != nil {
		return nil, err
	}
	return providerList[0], nil
}

func (client *Client) GetProviders(ctx context.Context, providers []string) ([]*Provider, error) {
	stream, err := client.GrpcConn.GetProviders(ctx)
	if err != nil {
		return nil, err
	}
	go func() {
		for _, provider := range providers {
			stream.Send(&pb.Name{Name: provider})
		}
		err := stream.CloseSend()
		if err != nil {
			client.Logger.Errorw("Failed to close send", "Err", err)
		}
	}()
	return client.parseProviderStream(stream)
}

type ProviderDef struct {
	Name             string
	Description      string
	Type             string
	Software         string
	Team             string
	SerializedConfig []byte
	Tags             Tags
	Properties       Properties
}

func (def ProviderDef) ResourceType() ResourceType {
	return PROVIDER
}

func (client *Client) CreateProvider(ctx context.Context, def ProviderDef) error {
	serialized := &pb.Provider{
		Name:             def.Name,
		Description:      def.Description,
		Type:             def.Type,
		Software:         def.Software,
		Team:             def.Team,
		Status:           &pb.ResourceStatus{Status: pb.ResourceStatus_NO_STATUS},
		SerializedConfig: def.SerializedConfig,
		Tags:             &pb.Tags{Tag: def.Tags},
		Properties:       def.Properties.Serialize(),
	}
	_, err := client.GrpcConn.CreateProvider(ctx, serialized)
	return err
}

type providerStream interface {
	Recv() (*pb.Provider, error)
}

func (client *Client) parseProviderStream(stream providerStream) ([]*Provider, error) {
	providers := make([]*Provider, 0)
	for {
		serial, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		providers = append(providers, wrapProtoProvider(serial))
	}
	return providers, nil
}

func (client *Client) ListEntities(ctx context.Context) ([]*Entity, error) {
	stream, err := client.GrpcConn.ListEntities(ctx, &pb.Empty{})
	if err != nil {
		return nil, err
	}
	return client.parseEntityStream(stream)
}

func (client *Client) GetEntity(ctx context.Context, entity string) (*Entity, error) {
	entityList, err := client.GetEntities(ctx, []string{entity})
	if err != nil {
		return nil, err
	}
	return entityList[0], nil
}

func (client *Client) GetEntities(ctx context.Context, entities []string) ([]*Entity, error) {
	stream, err := client.GrpcConn.GetEntities(ctx)
	if err != nil {
		return nil, err
	}
	go func() {
		for _, entity := range entities {
			stream.Send(&pb.Name{Name: entity})
		}
		err := stream.CloseSend()
		if err != nil {
			client.Logger.Errorw("Failed to close send", "Err", err)
		}
	}()
	return client.parseEntityStream(stream)
}

type EntityDef struct {
	Name        string
	Description string
	Tags        Tags
	Properties  Properties
}

func (def EntityDef) ResourceType() ResourceType {
	return ENTITY
}

func (client *Client) CreateEntity(ctx context.Context, def EntityDef) error {
	serialized := &pb.Entity{
		Name:        def.Name,
		Status:      &pb.ResourceStatus{Status: pb.ResourceStatus_NO_STATUS},
		Description: def.Description,
		Tags:        &pb.Tags{Tag: def.Tags},
		Properties:  def.Properties.Serialize(),
	}
	_, err := client.GrpcConn.CreateEntity(ctx, serialized)
	return err
}

type entityStream interface {
	Recv() (*pb.Entity, error)
}

func (client *Client) parseEntityStream(stream entityStream) ([]*Entity, error) {
	entities := make([]*Entity, 0)
	for {
		serial, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		entities = append(entities, wrapProtoEntity(serial))
	}
	return entities, nil
}

func (client *Client) ListModels(ctx context.Context) ([]*Model, error) {
	stream, err := client.GrpcConn.ListModels(ctx, &pb.Empty{})
	if err != nil {
		return nil, err
	}
	return client.parseModelStream(stream)
}

func (client *Client) GetModel(ctx context.Context, model string) (*Model, error) {
	modelList, err := client.GetModels(ctx, []string{model})
	if err != nil {
		return nil, err
	}
	return modelList[0], nil
}

func (client *Client) GetModels(ctx context.Context, models []string) ([]*Model, error) {
	stream, err := client.GrpcConn.GetModels(ctx)
	if err != nil {
		return nil, err
	}
	go func() {
		for _, model := range models {
			stream.Send(&pb.Name{Name: model})
		}
		err := stream.CloseSend()
		if err != nil {
			client.Logger.Errorw("Failed to close send", "Err", err)
		}
	}()
	return client.parseModelStream(stream)
}

type ModelDef struct {
	Name         string
	Description  string
	Features     NameVariants
	Trainingsets NameVariants
	Tags         Tags
	Properties   Properties
}

func (def ModelDef) ResourceType() ResourceType {
	return MODEL
}

func (client *Client) CreateModel(ctx context.Context, def ModelDef) error {
	serialized := &pb.Model{
		Name:         def.Name,
		Description:  def.Description,
		Features:     def.Features.Serialize(),
		Trainingsets: def.Trainingsets.Serialize(),
		Tags:         &pb.Tags{Tag: def.Tags},
		Properties:   def.Properties.Serialize(),
	}
	_, err := client.GrpcConn.CreateModel(ctx, serialized)
	return err
}

type modelStream interface {
	Recv() (*pb.Model, error)
}

func (client *Client) parseModelStream(stream modelStream) ([]*Model, error) {
	models := make([]*Model, 0)
	for {
		serial, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		models = append(models, wrapProtoModel(serial))
	}
	return models, nil
}

type protoStringer struct {
	msg proto.Message
}

func (stringer protoStringer) String() string {
	bytes, err := protojson.Marshal(stringer.msg)
	if err != nil {
		return err.Error()
	}
	return string(bytes)
}

type createdGetter interface {
	GetCreated() *tspb.Timestamp
}

type createdFn struct {
	getter createdGetter
}

func (fn createdFn) Created() time.Time {
	t := fn.getter.GetCreated().AsTime()
	return t
}

type lastUpdatedGetter interface {
	GetLastUpdated() *tspb.Timestamp
}

type lastUpdatedFn struct {
	getter lastUpdatedGetter
}

func (fn lastUpdatedFn) LastUpdated() time.Time {
	t := fn.getter.GetLastUpdated().AsTime()
	return t
}

type variantsDescriber interface {
	GetName() string
	GetDefaultVariant() string
	GetVariants() []string
}

type variantsFns struct {
	getter variantsDescriber
}

func (fns variantsFns) Name() string {
	return fns.getter.GetName()
}

func (fns variantsFns) DefaultVariant() string {
	return fns.getter.GetDefaultVariant()
}

func (fns variantsFns) Variants() []string {
	return fns.getter.GetVariants()
}

func (fns variantsFns) NameVariants() NameVariants {
	name := fns.getter.GetName()
	variants := fns.getter.GetVariants()
	nameVariants := make([]NameVariant, len(variants))
	for i, variant := range variants {
		nameVariants[i] = NameVariant{
			Name:    name,
			Variant: variant,
		}
	}
	return nameVariants
}

type providerGetter interface {
	GetProvider() string
}

type fetchProviderFns struct {
	getter providerGetter
}

func (fn fetchProviderFns) Provider() string {
	return fn.getter.GetProvider()
}

func (fn fetchProviderFns) FetchProvider(client *Client, ctx context.Context) (*Provider, error) {
	return client.GetProvider(ctx, fn.Provider())
}

type trainingSetsGetter interface {
	GetTrainingsets() []*pb.NameVariant
}

type fetchTrainingSetsFns struct {
	getter trainingSetsGetter
}

func (fn fetchTrainingSetsFns) TrainingSets() NameVariants {
	return parseNameVariants(fn.getter.GetTrainingsets())
}

func (fn fetchTrainingSetsFns) FetchTrainingSets(client *Client, ctx context.Context) ([]*TrainingSetVariant, error) {
	return client.GetTrainingSetVariants(ctx, fn.TrainingSets())
}

type labelsGetter interface {
	GetLabels() []*pb.NameVariant
}

type fetchLabelsFns struct {
	getter labelsGetter
}

func (fn fetchLabelsFns) Labels() NameVariants {
	return parseNameVariants(fn.getter.GetLabels())
}

func (fn fetchLabelsFns) FetchLabels(client *Client, ctx context.Context) ([]*LabelVariant, error) {
	return client.GetLabelVariants(ctx, fn.Labels())
}

type featuresGetter interface {
	GetFeatures() []*pb.NameVariant
}

type fetchFeaturesFns struct {
	getter featuresGetter
}

func (fn fetchFeaturesFns) Features() NameVariants {
	return parseNameVariants(fn.getter.GetFeatures())
}

func (fn fetchFeaturesFns) FetchFeatures(client *Client, ctx context.Context) ([]*FeatureVariant, error) {
	return client.GetFeatureVariants(ctx, fn.Features())
}

type sourcesGetter interface {
	GetSources() []*pb.NameVariant
}

type fetchSourcesFns struct {
	getter sourcesGetter
}

func (fn fetchSourcesFns) Sources() NameVariants {
	return parseNameVariants(fn.getter.GetSources())
}

func (fn fetchSourcesFns) FetchSources(client *Client, ctx context.Context) ([]*SourceVariant, error) {
	return client.GetSourceVariants(ctx, fn.Sources())
}

type sourceGetter interface {
	GetSource() *pb.NameVariant
}

type fetchSourceFns struct {
	getter sourceGetter
}

func (fn fetchSourceFns) Source() NameVariant {
	return parseNameVariant(fn.getter.GetSource())
}

func (fn fetchSourceFns) FetchSource(client *Client, ctx context.Context) (*SourceVariant, error) {
	return client.GetSourceVariant(ctx, fn.Source())
}

type tagsGetter interface {
	GetTags() *pb.Tags
}

type fetchTagsFn struct {
	getter tagsGetter
}

func (fn fetchTagsFn) Tags() Tags {
	tags := Tags{}
	proto := fn.getter.GetTags()
	if proto == nil || proto.Tag == nil {
		return tags
	}
	tags = append(tags, proto.Tag...)
	return tags
}

type propertiesGetter interface {
	GetProperties() *pb.Properties
}

type fetchPropertiesFn struct {
	getter propertiesGetter
}

func (fn fetchPropertiesFn) Properties() Properties {
	properties := Properties{}
	proto := fn.getter.GetProperties()
	if proto == nil || proto.Property == nil {
		return properties
	}
	for k, v := range proto.Property {
		properties[k] = v.GetStringValue()
	}
	return properties
}

type isEmbeddingGetter interface {
	GetIsEmbedding() bool
}

type fetchIsEmbeddingFn struct {
	getter isEmbeddingGetter
}

func (fn fetchIsEmbeddingFn) IsEmbedding() bool {
	return fn.getter.GetIsEmbedding()
}

type dimensionGetter interface {
	GetDimension() int32
}

type fetchDimensionFn struct {
	getter dimensionGetter
}

func (fn fetchDimensionFn) Dimension() int32 {
	return fn.getter.GetDimension()
}

type Feature struct {
	serialized *pb.Feature
	variantsFns
	protoStringer
}

func wrapProtoFeature(serialized *pb.Feature) *Feature {
	return &Feature{
		serialized:    serialized,
		variantsFns:   variantsFns{serialized},
		protoStringer: protoStringer{serialized},
	}
}

func (feature Feature) FetchVariants(client *Client, ctx context.Context) ([]*FeatureVariant, error) {
	return client.GetFeatureVariants(ctx, feature.NameVariants())
}

type FeatureVariant struct {
	serialized *pb.FeatureVariant
	fetchTrainingSetsFns
	fetchProviderFns
	fetchSourceFns
	createdFn
	lastUpdatedFn
	protoStringer
	fetchTagsFn
	fetchPropertiesFn
	fetchIsEmbeddingFn
	fetchDimensionFn
}

func wrapProtoFeatureVariant(serialized *pb.FeatureVariant) *FeatureVariant {
	return &FeatureVariant{
		serialized:           serialized,
		fetchTrainingSetsFns: fetchTrainingSetsFns{serialized},
		fetchProviderFns:     fetchProviderFns{serialized},
		fetchSourceFns:       fetchSourceFns{serialized},
		createdFn:            createdFn{serialized},
		lastUpdatedFn:        lastUpdatedFn{serialized},
		protoStringer:        protoStringer{serialized},
		fetchTagsFn:          fetchTagsFn{serialized},
		fetchPropertiesFn:    fetchPropertiesFn{serialized},
		fetchIsEmbeddingFn:   fetchIsEmbeddingFn{serialized},
		fetchDimensionFn:     fetchDimensionFn{serialized},
	}
}

func (variant *FeatureVariant) Name() string {
	return variant.serialized.GetName()
}

func (variant *FeatureVariant) Description() string {
	return variant.serialized.GetDescription()
}

func (variant *FeatureVariant) Variant() string {
	return variant.serialized.GetVariant()
}

func (variant *FeatureVariant) Type() string {
	return variant.serialized.GetType()
}

func (variant *FeatureVariant) Entity() string {
	return variant.serialized.GetEntity()
}

func (variant *FeatureVariant) Owner() string {
	return variant.serialized.GetOwner()
}

func (variant *FeatureVariant) Status() ResourceStatus {
	if variant.serialized.GetStatus() != nil {
		return ResourceStatus(variant.serialized.GetStatus().Status)
	}
	return ResourceStatus(0)
}

func (variant *FeatureVariant) Error() string {
	if variant.serialized.GetStatus() != nil {
		return variant.serialized.GetStatus().ErrorMessage
	}
	return ""
}

func (variant *FeatureVariant) Location() interface{} {
	return variant.serialized.GetLocation()
}

func (variant *FeatureVariant) isTable() bool {
	return reflect.TypeOf(variant.serialized.GetLocation()) == reflect.TypeOf(&pb.FeatureVariant_Columns{})
}

func (variant *FeatureVariant) LocationColumns() interface{} {
	if variant.Mode() != PRECOMPUTED {
		return nil
	}
	src := variant.serialized.GetColumns()
	columns := ResourceVariantColumns{
		Entity: src.Entity,
		Value:  src.Value,
		TS:     src.Ts,
	}
	return columns
}

func (variant *FeatureVariant) LocationFunction() interface{} {
	if variant.Mode() != CLIENT_COMPUTED {
		return nil
	}
	src := variant.serialized.GetFunction()
	function := PythonFunction{
		Query: src.Query,
	}
	return function
}

func (variant *FeatureVariant) Tags() Tags {
	return variant.fetchTagsFn.Tags()
}

func (variant *FeatureVariant) Properties() Properties {
	return variant.fetchPropertiesFn.Properties()
}

func (variant *FeatureVariant) Mode() ComputationMode {
	return ComputationMode(variant.serialized.GetMode())
}

func (variant *FeatureVariant) IsOnDemand() bool {
	switch variant.Mode() {
	case PRECOMPUTED:
		return false
	case CLIENT_COMPUTED:
		return true
	default:
		fmt.Printf("Unknown computation mode: %v\n", variant.Mode())
		return false
	}
}

func (variant *FeatureVariant) IsEmbedding() bool {
	return variant.fetchIsEmbeddingFn.IsEmbedding()
}

func (variant *FeatureVariant) Dimension() int32 {
	return variant.fetchDimensionFn.Dimension()
}

type User struct {
	serialized *pb.User
	fetchTrainingSetsFns
	fetchFeaturesFns
	fetchLabelsFns
	fetchSourcesFns
	protoStringer
	fetchTagsFn
	fetchPropertiesFn
}

func (u User) Variant() string {
	return ""
}

func wrapProtoUser(serialized *pb.User) *User {
	return &User{
		serialized:           serialized,
		fetchTrainingSetsFns: fetchTrainingSetsFns{serialized},
		fetchFeaturesFns:     fetchFeaturesFns{serialized},
		fetchLabelsFns:       fetchLabelsFns{serialized},
		fetchSourcesFns:      fetchSourcesFns{serialized},
		protoStringer:        protoStringer{serialized},
		fetchTagsFn:          fetchTagsFn{serialized},
		fetchPropertiesFn:    fetchPropertiesFn{serialized},
	}
}

func (user *User) Name() string {
	return user.serialized.GetName()
}

func (user *User) Status() ResourceStatus {
	if user.serialized.GetStatus() != nil {
		return ResourceStatus(user.serialized.GetStatus().Status)
	}
	return ResourceStatus(0)
}

func (user *User) Error() string {
	if user.serialized.GetStatus() != nil {
		return user.serialized.GetStatus().ErrorMessage
	}
	return ""
}

func (user *User) Tags() Tags {
	return user.fetchTagsFn.Tags()
}

func (user *User) Properties() Properties {
	return user.fetchPropertiesFn.Properties()
}

type Provider struct {
	serialized *pb.Provider
	fetchTrainingSetsFns
	fetchFeaturesFns
	fetchLabelsFns
	fetchSourcesFns
	protoStringer
	fetchTagsFn
	fetchPropertiesFn
}

func (p Provider) Variant() string {
	return ""
}

func wrapProtoProvider(serialized *pb.Provider) *Provider {
	return &Provider{
		serialized:           serialized,
		fetchTrainingSetsFns: fetchTrainingSetsFns{serialized},
		fetchFeaturesFns:     fetchFeaturesFns{serialized},
		fetchLabelsFns:       fetchLabelsFns{serialized},
		fetchSourcesFns:      fetchSourcesFns{serialized},
		protoStringer:        protoStringer{serialized},
		fetchTagsFn:          fetchTagsFn{serialized},
		fetchPropertiesFn:    fetchPropertiesFn{serialized},
	}
}

func (provider *Provider) Name() string {
	return provider.serialized.GetName()
}

func (provider *Provider) Description() string {
	return provider.serialized.GetDescription()
}

func (provider *Provider) Type() string {
	return provider.serialized.GetType()
}

func (provider *Provider) Software() string {
	return provider.serialized.GetSoftware()
}

func (provider *Provider) Team() string {
	return provider.serialized.GetTeam()
}

func (provider *Provider) SerializedConfig() []byte {
	return provider.serialized.GetSerializedConfig()
}

func (provider *Provider) Status() ResourceStatus {
	if provider.serialized.GetStatus() != nil {
		return ResourceStatus(provider.serialized.GetStatus().Status)
	}
	return ResourceStatus(0)
}

func (provider *Provider) Error() string {
	if provider.serialized.GetStatus() != nil {
		return provider.serialized.GetStatus().ErrorMessage
	}
	return ""
}

func (provider *Provider) Tags() Tags {
	return provider.fetchTagsFn.Tags()
}

func (provider *Provider) Properties() Properties {
	return provider.fetchPropertiesFn.Properties()
}

type Model struct {
	serialized *pb.Model
	fetchTrainingSetsFns
	fetchFeaturesFns
	fetchLabelsFns
	protoStringer
	fetchTagsFn
	fetchPropertiesFn
}

func (m Model) Variant() string {
	return ""
}

func wrapProtoModel(serialized *pb.Model) *Model {
	return &Model{
		serialized:           serialized,
		fetchTrainingSetsFns: fetchTrainingSetsFns{serialized},
		fetchFeaturesFns:     fetchFeaturesFns{serialized},
		fetchLabelsFns:       fetchLabelsFns{serialized},
		protoStringer:        protoStringer{serialized},
		fetchTagsFn:          fetchTagsFn{serialized},
		fetchPropertiesFn:    fetchPropertiesFn{serialized},
	}
}

func (model *Model) Name() string {
	return model.serialized.GetName()
}

func (model *Model) Description() string {
	return model.serialized.GetDescription()
}

func (model *Model) Status() ResourceStatus {
	return ResourceStatus(0)
}

func (model *Model) Error() string {
	return ""
}

func (model *Model) Tags() Tags {
	return model.fetchTagsFn.Tags()
}

func (model *Model) Properties() Properties {
	return model.fetchPropertiesFn.Properties()
}

type Label struct {
	serialized *pb.Label
	variantsFns
	protoStringer
}

func wrapProtoLabel(serialized *pb.Label) *Label {
	return &Label{
		serialized:    serialized,
		variantsFns:   variantsFns{serialized},
		protoStringer: protoStringer{serialized},
	}
}

func (label Label) FetchVariants(client *Client, ctx context.Context) ([]*LabelVariant, error) {
	return client.GetLabelVariants(ctx, label.NameVariants())
}

type LabelVariant struct {
	serialized *pb.LabelVariant
	fetchTrainingSetsFns
	fetchProviderFns
	fetchSourceFns
	createdFn
	protoStringer
	fetchTagsFn
	fetchPropertiesFn
}

func wrapProtoLabelVariant(serialized *pb.LabelVariant) *LabelVariant {
	return &LabelVariant{
		serialized:           serialized,
		fetchTrainingSetsFns: fetchTrainingSetsFns{serialized},
		fetchProviderFns:     fetchProviderFns{serialized},
		fetchSourceFns:       fetchSourceFns{serialized},
		createdFn:            createdFn{serialized},
		protoStringer:        protoStringer{serialized},
		fetchTagsFn:          fetchTagsFn{serialized},
		fetchPropertiesFn:    fetchPropertiesFn{serialized},
	}
}

func (variant *LabelVariant) Name() string {
	return variant.serialized.GetName()
}

func (variant *LabelVariant) Description() string {
	return variant.serialized.GetDescription()
}

func (variant *LabelVariant) Variant() string {
	return variant.serialized.GetVariant()
}

func (variant *LabelVariant) Type() string {
	return variant.serialized.GetType()
}

func (variant *LabelVariant) Entity() string {
	return variant.serialized.GetEntity()
}

func (variant *LabelVariant) Owner() string {
	return variant.serialized.GetOwner()
}

func (variant *LabelVariant) Status() ResourceStatus {
	if variant.serialized.GetStatus() != nil {
		return ResourceStatus(variant.serialized.GetStatus().Status)
	}
	return ResourceStatus(0)
}

func (variant *LabelVariant) Error() string {
	if variant.serialized.GetStatus() != nil {
		return variant.serialized.GetStatus().ErrorMessage
	}
	return ""
}

func (variant *LabelVariant) Location() interface{} {
	return variant.serialized.GetLocation()
}

func (variant *LabelVariant) isTable() bool {
	return reflect.TypeOf(variant.serialized.GetLocation()) == reflect.TypeOf(&pb.LabelVariant_Columns{})
}

func (variant *LabelVariant) LocationColumns() interface{} {
	src := variant.serialized.GetColumns()
	columns := ResourceVariantColumns{
		Entity: src.Entity,
		Value:  src.Value,
		TS:     src.Ts,
	}
	return columns
}

func (variant *LabelVariant) Tags() Tags {
	return variant.fetchTagsFn.Tags()
}

func (variant *LabelVariant) Properties() Properties {
	return variant.fetchPropertiesFn.Properties()
}

type TrainingSet struct {
	serialized *pb.TrainingSet
	variantsFns
	protoStringer
}

func wrapProtoTrainingSet(serialized *pb.TrainingSet) *TrainingSet {
	return &TrainingSet{
		serialized:    serialized,
		variantsFns:   variantsFns{serialized},
		protoStringer: protoStringer{serialized},
	}
}

func (trainingSet TrainingSet) FetchVariants(client *Client, ctx context.Context) ([]*TrainingSetVariant, error) {
	return client.GetTrainingSetVariants(ctx, trainingSet.NameVariants())
}

type TrainingSetVariant struct {
	serialized *pb.TrainingSetVariant
	fetchFeaturesFns
	fetchProviderFns
	createdFn
	lastUpdatedFn
	protoStringer
	fetchTagsFn
	fetchPropertiesFn
}

func wrapProtoTrainingSetVariant(serialized *pb.TrainingSetVariant) *TrainingSetVariant {
	return &TrainingSetVariant{
		serialized:        serialized,
		fetchFeaturesFns:  fetchFeaturesFns{serialized},
		fetchProviderFns:  fetchProviderFns{serialized},
		createdFn:         createdFn{serialized},
		lastUpdatedFn:     lastUpdatedFn{serialized},
		protoStringer:     protoStringer{serialized},
		fetchTagsFn:       fetchTagsFn{serialized},
		fetchPropertiesFn: fetchPropertiesFn{serialized},
	}
}

func (variant *TrainingSetVariant) Name() string {
	return variant.serialized.GetName()
}

func (variant *TrainingSetVariant) Description() string {
	return variant.serialized.GetDescription()
}

func (variant *TrainingSetVariant) Variant() string {
	return variant.serialized.GetVariant()
}

func (variant *TrainingSetVariant) Owner() string {
	return variant.serialized.GetOwner()
}

func (variant *TrainingSetVariant) Status() ResourceStatus {
	if variant.serialized.GetStatus() != nil {
		return ResourceStatus(variant.serialized.GetStatus().Status)
	}
	return ResourceStatus(0)
}

func (variant *TrainingSetVariant) Error() string {
	if variant.serialized.GetStatus() == nil {
		return ""
	}
	return variant.serialized.GetStatus().ErrorMessage
}

func (variant *TrainingSetVariant) Label() NameVariant {
	return parseNameVariant(variant.serialized.GetLabel())
}

func (variant *TrainingSetVariant) LagFeatures() []*pb.FeatureLag {
	return variant.serialized.GetFeatureLags()
}

func (variant *TrainingSetVariant) FetchLabel(client *Client, ctx context.Context) (*LabelVariant, error) {
	labelList, err := client.GetLabelVariants(ctx, []NameVariant{variant.Label()})
	if err != nil {
		return nil, err
	}
	return labelList[0], nil
}

func (variant *TrainingSetVariant) Tags() Tags {
	return variant.fetchTagsFn.Tags()
}

func (variant *TrainingSetVariant) Properties() Properties {
	return variant.fetchPropertiesFn.Properties()
}

type Source struct {
	serialized *pb.Source
	variantsFns
	protoStringer
}

func wrapProtoSource(serialized *pb.Source) *Source {
	return &Source{
		serialized:    serialized,
		variantsFns:   variantsFns{serialized},
		protoStringer: protoStringer{serialized},
	}
}

func (source Source) FetchVariants(client *Client, ctx context.Context) ([]*SourceVariant, error) {
	return client.GetSourceVariants(ctx, source.NameVariants())
}

type SourceVariant struct {
	serialized *pb.SourceVariant
	fetchTrainingSetsFns
	fetchFeaturesFns
	fetchLabelsFns
	fetchProviderFns
	createdFn
	lastUpdatedFn
	protoStringer
	fetchTagsFn
	fetchPropertiesFn
}

type TransformationArgType string

const (
	NoArgs  TransformationArgType = "NONE"
	K8sArgs TransformationArgType = "K8S"
)

type TransformationArgs interface {
	Format() map[string]string
	Type() TransformationArgType
}

type KubernetesResourceSpecs struct {
	CPURequest    string
	CPULimit      string
	MemoryRequest string
	MemoryLimit   string
}

type KubernetesArgs struct {
	DockerImage string `json:"Docker Image" mapstructure:"Docker Image"`
	Specs       KubernetesResourceSpecs
}

func (arg KubernetesArgs) Format() map[string]string {
	return map[string]string{
		"Docker Image":   arg.DockerImage,
		"CPU Request":    arg.Specs.CPURequest,
		"CPU Limit":      arg.Specs.CPULimit,
		"Memory Request": arg.Specs.MemoryRequest,
		"Memory Limit":   arg.Specs.MemoryLimit,
	}
}

func (arg KubernetesArgs) Type() TransformationArgType {
	return K8sArgs
}

func (variant *SourceVariant) parseKubernetesArgs() KubernetesArgs {
	args := variant.serialized.GetTransformation().GetKubernetesArgs()
	specs := args.GetSpecs()
	return KubernetesArgs{
		DockerImage: args.GetDockerImage(),
		Specs: KubernetesResourceSpecs{
			CPURequest:    specs.GetCpuRequest(),
			CPULimit:      specs.GetCpuLimit(),
			MemoryRequest: specs.GetMemoryRequest(),
			MemoryLimit:   specs.GetMemoryLimit(),
		},
	}
}

func (variant *SourceVariant) DFTransformationQuerySource() string {
	if !variant.IsDFTransformation() {
		return ""
	}
	return variant.serialized.GetTransformation().GetDFTransformation().GetSourceText()
}

func wrapProtoSourceVariant(serialized *pb.SourceVariant) *SourceVariant {
	return &SourceVariant{
		serialized:           serialized,
		fetchTrainingSetsFns: fetchTrainingSetsFns{serialized},
		fetchFeaturesFns:     fetchFeaturesFns{serialized},
		fetchLabelsFns:       fetchLabelsFns{serialized},
		fetchProviderFns:     fetchProviderFns{serialized},
		createdFn:            createdFn{serialized},
		lastUpdatedFn:        lastUpdatedFn{serialized},
		protoStringer:        protoStringer{serialized},
		fetchTagsFn:          fetchTagsFn{serialized},
		fetchPropertiesFn:    fetchPropertiesFn{serialized},
	}
}

func (variant *SourceVariant) Name() string {
	return variant.serialized.GetName()
}

func (variant *SourceVariant) Schedule() string {
	return variant.serialized.GetSchedule()
}

func (variant *SourceVariant) Variant() string {
	return variant.serialized.GetVariant()
}

func (variant *SourceVariant) Description() string {
	return variant.serialized.GetDescription()
}

func (variant *SourceVariant) Definition() interface{} {
	return variant.serialized.GetDefinition()
}

func (variant *SourceVariant) Owner() string {
	return variant.serialized.GetOwner()
}

func (variant *SourceVariant) Status() ResourceStatus {
	if variant.serialized.GetStatus() != nil {
		return ResourceStatus(variant.serialized.GetStatus().Status)
	}
	return ResourceStatus(0)
}

func (variant *SourceVariant) Error() string {
	if variant.serialized.GetStatus() == nil {
		return ""
	}
	return variant.serialized.GetStatus().ErrorMessage
}

func (variant *SourceVariant) IsTransformation() bool {
	return reflect.TypeOf(variant.serialized.GetDefinition()) == reflect.TypeOf(&pb.SourceVariant_Transformation{})
}

func (variant *SourceVariant) IsSQLTransformation() bool {
	if !variant.IsTransformation() {
		return false
	}
	return reflect.TypeOf(variant.serialized.GetTransformation().Type) == reflect.TypeOf(&pb.Transformation_SQLTransformation{})
}

func (variant *SourceVariant) SQLTransformationQuery() string {
	if !variant.IsSQLTransformation() {
		return ""
	}
	return variant.serialized.GetTransformation().GetSQLTransformation().GetQuery()
}

func (variant *SourceVariant) SQLTransformationSources() []NameVariant {
	if !variant.IsSQLTransformation() {
		return nil
	}
	nameVariants := variant.serialized.GetTransformation().GetSQLTransformation().GetSource()
	var variants []NameVariant
	for _, nv := range nameVariants {
		variants = append(variants, NameVariant{Name: nv.Name, Variant: nv.Variant})
	}
	return variants
}

func (variant *SourceVariant) IsDFTransformation() bool {
	if !variant.IsTransformation() {
		return false
	}
	return reflect.TypeOf(variant.serialized.GetTransformation().Type) == reflect.TypeOf(&pb.Transformation_DFTransformation{})
}

func (variant *SourceVariant) DFTransformationQuery() []byte {
	if !variant.IsDFTransformation() {
		return nil
	}
	return variant.serialized.GetTransformation().GetDFTransformation().GetQuery()
}

func (variant *SourceVariant) DFTransformationSources() []NameVariant {
	if !variant.IsDFTransformation() {
		return nil
	}
	inputSources := variant.serialized.GetTransformation().GetDFTransformation().GetInputs()

	var variants []NameVariant
	for _, nv := range inputSources {
		variants = append(variants, NameVariant{Name: nv.Name, Variant: nv.Variant})
	}
	return variants
}

func (variant *SourceVariant) HasKubernetesArgs() bool {
	return variant.serialized.GetTransformation().GetKubernetesArgs() != nil
}

func (variant *SourceVariant) TransformationArgs() TransformationArgs {
	if !variant.IsTransformation() {
		return nil
	}

	if variant.HasKubernetesArgs() {
		return variant.parseKubernetesArgs()
	}
	return nil
}

func (variant *SourceVariant) isPrimaryData() bool {
	return reflect.TypeOf(variant.serialized.GetDefinition()) == reflect.TypeOf(&pb.SourceVariant_PrimaryData{})
}

func (variant *SourceVariant) IsPrimaryDataSQLTable() bool {
	if !variant.isPrimaryData() {
		return false
	}
	return reflect.TypeOf(variant.serialized.GetPrimaryData().GetLocation()) == reflect.TypeOf(&pb.PrimaryData_Table{})
}

func (variant *SourceVariant) PrimaryDataSQLTableName() string {
	if !variant.IsPrimaryDataSQLTable() {
		return ""
	}
	return variant.serialized.GetPrimaryData().GetTable().GetName()
}

func (variant *SourceVariant) Tags() Tags {
	return variant.fetchTagsFn.Tags()
}

func (variant *SourceVariant) Properties() Properties {
	return variant.fetchPropertiesFn.Properties()
}

type Entity struct {
	serialized *pb.Entity
	fetchTrainingSetsFns
	fetchFeaturesFns
	fetchLabelsFns
	protoStringer
	fetchTagsFn
	fetchPropertiesFn
}

func (e Entity) Variant() string {
	return ""
}

func wrapProtoEntity(serialized *pb.Entity) *Entity {
	return &Entity{
		serialized:           serialized,
		fetchTrainingSetsFns: fetchTrainingSetsFns{serialized},
		fetchFeaturesFns:     fetchFeaturesFns{serialized},
		fetchLabelsFns:       fetchLabelsFns{serialized},
		protoStringer:        protoStringer{serialized},
		fetchTagsFn:          fetchTagsFn{serialized},
		fetchPropertiesFn:    fetchPropertiesFn{serialized},
	}
}

func (entity *Entity) Name() string {
	return entity.serialized.GetName()
}

func (entity *Entity) Description() string {
	return entity.serialized.GetDescription()
}

func (entity *Entity) Status() ResourceStatus {
	if entity.serialized.GetStatus() != nil {
		return ResourceStatus(entity.serialized.GetStatus().Status)
	}
	return ResourceStatus(0)
}

func (entity *Entity) Error() string {
	if entity.serialized.GetStatus() == nil {
		return ""
	}
	return entity.serialized.GetStatus().ErrorMessage
}

func (entity *Entity) Tags() Tags {
	return entity.fetchTagsFn.Tags()
}

func (entity *Entity) Properties() Properties {
	return entity.fetchPropertiesFn.Properties()
}

func NewClient(host string, logger *zap.SugaredLogger) (*Client, error) {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	conn, err := grpc.Dial(host, opts...)
	if err != nil {
		return nil, err
	}
	client := pb.NewMetadataClient(conn)
	return &Client{
		Logger:   logger,
		conn:     conn,
		GrpcConn: client,
	}, nil
}

func (client *Client) Close() {
	client.conn.Close()
}
