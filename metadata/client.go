// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package metadata

import (
	"context"
	"fmt"
	"io"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/featureform/filestore"
	pc "github.com/featureform/provider/provider_config"
	"github.com/featureform/scheduling"
	sch "github.com/featureform/scheduling/proto"

	help "github.com/featureform/helpers/notifications"

	"github.com/featureform/fferr"
	"github.com/featureform/logging"
	pb "github.com/featureform/metadata/proto"
	pl "github.com/featureform/provider/location"
	"github.com/featureform/provider/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	grpc_status "google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
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

func (variants NameVariants) Contains(nv NameVariant) bool {
	for _, variant := range variants {
		if variant == nv {
			return true
		}
	}
	return false
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
	Logger        logging.Logger
	conn          *grpc.ClientConn
	GrpcConn      pb.MetadataClient
	Tasks         TaskService
	slackNotifier help.Notifier
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
		return fferr.NewInvalidArgumentError(fmt.Errorf("%T not implemented in Create", casted))
	}
}

func (client *Client) ListFeatures(ctx context.Context) ([]*Feature, error) {
	logger := logging.GetLoggerFromContext(ctx)
	stream, err := client.GrpcConn.ListFeatures(ctx, &pb.ListRequest{RequestId: logging.GetRequestIDFromContext(ctx)})
	if err != nil {
		logger.Errorw("Failed to list features", "error", err)
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
	logger := logging.GetLoggerFromContext(ctx)
	stream, err := client.GrpcConn.GetFeatures(ctx)
	if err != nil {
		logger.Errorw("Failed to get features", "features", features, "error", err)
		return nil, err
	}
	go func() {
		for _, feature := range features {
			stream.Send(&pb.NameRequest{Name: &pb.Name{Name: feature}, RequestId: logging.GetRequestIDFromContext(ctx)})
		}
		err := stream.CloseSend()
		if err != nil {
			logger.Errorw("Failed to close send", "error", err)
		}
	}()
	return client.parseFeatureStream(stream)
}

func (client *Client) GetFeatureVariants(ctx context.Context, ids []NameVariant) ([]*FeatureVariant, error) {
	logger := logging.GetLoggerFromContext(ctx)
	stream, err := client.GrpcConn.GetFeatureVariants(ctx)
	if err != nil {
		logger.Errorw("Failed to get feature variants", "ids", ids, "error", err)
		return nil, err
	}
	go func() {
		for _, id := range ids {
			stream.Send(&pb.NameVariantRequest{NameVariant: &pb.NameVariant{Name: id.Name, Variant: id.Variant}, RequestId: logging.GetRequestIDFromContext(ctx)})
		}
		err := stream.CloseSend()
		if err != nil {
			logger.Errorw("Failed to close send", "error", err)
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
	Definition  string
	Type        types.ValueType
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

type Streaming struct {
	OfflineProvider string
}

func (s Streaming) SerializeStream() *pb.Stream {
	return &pb.Stream{
		OfflineProvider: s.OfflineProvider,
	}
}

func (def FeatureDef) ResourceType() ResourceType {
	return FEATURE_VARIANT
}

func (def FeatureDef) Serialize(requestID string) (*pb.FeatureVariantRequest, error) {
	var typeProto *pb.ValueType
	if def.Type == nil {
		typeProto = types.NilType.ToProto()
	} else {
		typeProto = def.Type.ToProto()
	}
	serialized := &pb.FeatureVariantRequest{
		FeatureVariant: &pb.FeatureVariant{
			Name:        def.Name,
			Variant:     def.Variant,
			Source:      def.Source.Serialize(),
			Type:        typeProto,
			Entity:      def.Entity,
			Owner:       def.Owner,
			Description: def.Description,
			Status:      &pb.ResourceStatus{Status: pb.ResourceStatus_CREATED},
			Provider:    def.Provider,
			Schedule:    def.Schedule,
			Tags:        &pb.Tags{Tag: def.Tags},
			Properties:  def.Properties.Serialize(),
			Mode:        pb.ComputationMode(def.Mode),
		},
		RequestId: requestID,
	}

	switch x := def.Location.(type) {
	case ResourceVariantColumns:
		serialized.FeatureVariant.Location = def.Location.(ResourceVariantColumns).SerializeFeatureColumns()
	case PythonFunction:
		serialized.FeatureVariant.Location = def.Location.(PythonFunction).SerializePythonFunction()
	case Streaming:
		serializedStream := def.Location.(Streaming).SerializeStream()
		serialized.FeatureVariant.Location = &pb.FeatureVariant_Stream{Stream: serializedStream}
	case nil:
		return nil, fferr.NewInvalidArgumentError(fmt.Errorf("FeatureDef Columns not set"))
	default:
		return nil, fferr.NewInvalidArgumentError(fmt.Errorf("FeatureDef Columns has unexpected type %T", x))
	}
	return serialized, nil
}

func (client *Client) CreateFeatureVariant(ctx context.Context, def FeatureDef) error {
	requestID := logging.GetRequestIDFromContext(ctx)
	serialized, err := def.Serialize(requestID)
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
		features = append(features, WrapProtoFeature(serial))
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
		features = append(features, WrapProtoFeatureVariant(serial))
	}
	return features, nil
}

func (client *Client) ListLabels(ctx context.Context) ([]*Label, error) {
	logger := logging.GetLoggerFromContext(ctx)
	stream, err := client.GrpcConn.ListLabels(ctx, &pb.ListRequest{RequestId: logging.GetRequestIDFromContext(ctx)})
	if err != nil {
		logger.Errorw("Failed to list labels", "error", err)
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
	logger := logging.GetLoggerFromContext(ctx)
	stream, err := client.GrpcConn.GetLabels(ctx)
	if err != nil {
		logger.Errorw("Failed to get labels", "labels", labels, "error", err)
		return nil, err
	}
	go func() {
		for _, label := range labels {
			stream.Send(&pb.NameRequest{Name: &pb.Name{Name: label}, RequestId: logging.GetRequestIDFromContext(ctx)})
		}
		err := stream.CloseSend()
		if err != nil {
			logger.Errorw("Failed to close send", "error", err)
		}
	}()
	return client.parseLabelStream(stream)
}

type LabelDef struct {
	Name        string
	Variant     string
	Description string
	Type        types.ValueType
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

func (def LabelDef) Serialize(requestID string) (*pb.LabelVariantRequest, error) {
	var typeProto *pb.ValueType
	if def.Type == nil {
		typeProto = types.NilType.ToProto()
	} else {
		typeProto = def.Type.ToProto()
	}
	serialized := &pb.LabelVariantRequest{
		LabelVariant: &pb.LabelVariant{
			Name:        def.Name,
			Variant:     def.Variant,
			Description: def.Description,
			Type:        typeProto,
			Source:      def.Source.Serialize(),
			Entity:      def.Entity,
			Owner:       def.Owner,
			Status:      &pb.ResourceStatus{Status: pb.ResourceStatus_NO_STATUS},
			Provider:    def.Provider,
			Tags:        &pb.Tags{Tag: def.Tags},
			Properties:  def.Properties.Serialize(),
		},
		RequestId: requestID,
	}

	switch x := def.Location.(type) {
	case ResourceVariantColumns:
		serialized.LabelVariant.Location = def.Location.(ResourceVariantColumns).SerializeLabelColumns()
	case Streaming:
		serializedStream := def.Location.(Streaming).SerializeStream()
		serialized.LabelVariant.Location = &pb.LabelVariant_Stream{Stream: serializedStream}
	case nil:
		return nil, fferr.NewInvalidArgumentError(fmt.Errorf("LabelDef Source not set"))
	default:
		return nil, fferr.NewInvalidArgumentError(fmt.Errorf("LabelDef Source has unexpected type %T", x))
	}
	return serialized, nil
}

func (client *Client) CreateLabelVariant(ctx context.Context, def LabelDef) error {
	requestID := logging.GetRequestIDFromContext(ctx)
	serialized, err := def.Serialize(requestID)
	client.Logger.Debugw("Creating label variant", "serialized", serialized)
	if err != nil {
		return err
	}
	_, err = client.GrpcConn.CreateLabelVariant(ctx, serialized)
	return err
}

func (client *Client) GetLabelVariants(ctx context.Context, ids []NameVariant) ([]*LabelVariant, error) {
	logger := logging.GetLoggerFromContext(ctx)
	stream, err := client.GrpcConn.GetLabelVariants(ctx)
	if err != nil {
		logger.Errorw("Failed to get label variants", "ids", ids, "error", err)
		return nil, err
	}
	go func() {
		for _, id := range ids {
			stream.Send(&pb.NameVariantRequest{NameVariant: &pb.NameVariant{Name: id.Name, Variant: id.Variant}, RequestId: logging.GetRequestIDFromContext(ctx)})
		}
		err := stream.CloseSend()
		if err != nil {
			logger.Errorw("Failed to close send", "error", err)
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
		labels = append(labels, WrapProtoLabel(serial))
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
		features = append(features, WrapProtoLabelVariant(serial))
	}
	return features, nil
}

func (client *Client) ListTrainingSets(ctx context.Context) ([]*TrainingSet, error) {
	logger := logging.GetLoggerFromContext(ctx)
	stream, err := client.GrpcConn.ListTrainingSets(
		ctx,
		&pb.ListRequest{RequestId: logging.GetRequestIDFromContext(ctx)},
	)
	if err != nil {
		logger.Errorw("Failed to list training sets", "error", err)
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
	logger := logging.GetLoggerFromContext(ctx)
	stream, err := client.GrpcConn.GetTrainingSets(ctx)
	if err != nil {
		logger.Errorw("Failed to get training sets", "trainingSets", trainingSets, "error", err)
		return nil, err
	}
	go func() {
		for _, trainingSet := range trainingSets {
			stream.Send(&pb.NameRequest{Name: &pb.Name{Name: trainingSet}, RequestId: logging.GetRequestIDFromContext(ctx)})
		}
		err := stream.CloseSend()
		if err != nil {
			logger.Errorw("Failed to close send", "error", err)
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

func (def TrainingSetDef) Serialize(requestID string) *pb.TrainingSetVariantRequest {
	return &pb.TrainingSetVariantRequest{
		TrainingSetVariant: &pb.TrainingSetVariant{
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
		},
		RequestId: requestID,
	}

}

func (client *Client) CreateTrainingSetVariant(ctx context.Context, def TrainingSetDef) error {
	requestID := logging.GetRequestIDFromContext(ctx)
	serialized := def.Serialize(requestID)
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
	logger := logging.GetLoggerFromContext(ctx)
	stream, err := client.GrpcConn.GetTrainingSetVariants(ctx)
	if err != nil {
		logger.Errorw("Failed to get training set variants", "ids", ids, "error", err)
		return nil, err
	}
	go func() {
		for _, id := range ids {
			stream.Send(&pb.NameVariantRequest{NameVariant: &pb.NameVariant{Name: id.Name, Variant: id.Variant}, RequestId: logging.GetRequestIDFromContext(ctx)})
		}
		err := stream.CloseSend()
		if err != nil {
			logger.Errorw("Failed to close send", "error", err)
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
		trainingSets = append(trainingSets, WrapProtoTrainingSet(serial))
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
		features = append(features, WrapProtoTrainingSetVariant(serial))
	}
	return features, nil
}

func (client *Client) ListSources(ctx context.Context) ([]*Source, error) {
	logger := logging.GetLoggerFromContext(ctx)
	stream, err := client.GrpcConn.ListSources(ctx, &pb.ListRequest{RequestId: logging.GetRequestIDFromContext(ctx)})
	if err != nil {
		logger.Errorw("Failed to list sources", "error", err)
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
	logger := logging.GetLoggerFromContext(ctx)
	stream, err := client.GrpcConn.GetSources(ctx)
	if err != nil {
		logger.Errorw("Failed to get sources", "sources", sources, "error", err)
		return nil, err
	}
	go func() {
		for _, source := range sources {
			stream.Send(&pb.NameRequest{Name: &pb.Name{Name: source}, RequestId: logging.GetRequestIDFromContext(ctx)})
		}
		err := stream.CloseSend()
		if err != nil {
			logger.Errorw("Failed to close send", "error", err)
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
	Location        PrimaryDataLocationType
	TimestampColumn string
}

type PrimaryDataLocationType interface {
	isPrimaryData() bool
}

type SQLTable struct {
	Name string
}

func (t SQLTable) isPrimaryData() bool {
	return true
}

type TransformationSourceDef struct {
	Def interface{}
}

func (t TransformationSource) Serialize() (*pb.SourceVariant_Transformation, error) {
	var transformation *pb.Transformation
	switch x := t.TransformationType.(type) {
	case SQLTransformationType:
		transformation = &pb.Transformation{
			Type: &pb.Transformation_SQLTransformation{
				SQLTransformation: &pb.SQLTransformation{
					Query:  t.TransformationType.(SQLTransformationType).Query,
					Source: t.TransformationType.(SQLTransformationType).Sources.Serialize(),
				},
			},
		}
	case nil:
		return nil, fferr.NewInvalidArgumentError(fmt.Errorf("TransformationSource Type not set"))
	default:
		return nil, fferr.NewInvalidArgumentError(fmt.Errorf("TransformationSource Type has unexpected type %T", x))
	}
	return &pb.SourceVariant_Transformation{
		Transformation: transformation,
	}, nil
}

func (t PrimaryDataSource) Serialize() (*pb.SourceVariant_PrimaryData, error) {
	var primaryData *pb.PrimaryData
	switch x := t.Location.(type) {
	case SQLTable:
		primaryData = &pb.PrimaryData{
			Location: &pb.PrimaryData_Table{
				Table: &pb.SQLTable{
					Name: t.Location.(SQLTable).Name,
				},
			},
			TimestampColumn: t.TimestampColumn,
		}
	case nil:
		return nil, fferr.NewInvalidArgumentError(fmt.Errorf("PrimaryDataSource Type not set"))
	default:
		return nil, fferr.NewInvalidArgumentError(fmt.Errorf("PrimaryDataSource Type has unexpected type %T", x))
	}
	return &pb.SourceVariant_PrimaryData{
		PrimaryData: primaryData,
	}, nil
}

func (def SourceDef) ResourceType() ResourceType {
	return SOURCE_VARIANT
}

func (def SourceDef) Serialize(requestID string) (*pb.SourceVariantRequest, error) {
	serialized := &pb.SourceVariantRequest{
		SourceVariant: &pb.SourceVariant{
			Name:        def.Name,
			Variant:     def.Variant,
			Description: def.Description,
			Owner:       def.Owner,
			Status:      &pb.ResourceStatus{Status: pb.ResourceStatus_CREATED},
			Provider:    def.Provider,
			Schedule:    def.Schedule,
			Tags:        &pb.Tags{Tag: def.Tags},
			Properties:  def.Properties.Serialize(),
		},
		RequestId: requestID,
	}
	var err error
	switch x := def.Definition.(type) {
	case TransformationSource:
		serialized.SourceVariant.Definition, err = def.Definition.(TransformationSource).Serialize()
	case PrimaryDataSource:
		serialized.SourceVariant.Definition, err = def.Definition.(PrimaryDataSource).Serialize()
	case nil:
		return nil, fferr.NewInvalidArgumentError(fmt.Errorf("SourceDef Definition not set"))
	default:
		return nil, fferr.NewInvalidArgumentError(fmt.Errorf("SourceDef Definition has unexpected type %T", x))
	}
	if err != nil {
		return nil, err
	}
	return serialized, nil
}

func (client *Client) CreateSourceVariant(ctx context.Context, def SourceDef) error {
	requestID := logging.GetRequestIDFromContext(ctx)
	serialized, err := def.Serialize(requestID)
	if err != nil {
		return err
	}
	_, err = client.GrpcConn.CreateSourceVariant(ctx, serialized)
	return err
}

func (client *Client) GetSourceVariants(ctx context.Context, ids []NameVariant) ([]*SourceVariant, error) {
	logger := logging.GetLoggerFromContext(ctx)
	stream, err := client.GrpcConn.GetSourceVariants(ctx)
	if err != nil {
		logger.Errorw("Failed to get source variants", "ids", ids, "error", err)
		return nil, err
	}
	go func() {
		for _, id := range ids {
			req := &pb.NameVariantRequest{
				NameVariant: &pb.NameVariant{
					Name:    id.Name,
					Variant: id.Variant,
				},
				RequestId: logging.GetRequestIDFromContext(ctx)}
			err := stream.Send(req)
			if err != nil {
				logger.Errorw(
					"Failed to send source variant",
					"name",
					id.Name,
					"variant",
					id.Variant,
					"error",
					err,
				)
			}
		}
		err := stream.CloseSend()
		if err != nil {
			logger.Errorw("Failed to close send", "error", err)
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
		return nil, err
	}
	return variants[0], nil
}

func (client *Client) FinalizeDelete(ctx context.Context, resId ResourceID) error {
	nameVariant := pb.NameVariant{Name: resId.Name, Variant: resId.Variant}
	resourceID := pb.ResourceID{Resource: &nameVariant, ResourceType: resId.Type.Serialized()}
	_, err := client.GrpcConn.FinalizeDeletion(ctx, &pb.FinalizeDeletionRequest{ResourceId: &resourceID})
	return err
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
		sources = append(sources, WrapProtoSource(serial))
	}
	return sources, nil
}

type sourceVariantStream interface {
	Recv() (*pb.SourceVariant, error)
}

func (client *Client) parseSourceVariantStream(stream sourceVariantStream) ([]*SourceVariant, error) {
	sourceVariants := make([]*SourceVariant, 0)
	for {
		serial, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			client.Logger.Errorw("Error receiving parsed stream", "error", err)
			// print if this is a grpc status error
			if grpcStatus, ok := grpc_status.FromError(err); ok {
				client.Logger.Errorw(
					"GRPC status error",
					"code",
					grpcStatus.Code(),
					"message",
					grpcStatus.Message(),
					"details",
					grpcStatus.Details(),
				)
			} else {
				client.Logger.Errorw("Error is not a grpc status error", "error", err)
			}
			return nil, err
		}
		sourceVariants = append(sourceVariants, WrapProtoSourceVariant(serial))
	}
	return sourceVariants, nil
}

func (client *Client) ListUsers(ctx context.Context) ([]*User, error) {
	logger := logging.GetLoggerFromContext(ctx)
	stream, err := client.GrpcConn.ListUsers(ctx, &pb.ListRequest{RequestId: logging.GetRequestIDFromContext(ctx)})
	if err != nil {
		logger.Errorw("Failed to list users", "error", err)
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
	logger := logging.GetLoggerFromContext(ctx)
	stream, err := client.GrpcConn.GetUsers(ctx)
	if err != nil {
		logger.Errorw("Failed to get users", "users", users, "error", err)
		return nil, err
	}
	go func() {
		for _, user := range users {
			stream.Send(&pb.NameRequest{Name: &pb.Name{Name: user}, RequestId: logging.GetRequestIDFromContext(ctx)})
		}
		err := stream.CloseSend()
		if err != nil {
			logger.Errorw("Failed to close send", "error", err)
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
	requestID := logging.GetRequestIDFromContext(ctx)

	serialized := &pb.UserRequest{
		User: &pb.User{
			Name:       def.Name,
			Tags:       &pb.Tags{Tag: def.Tags},
			Properties: def.Properties.Serialize(),
		},
		RequestId: requestID,
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
		users = append(users, WrapProtoUser(serial))
	}
	return users, nil
}

func (client *Client) ListProviders(ctx context.Context) ([]*Provider, error) {
	logger := logging.GetLoggerFromContext(ctx)
	stream, err := client.GrpcConn.ListProviders(ctx, &pb.ListRequest{RequestId: logging.GetRequestIDFromContext(ctx)})
	if err != nil {
		logger.Errorw("Failed to list providers", "error", err)
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
	logger := logging.GetLoggerFromContext(ctx)
	stream, err := client.GrpcConn.GetProviders(ctx)
	if err != nil {
		logger.Errorw("Failed to get providers", "providers", providers, "error", err)
		return nil, err
	}
	go func() {
		for _, provider := range providers {
			stream.Send(&pb.NameRequest{Name: &pb.Name{Name: provider}, RequestId: logging.GetRequestIDFromContext(ctx)})
		}
		err := stream.CloseSend()
		if err != nil {
			logger.Errorw("Failed to close send", "error", err)
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
	requestID := logging.GetRequestIDFromContext(ctx)

	serialized := &pb.ProviderRequest{
		Provider: &pb.Provider{
			Name:             def.Name,
			Description:      def.Description,
			Type:             def.Type,
			Software:         def.Software,
			Team:             def.Team,
			Status:           &pb.ResourceStatus{Status: pb.ResourceStatus_NO_STATUS},
			SerializedConfig: def.SerializedConfig,
			Tags:             &pb.Tags{Tag: def.Tags},
			Properties:       def.Properties.Serialize(),
		},
		RequestId: requestID,
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
		providers = append(providers, WrapProtoProvider(serial))
	}
	return providers, nil
}

func (client *Client) ListEntities(ctx context.Context) ([]*Entity, error) {
	logger := logging.GetLoggerFromContext(ctx)
	stream, err := client.GrpcConn.ListEntities(ctx, &pb.ListRequest{RequestId: logging.GetRequestIDFromContext(ctx)})
	if err != nil {
		logger.Errorw("Failed to list entities", "error", err)
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
	logger := logging.GetLoggerFromContext(ctx)
	stream, err := client.GrpcConn.GetEntities(ctx)
	if err != nil {
		logger.Errorw("Failed to get entities", "entities", entities, "error", err)
		return nil, err
	}
	go func() {
		for _, entity := range entities {
			stream.Send(&pb.NameRequest{Name: &pb.Name{Name: entity}, RequestId: logging.GetRequestIDFromContext(ctx)})
		}
		err := stream.CloseSend()
		if err != nil {
			logger.Errorw("Failed to close send", "error", err)
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
	requestID := logging.GetRequestIDFromContext(ctx)
	serialized := &pb.EntityRequest{
		Entity: &pb.Entity{
			Name:        def.Name,
			Status:      &pb.ResourceStatus{Status: pb.ResourceStatus_NO_STATUS},
			Description: def.Description,
			Tags:        &pb.Tags{Tag: def.Tags},
			Properties:  def.Properties.Serialize(),
		},
		RequestId: requestID,
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
		entities = append(entities, WrapProtoEntity(serial))
	}
	return entities, nil
}

func (client *Client) ListModels(ctx context.Context) ([]*Model, error) {
	logger := logging.GetLoggerFromContext(ctx)
	stream, err := client.GrpcConn.ListModels(ctx, &pb.ListRequest{RequestId: logging.GetRequestIDFromContext(ctx)})
	if err != nil {
		logger.Errorw("Failed to list models", "error", err)
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
	logger := logging.GetLoggerFromContext(ctx)
	stream, err := client.GrpcConn.GetModels(ctx)
	if err != nil {
		logger.Errorw("Failed to get models", "models", models, "error", err)
		return nil, err
	}
	go func() {
		for _, model := range models {
			stream.Send(&pb.NameRequest{Name: &pb.Name{Name: model}, RequestId: logging.GetRequestIDFromContext(ctx)})
		}
		err := stream.CloseSend()
		if err != nil {
			logger.Errorw("Failed to close send", "error", err)
		}
	}()
	return client.parseModelStream(stream)
}

func (client *Client) GetStagedForDeletionSourceVariant(ctx context.Context, id NameVariant) (*SourceVariant, error) {
	res, err := client.GetStagedForDeletionResource(ctx, ResourceID{
		Name:    id.Name,
		Variant: id.Variant,
		Type:    SOURCE_VARIANT,
	})
	if err != nil {
		return nil, err
	}
	return WrapProtoSourceVariant(res.GetSourceVariant()), nil
}

func (client *Client) GetStagedForDeletionTrainingSetVariant(ctx context.Context, id NameVariant) (*TrainingSetVariant, error) {
	res, err := client.GetStagedForDeletionResource(ctx, ResourceID{
		Name:    id.Name,
		Variant: id.Variant,
		Type:    TRAINING_SET_VARIANT,
	})
	if err != nil {
		return nil, err
	}
	return WrapProtoTrainingSetVariant(res.GetTrainingSetVariant()), nil
}

func (client *Client) GetStagedForDeletionFeatureVariant(ctx context.Context, id NameVariant) (*FeatureVariant, error) {
	res, err := client.GetStagedForDeletionResource(ctx, ResourceID{
		Name:    id.Name,
		Variant: id.Variant,
		Type:    FEATURE_VARIANT,
	})
	if err != nil {
		return nil, err
	}
	return WrapProtoFeatureVariant(res.GetFeatureVariant()), nil
}

func (client *Client) GetStagedForDeletionLabelVariant(ctx context.Context, id NameVariant) (*LabelVariant, error) {
	res, err := client.GetStagedForDeletionResource(ctx, ResourceID{
		Name:    id.Name,
		Variant: id.Variant,
		Type:    LABEL_VARIANT,
	})
	if err != nil {
		return nil, err
	}
	return WrapProtoLabelVariant(res.GetLabelVariant()), nil
}

func (client *Client) GetStagedForDeletionResource(ctx context.Context, id ResourceID) (*pb.ResourceVariant, error) {
	client.Logger.Debugw("Getting staged for deletion resource", "id", id)
	nameVariant := pb.NameVariant{Name: id.Name, Variant: id.Variant}
	resourceID := pb.ResourceID{Resource: &nameVariant, ResourceType: id.Type.Serialized()}

	resp, err := client.GrpcConn.GetStagedForDeletionResource(ctx, &pb.GetStagedForDeletionResourceRequest{ResourceId: &resourceID})
	if err != nil {
		return nil, err
	}

	return resp.ResourceVariant, err
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
	requestID := logging.GetRequestIDFromContext(ctx)
	serialized := &pb.ModelRequest{
		Model: &pb.Model{
			Name:         def.Name,
			Description:  def.Description,
			Features:     def.Features.Serialize(),
			Trainingsets: def.Trainingsets.Serialize(),
			Tags:         &pb.Tags{Tag: def.Tags},
			Properties:   def.Properties.Serialize(),
		},
		RequestId: requestID,
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
		models = append(models, WrapProtoModel(serial))
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

type StreamGetter interface {
	GetStream() Stream
}

type Stream interface {
	FetchProvider(client *Client, ctx context.Context) (*Provider, error)
}

type StreamDef struct {
	offlineProvider string
}

func (s StreamDef) FetchProvider(client *Client, ctx context.Context) (*Provider, error) {
	return client.GetProvider(ctx, s.offlineProvider)
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

type serializedConfigGetter interface {
	GetSerializedConfig() []byte
}

type fetchSerializedConfigFn struct {
	getter serializedConfigGetter
}

func (fn fetchSerializedConfigFn) SerializedConfig() pc.SerializedConfig {
	return fn.getter.GetSerializedConfig()
}

type maxJobDurationGetter interface {
	GetMaxJobDuration() *durationpb.Duration
}

type fetchMaxJobDurationFn struct {
	getter maxJobDurationGetter
}

func (fn fetchMaxJobDurationFn) MaxJobDuration() time.Duration {
	duration := fn.getter.GetMaxJobDuration()

	if duration == nil || (duration.Seconds == 0 && duration.Nanos == 0) {
		return time.Hour * 48
	}

	return duration.AsDuration()
}

type Feature struct {
	serialized *pb.Feature
	variantsFns
	protoStringer
}

func WrapProtoFeature(serialized *pb.Feature) *Feature {
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
}

func WrapProtoFeatureVariant(serialized *pb.FeatureVariant) *FeatureVariant {
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
	}
}

func columnsToMap(columns ResourceVariantColumns) map[string]string {
	columnNameValues := reflect.ValueOf(columns)
	featureColumns := make(map[string]string)
	for i := 0; i < columnNameValues.NumField(); i++ {
		featureColumns[columnNameValues.Type().Field(i).Name] = fmt.Sprintf("%v", columnNameValues.Field(i).Interface())
	}
	return featureColumns
}

type typedVariant interface {
	Type() (types.ValueType, error)
}

func typeString(t typedVariant) string {
	dataType, err := t.Type()
	var dataTypeStr string
	if err != nil {
		dataTypeStr = "Unknown"
	} else {
		dataTypeStr = dataType.String()
	}
	return dataTypeStr
}

func (variant *FeatureVariant) ToShallowMap() FeatureVariantResource {
	fv := FeatureVariantResource{}
	switch variant.Mode() {
	case PRECOMPUTED:
		fv = FeatureVariantResource{
			Created:     variant.Created(),
			Description: variant.Description(),
			Entity:      variant.Entity(),
			Name:        variant.Name(),
			DataType:    typeString(variant),
			Variant:     variant.Variant(),
			Owner:       variant.Owner(),
			Provider:    variant.Provider(),
			Source:      variant.Source(),
			Location:    columnsToMap(variant.LocationColumns().(ResourceVariantColumns)),
			Status:      variant.Status().String(),
			Error:       variant.Error(),
			Tags:        variant.Tags(),
			Properties:  variant.Properties(),
			Mode:        variant.Mode().String(),
			IsOnDemand:  variant.IsOnDemand(),
		}
	case CLIENT_COMPUTED:
		location := make(map[string]string)
		if pyFunc, ok := variant.LocationFunction().(PythonFunction); ok {
			location["query"] = string(pyFunc.Query)
		}
		fv = FeatureVariantResource{
			Created:     variant.Created(),
			Description: variant.Description(),
			Name:        variant.Name(),
			Variant:     variant.Variant(),
			Owner:       variant.Owner(),
			Location:    location,
			Status:      variant.Status().String(),
			Error:       variant.Error(),
			Tags:        variant.Tags(),
			Properties:  variant.Properties(),
			Mode:        variant.Mode().String(),
			IsOnDemand:  variant.IsOnDemand(),
			Definition:  variant.Definition(),
		}
	default:
		fmt.Printf("Unknown computation mode %v\n", variant.Mode())
	}
	return fv
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

func (variant *FeatureVariant) Type() (types.ValueType, error) {
	return types.ValueTypeFromProto(variant.serialized.GetType())
}

func (variant *FeatureVariant) Entity() string {
	return variant.serialized.GetEntity()
}

func (variant *FeatureVariant) Owner() string {
	return variant.serialized.GetOwner()
}

func (variant *FeatureVariant) Status() scheduling.Status {
	if variant.serialized.GetStatus() != nil {
		return scheduling.Status(variant.serialized.GetStatus().Status)
	}
	return scheduling.CREATED
}

func (variant *FeatureVariant) Error() string {
	if variant.serialized.GetStatus() != nil {
		return fferr.ToDashboardError(variant.serialized.GetStatus())
	}
	return ""
}

func (variant *FeatureVariant) Location() interface{} {
	return variant.serialized.GetLocation()
}

func (variant *FeatureVariant) Definition() string {
	def := ""
	if variant.IsOnDemand() {
		def = variant.serialized.GetAdditionalParameters().GetOndemand().GetDefinition()
	}
	return def
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

func (variant *FeatureVariant) LocationStream() interface{} {
	if variant.Mode() != STREAMING {
		return nil
	}
	src := variant.serialized.GetStream()
	stream := Streaming{
		OfflineProvider: src.OfflineProvider,
	}
	return stream
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
	case PRECOMPUTED, STREAMING:
		return false
	case CLIENT_COMPUTED:
		return true
	default:
		fmt.Printf("Unknown computation mode: %v\n", variant.Mode())
		return false
	}
}

func (variant *FeatureVariant) IsEmbedding() bool {
	t, err := variant.Type()
	if err != nil {
		return false
	}
	if vec, ok := t.(types.VectorType); ok {
		return vec.IsEmbedding
	}
	return false
}

func (variant *FeatureVariant) Dimension() int32 {
	t, err := variant.Type()
	if err != nil {
		return -1
	}
	if vec, ok := t.(types.VectorType); ok {
		return vec.Dimension
	}
	return 0
}

func (variant *FeatureVariant) GetStream() Stream {
	mode := variant.Mode()
	if mode != STREAMING {
		return nil
	}
	location := variant.Location().(*pb.FeatureVariant_Stream)
	return StreamDef{
		offlineProvider: location.Stream.OfflineProvider,
	}
}

func (variant *FeatureVariant) ResourceSnowflakeConfig() (*ResourceSnowflakeConfig, error) {
	if variant.Mode() != PRECOMPUTED {
		return nil, fferr.NewInvalidArgumentErrorf("Ondemand features do not support Snowflake Dynamic Table configurations")
	}

	return getResourceSnowflakeConfig(variant.serialized)
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

func WrapProtoUser(serialized *pb.User) *User {
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

func (user *User) Status() scheduling.Status {
	if user.serialized.GetStatus() != nil {
		return scheduling.Status(user.serialized.GetStatus().Status)
	}
	return scheduling.CREATED
}

func (user *User) Error() string {
	if user.serialized.GetStatus() != nil {
		return fferr.ToDashboardError(user.serialized.GetStatus())
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
	fetchSerializedConfigFn
}

func (p Provider) Variant() string {
	return ""
}

func WrapProtoProvider(serialized *pb.Provider) *Provider {
	return &Provider{
		serialized:              serialized,
		fetchTrainingSetsFns:    fetchTrainingSetsFns{serialized},
		fetchFeaturesFns:        fetchFeaturesFns{serialized},
		fetchLabelsFns:          fetchLabelsFns{serialized},
		fetchSourcesFns:         fetchSourcesFns{serialized},
		protoStringer:           protoStringer{serialized},
		fetchTagsFn:             fetchTagsFn{serialized},
		fetchPropertiesFn:       fetchPropertiesFn{serialized},
		fetchSerializedConfigFn: fetchSerializedConfigFn{serialized},
	}
}

func (provider *Provider) ToShallowMap() ProviderResource {
	return ProviderResource{
		Name:             provider.Name(),
		Description:      provider.Description(),
		Type:             "Provider",
		ProviderType:     provider.Type(),
		Software:         provider.Software(),
		Team:             provider.Team(),
		SerializedConfig: provider.SerializedConfig(),
		Status:           provider.Status().String(),
		Error:            provider.Error(),
		Tags:             provider.Tags(),
		Properties:       provider.Properties(),
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

func (provider *Provider) Status() scheduling.Status {
	if provider.serialized.GetStatus() != nil {
		return scheduling.Status(provider.serialized.GetStatus().Status)
	}
	return scheduling.CREATED
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

func WrapProtoModel(serialized *pb.Model) *Model {
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

func (model *Model) Status() scheduling.Status {
	return scheduling.CREATED
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

func WrapProtoLabel(serialized *pb.Label) *Label {
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

func WrapProtoLabelVariant(serialized *pb.LabelVariant) *LabelVariant {
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

func (variant *LabelVariant) ToShallowMap() LabelVariantResource {
	return LabelVariantResource{
		Created:     variant.Created(),
		Description: variant.Description(),
		Entity:      variant.Entity(),
		Name:        variant.Name(),
		DataType:    typeString(variant),
		Variant:     variant.Variant(),
		Owner:       variant.Owner(),
		Provider:    variant.Provider(),
		Source:      variant.Source(),
		Location:    columnsToMap(variant.LocationColumns().(ResourceVariantColumns)),
		Status:      variant.Status().String(),
		Error:       variant.Error(),
		Tags:        variant.Tags(),
		Properties:  variant.Properties(),
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

func (variant *LabelVariant) Type() (types.ValueType, error) {
	return types.ValueTypeFromProto(variant.serialized.GetType())
}

func (variant *LabelVariant) Entity() string {
	return variant.serialized.GetEntity()
}

func (variant *LabelVariant) Owner() string {
	return variant.serialized.GetOwner()
}

func (variant *LabelVariant) Status() scheduling.Status {
	if variant.serialized.GetStatus() != nil {
		return scheduling.Status(variant.serialized.GetStatus().Status)
	}
	return scheduling.CREATED
}

func (variant *LabelVariant) Error() string {
	if variant.serialized.GetStatus() != nil {
		return fferr.ToDashboardError(variant.serialized.GetStatus())
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

func (variant *LabelVariant) GetStream() Stream {
	stream := variant.serialized.GetStream()
	if stream == nil {
		return nil
	}
	return StreamDef{
		offlineProvider: stream.OfflineProvider,
	}
}

func (variant *LabelVariant) ResourceSnowflakeConfig() (*ResourceSnowflakeConfig, error) {
	return getResourceSnowflakeConfig(variant.serialized)
}

type TrainingSet struct {
	serialized *pb.TrainingSet
	variantsFns
	protoStringer
}

func WrapProtoTrainingSet(serialized *pb.TrainingSet) *TrainingSet {
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

func WrapProtoTrainingSetVariant(serialized *pb.TrainingSetVariant) *TrainingSetVariant {
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

func (variant *TrainingSetVariant) ToShallowMap() TrainingSetVariantResource {
	return TrainingSetVariantResource{
		Created:     variant.Created(),
		Description: variant.Description(),
		Name:        variant.Name(),
		Variant:     variant.Variant(),
		Owner:       variant.Owner(),
		Provider:    variant.Provider(),
		Label:       variant.Label(),
		Status:      variant.Status().String(),
		Error:       variant.Error(),
		Tags:        variant.Tags(),
		Properties:  variant.Properties(),
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

func (variant *TrainingSetVariant) Status() scheduling.Status {
	if variant.serialized.GetStatus() != nil {
		return scheduling.Status(variant.serialized.GetStatus().Status)
	}
	return scheduling.CREATED
}

func (variant *TrainingSetVariant) Error() string {
	if variant.serialized.GetStatus() == nil {
		return ""
	}
	return fferr.ToDashboardError(variant.serialized.GetStatus())
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

func (variant *TrainingSetVariant) ResourceSnowflakeConfig() (*ResourceSnowflakeConfig, error) {
	return getResourceSnowflakeConfig(variant.serialized)
}

type Source struct {
	serialized *pb.Source
	variantsFns
	protoStringer
}

func WrapProtoSource(serialized *pb.Source) *Source {
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
	fetchMaxJobDurationFn
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

type RefreshMode string
type Initialize string

const (
	AutoRefresh        RefreshMode = "AUTO" // Default
	FullRefresh        RefreshMode = "FULL"
	IncrementalRefresh RefreshMode = "INCREMENTAL"

	InitializeOnCreate   Initialize = "ON_CREATE" // Default
	InitializeOnSchedule Initialize = "ON_SCHEDULE"
)

func RefreshModeFromProto(proto pb.RefreshMode) (RefreshMode, error) {
	var refreshMode RefreshMode
	switch proto {
	case pb.RefreshMode_REFRESH_MODE_AUTO:
		refreshMode = AutoRefresh
	case pb.RefreshMode_REFRESH_MODE_FULL:
		refreshMode = FullRefresh
	case pb.RefreshMode_REFRESH_MODE_INCREMENTAL:
		refreshMode = IncrementalRefresh
	case pb.RefreshMode_REFRESH_MODE_UNSPECIFIED:
		return refreshMode, fferr.NewInvalidArgumentErrorf("Refresh mode unspecified")
	default:
		return refreshMode, fferr.NewInternalErrorf("Unknown refresh mode %v", proto)
	}
	return refreshMode, nil
}

func RefreshModeFromString(refreshMode string) (RefreshMode, error) {
	switch refreshMode {
	case "AUTO":
		return AutoRefresh, nil
	case "FULL":
		return FullRefresh, nil
	case "INCREMENTAL":
		return IncrementalRefresh, nil
	default:
		return "", fferr.NewInvalidArgumentErrorf("Invalid refresh mode %s", refreshMode)
	}
}

func InitializeFromProto(proto pb.Initialize) (Initialize, error) {
	var initialize Initialize
	switch proto {
	case pb.Initialize_INITIALIZE_ON_CREATE:
		initialize = InitializeOnCreate
	case pb.Initialize_INITIALIZE_ON_SCHEDULE:
		initialize = InitializeOnSchedule
	case pb.Initialize_INITIALIZE_UNSPECIFIED:
		return initialize, fferr.NewInvalidArgumentErrorf("Initialize unspecified")
	default:
		return initialize, fferr.NewInternalErrorf("Unknown initialize mode %v", proto)
	}
	return initialize, nil
}

func InitializeFromString(initialize string) (Initialize, error) {
	switch initialize {
	case "ON_CREATE":
		return InitializeOnCreate, nil
	case "ON_SCHEDULE":
		return InitializeOnSchedule, nil
	default:
		return "", fferr.NewInvalidArgumentErrorf("Invalid initialize mode %s", initialize)
	}
}

type ResourceSnowflakeConfig struct {
	DynamicTableConfig *SnowflakeDynamicTableConfig
	Warehouse          string
}

func (config *ResourceSnowflakeConfig) Merge(c *pc.SnowflakeConfig) error {
	if config.DynamicTableConfig == nil {
		config.DynamicTableConfig = &SnowflakeDynamicTableConfig{}
	}

	if err := config.ValidateConfig(c); err != nil {
		return err
	}

	// Merge ExternalVolume and BaseLocation from catalog to resource level
	// No additional validation is needed here because these values are coming from the catalog level
	config.DynamicTableConfig.ExternalVolume = c.Catalog.ExternalVolume
	config.DynamicTableConfig.BaseLocation = c.Catalog.BaseLocation

	if err := config.ValidateMergeableFields(c); err != nil {
		return err
	}

	if config.DynamicTableConfig.TargetLag == "" {
		config.DynamicTableConfig.TargetLag = c.Catalog.TableConfig.TargetLag
	}

	if config.DynamicTableConfig.RefreshMode == "" {
		refreshMode, err := RefreshModeFromString(c.Catalog.TableConfig.RefreshMode)
		if err != nil {
			return err
		}
		config.DynamicTableConfig.RefreshMode = refreshMode
	}

	if config.DynamicTableConfig.Initialize == "" {
		initialize, err := InitializeFromString(c.Catalog.TableConfig.Initialize)
		if err != nil {
			return err
		}

		config.DynamicTableConfig.Initialize = initialize
	}

	if config.Warehouse == "" {
		config.Warehouse = c.Warehouse
	}

	return nil
}

func (config ResourceSnowflakeConfig) ValidateConfig(c *pc.SnowflakeConfig) error {
	if c.Catalog == nil {
		return fferr.NewInvalidArgumentErrorf("Snowflake catalog configuration is required")
	}

	if c.Catalog.ExternalVolume == "" {
		return fferr.NewInvalidArgumentErrorf("Snowflake catalog configuration requires an external volume")
	}

	if c.Catalog.BaseLocation == "" {
		return fferr.NewInvalidArgumentErrorf("Snowflake catalog configuration requires a base location")
	}

	if c.Warehouse == "" {
		return fferr.NewInvalidArgumentErrorf("Snowflake configuration requires a warehouse")
	}

	return nil
}

func (config ResourceSnowflakeConfig) ValidateMergeableFields(c *pc.SnowflakeConfig) error {
	if err := config.ValidateConfig(c); err != nil {
		return err
	}

	if config.DynamicTableConfig.TargetLag == "" && c.Catalog.TableConfig.TargetLag == "" {
		return fferr.NewInvalidArgumentErrorf("Snowflake dynamic table configuration requires a target lag")
	}

	if config.DynamicTableConfig.RefreshMode == "" && c.Catalog.TableConfig.RefreshMode == "" {
		return fferr.NewInvalidArgumentErrorf("Snowflake dynamic table configuration requires a refresh mode")
	}

	if config.DynamicTableConfig.Initialize == "" && c.Catalog.TableConfig.Initialize == "" {
		return fferr.NewInvalidArgumentErrorf("Snowflake dynamic table configuration requires an initialize mode")
	}

	if config.Warehouse == "" && c.Warehouse == "" {
		return fferr.NewInvalidArgumentErrorf("Snowflake configuration requires a warehouse")
	}

	return nil
}

func (config ResourceSnowflakeConfig) Validate() error {
	if config.DynamicTableConfig.ExternalVolume == "" {
		return fferr.NewInvalidArgumentErrorf("Snowflake dynamic table configuration requires an external volume")
	}

	if config.DynamicTableConfig.BaseLocation == "" {
		return fferr.NewInvalidArgumentErrorf("Snowflake dynamic table configuration requires a base location")
	}

	if err := config.DynamicTableConfig.ValidateTargetLag(); err != nil {
		return err
	}

	if config.DynamicTableConfig.RefreshMode == "" {
		return fferr.NewInvalidArgumentErrorf("Snowflake dynamic table configuration requires a refresh mode")
	}

	if config.DynamicTableConfig.Initialize == "" {
		return fferr.NewInvalidArgumentErrorf("Snowflake dynamic table configuration requires an initialize mode")
	}

	if config.Warehouse == "" {
		return fferr.NewInvalidArgumentErrorf("Snowflake configuration requires a warehouse")
	}

	return nil
}

func (config SnowflakeDynamicTableConfig) ValidateTargetLag() error {
	if config.TargetLag == "" {
		return fferr.NewInvalidArgumentErrorf("Snowflake dynamic table configuration requires a target lag")
	}

	// Regex to match the pattern `<num> <unit>` or 'DOWNSTREAM'
	re := regexp.MustCompile(`^(\d+)\s+(seconds|minutes|hours|days)$|^(?i:DOWNSTREAM)$`)

	targetLagTrimmed := strings.TrimSpace(config.TargetLag)

	if !re.MatchString(targetLagTrimmed) {
		return fferr.NewInvalidArgumentErrorf("Snowflake dynamic table configuration target lag is invalid: %s", config.TargetLag)
	}

	// If it's "DOWNSTREAM", it's valid
	if strings.EqualFold(targetLagTrimmed, "DOWNSTREAM") {
		return nil
	}

	// Extract the number and the unit
	matches := re.FindStringSubmatch(targetLagTrimmed)
	if len(matches) < 3 {
		return fferr.NewInvalidArgumentErrorf("Failed to parse target lag")
	}

	value, err := strconv.Atoi(matches[1]) // The numeric part
	if err != nil {
		return fferr.NewInvalidArgumentErrorf("Invalid number in target lag: %v", err)
	}
	unit := matches[2] // The unit part (seconds, minutes, etc.)

	// Enforce the minimum target lag (e.g., at least 1 minute)
	switch unit {
	case "seconds":
		if value < 60 {
			return fferr.NewInvalidArgumentErrorf("Minimum target lag is 1 minute, but got less than 60 seconds")
		}
	case "minutes":
		if value < 1 {
			return fferr.NewInvalidArgumentErrorf("Minimum target lag is 1 minute")
		}
	}

	// If the target lag meets the criteria, return nil (valid)
	return nil
}

type SnowflakeDynamicTableConfig struct {
	// ExternalVolume and BaseLocation are required for Snowflake Dynamic Table configuration;
	// however, we have specified these at the catalog level given it's less likely users will
	// want to change these values resource to resource.
	// Given this, we'll need to grab the values from the catalog level and pass them to the
	// resource level via the SnowflakeDynamicTableConfig.
	ExternalVolume string
	BaseLocation   string
	TargetLag      string
	RefreshMode    RefreshMode
	Initialize     Initialize
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

func (variant *SourceVariant) TaskIDs() ([]scheduling.TaskID, error) {
	// Check if using a deprecated taskID singleton
	if variant.serialized.TaskId != "" {
		return parseResourceTasks([]string{variant.serialized.TaskId})
	}
	return parseResourceTasks(variant.serialized.TaskIdList)
}

func WrapProtoSourceVariant(serialized *pb.SourceVariant) *SourceVariant {
	return &SourceVariant{
		serialized:            serialized,
		fetchTrainingSetsFns:  fetchTrainingSetsFns{serialized},
		fetchFeaturesFns:      fetchFeaturesFns{serialized},
		fetchLabelsFns:        fetchLabelsFns{serialized},
		fetchProviderFns:      fetchProviderFns{serialized},
		createdFn:             createdFn{serialized},
		lastUpdatedFn:         lastUpdatedFn{serialized},
		protoStringer:         protoStringer{serialized},
		fetchTagsFn:           fetchTagsFn{serialized},
		fetchPropertiesFn:     fetchPropertiesFn{serialized},
		fetchMaxJobDurationFn: fetchMaxJobDurationFn{serialized},
	}
}

func (variant *SourceVariant) getInputs() []NameVariant {
	if variant.IsSQLTransformation() {
		return variant.SQLTransformationSources()
	} else if variant.IsDFTransformation() {
		return variant.DFTransformationSources()
	} else {
		return []NameVariant{}
	}
}

func (variant *SourceVariant) ToShallowMap() SourceVariantResource {
	return SourceVariantResource{
		Name:           variant.Name(),
		Variant:        variant.Variant(),
		Definition:     getSourceString(variant),
		Owner:          variant.Owner(),
		Description:    variant.Description(),
		Provider:       variant.Provider(),
		Created:        variant.Created(),
		Status:         variant.Status().String(),
		LastUpdated:    variant.LastUpdated(),
		Schedule:       variant.Schedule(),
		Tags:           variant.Tags(),
		SourceType:     getSourceType(variant),
		Properties:     variant.Properties(),
		Error:          variant.Error(),
		Specifications: getSourceArgs(variant),
		Inputs:         variant.getInputs(),
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

func (variant *SourceVariant) Status() scheduling.Status {
	if variant.serialized.GetStatus() != nil {
		return scheduling.Status(variant.serialized.GetStatus().Status)
	}
	return scheduling.CREATED
}

func (variant *SourceVariant) Error() string {
	if variant.serialized.GetStatus() == nil {
		return ""
	}
	return fferr.ToDashboardError(variant.serialized.GetStatus())
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

func (variant *SourceVariant) HasResourceSnowflakeConfig() bool {
	if !variant.IsTransformation() {
		return false
	}
	isSqlTransformation := reflect.TypeOf(variant.serialized.GetTransformation().Type) == reflect.TypeOf(&pb.Transformation_SQLTransformation{})

	if !isSqlTransformation {
		return false
	}

	return variant.serialized.GetTransformation().GetSQLTransformation().GetResourceSnowflakeConfig() != nil
}

func (variant *SourceVariant) ResourceSnowflakeConfig() (*ResourceSnowflakeConfig, error) {
	if !variant.HasResourceSnowflakeConfig() {
		return nil, nil
	}

	return getResourceSnowflakeConfig(variant.serialized.GetTransformation().GetSQLTransformation())
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

func (variant *SourceVariant) IsPrimaryData() bool {
	if !variant.isPrimaryData() {
		return false
	}
	return reflect.TypeOf(variant.serialized.GetPrimaryData()) == reflect.TypeOf(&pb.PrimaryData{})
}

func (variant *SourceVariant) PrimaryDataSQLTableName() string {
	if !variant.IsPrimaryData() {
		return ""
	}
	return variant.serialized.GetPrimaryData().GetTable().GetName()
}

func (variant *SourceVariant) PrimaryDataTimestampColumn() string {
	if !variant.IsPrimaryData() {
		return ""
	}
	return variant.serialized.GetPrimaryData().GetTimestampColumn()
}

func (variant *SourceVariant) GetPrimaryLocation() (pl.Location, error) {
	if !variant.isPrimaryData() {
		fmt.Println("Variant is not primary data, returning returning nil values")
		return nil, nil
	}
	switch pt := variant.serialized.GetPrimaryData().GetLocation().(type) {
	case *pb.PrimaryData_Table:
		return pl.NewSQLLocationWithDBSchemaTable(
			pt.Table.GetDatabase(),
			pt.Table.GetSchema(),
			pt.Table.GetName(),
		), nil
	case *pb.PrimaryData_Filestore:
		fp := filestore.FilePath{}
		if err := fp.ParseFilePath(pt.Filestore.GetPath()); err != nil {
			return nil, err
		}
		return pl.NewFileLocation(&fp), nil
	case *pb.PrimaryData_Catalog:
		return pl.NewCatalogLocation(pt.Catalog.GetDatabase(), pt.Catalog.GetTable(), pt.Catalog.GetTableFormat()), nil
	default:
		fmt.Printf("Default case. Unknown primary data type: %v\n", reflect.TypeOf(pt))
		return nil, nil
	}
}

func (variant *SourceVariant) GetTransformationLocation() (pl.Location, error) {
	if !variant.IsTransformation() {
		return nil, nil
	}
	switch pt := variant.serialized.GetTransformation().GetLocation().(type) {
	case *pb.Transformation_Table:
		table := pt.Table.GetName()
		return pl.NewSQLLocationWithDBSchemaTable(pt.Table.GetDatabase(), pt.Table.GetSchema(), table), nil
		//return pl.NewSQLLocation(table), nil
	case *pb.Transformation_Filestore:
		fp := filestore.FilePath{}
		if err := fp.ParseDirPath(pt.Filestore.GetPath()); err != nil {
			return nil, err
		}
		return pl.NewFileLocation(&fp), nil
	case *pb.Transformation_Catalog:
		return pl.NewCatalogLocation(pt.Catalog.GetDatabase(), pt.Catalog.GetTable(), pt.Catalog.GetTableFormat()), nil
	default:
		return nil, fferr.NewInternalErrorf("Unknown SQLTransformation location type %v", pt)
	}
}

func (variant *SourceVariant) SparkFlags() pc.SparkFlags {
	if !variant.IsTransformation() {
		return pc.SparkFlags{}
	}

	sparkFlagsProto := variant.serialized.GetTransformation().GetSparkFlags()

	sparkParams := make(map[string]string)
	for _, param := range sparkFlagsProto.GetSparkParams() {
		sparkParams[param.Key] = param.Value
	}

	writeOptions := make(map[string]string)
	for _, option := range sparkFlagsProto.GetWriteOptions() {
		writeOptions[option.Key] = option.Value
	}

	tableProperties := make(map[string]string)
	for _, prop := range sparkFlagsProto.GetTableProperties() {
		tableProperties[prop.Key] = prop.Value
	}

	return pc.SparkFlags{
		SparkParams:     sparkParams,
		WriteOptions:    writeOptions,
		TableProperties: tableProperties,
	}
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

func (entity Entity) Variant() string {
	return ""
}

func WrapProtoEntity(serialized *pb.Entity) *Entity {
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

func (entity *Entity) ToShallowMap() EntityResource {
	return EntityResource{
		Name:        entity.Name(),
		Type:        "Entity",
		Description: entity.Description(),
		Status:      entity.Status().String(),
		Tags:        entity.Tags(),
		Properties:  entity.Properties(),
	}
}

func (entity *Entity) Name() string {
	return entity.serialized.GetName()
}

func (entity *Entity) Description() string {
	return entity.serialized.GetDescription()
}

func (entity *Entity) Status() scheduling.Status {
	if entity.serialized.GetStatus() != nil {
		return scheduling.Status(entity.serialized.GetStatus().Status)
	}
	return scheduling.CREATED
}

func (entity *Entity) Error() string {
	if entity.serialized.GetStatus() == nil {
		return ""
	}
	return fferr.ToDashboardError(entity.serialized.GetStatus())
}

func (entity *Entity) Tags() Tags {
	return entity.fetchTagsFn.Tags()
}

func (entity *Entity) Properties() Properties {
	return entity.fetchPropertiesFn.Properties()
}

func NewClient(host string, logger logging.Logger) (*Client, error) {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		// grpc.WithUnaryInterceptor(fferr.UnaryClientInterceptor()),
		// grpc.WithStreamInterceptor(fferr.StreamClientInterceptor()),
	}
	conn, err := grpc.Dial(host, opts...)
	if err != nil {
		return nil, fferr.NewInternalError(err)
	}
	metadataClient := pb.NewMetadataClient(conn)
	tasksClient := sch.NewTasksClient(conn)
	slackNotifier := help.NewSlackNotifier(os.Getenv("SLACK_CHANNEL_ID"), logger)
	return &Client{
		Logger:   logger,
		conn:     conn,
		GrpcConn: metadataClient,
		Tasks: &Tasks{
			logger:   logger,
			conn:     conn,
			GrpcConn: tasksClient,
		},
		slackNotifier: slackNotifier,
	}, nil
}

func (client *Client) GetResourceDAG(ctx context.Context, resource Resource) (ResourceDAG, error) {
	// TODO: Create the lookup object for the resource
	dag, err := NewResourceDAG(ctx, nil, resource)
	if err != nil {
		return ResourceDAG{}, err
	}

	return dag, nil
}

func (client *Client) Close() {
	if err := client.conn.Close(); err != nil {
		client.Logger.Errorw("Failed to close connection", "error", err)
	}
}

type resourceSnowflakeConfigGetter interface {
	GetResourceSnowflakeConfig() *pb.ResourceSnowflakeConfig
}

func getResourceSnowflakeConfig(getter resourceSnowflakeConfigGetter) (*ResourceSnowflakeConfig, error) {
	resConfig := &ResourceSnowflakeConfig{
		DynamicTableConfig: &SnowflakeDynamicTableConfig{},
	}
	config := getter.GetResourceSnowflakeConfig()
	if config == nil {
		return resConfig, nil
	}

	if config.GetDynamicTableConfig() != nil {
		refreshMode, refreshModeErr := RefreshModeFromProto(config.DynamicTableConfig.GetRefreshMode())
		if refreshModeErr != nil {
			return nil, refreshModeErr
		}

		initialize, initializeErr := InitializeFromProto(config.DynamicTableConfig.GetInitialize())
		if initializeErr != nil {
			return nil, initializeErr
		}

		resConfig.DynamicTableConfig.RefreshMode = refreshMode
		resConfig.DynamicTableConfig.Initialize = initialize
		resConfig.DynamicTableConfig.TargetLag = config.DynamicTableConfig.GetTargetLag()
	}

	if config.GetWarehouse() != "" {
		resConfig.Warehouse = config.GetWarehouse()
	}

	return resConfig, nil
}
