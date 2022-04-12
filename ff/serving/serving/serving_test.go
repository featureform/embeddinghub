package serving

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"

	"github.com/featureform/serving/metadata"
	"github.com/featureform/serving/metrics"
	pb "github.com/featureform/serving/proto"
	"github.com/featureform/serving/provider"
	grpcmeta "google.golang.org/grpc/metadata"
)

const metadataAddr string = ":8989"

var serv *metadata.MetadataServer

func simpleFeatureRecords() map[provider.ResourceID][]provider.ResourceRecord {
	featureId := provider.ResourceID{
		Name:    "feature",
		Variant: "variant",
		Type:    provider.Feature,
	}
	featureRecs := []provider.ResourceRecord{
		{Entity: "a", Value: 12.5},
		{Entity: "b", Value: "def"},
	}
	labelId := provider.ResourceID{
		Name:    "label",
		Variant: "variant",
		Type:    provider.Label,
	}
	labelRecs := []provider.ResourceRecord{
		{Entity: "a", Value: true},
		{Entity: "b", Value: false},
	}
	return map[provider.ResourceID][]provider.ResourceRecord{
		featureId: featureRecs,
		labelId:   labelRecs,
	}
}

func invalidTypeFeatureRecords() map[provider.ResourceID][]provider.ResourceRecord {
	id := provider.ResourceID{
		Name:    "feature",
		Variant: "variant",
		Type:    provider.Feature,
	}
	recs := []provider.ResourceRecord{
		{Entity: "a", Value: make([]string, 0)},
	}
	return map[provider.ResourceID][]provider.ResourceRecord{
		id: recs,
	}
}

func allTypesFeatureRecords() map[provider.ResourceID][]provider.ResourceRecord {
	idToVal := map[provider.ResourceID]interface{}{
		provider.ResourceID{
			Name:    "feature",
			Variant: "double",
		}: 12.5,
		provider.ResourceID{
			Name:    "feature",
			Variant: "float",
		}: float32(2.3),
		provider.ResourceID{
			Name:    "feature",
			Variant: "str",
		}: "abc",
		provider.ResourceID{
			Name:    "feature",
			Variant: "int",
		}: 5,
		provider.ResourceID{
			Name:    "feature",
			Variant: "smallint",
		}: int32(4),
		provider.ResourceID{
			Name:    "feature",
			Variant: "bigint",
		}: int64(3),
		provider.ResourceID{
			Name:    "feature",
			Variant: "bool",
		}: true,
		provider.ResourceID{
			Name:    "feature",
			Variant: "proto",
		}: &pb.Value{
			Value: &pb.Value_StrValue{"proto"},
		},
	}
	recs := make(map[provider.ResourceID][]provider.ResourceRecord)
	for id, val := range idToVal {
		id.Type = provider.Feature
		recs[id] = []provider.ResourceRecord{
			{Entity: "a", Value: val},
		}
	}
	return recs
}

func allTypesResourceDefsFn(providerType string) []metadata.ResourceDef {
	return []metadata.ResourceDef{
		metadata.UserDef{
			Name: "Featureform",
		},
		metadata.ProviderDef{
			Name: "mockOnline",
			Type: providerType,
		},
		metadata.EntityDef{
			Name: "mockEntity",
		},
		metadata.SourceDef{
			Name:     "mockSource",
			Variant:  "var",
			Owner:    "Featureform",
			Provider: "mockOnline",
		},
		metadata.FeatureDef{
			Name:     "feature",
			Variant:  "double",
			Provider: "mockOnline",
			Entity:   "mockEntity",
			Source:   metadata.NameVariant{"mockSource", "var"},
			Owner:    "Featureform",
		},
		metadata.FeatureDef{
			Name:     "feature",
			Variant:  "float",
			Provider: "mockOnline",
			Entity:   "mockEntity",
			Source:   metadata.NameVariant{"mockSource", "var"},
			Owner:    "Featureform",
		},
		metadata.FeatureDef{
			Name:     "feature",
			Variant:  "str",
			Provider: "mockOnline",
			Entity:   "mockEntity",
			Source:   metadata.NameVariant{"mockSource", "var"},
			Owner:    "Featureform",
		},
		metadata.FeatureDef{
			Name:     "feature",
			Variant:  "int",
			Provider: "mockOnline",
			Entity:   "mockEntity",
			Source:   metadata.NameVariant{"mockSource", "var"},
			Owner:    "Featureform",
		},
		metadata.FeatureDef{
			Name:     "feature",
			Variant:  "smallint",
			Provider: "mockOnline",
			Entity:   "mockEntity",
			Source:   metadata.NameVariant{"mockSource", "var"},
			Owner:    "Featureform",
		},
		metadata.FeatureDef{
			Name:     "feature",
			Variant:  "bigint",
			Provider: "mockOnline",
			Entity:   "mockEntity",
			Source:   metadata.NameVariant{"mockSource", "var"},
			Owner:    "Featureform",
		},
		metadata.FeatureDef{
			Name:     "feature",
			Variant:  "bool",
			Provider: "mockOnline",
			Entity:   "mockEntity",
			Source:   metadata.NameVariant{"mockSource", "var"},
			Owner:    "Featureform",
		},
		metadata.FeatureDef{
			Name:     "feature",
			Variant:  "proto",
			Provider: "mockOnline",
			Entity:   "mockEntity",
			Source:   metadata.NameVariant{"mockSource", "var"},
			Owner:    "Featureform",
		},
	}
}

func simpleResourceDefsFn(providerType string) []metadata.ResourceDef {
	return []metadata.ResourceDef{
		metadata.UserDef{
			Name: "Featureform",
		},
		metadata.ProviderDef{
			Name: "mockOnline",
			Type: providerType,
		},
		metadata.EntityDef{
			Name: "mockEntity",
		},
		metadata.SourceDef{
			Name:     "mockSource",
			Variant:  "var",
			Owner:    "Featureform",
			Provider: "mockOnline",
		},
		metadata.FeatureDef{
			Name:     "feature",
			Variant:  "variant",
			Provider: "mockOnline",
			Entity:   "mockEntity",
			Source:   metadata.NameVariant{"mockSource", "var"},
			Owner:    "Featureform",
		},
		metadata.LabelDef{
			Name:     "label",
			Variant:  "variant",
			Provider: "mockOnline",
			Entity:   "mockEntity",
			Source:   metadata.NameVariant{"mockSource", "var"},
			Owner:    "Featureform",
		},
		metadata.TrainingSetDef{
			Name:     "training-set",
			Variant:  "variant",
			Provider: "mockOnline",
			Label:    metadata.NameVariant{"label", "variant"},
			Features: metadata.NameVariants{{"feature", "variant"}},
			Owner:    "Featureform",
		},
	}
}

func simpleTrainingSetDefs() []provider.TrainingSetDef {
	return []provider.TrainingSetDef{
		{
			ID: provider.ResourceID{
				Name:    "training-set",
				Variant: "variant",
			},
			Label: provider.ResourceID{
				Name:    "label",
				Variant: "variant",
			},
			Features: []provider.ResourceID{
				{
					Name:    "feature",
					Variant: "variant",
				},
			},
		},
	}
}

type resourceDefsFn func(providerType string) []metadata.ResourceDef

type onlineTestContext struct {
	ResourceDefsFn resourceDefsFn
	FactoryFn      provider.Factory
}

func (ctx onlineTestContext) Create(t *testing.T) *FeatureServer {
	startMetadata()
	time.Sleep(time.Second)
	providerType := uuid.NewString()
	if ctx.FactoryFn != nil {
		if err := provider.RegisterFactory(provider.Type(providerType), ctx.FactoryFn); err != nil {
			t.Fatalf("Failed to register factory: %s", err)
		}
	}
	meta := metadataClient(t)
	if ctx.ResourceDefsFn != nil {
		defs := ctx.ResourceDefsFn(providerType)
		if err := meta.CreateAll(context.Background(), defs); err != nil {
			t.Fatalf("Failed to create metdata entries: %s", err)
		}
	}
	logger := zaptest.NewLogger(t).Sugar()
	serv, err := NewFeatureServer(meta, metrics.NewMetrics(randomMetricsId()), logger)
	if err != nil {
		t.Fatalf("Failed to create feature server: %s", err)
	}
	return serv
}

func (ctx onlineTestContext) Destroy() {
	stopMetadata()
}

// Metrics can't have numbers in it, so we can't just use a UUID.
func randomMetricsId() string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	id := make([]rune, 24)
	for i := range id {
		id[i] = letters[rand.Intn(len(letters))]
	}
	return string(id)
}

func createMockOnlineStoreFactory(recsMap map[provider.ResourceID][]provider.ResourceRecord) provider.Factory {
	return func(cfg provider.SerializedConfig) (provider.Provider, error) {
		store := provider.NewLocalOnlineStore()
		for id, recs := range recsMap {
			if id.Type != provider.Feature {
				continue
			}
			table, err := store.CreateTable(id.Name, id.Variant)
			if err != nil {
				panic(err)
			}
			for _, rec := range recs {
				if err := table.Set(rec.Entity, rec.Value); err != nil {
					panic(err)
				}
			}
		}
		return store, nil
	}
}

func createMockOfflineStoreFactory(recsMap map[provider.ResourceID][]provider.ResourceRecord, defs []provider.TrainingSetDef) provider.Factory {
	return func(cfg provider.SerializedConfig) (provider.Provider, error) {
		store := provider.NewMemoryOfflineStore()
		for id, recs := range recsMap {
			table, err := store.CreateResourceTable(id, nil)
			if err != nil {
				panic(err)
			}
			for _, rec := range recs {
				if err := table.Write(rec); err != nil {
					panic(err)
				}
			}
		}
		for _, def := range defs {
			if err := store.CreateTrainingSet(def); err != nil {
				panic(err)
			}
		}
		return store, nil
	}
}

func onlineStoreNoTables(cfg provider.SerializedConfig) (provider.Provider, error) {
	store := provider.NewLocalOnlineStore()
	return store, nil
}

func startMetadata() {
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	config := &metadata.Config{
		Logger:          logger.Sugar(),
		Address:         metadataAddr,
		StorageProvider: metadata.LocalStorageProvider{},
	}
	serv, err = metadata.NewMetadataServer(config)
	if err != nil {
		panic(err)
	}
	go func() {
		if err := serv.Serve(); err != nil {
			panic(err)
		}
	}()
}

func stopMetadata() {
	serv.Stop()
}

func metadataClient(t *testing.T) *metadata.Client {
	logger := zaptest.NewLogger(t).Sugar()
	client, err := metadata.NewClient(metadataAddr, logger)
	if err != nil {
		t.Fatalf("Failed to create client: %s", err)
	}
	return client
}

func unwrapVal(val *pb.Value) interface{} {
	switch casted := val.Value.(type) {
	case *pb.Value_DoubleValue:
		return casted.DoubleValue
	case *pb.Value_FloatValue:
		return casted.FloatValue
	case *pb.Value_StrValue:
		return casted.StrValue
	case *pb.Value_IntValue:
		return int(casted.IntValue)
	case *pb.Value_Int32Value:
		return casted.Int32Value
	case *pb.Value_Int64Value:
		return casted.Int64Value
	case *pb.Value_BoolValue:
		return casted.BoolValue
	default:
		panic(fmt.Sprintf("Unable to unwrap value: %T", val.Value))
	}
}

func TestFeatureServe(t *testing.T) {
	ctx := onlineTestContext{
		ResourceDefsFn: simpleResourceDefsFn,
		FactoryFn:      createMockOnlineStoreFactory(simpleFeatureRecords()),
	}
	serv := ctx.Create(t)
	defer ctx.Destroy()
	req := &pb.FeatureServeRequest{
		Features: []*pb.FeatureID{
			&pb.FeatureID{
				Name:    "feature",
				Version: "variant",
			},
		},
		Entities: []*pb.Entity{
			&pb.Entity{
				Name:  "mockEntity",
				Value: "a",
			},
		},
	}
	resp, err := serv.FeatureServe(context.Background(), req)
	if err != nil {
		t.Fatalf("Failed to serve feature: %s", err)
	}
	vals := resp.Values
	if len(vals) != len(req.Features) {
		t.Fatalf("Wrong number of values: %d\nExpcted: %d", len(vals), len(req.Features))
	}
	dblVal := unwrapVal(vals[0])
	if dblVal != 12.5 {
		t.Fatalf("Wrong feature value: %v\nExpcted: %v", dblVal, 12.5)
	}
}

func TestFeatureNotFound(t *testing.T) {
	ctx := onlineTestContext{
		ResourceDefsFn: simpleResourceDefsFn,
		FactoryFn:      createMockOnlineStoreFactory(simpleFeatureRecords()),
	}
	serv := ctx.Create(t)
	defer ctx.Destroy()
	req := &pb.FeatureServeRequest{
		Features: []*pb.FeatureID{
			&pb.FeatureID{
				Name:    "nonexistantFeature",
				Version: "variant",
			},
		},
		Entities: []*pb.Entity{
			&pb.Entity{
				Name:  "mockEntity",
				Value: "a",
			},
		},
	}
	if _, err := serv.FeatureServe(context.Background(), req); err == nil {
		t.Fatalf("Succeeded in serving non-existant feature")
	}
}

func TestProviderNotRegistered(t *testing.T) {
	ctx := onlineTestContext{
		ResourceDefsFn: simpleResourceDefsFn,
		FactoryFn:      nil,
	}
	serv := ctx.Create(t)
	defer ctx.Destroy()
	req := &pb.FeatureServeRequest{
		Features: []*pb.FeatureID{
			&pb.FeatureID{
				Name:    "feature",
				Version: "variant",
			},
		},
		Entities: []*pb.Entity{
			&pb.Entity{
				Name:  "mockEntity",
				Value: "a",
			},
		},
	}
	if _, err := serv.FeatureServe(context.Background(), req); err == nil {
		t.Fatalf("Succeeded in serving feature with no registered provider factory")
	}
}

func TestOfflineStoreAsOnlineStore(t *testing.T) {
	ctx := onlineTestContext{
		ResourceDefsFn: simpleResourceDefsFn,
		FactoryFn:      createMockOfflineStoreFactory(simpleFeatureRecords(), nil),
	}
	serv := ctx.Create(t)
	defer ctx.Destroy()
	req := &pb.FeatureServeRequest{
		Features: []*pb.FeatureID{
			&pb.FeatureID{
				Name:    "feature",
				Version: "variant",
			},
		},
		Entities: []*pb.Entity{
			&pb.Entity{
				Name:  "mockEntity",
				Value: "a",
			},
		},
	}
	if _, err := serv.FeatureServe(context.Background(), req); err == nil {
		t.Fatalf("Succeeded in serving feature stored on OfflineStore")
	}
}

func TestTableNotFoundInOnlineStore(t *testing.T) {
	ctx := onlineTestContext{
		ResourceDefsFn: simpleResourceDefsFn,
		FactoryFn:      onlineStoreNoTables,
	}
	serv := ctx.Create(t)
	defer ctx.Destroy()
	req := &pb.FeatureServeRequest{
		Features: []*pb.FeatureID{
			&pb.FeatureID{
				Name:    "feature",
				Version: "variant",
			},
		},
		Entities: []*pb.Entity{
			&pb.Entity{
				Name:  "mockEntity",
				Value: "a",
			},
		},
	}
	if _, err := serv.FeatureServe(context.Background(), req); err == nil {
		t.Fatalf("Succeeded in serving feature in an online store without a valid table")
	}
}

func TestEntityNotFoundInOnlineStore(t *testing.T) {
	ctx := onlineTestContext{
		ResourceDefsFn: simpleResourceDefsFn,
		FactoryFn:      createMockOnlineStoreFactory(simpleFeatureRecords()),
	}
	serv := ctx.Create(t)
	defer ctx.Destroy()
	req := &pb.FeatureServeRequest{
		Features: []*pb.FeatureID{
			&pb.FeatureID{
				Name:    "feature",
				Version: "variant",
			},
		},
		Entities: []*pb.Entity{
			&pb.Entity{
				Name:  "mockEntity",
				Value: "NonExistantEntity",
			},
		},
	}
	if _, err := serv.FeatureServe(context.Background(), req); err == nil {
		t.Fatalf("Succeeded in serving feature with non-existant entity")
	}
}

func TestEntityNotInRequest(t *testing.T) {
	ctx := onlineTestContext{
		ResourceDefsFn: simpleResourceDefsFn,
		FactoryFn:      createMockOnlineStoreFactory(simpleFeatureRecords()),
	}
	serv := ctx.Create(t)
	defer ctx.Destroy()
	req := &pb.FeatureServeRequest{
		Features: []*pb.FeatureID{
			&pb.FeatureID{
				Name:    "feature",
				Version: "variant",
			},
		},
		Entities: []*pb.Entity{
			&pb.Entity{
				Name:  "wrongEntity",
				Value: "a",
			},
		},
	}
	if _, err := serv.FeatureServe(context.Background(), req); err == nil {
		t.Fatalf("Succeeded in serving feature without the right entity set")
	}
}

func TestInvalidFeatureType(t *testing.T) {
	ctx := onlineTestContext{
		ResourceDefsFn: simpleResourceDefsFn,
		FactoryFn:      createMockOnlineStoreFactory(invalidTypeFeatureRecords()),
	}
	serv := ctx.Create(t)
	defer ctx.Destroy()
	req := &pb.FeatureServeRequest{
		Features: []*pb.FeatureID{
			&pb.FeatureID{
				Name:    "feature",
				Version: "variant",
			},
		},
		Entities: []*pb.Entity{
			&pb.Entity{
				Name:  "mockEntity",
				Value: "a",
			},
		},
	}
	if _, err := serv.FeatureServe(context.Background(), req); err == nil {
		t.Fatalf("Succeeded in serving feature with invalid type")
	}
}

func TestAllFeatureTypes(t *testing.T) {
	ctx := onlineTestContext{
		ResourceDefsFn: allTypesResourceDefsFn,
		FactoryFn:      createMockOnlineStoreFactory(allTypesFeatureRecords()),
	}
	serv := ctx.Create(t)
	defer ctx.Destroy()
	req := &pb.FeatureServeRequest{
		Features: []*pb.FeatureID{
			&pb.FeatureID{
				Name:    "feature",
				Version: "double",
			},
			&pb.FeatureID{
				Name:    "feature",
				Version: "float",
			},
			&pb.FeatureID{
				Name:    "feature",
				Version: "str",
			},
			&pb.FeatureID{
				Name:    "feature",
				Version: "int",
			},
			&pb.FeatureID{
				Name:    "feature",
				Version: "smallint",
			},
			&pb.FeatureID{
				Name:    "feature",
				Version: "bigint",
			},
			&pb.FeatureID{
				Name:    "feature",
				Version: "bool",
			},
			&pb.FeatureID{
				Name:    "feature",
				Version: "proto",
			},
		},
		Entities: []*pb.Entity{
			&pb.Entity{
				Name:  "mockEntity",
				Value: "a",
			},
		},
	}
	resp, err := serv.FeatureServe(context.Background(), req)
	if err != nil {
		t.Fatalf("Failed to get multiple features with all types: %s", err)
	}
	expected := []interface{}{
		12.5, float32(2.3), "abc", 5, int32(4), int64(3), true, "proto",
	}
	vals := resp.Values
	if len(vals) != len(req.Features) {
		t.Fatalf("Wrong number of values: %d\nExpcted: %d", len(vals), len(req.Features))
	}
	for i, exp := range expected {
		if unwrapVal(vals[i]) != exp {
			t.Fatalf("Values not equal %v %v", vals[i], exp)
		}
	}
}

type mockTrainingStream struct {
	RowChan chan *pb.TrainingDataRow
}

func newMockTrainingStream() *mockTrainingStream {
	return &mockTrainingStream{make(chan *pb.TrainingDataRow)}
}

func (stream *mockTrainingStream) Send(row *pb.TrainingDataRow) error {
	stream.RowChan <- row
	return nil
}

func (stream *mockTrainingStream) Context() context.Context {
	return context.Background()
}

func (stream *mockTrainingStream) SetHeader(grpcmeta.MD) error {
	return nil
}

func (stream *mockTrainingStream) SendHeader(grpcmeta.MD) error {
	return nil
}

func (stream *mockTrainingStream) SetTrailer(grpcmeta.MD) {
}

func (stream *mockTrainingStream) SendMsg(interface{}) error {
	return nil
}

func (stream *mockTrainingStream) RecvMsg(interface{}) error {
	return nil
}

func TestSimpleTrainingSetServe(t *testing.T) {
	ctx := onlineTestContext{
		ResourceDefsFn: simpleResourceDefsFn,
		FactoryFn:      createMockOfflineStoreFactory(simpleFeatureRecords(), simpleTrainingSetDefs()),
	}
	serv := ctx.Create(t)
	defer ctx.Destroy()
	req := &pb.TrainingDataRequest{
		Id: &pb.TrainingDataID{
			Name:    "training-set",
			Version: "variant",
		},
	}
	stream := newMockTrainingStream()
	errChan := make(chan error)
	go func() {
		if err := serv.TrainingData(req, stream); err != nil {
			errChan <- err
		}
		close(errChan)
	}()
	select {
	case row := <-stream.RowChan:
		fmt.Println(row)
	case err := <-errChan:
		t.Fatalf("Failed to get training data: %s", err)
	}
}
