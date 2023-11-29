// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package serving

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"sync"

	filestore "github.com/featureform/filestore"
	"github.com/featureform/metadata"
	"github.com/featureform/metrics"
	pb "github.com/featureform/proto"
	"github.com/featureform/provider"
	pt "github.com/featureform/provider/provider_type"

	"go.uber.org/zap"
)

type FeatureServer struct {
	pb.UnimplementedFeatureServer
	Metrics   metrics.MetricsHandler
	Metadata  *metadata.Client
	Logger    *zap.SugaredLogger
	Providers *sync.Map
	Tables    *sync.Map
	Features  *sync.Map
}

func NewFeatureServer(meta *metadata.Client, promMetrics metrics.MetricsHandler, logger *zap.SugaredLogger) (*FeatureServer, error) {
	logger.Debug("Creating new training data server")
	return &FeatureServer{
		Metadata:  meta,
		Metrics:   promMetrics,
		Logger:    logger,
		Providers: &sync.Map{},
		Tables:    &sync.Map{},
		Features:  &sync.Map{},
	}, nil
}

func (serv *FeatureServer) TrainingData(req *pb.TrainingDataRequest, stream pb.Feature_TrainingDataServer) error {
	id := req.GetId()
	name, variant := id.GetName(), id.GetVersion()
	featureObserver := serv.Metrics.BeginObservingTrainingServe(name, variant)
	defer featureObserver.Finish()
	logger := serv.Logger.With("Name", name, "Variant", variant)
	logger.Info("Serving training data")
	if model := req.GetModel(); model != nil {
		trainingSets := []metadata.NameVariant{{Name: name, Variant: variant}}
		err := serv.Metadata.CreateModel(stream.Context(), metadata.ModelDef{Name: model.GetName(), Trainingsets: trainingSets})
		if err != nil {
			return err
		}
	}
	iter, err := serv.getTrainingSetIterator(name, variant)
	if err != nil {
		logger.Errorw("Failed to get training set iterator", "Error", err)
		featureObserver.SetError()
		return err
	}
	for iter.Next() {
		sRow, err := serializedRow(iter.Features(), iter.Label())
		if err != nil {
			return err
		}
		if err := stream.Send(sRow); err != nil {
			logger.Errorw("Failed to write to stream", "Error", err)
			featureObserver.SetError()
			return err
		}
		featureObserver.ServeRow()
	}
	if err := iter.Err(); err != nil {
		logger.Errorw("Dataset error", "Error", err)
		featureObserver.SetError()
		return err
	}
	return nil
}

func (serv *FeatureServer) TrainingDataColumns(ctx context.Context, req *pb.TrainingDataColumnsRequest) (*pb.TrainingColumns, error) {
	id := req.GetId()
	name, variant := id.GetName(), id.GetVersion()
	serv.Logger.Infow("Getting training set columns", "Name", name, "Variant", variant)
	ts, err := serv.Metadata.GetTrainingSetVariant(ctx, metadata.NameVariant{Name: name, Variant: variant})
	if err != nil {
		return nil, errors.Wrap(err, "could not get training set variant")
	}
	fv := ts.Features()
	features := make([]string, len(fv))
	for i, f := range fv {
		features[i] = fmt.Sprintf("feature__%s__%s", f.Name, f.Variant)
	}
	lv := ts.Label()
	label := fmt.Sprintf("label__%s__%s", lv.Name, lv.Variant)
	return &pb.TrainingColumns{
		Features: features,
		Label:    label,
	}, nil
}

func (serv *FeatureServer) SourceData(req *pb.SourceDataRequest, stream pb.Feature_SourceDataServer) error {
	id := req.GetId()
	name, variant := id.GetName(), id.GetVersion()
	limit := req.GetLimit()
	logger := serv.Logger.With("Name", name, "Variant", variant)
	logger.Info("Serving source data")
	iter, err := serv.getSourceDataIterator(name, variant, limit)
	if err != nil {
		logger.Errorw("Failed to get source data iterator", "Error", err)
		return err
	}
	for iter.Next() {
		sRow, err := SerializedSourceRow(iter.Values())
		if err != nil {
			return err
		}
		if err := stream.Send(sRow); err != nil {
			logger.Errorw("Failed to write to source data stream", "Error", err)
			return err
		}
	}
	if err := iter.Err(); err != nil {
		logger.Errorw("Source data set error", "Error", err)
		return err
	}
	return nil
}

func (serv *FeatureServer) getTrainingSetIterator(name, variant string) (provider.TrainingSetIterator, error) {
	ctx := context.TODO()
	serv.Logger.Infow("Getting Training Set Iterator", "name", name, "variant", variant)
	ts, err := serv.Metadata.GetTrainingSetVariant(ctx, metadata.NameVariant{name, variant})
	if err != nil {
		return nil, errors.Wrap(err, "could not get training set variant")
	}
	serv.Logger.Debugw("Fetching Training Set Provider", "name", name, "variant", variant)
	providerEntry, err := ts.FetchProvider(serv.Metadata, ctx)
	if err != nil {
		return nil, errors.Wrap(err, "could not get fetch provider")
	}
	p, err := provider.Get(pt.Type(providerEntry.Type()), providerEntry.SerializedConfig())
	if err != nil {
		return nil, errors.Wrap(err, "could not get provider")
	}
	store, err := p.AsOfflineStore()
	if err != nil {
		// This means that the provider of the training set isn't an offline store.
		// That shouldn't be possible.
		return nil, errors.Wrap(err, "could not open as offline store")
	}
	serv.Logger.Debugw("Get Training Set From Store", "name", name, "variant", variant)
	return store.GetTrainingSet(provider.ResourceID{Name: name, Variant: variant})
}

func (serv *FeatureServer) getBatchFeatureIterator(ids []provider.ResourceID) (provider.BatchFeatureIterator, error) {
	ctx := context.TODO()
	_, err := serv.checkEntityOfFeature(ids)
	if err != nil {
		return nil, err
	}

	// Assuming that all the features have the same offline provider
	feat, err := serv.Metadata.GetFeatureVariant(ctx, metadata.NameVariant{ids[0].Name, ids[0].Variant})
	if err != nil {
		return nil, errors.Wrap(err, "could not get Feature Variant")
	}
	serv.Logger.Debugw("Fetching Feature Provider from ", "name", ids[0].Name, "variant", ids[0].Variant)
	featureSource, err := feat.FetchSource(serv.Metadata, ctx)
	if err != nil {
		return nil, errors.Wrap(err, "could not fetch source")
	}
	providerEntry, err := featureSource.FetchProvider(serv.Metadata, ctx)
	if err != nil {
		return nil, errors.Wrap(err, "could not fetch provider")
	}
	providerName := providerEntry.Name()
	err = serv.checkFeatureSources(providerName, ids, ctx)
	if err != nil {
		return nil, err
	}

	p, err := provider.Get(pt.Type(providerEntry.Type()), providerEntry.SerializedConfig())
	if err != nil {
		return nil, errors.Wrap(err, "could not get provider")
	}

	store, err := p.AsOfflineStore()
	if err != nil {
		// This means that the provider of the feature isn't an offline store.
		// That shouldn't be possible.
		return nil, errors.Wrap(err, "could not open as offline store")
	}
	return store.GetBatchFeatures(ids)
}

func (serv *FeatureServer) checkFeatureSources(firstProvider string, ids []provider.ResourceID, ctx context.Context) error {
	for id := range ids {
		id_feature, err := serv.Metadata.GetFeatureVariant(ctx, metadata.NameVariant{ids[id].Name, ids[id].Variant})
		if err != nil {
			return errors.Wrap(err, "could not get Feature Variant")
		}
		id_featureSource, err := id_feature.FetchSource(serv.Metadata, ctx)
		if err != nil {
			return errors.Wrap(err, "could not fetch source")
		}
		id_providerEntry, err := id_featureSource.FetchProvider(serv.Metadata, ctx)
		if err != nil {
			return errors.Wrap(err, "could not fetch provider")
		}
		if id_providerEntry.Name() != firstProvider {
			return errors.Wrap(err, "features have different providers")
		}
	}
	return nil
}

// Takes in a list of feature names and returns true if they all have the same entity name
func (serv *FeatureServer) checkEntityOfFeature(ids []provider.ResourceID) (bool, error) {
	ctx := context.TODO()
	entityName := ""
	// if len(features) != len(variants) {
	// 	return false, fmt.Errorf("Feature and Variant lists have different lengths")
	// }
	for _, resourceID := range ids {
		serv.Logger.Infow("Getting Feature Variant Iterator", "name", resourceID.Name, "variant", resourceID.Variant)
		feature, err := serv.Metadata.GetFeatureVariant(ctx, metadata.NameVariant{Name: resourceID.Name, Variant: resourceID.Variant})
		if err != nil {
			return false, errors.Wrap(err, "could not get feature variant")
		}
		correspondingEntity := feature.Entity()
		if entityName == "" {
			entityName = correspondingEntity
		} else if correspondingEntity != entityName {
			return false, fmt.Errorf("Entity names are not the same")
		}
	}
	return true, nil
}

func (serv *FeatureServer) getSourceDataIterator(name, variant string, limit int64) (provider.GenericTableIterator, error) {
	ctx := context.TODO()
	serv.Logger.Infow("Getting Source Variant Iterator", "name", name, "variant", variant)
	sv, err := serv.Metadata.GetSourceVariant(ctx, metadata.NameVariant{Name: name, Variant: variant})
	if err != nil {
		return nil, errors.Wrap(err, "could not get source variant")
	}
	// TODO: Determine if we want to add a backoff here to wait for the source
	if sv.Status() != metadata.READY {
		return nil, fmt.Errorf("source variant is not ready; current status is %v", sv.Status())
	}
	providerEntry, err := sv.FetchProvider(serv.Metadata, ctx)
	serv.Logger.Debugw("Fetched Source Variant Provider", "name", providerEntry.Name(), "type", providerEntry.Type())
	if err != nil {
		return nil, errors.Wrap(err, "could not get fetch provider")
	}
	p, err := provider.Get(pt.Type(providerEntry.Type()), providerEntry.SerializedConfig())
	if err != nil {
		return nil, errors.Wrap(err, "could not get provider")
	}
	store, err := p.AsOfflineStore()
	if err != nil {
		return nil, errors.Wrap(err, "could not open as offline store")
	}
	var primary provider.PrimaryTable
	var providerErr error
	if sv.IsTransformation() {
		serv.Logger.Debugw("Getting transformation table", "name", name, "variant", variant)
		t, err := store.GetTransformationTable(provider.ResourceID{Name: name, Variant: variant, Type: provider.Transformation})
		if err != nil {
			serv.Logger.Errorw("Could not get transformation table", "name", name, "variant", variant, "Error", err)
			providerErr = err
		} else {
			// TransformationTable inherits from PrimaryTable, which is where
			// IterateSegment is defined; we assert this type to get access to
			// the method. This assertion should never fail.
			if tbl, isPrimaryTable := t.(provider.PrimaryTable); !isPrimaryTable {
				serv.Logger.Errorw("transformation table is not a primary table", "name", name, "variant", variant)
				providerErr = fmt.Errorf("transformation table is not a primary table")
			} else {
				primary = tbl
			}
		}
	} else {
		serv.Logger.Debugw("Getting primary table", "name", name, "variant", variant)
		primary, providerErr = store.GetPrimaryTable(provider.ResourceID{Name: name, Variant: variant, Type: provider.Primary})
	}
	if providerErr != nil {
		serv.Logger.Errorw("Could not get primary table", "name", name, "variant", variant, "Error", providerErr)
		return nil, errors.Wrap(err, "could not get primary table")
	}
	serv.Logger.Debugw("Getting source data iterator", "name", name, "variant", variant)
	return primary.IterateSegment(limit)
}

func (serv *FeatureServer) addModel(ctx context.Context, model *pb.Model, features []*pb.FeatureID) error {
	modelFeatures := make([]metadata.NameVariant, len(features))
	for i, feature := range features {
		modelFeatures[i] = metadata.NameVariant{Name: feature.Name, Variant: feature.Version}
	}
	serv.Logger.Infow("Creating model", "Name", model.GetName())
	err := serv.Metadata.CreateModel(ctx, metadata.ModelDef{Name: model.GetName(), Features: modelFeatures})
	if err != nil {
		return err
	}
	return nil
}

type observer struct{}

// TODO: test serving embedding features
func (serv *FeatureServer) FeatureServe(ctx context.Context, req *pb.FeatureServeRequest) (*pb.FeatureRow, error) {
	features := req.GetFeatures()
	entities := req.GetEntities()
	entityMap := make(map[string][]string)

	for _, entity := range entities {
		entityMap[entity.GetName()] = entity.GetValues()
	}

	if model := req.GetModel(); model != nil {
		err := serv.addModel(ctx, model, features)
		if err != nil {
			return nil, err
		}
	}

	rows, err := serv.getFeatureRows(ctx, features, entityMap)
	if err != nil {
		return nil, err
	}

	return &pb.FeatureRow{
		ValueLists: rows,
	}, nil
}

func (serv *FeatureServer) getNVCacheKey(name, variant string) string {
	return fmt.Sprintf("%s:%s", name, variant)
}

func (serv *FeatureServer) BatchFeatureServe(req *pb.BatchFeatureServeRequest, stream pb.Feature_BatchFeatureServeServer) error {
	features := req.GetFeatures()
	resourceIDList := make([]provider.ResourceID, len(features))
	for i, feature := range features {
		name, variant := feature.GetName(), feature.GetVersion()
		serv.Logger.Infow("Serving feature", "Name", name, "Variant", variant)
		resourceIDList[i] = provider.ResourceID{Name: name, Variant: variant, Type: provider.Feature}
	}
	iter, err := serv.getBatchFeatureIterator(resourceIDList)
	if err != nil {
		return err
	}
	for iter.Next() {
		sRow, err := serializedBatchRow(iter.Entity(), iter.Features())
		if err != nil {
			return err
		}
		if err := stream.Send(sRow); err != nil {
			return err
		}
	}
	if err := iter.Err(); err != nil {
		return err
	}
	return nil
}

func (serv *FeatureServer) SourceColumns(ctx context.Context, req *pb.SourceColumnRequest) (*pb.SourceDataColumns, error) {
	id := req.GetId()
	name, variant := id.GetName(), id.GetVersion()
	serv.Logger.Infow("Getting source columns", "Name", name, "Variant", variant)
	it, err := serv.getSourceDataIterator(name, variant, 0) // Set limit to zero to fetch columns only
	if err != nil {
		return nil, err
	}
	if it == nil {
		serv.Logger.Errorf("source data iterator is nil", "Name", name, "Variant", variant, "Error", err.Error())
		return nil, fmt.Errorf("could not fetch source data due to error; check the data source registration to ensure it is valid")
	}
	defer it.Close()
	return &pb.SourceDataColumns{
		Columns: it.Columns(),
	}, nil
}

func (serv *FeatureServer) Nearest(ctx context.Context, req *pb.NearestRequest) (*pb.NearestResponse, error) {
	id := req.GetId()
	name, variant := id.GetName(), id.GetVersion()
	serv.Logger.Infow("Searching nearest", "Name", name, "Variant", variant)
	fv, err := serv.Metadata.GetFeatureVariant(ctx, metadata.NameVariant{Name: name, Variant: variant})
	if err != nil {
		serv.Logger.Errorw("metadata lookup failed", "Err", err)
		return nil, err
	}
	vectorTable, err := serv.getVectorTable(ctx, fv)
	if err != nil {
		serv.Logger.Errorw("failed to get vector table", "Error", err)
		return nil, err
	}
	searchVector := req.GetVector()
	k := req.GetK()
	if searchVector == nil {
		return nil, fmt.Errorf("no embedding provided")
	}
	entities, err := vectorTable.Nearest(name, variant, searchVector.Value, k)
	if err != nil {
		serv.Logger.Errorw("nearest search failed", "Error", err)
		return nil, err
	}
	return &pb.NearestResponse{
		Entities: entities,
	}, nil
}

func (serv *FeatureServer) getVectorTable(ctx context.Context, fv *metadata.FeatureVariant) (provider.VectorStoreTable, error) {
	providerEntry, err := fv.FetchProvider(serv.Metadata, ctx)
	if err != nil {
		serv.Logger.Errorw("fetching provider metadata failed", "Error", err)
		return nil, err
	}
	p, err := provider.Get(pt.Type(providerEntry.Type()), providerEntry.SerializedConfig())
	if err != nil {
		serv.Logger.Errorw("failed to get provider", "Error", err)
		return nil, err
	}
	store, err := p.AsOnlineStore()
	if err != nil {
		serv.Logger.Errorw("failed to use provider as online store for feature", "Error", err)
		return nil, err
	}
	table, err := store.GetTable(fv.Name(), fv.Variant())
	if err != nil {
		serv.Logger.Errorw("feature not found", "Error", err)
		return nil, err
	}
	vectorTable, ok := table.(provider.VectorStoreTable)
	if !ok {
		serv.Logger.Errorw("failed to use table as vector store table", "Error", err)
	}
	return vectorTable, nil
}

func (serv *FeatureServer) ResourceLocation(ctx context.Context, req *pb.TrainingDataRequest) (*pb.ResourceFileLocation, error) {
	// TODO: Modify this method to return the location of any resource within Featureform
	// - This will require a change in the input to be a ResourceID (name, variant, and type)
	// - Modifications to the Offline Store interface to pull the latest file location of a resource

	id := req.GetId()
	name, variant := id.GetName(), id.GetVersion()
	serv.Logger.Infow("Getting the Resource Location:", "Name", name, "Variant", variant)

	tv, err := serv.Metadata.GetTrainingSetVariant(ctx, metadata.NameVariant{Name: name, Variant: variant})
	if err != nil {
		return nil, errors.Wrap(err, "could not get training set variant")
	}

	// There might be an edge case where you have successfully ran a previous job and currently, you run a new job
	// the status would be PENDING
	if tv.Status() != metadata.READY {
		return nil, fmt.Errorf("training set variant is not ready; current status is %v", tv.Status())
	}

	providerEntry, err := tv.FetchProvider(serv.Metadata, ctx)
	serv.Logger.Debugw("Fetched Source Variant Provider", "name", providerEntry.Name(), "type", providerEntry.Type())
	if err != nil {
		return nil, errors.Wrap(err, "could not get fetch provider")
	}
	p, err := provider.Get(pt.Type(providerEntry.Type()), providerEntry.SerializedConfig())
	if err != nil {
		return nil, errors.Wrap(err, "could not get provider")
	}
	store, err := p.AsOfflineStore()
	if err != nil {
		return nil, errors.Wrap(err, "could not open as offline store")
	}

	spark, ok := store.(*provider.SparkOfflineStore)
	if !ok {
		return nil, errors.Wrap(err, "could not cast to spark store")
	}
	resourceID := provider.ResourceID{Name: name, Variant: variant, Type: provider.TrainingSet}

	path, err := spark.Store.CreateDirPath(resourceID.ToFilestorePath())
	if err != nil {
		return nil, errors.Wrap(err, "could not create dir path")
	}

	serv.Logger.Debugw("Getting resource location", "name", name, "variant", variant)
	newestFile, err := spark.Store.NewestFileOfType(path, filestore.Parquet)
	if err != nil {
		return nil, errors.Wrap(err, "could not get newest file")
	}

	return &pb.ResourceFileLocation{
		Location: newestFile.ToURI(),
	}, nil
}
