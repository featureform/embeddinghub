// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package dashboard_metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/featureform/fferr"
	help "github.com/featureform/helpers"
	"github.com/featureform/helpers/postgres"
	"github.com/featureform/logging"
	"github.com/featureform/metadata"
	pb "github.com/featureform/metadata/proto"
	"github.com/featureform/metadata/search"
	"github.com/featureform/proto"
	"github.com/featureform/provider"
	pt "github.com/featureform/provider/provider_type"
	sc "github.com/featureform/scheduling"
	"github.com/featureform/serving"
	"github.com/featureform/storage"
	"github.com/featureform/storage/query"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

var SearchClient search.Searcher

const (
	Online       = "online"
	Offline      = "offline"
	File         = "file"
	Connected    = "connected"
	Disconnected = "disconnected"
)

const typeListLimit int = 8
const serializedVersion string = "SerializedVersion"
const serializedV1 string = "1"

type StorageProvider interface {
	GetResourceLookup() (metadata.ResourceLookup, error)
}

type LocalStorageProvider struct {
}

func (sp LocalStorageProvider) GetResourceLookup() (metadata.ResourceLookup, error) {
	lookup := make(metadata.LocalResourceLookup)
	return lookup, nil
}

type MetadataServer struct {
	lookup          metadata.ResourceLookup
	client          *metadata.Client
	logger          logging.Logger
	StorageProvider storage.MetadataStorage
}

func NewMetadataServer(logger logging.Logger, client *metadata.Client, storageProvider storage.MetadataStorage) (*MetadataServer, error) {
	logger.Debug("Creating new metadata server")

	return &MetadataServer{
		client:          client,
		logger:          logger,
		StorageProvider: storageProvider,
		lookup:          &metadata.MemoryResourceLookup{Connection: storageProvider},
	}, nil
}

type FeatureResource struct {
	AllVariants    []string                                   `json:"all-variants"`
	Type           string                                     `json:"type"`
	DefaultVariant string                                     `json:"default-variant"`
	Name           string                                     `json:"name"`
	Variants       map[string]metadata.FeatureVariantResource `json:"variants"`
}

type TrainingSetResource struct {
	AllVariants    []string                                       `json:"all-variants"`
	Type           string                                         `json:"type"`
	DefaultVariant string                                         `json:"default-variant"`
	Name           string                                         `json:"name"`
	Variants       map[string]metadata.TrainingSetVariantResource `json:"variants"`
}

type SourceResource struct {
	AllVariants    []string                                  `json:"all-variants"`
	Type           string                                    `json:"type"`
	DefaultVariant string                                    `json:"default-variant"`
	Name           string                                    `json:"name"`
	Variants       map[string]metadata.SourceVariantResource `json:"variants"`
}

type LabelResource struct {
	AllVariants    []string                                 `json:"all-variants"`
	Type           string                                   `json:"type"`
	DefaultVariant string                                   `json:"default-variant"`
	Name           string                                   `json:"name"`
	Variants       map[string]metadata.LabelVariantResource `json:"variants"`
}

type UserResource struct {
	Name         string                                           `json:"name"`
	Type         string                                           `json:"type"`
	Features     map[string][]metadata.FeatureVariantResource     `json:"features"`
	Labels       map[string][]metadata.LabelVariantResource       `json:"labels"`
	TrainingSets map[string][]metadata.TrainingSetVariantResource `json:"training-sets"`
	Sources      map[string][]metadata.SourceVariantResource      `json:"sources"`
	Status       string                                           `json:"status"`
	Tags         metadata.Tags                                    `json:"tags"`
	Properties   metadata.Properties                              `json:"properties"`
}

type ModelResource struct {
	Name         string                                           `json:"name"`
	Type         string                                           `json:"type"`
	Description  string                                           `json:"description"`
	Features     map[string][]metadata.FeatureVariantResource     `json:"features"`
	Labels       map[string][]metadata.LabelVariantResource       `json:"labels"`
	TrainingSets map[string][]metadata.TrainingSetVariantResource `json:"training-sets"`
	Status       string                                           `json:"status"`
	Tags         metadata.Tags                                    `json:"tags"`
	Properties   metadata.Properties                              `json:"properties"`
}

type FetchError struct {
	StatusCode int
	Type       string
}

func (m *FetchError) Error() string {
	return fmt.Sprintf("Error %d: Failed to fetch %s", m.StatusCode, m.Type)
}

func (m *MetadataServer) getTrainingSets(nameVariants []metadata.NameVariant) (map[string][]metadata.TrainingSetVariantResource, error) {
	trainingSetMap := make(map[string][]metadata.TrainingSetVariantResource)
	trainingSetVariants, err := m.client.GetTrainingSetVariants(context.Background(), nameVariants)
	if err != nil {
		return nil, err
	}
	for _, variant := range trainingSetVariants {
		if _, has := trainingSetMap[variant.Name()]; !has {
			trainingSetMap[variant.Name()] = []metadata.TrainingSetVariantResource{}
		}
		trainingSetMap[variant.Name()] = append(trainingSetMap[variant.Name()], variant.ToShallowMap())
	}
	return trainingSetMap, nil
}

func (m *MetadataServer) getFeatures(nameVariants []metadata.NameVariant) (map[string][]metadata.FeatureVariantResource, error) {
	featureMap := make(map[string][]metadata.FeatureVariantResource)
	featureVariants, err := m.client.GetFeatureVariants(context.Background(), nameVariants)
	if err != nil {
		return nil, err
	}
	for _, variant := range featureVariants {
		if _, has := featureMap[variant.Name()]; !has {
			featureMap[variant.Name()] = []metadata.FeatureVariantResource{}
		}
		featureMap[variant.Name()] = append(featureMap[variant.Name()], variant.ToShallowMap())
	}
	return featureMap, nil
}

func (m *MetadataServer) getLabels(nameVariants []metadata.NameVariant) (map[string][]metadata.LabelVariantResource, error) {
	labelMap := make(map[string][]metadata.LabelVariantResource)
	labelVariants, err := m.client.GetLabelVariants(context.Background(), nameVariants)
	if err != nil {
		return nil, err
	}
	for _, variant := range labelVariants {
		if _, has := labelMap[variant.Name()]; !has {
			labelMap[variant.Name()] = []metadata.LabelVariantResource{}
		}
		labelMap[variant.Name()] = append(labelMap[variant.Name()], variant.ToShallowMap())
	}
	return labelMap, nil
}

func (m *MetadataServer) getSources(nameVariants []metadata.NameVariant) (map[string][]metadata.SourceVariantResource, error) {
	sourceMap := make(map[string][]metadata.SourceVariantResource)
	sourceVariants, err := m.client.GetSourceVariants(context.Background(), nameVariants)
	if err != nil {
		return nil, err
	}
	for _, variant := range sourceVariants {
		if _, has := sourceMap[variant.Name()]; !has {
			sourceMap[variant.Name()] = []metadata.SourceVariantResource{}
		}
		sourceMap[variant.Name()] = append(sourceMap[variant.Name()], variant.ToShallowMap())
	}
	return sourceMap, nil
}

func (m *MetadataServer) readFromFeature(feature *metadata.Feature, deepCopy bool) (map[string]metadata.FeatureVariantResource, *FetchError) {
	variantMap := make(map[string]metadata.FeatureVariantResource)
	variants, err := feature.FetchVariants(m.client, context.Background())
	if err != nil {
		fetchError := &FetchError{StatusCode: http.StatusInternalServerError, Type: "feature variants"}
		m.logger.Errorw(fetchError.Error(), "Internal Error", err)
		return nil, fetchError
	}
	for _, variant := range variants {

		featResource := variant.ToShallowMap()
		if deepCopy {
			ts, err := m.getTrainingSets(variant.TrainingSets())
			if err != nil {
				m.logger.Errorw(err.Error(), "Internal Error", err)
				return nil, &FetchError{StatusCode: http.StatusInternalServerError, Type: "Cannot get information from training sets"}
			}
			featResource.TrainingSets = ts
		}
		variantMap[variant.Variant()] = featResource

	}
	return variantMap, nil
}

func (m *MetadataServer) readFromTrainingSet(trainingSet *metadata.TrainingSet, deepCopy bool) (map[string]metadata.TrainingSetVariantResource, *FetchError) {
	variantMap := make(map[string]metadata.TrainingSetVariantResource)
	variants, err := trainingSet.FetchVariants(m.client, context.Background())
	if err != nil {
		fetchError := &FetchError{StatusCode: http.StatusInternalServerError, Type: "training set variants"}
		m.logger.Errorw(fetchError.Error(), "Internal Error", err)
		return nil, fetchError
	}
	for _, variant := range variants {

		trainingResource := variant.ToShallowMap()
		if deepCopy {
			f, err := m.getFeatures(variant.Features())
			if err != nil {
				m.logger.Errorw(err.Error(), "Internal Error", err)
				return nil, &FetchError{StatusCode: http.StatusInternalServerError, Type: "Cannot get information from features"}
			}
			trainingResource.Features = f
		}
		variantMap[variant.Variant()] = trainingResource

	}
	return variantMap, nil
}

func (m *MetadataServer) readFromSource(source *metadata.Source, deepCopy bool) (map[string]metadata.SourceVariantResource, *FetchError) {
	variantMap := make(map[string]metadata.SourceVariantResource)
	variants, err := source.FetchVariants(m.client, context.Background())
	if err != nil {
		fetchError := &FetchError{StatusCode: http.StatusInternalServerError, Type: "source variants"}
		m.logger.Errorw(fetchError.Error(), "Internal Error", err)
		return nil, fetchError
	}
	for _, variant := range variants {

		sourceResource := variant.ToShallowMap()
		if deepCopy {
			fetchGroup := new(errgroup.Group)
			fetchGroup.Go(func() error {
				f, err := m.getFeatures(variant.Features())
				if err != nil {
					m.logger.Errorw(err.Error(), "Internal Error", err)
					return &FetchError{StatusCode: http.StatusInternalServerError, Type: "Cannot get information from features"}
				}
				sourceResource.Features = f
				return nil
			})
			fetchGroup.Go(func() error {
				l, err := m.getLabels(variant.Labels())
				if err != nil {
					m.logger.Errorw(err.Error(), "Internal Error", err)
					return &FetchError{StatusCode: http.StatusInternalServerError, Type: "Cannot get information from labels"}
				}
				sourceResource.Labels = l
				return nil
			})
			fetchGroup.Go(func() error {
				ts, err := m.getTrainingSets(variant.TrainingSets())
				if err != nil {
					m.logger.Errorw(err.Error(), "Internal Error", err)
					return &FetchError{StatusCode: http.StatusInternalServerError, Type: "Cannot get information from training sets"}
				}
				sourceResource.TrainingSets = ts
				return nil
			})
			if err := fetchGroup.Wait(); err != nil {
				return nil, err.(*FetchError)
			}
		}
		variantMap[variant.Variant()] = sourceResource

	}
	return variantMap, nil
}

func (m *MetadataServer) readFromLabel(label *metadata.Label, deepCopy bool) (map[string]metadata.LabelVariantResource, *FetchError) {
	variantMap := make(map[string]metadata.LabelVariantResource)
	variants, err := label.FetchVariants(m.client, context.Background())
	if err != nil {
		fetchError := &FetchError{StatusCode: http.StatusInternalServerError, Type: "label variants"}
		m.logger.Errorw(fetchError.Error(), "Internal Error", err)
		return nil, fetchError
	}
	for _, variant := range variants {
		labelResource := variant.ToShallowMap()
		if deepCopy {
			ts, err := m.getTrainingSets(variant.TrainingSets())
			if err != nil {
				m.logger.Errorw(err.Error(), "Internal Error", err)
				return nil, &FetchError{StatusCode: http.StatusInternalServerError, Type: "Cannot get information from training sets"}
			}
			labelResource.TrainingSets = ts
		}
		variantMap[variant.Variant()] = labelResource
	}
	return variantMap, nil
}

func (m *MetadataServer) GetMetadata(c *gin.Context) {
	switch c.Param("type") {
	case "features":
		feature, err := m.client.GetFeature(context.Background(), c.Param("resource"))
		if err != nil {
			fetchError := &FetchError{StatusCode: http.StatusInternalServerError, Type: "feature"}
			m.logger.Errorw(fetchError.Error(), "Metadata error", err)
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		variantList, fetchError := m.readFromFeature(feature, true)
		if fetchError != nil {
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		c.JSON(http.StatusOK, FeatureResource{
			AllVariants:    feature.Variants(),
			Type:           "Feature",
			DefaultVariant: feature.DefaultVariant(),
			Name:           feature.Name(),
			Variants:       variantList,
		})
	case "labels":
		label, err := m.client.GetLabel(context.Background(), c.Param("resource"))
		if err != nil {
			fetchError := &FetchError{StatusCode: http.StatusInternalServerError, Type: "label"}
			m.logger.Errorw(fetchError.Error(), "Metadata error", err)
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		variantList, fetchError := m.readFromLabel(label, true)
		if fetchError != nil {
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		c.JSON(http.StatusOK, LabelResource{
			AllVariants:    label.Variants(),
			Type:           "Label",
			DefaultVariant: label.DefaultVariant(),
			Name:           label.Name(),
			Variants:       variantList,
		})
	case "training-sets":
		trainingSet, err := m.client.GetTrainingSet(context.Background(), c.Param("resource"))
		if err != nil {
			fetchError := &FetchError{StatusCode: http.StatusInternalServerError, Type: "training set"}
			m.logger.Errorw(fetchError.Error(), "Metadata error", err)
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		variantList, fetchError := m.readFromTrainingSet(trainingSet, true)
		if fetchError != nil {
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		c.JSON(http.StatusOK, TrainingSetResource{
			AllVariants:    trainingSet.Variants(),
			Type:           "TrainingSet",
			DefaultVariant: trainingSet.DefaultVariant(),
			Name:           trainingSet.Name(),
			Variants:       variantList,
		})
	case "sources":
		source, err := m.client.GetSource(context.Background(), c.Param("resource"))
		if err != nil {
			fetchError := &FetchError{StatusCode: http.StatusInternalServerError, Type: "source"}
			m.logger.Errorw(fetchError.Error(), "Metadata error", err)
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		variantList, fetchError := m.readFromSource(source, true)
		if fetchError != nil {
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		c.JSON(http.StatusOK, SourceResource{
			AllVariants:    source.Variants(),
			Type:           "Source",
			DefaultVariant: source.DefaultVariant(),
			Name:           source.Name(),
			Variants:       variantList,
		})
	case "entities":
		entity, err := m.client.GetEntity(context.Background(), c.Param("resource"))
		if err != nil {
			fetchError := &FetchError{StatusCode: http.StatusInternalServerError, Type: "entity"}
			m.logger.Errorw(fetchError.Error(), "Metadata error", err)
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		entityResource := metadata.EntityResource{
			Name:        entity.Name(),
			Type:        "Entity",
			Description: entity.Description(),
			Status:      entity.Status().String(),
			Tags:        entity.Tags(),
			Properties:  entity.Properties(),
		}
		fetchGroup := new(errgroup.Group)
		fetchGroup.Go(func() error {
			f, err := m.getFeatures(entity.Features())
			if err != nil {
				m.logger.Errorw(err.Error(), "Internal error", err)
				return &FetchError{StatusCode: http.StatusInternalServerError, Type: "feature"}
			}
			entityResource.Features = f
			return nil
		})
		fetchGroup.Go(func() error {
			l, err := m.getLabels(entity.Labels())
			if err != nil {
				m.logger.Errorw(err.Error(), "Internal error", err)
				return &FetchError{StatusCode: http.StatusInternalServerError, Type: "label"}
			}
			entityResource.Labels = l
			return nil
		})
		fetchGroup.Go(func() error {
			ts, err := m.getTrainingSets(entity.TrainingSets())
			if err != nil {
				m.logger.Errorw(err.Error(), "Internal error", err)
				return &FetchError{StatusCode: http.StatusInternalServerError, Type: "training set"}
			}
			entityResource.TrainingSets = ts
			return nil
		})
		if err := fetchGroup.Wait(); err != nil {
			c.JSON(err.(*FetchError).StatusCode, err.Error())
		}
		c.JSON(http.StatusOK, entityResource)
	case "users":
		user, err := m.client.GetUser(context.Background(), c.Param("resource"))
		if err != nil {
			fetchError := &FetchError{StatusCode: http.StatusInternalServerError, Type: "user"}
			m.logger.Errorw(err.Error(), "Metadata error", err)
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		userResource := &UserResource{
			Name:       user.Name(),
			Type:       "User",
			Status:     user.Status().String(),
			Tags:       user.Tags(),
			Properties: user.Properties(),
		}
		fetchGroup := new(errgroup.Group)
		fetchGroup.Go(func() error {
			f, err := m.getFeatures(user.Features())
			if err != nil {
				m.logger.Errorw(err.Error(), "Internal error", err)
				return &FetchError{StatusCode: http.StatusInternalServerError, Type: "feature"}
			}
			userResource.Features = f
			return nil
		})
		fetchGroup.Go(func() error {
			l, err := m.getLabels(user.Labels())
			if err != nil {
				m.logger.Errorw(err.Error(), "Internal error", err)
				return &FetchError{StatusCode: http.StatusInternalServerError, Type: "label"}
			}
			userResource.Labels = l
			return nil
		})
		fetchGroup.Go(func() error {
			ts, err := m.getTrainingSets(user.TrainingSets())
			if err != nil {
				m.logger.Errorw(err.Error(), "Internal error", err)
				return &FetchError{StatusCode: http.StatusInternalServerError, Type: "training set"}
			}
			userResource.TrainingSets = ts
			return nil
		})
		fetchGroup.Go(func() error {
			s, err := m.getSources(user.Sources())
			if err != nil {
				m.logger.Errorw(err.Error(), "Internal error", err)
				return &FetchError{StatusCode: http.StatusInternalServerError, Type: "source"}
			}
			userResource.Sources = s
			return nil
		})
		if err := fetchGroup.Wait(); err != nil {
			c.JSON(err.(*FetchError).StatusCode, err.Error())
		}
		c.JSON(http.StatusOK, userResource)
	case "models":
		model, err := m.client.GetModel(context.Background(), c.Param("resource"))
		if err != nil {
			fetchError := &FetchError{StatusCode: http.StatusInternalServerError, Type: "model"}
			m.logger.Errorw(fetchError.Error(), "Metadata error", err)
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		modelResource := &ModelResource{
			Name:        model.Name(),
			Type:        "Model",
			Description: model.Description(),
			Status:      model.Status().String(),
			Tags:        model.Tags(),
			Properties:  model.Properties(),
		}
		fetchGroup := new(errgroup.Group)
		fetchGroup.Go(func() error {
			f, err := m.getFeatures(model.Features())
			if err != nil {
				m.logger.Errorw(err.Error(), "Internal error", err)
				return &FetchError{StatusCode: http.StatusInternalServerError, Type: "feature"}
			}
			modelResource.Features = f
			return nil
		})
		fetchGroup.Go(func() error {
			l, err := m.getLabels(model.Labels())
			if err != nil {
				m.logger.Errorw(err.Error(), "Internal error", err)
				return &FetchError{StatusCode: http.StatusInternalServerError, Type: "label"}
			}
			modelResource.Labels = l
			return nil
		})
		fetchGroup.Go(func() error {
			ts, err := m.getTrainingSets(model.TrainingSets())
			if err != nil {
				m.logger.Errorw(err.Error(), "Internal error", err)
				return &FetchError{StatusCode: http.StatusInternalServerError, Type: "training set"}
			}
			modelResource.TrainingSets = ts
			return nil
		})
		if err := fetchGroup.Wait(); err != nil {
			c.JSON(err.(*FetchError).StatusCode, err.Error())
		}
		c.JSON(http.StatusOK, modelResource)
	case "providers":
		provider, err := m.client.GetProvider(context.Background(), c.Param("resource"))
		if err != nil {
			fetchError := &FetchError{StatusCode: http.StatusInternalServerError, Type: "provider"}
			m.logger.Errorw(fetchError.Error(), "Metadata error", err)
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		providerResource := &metadata.ProviderResource{
			Name:         provider.Name(),
			Description:  provider.Description(),
			Type:         "Provider",
			ProviderType: provider.Type(),
			Software:     provider.Software(),
			Team:         provider.Team(),
			Status:       provider.Status().String(),
			Error:        provider.Error(),
			Tags:         provider.Tags(),
			Properties:   provider.Properties(),
		}
		fetchGroup := new(errgroup.Group)
		fetchGroup.Go(func() error {
			f, err := m.getFeatures(provider.Features())
			if err != nil {
				m.logger.Errorw(err.Error(), "Internal error", err)
				return &FetchError{StatusCode: http.StatusInternalServerError, Type: "feature"}
			}
			providerResource.Features = f
			return nil
		})
		fetchGroup.Go(func() error {
			l, err := m.getLabels(provider.Labels())
			if err != nil {
				m.logger.Errorw(err.Error(), "Internal error", err)
				return &FetchError{StatusCode: http.StatusInternalServerError, Type: "label"}
			}
			providerResource.Labels = l
			return nil
		})
		fetchGroup.Go(func() error {
			ts, err := m.getTrainingSets(provider.TrainingSets())
			if err != nil {
				m.logger.Errorw(err.Error(), "Internal error", err)
				return &FetchError{StatusCode: http.StatusInternalServerError, Type: "training set"}
			}
			providerResource.TrainingSets = ts
			return nil
		})
		fetchGroup.Go(func() error {
			s, err := m.getSources(provider.Sources())
			if err != nil {
				m.logger.Errorw(err.Error(), "Internal error", err)
				return &FetchError{StatusCode: http.StatusInternalServerError, Type: "source"}
			}
			providerResource.Sources = s
			return nil
		})
		if err := fetchGroup.Wait(); err != nil {
			c.JSON(err.(*FetchError).StatusCode, err.Error())
		}
		c.JSON(http.StatusOK, providerResource)
	}
}

func GetMetadataResources(paginatedResources map[string]string, resourceType metadata.ResourceType) ([]metadata.Resource, error) {
	mResources := make([]metadata.Resource, 0)
	for _, currentResource := range paginatedResources {
		var tmp metadata.EtcdRowTemp
		err := json.Unmarshal([]byte(currentResource), &tmp)
		if err != nil {
			return nil, err
		}
		msg := metadata.EtcdRow{
			ResourceType:      metadata.ResourceType(tmp.ResourceType),
			StorageType:       tmp.StorageType,
			Message:           tmp.Message,
			SerializedVersion: tmp.SerializedVersion,
		}
		resource, _ := metadata.CreateEmptyResource(msg.ResourceType)
		parsedResource, err := metadata.ParseResource(msg, resource)
		if err != nil {
			logging.GlobalLogger.Errorw("Error with parsing resources", "error", err)
			return nil, err
		}
		if parsedResource.ID().Type == resourceType {
			mResources = append(mResources, parsedResource)
		}
	}
	return mResources, nil
}

type GetMetadataListResp struct {
	Count        int `json:"count"`
	ResourceList any `json:"resourceList"`
}

type GetFeatureVariantListResp struct {
	Count int                               `json:"count"`
	Data  []metadata.FeatureVariantResource `json:"data"`
}

type GetSourceVariantListResp struct {
	Count int                              `json:"count"`
	Data  []metadata.SourceVariantResource `json:"data"`
}

type GetLabelVariantListResp struct {
	Count int                             `json:"count"`
	Data  []metadata.LabelVariantResource `json:"data"`
}

type GetEntityListResp struct {
	Count int                       `json:"count"`
	Data  []metadata.EntityResource `json:"data"`
}

type GetProviderListResp struct {
	Count int                         `json:"count"`
	Data  []metadata.ProviderResource `json:"data"`
}

type GetTrainingSetVariantListResp struct {
	Count int                                   `json:"count"`
	Data  []metadata.TrainingSetVariantResource `json:"data"`
}

type GetModelListResp struct {
	Count int                      `json:"count"`
	Data  []metadata.ModelResource `json:"data"`
}

func (m *MetadataServer) getCountAndResources(resourceType metadata.ResourceType, pageSize, offset int, filterOpts ...query.Query) (int, []metadata.Resource, error) {
	queryList := m.getResourceQuery(resourceType.String())
	queryList = append(queryList, filterOpts...)
	count, err := m.StorageProvider.Count(resourceType.String(), queryList...)
	if err != nil {
		m.logger.Errorw("Error getting count of query list", "error", err)
		return 0, nil, err
	}
	pagination := query.Limit{
		Limit:  pageSize,
		Offset: offset * pageSize,
	}
	keySort := query.KeySort{Dir: query.Desc}
	queryList = append(queryList, pagination, keySort)
	m.logger.Debugw("Query list", "queryList", queryList)
	paginatedResources, err := m.StorageProvider.List(resourceType.String(), queryList...)
	if err != nil {
		m.logger.Errorw("Error getting paginated resources", "query list", queryList, "error", err)
		return 0, nil, err
	}
	m.logger.Debugw("Paginated resources", "paginatedResources", paginatedResources)
	mResources, err := GetMetadataResources(paginatedResources, resourceType)
	if err != nil {
		m.logger.Errorw("Error getting metadata resources", "error", err)
		return 0, nil, err
	}
	return count, mResources, nil
}

type FeatureVariantFilters struct {
	SearchTxt string   `json:"SearchTxt"`
	Owners    []string `json:"Owners"`
	Statuses  []string `json:"Statuses"`
	Tags      []string `json:"Tags"`
	PageSize  int      `json:"pageSize"`
	Offset    int      `json:"offset"`
}

func (m *MetadataServer) GetFeatureVariantResources(c *gin.Context) {
	var filterBody FeatureVariantFilters
	if bindErr := c.BindJSON(&filterBody); bindErr != nil {
		fetchError := m.GetRequestError(http.StatusInternalServerError, bindErr, c, "Error binding the request body")
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}

	filterOpts := m.buildFeatureVariantFilterOpts(filterBody)
	resourceType := metadata.FEATURE

	count, featureResources, err := m.getCountAndResources(resourceType, filterBody.PageSize, filterBody.Offset, filterOpts...)
	if err != nil {
		fetchError := m.GetRequestError(http.StatusInternalServerError, err, c, "Failed to get count or list")
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}

	//pull the provider name - type map
	providerMap, mapErr := m.getProviderNameTypeMap()
	if mapErr != nil {
		// log but continue
		m.logger.Errorf("an error occurred pulling the provider name - type map: %v", mapErr)
	}

	variantList := make([]metadata.FeatureVariantResource, len(featureResources))
	for i, parsedFeature := range featureResources {
		deserialized := parsedFeature.Proto()
		feature, ok := deserialized.(*pb.Feature)
		if !ok {
			m.logger.Errorw("Could not deserialize resource with ID: %s", parsedFeature.ID().String())
			continue
		}
		featureNameVariant := metadata.NameVariant{Name: feature.Name, Variant: feature.DefaultVariant}
		wrappedVariant, err := m.client.GetFeatureVariant(c, featureNameVariant)
		if err != nil {
			m.logger.Errorw("Could not get feature variant", "error", err)
			continue
		}

		shallowVariant := wrappedVariant.ToShallowMap()

		//check in the providerMap association
		providerType, mapOk := providerMap[shallowVariant.Provider]
		if mapOk {
			shallowVariant.ProviderType = providerType
		}

		variantList[i] = shallowVariant
	}

	resp := GetFeatureVariantListResp{
		Count: count,
		Data:  variantList,
	}
	c.JSON(http.StatusOK, resp)
}

type SourceVariantFilters struct {
	SearchTxt string   `json:"SearchTxt"`
	Types     []string `json:"Types"`
	Modes     []string `json:"Modes"`
	Tags      []string `json:"Tags"`
	Statuses  []string `json:"Statuses"`
	Owners    []string `json:"Owners"`
	PageSize  int      `json:"pageSize"`
	Offset    int      `json:"offset"`
}

func (m *MetadataServer) GetSourceVariantResources(c *gin.Context) {
	var filterBody SourceVariantFilters
	if bindErr := c.BindJSON(&filterBody); bindErr != nil {
		fetchError := m.GetRequestError(http.StatusInternalServerError, bindErr, c, "Error binding the request body")
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}

	filterOpts := m.buildSourceVariantFilterOpts(filterBody)
	resourceType := metadata.SOURCE

	count, sourceResources, err := m.getCountAndResources(resourceType, filterBody.PageSize, filterBody.Offset, filterOpts...)
	if err != nil {
		fetchError := m.GetRequestError(http.StatusInternalServerError, err, c, "Failed to get count or list")
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}

	variantList := make([]metadata.SourceVariantResource, len(sourceResources))
	for i, parsedSource := range sourceResources {
		deserialized := parsedSource.Proto()
		source, ok := deserialized.(*pb.Source)
		if !ok {
			m.logger.Errorw("Could not deserialize resource with ID: %s", parsedSource.ID().String())
			continue
		}
		sourceNameVariant := metadata.NameVariant{Name: source.Name, Variant: source.DefaultVariant}
		wrappedVariant, err := m.client.GetSourceVariant(c, sourceNameVariant)
		if err != nil {
			m.logger.Errorw("Could not get source variant", "error", err)
			continue
		}
		shallowVariant := wrappedVariant.ToShallowMap()
		variantList[i] = shallowVariant
	}
	resp := GetSourceVariantListResp{
		Count: count,
		Data:  variantList,
	}
	c.JSON(http.StatusOK, resp)
}

type LabelVariantFilters struct {
	SearchTxt string   `json:"SearchTxt"`
	Owners    []string `json:"Owners"`
	Statuses  []string `json:"Statuses"`
	Tags      []string `json:"Tags"`
	PageSize  int      `json:"pageSize"`
	Offset    int      `json:"offset"`
}

func (m *MetadataServer) GetLabelVariantResources(c *gin.Context) {
	var filterBody LabelVariantFilters
	if bindErr := c.BindJSON(&filterBody); bindErr != nil {
		fetchError := m.GetRequestError(http.StatusInternalServerError, bindErr, c, "Error binding the request body")
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}

	filterOpts := m.buildLabelVariantFilterOpts(filterBody)
	resourceType := metadata.LABEL

	count, labelResources, err := m.getCountAndResources(resourceType, filterBody.PageSize, filterBody.Offset, filterOpts...)
	if err != nil {
		fetchError := m.GetRequestError(http.StatusInternalServerError, err, c, "Failed to get count or list")
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}

	//pull the provider name - type map
	providerMap, mapErr := m.getProviderNameTypeMap()
	if mapErr != nil {
		// log but continue
		m.logger.Errorf("an error occurred pulling the provider name - type map: %v", mapErr)
	}

	variantList := make([]metadata.LabelVariantResource, len(labelResources))
	for i, parsedLabel := range labelResources {
		deserialized := parsedLabel.Proto()
		label, ok := deserialized.(*pb.Label)
		if !ok {
			m.logger.Errorw("Could not deserialize resource with ID: %s", parsedLabel.ID().String())
			continue
		}
		labelNameVariant := metadata.NameVariant{Name: label.Name, Variant: label.DefaultVariant}
		wrappedVariant, err := m.client.GetLabelVariant(c, labelNameVariant)
		if err != nil {
			m.logger.Errorw("Could not get label variant", "error", err)
			continue
		}
		shallowVariant := wrappedVariant.ToShallowMap()

		//check in the providerMap association
		providerType, mapOk := providerMap[shallowVariant.Provider]
		if mapOk {
			shallowVariant.ProviderType = providerType
		}

		variantList[i] = shallowVariant
	}

	resp := GetLabelVariantListResp{
		Count: count,
		Data:  variantList,
	}
	c.JSON(http.StatusOK, resp)
}

type EntityFilters struct {
	SearchTxt string   `json:"SearchTxt"`
	Owners    []string `json:"Owners"`
	Statuses  []string `json:"Statuses"`
	Tags      []string `json:"Tags"`
	Labels    []string `json:"Labels"`
	Features  []string `json:"Features"`
	PageSize  int      `json:"pageSize"`
	Offset    int      `json:"offset"`
}

func (m *MetadataServer) GetEntityResources(c *gin.Context) {
	var filterBody EntityFilters
	if bindErr := c.BindJSON(&filterBody); bindErr != nil {
		fetchError := m.GetRequestError(http.StatusInternalServerError, bindErr, c, "Error binding the request body")
		m.logger.Errorw(fetchError.Error())
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}
	filterOpts := m.buildEntityFilterOpts(filterBody)
	resourceType := metadata.ENTITY

	count, resources, err := m.getCountAndResources(resourceType, filterBody.PageSize, filterBody.Offset, filterOpts...)
	if err != nil {
		fetchError := m.GetRequestError(http.StatusInternalServerError, err, c, "Failed to get count or list")
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}
	resourceList := make([]metadata.EntityResource, len(resources))
	for i, parsedResource := range resources {
		deserialized := parsedResource.Proto()
		entity, ok := deserialized.(*pb.Entity)
		if !ok {
			m.logger.Errorw("Could not deserialize resource with ID: %s", parsedResource.ID().String())
			continue
		}
		wrappedEntity := metadata.WrapProtoEntity(entity)
		shallowEntity := wrappedEntity.ToShallowMap()

		resourceList[i] = shallowEntity
	}

	resp := GetEntityListResp{
		Count: count,
		Data:  resourceList,
	}
	c.JSON(http.StatusOK, resp)
}

type ProviderFilters struct {
	SearchTxt    string   `json:"SearchTxt"`
	ProviderType []string `json:"ProviderType"`
	Status       []string `json:"Status"`
	PageSize     int      `json:"pageSize"`
	Offset       int      `json:"offset"`
}

func (m *MetadataServer) GetProviderResources(c *gin.Context) {
	var filterBody ProviderFilters
	if bindErr := c.BindJSON(&filterBody); bindErr != nil {
		fetchError := m.GetRequestError(http.StatusInternalServerError, bindErr, c, "Error binding the request body")
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}
	filterOpts := m.buildProviderFilterOpts(filterBody)
	resourceType := metadata.PROVIDER

	count, resources, err := m.getCountAndResources(resourceType, filterBody.PageSize, filterBody.Offset, filterOpts...)
	if err != nil {
		fetchError := m.GetRequestError(http.StatusInternalServerError, err, c, "Failed to get count or list")
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}

	variantList := make([]metadata.ProviderResource, len(resources))
	for i, parsedProvider := range resources {
		deserialized := parsedProvider.Proto()
		provider, ok := deserialized.(*pb.Provider)
		if !ok {
			m.logger.Errorw("Could not deserialize resource with ID: %s", parsedProvider.ID().String())
			continue
		}
		wrappedProvider := metadata.WrapProtoProvider(provider)
		shallowProvider := wrappedProvider.ToShallowMap()

		variantList[i] = shallowProvider
	}

	resp := GetProviderListResp{
		Count: count,
		Data:  variantList,
	}
	c.JSON(http.StatusOK, resp)
}

type TrainingSetVariantFilters struct {
	SearchTxt string   `json:"SearchTxt"`
	Owners    []string `json:"Owners"`
	Statuses  []string `json:"Statuses"`
	Tags      []string `json:"Tags"`
	Labels    []string `json:"Labels"`
	Providers []string `json:"Providers"`
	PageSize  int      `json:"pageSize"`
	Offset    int      `json:"offset"`
}

func (m *MetadataServer) GetTrainingSetVariantResources(c *gin.Context) {
	var filterBody TrainingSetVariantFilters
	if bindErr := c.BindJSON(&filterBody); bindErr != nil {
		fetchError := m.GetRequestError(http.StatusInternalServerError, bindErr, c, "Error binding the request body")
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}

	filterOpts := m.buildTrainingSetVariantFilterOpts(filterBody)
	resourceType := metadata.TRAINING_SET

	count, trainingSetResources, err := m.getCountAndResources(resourceType, filterBody.PageSize, filterBody.Offset, filterOpts...)
	if err != nil {
		fetchError := m.GetRequestError(http.StatusInternalServerError, err, c, "Failed to get count or list")
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}
	//pull the provider name - type map
	providerMap, mapErr := m.getProviderNameTypeMap()
	if mapErr != nil {
		// log but continue
		m.logger.Errorf("an error occurred pulling the provider name - type map: %v", mapErr)
	}

	variantList := make([]metadata.TrainingSetVariantResource, len(trainingSetResources))
	for i, parsedTrainingSet := range trainingSetResources {
		deserialized := parsedTrainingSet.Proto()
		trainingSet, ok := deserialized.(*pb.TrainingSet)
		if !ok {
			m.logger.Errorw("Could not deserialize resource with ID: %s", parsedTrainingSet.ID().String())
			continue
		}
		trainingSetNameVariant := metadata.NameVariant{Name: trainingSet.Name, Variant: trainingSet.DefaultVariant}
		wrappedVariant, err := m.client.GetTrainingSetVariant(c, trainingSetNameVariant)
		if err != nil {
			m.logger.Errorw("Could not get training set variant", "error", err)
			continue
		}
		shallowVariant := wrappedVariant.ToShallowMap()
		//check in the providerMap association
		providerType, mapOk := providerMap[shallowVariant.Provider]
		if mapOk {
			shallowVariant.ProviderType = providerType
		}
		variantList[i] = shallowVariant
	}

	resp := GetTrainingSetVariantListResp{
		Count: count,
		Data:  variantList,
	}
	c.JSON(http.StatusOK, resp)
}

type ModelFilters struct {
	SearchTxt    string   `json:"SearchTxt"`
	Tags         []string `json:"Tags"`
	Labels       []string `json:"Labels"`
	Features     []string `json:"Features"`
	TrainingSets []string `json:"TrainingSets"`
	PageSize     int      `json:"pageSize"`
	Offset       int      `json:"offset"`
}

func (m *MetadataServer) GetModelResources(c *gin.Context) {
	var filterBody ModelFilters
	if bindErr := c.BindJSON(&filterBody); bindErr != nil {
		fetchError := m.GetRequestError(http.StatusInternalServerError, bindErr, c, "Error binding the request body")
		m.logger.Errorw(fetchError.Error())
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}
	filterOpts := m.buildModelFilterOpts(filterBody)
	resourceType := metadata.MODEL

	count, resources, err := m.getCountAndResources(resourceType, filterBody.PageSize, filterBody.Offset, filterOpts...)
	if err != nil {
		fetchError := m.GetRequestError(http.StatusInternalServerError, err, c, "Failed to get count or list")
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}
	resourceList := make([]metadata.ModelResource, len(resources))
	for i, parsedResource := range resources {
		deserialized := parsedResource.Proto()
		model, ok := deserialized.(*pb.Model)
		if !ok {
			m.logger.Errorw("Could not deserialize resource with ID: %s", parsedResource.ID().String())
			continue
		}
		wrappedModel := metadata.WrapProtoModel(model)
		shallowModel := wrappedModel.ToShallowMap()

		resourceList[i] = shallowModel
	}

	resp := GetModelListResp{
		Count: count,
		Data:  resourceList,
	}
	c.JSON(http.StatusOK, resp)
}

// helper function
func convertToAny[T any](slice []T) []any {
	converted := make([]any, len(slice))
	for i, v := range slice {
		converted[i] = v
	}
	return converted
}

func (m *MetadataServer) buildModelFilterOpts(filterBody ModelFilters) []query.Query {
	filterOpts := []query.Query{}
	usingV1Filters := false

	if filterBody.SearchTxt != "" {
		m.logger.Debugw("buildModelFilterOpts - adding a search txt filter: ", filterBody.SearchTxt)
		filterOpts = append(filterOpts, query.ValueLike{
			Column: query.JSONColumn{
				Path: []query.JSONPathStep{{Key: "Message", IsJsonString: true}, {Key: "name"}},
				Type: query.String,
			},
			Value: filterBody.SearchTxt,
		})
	}

	if len(filterBody.Tags) > 0 {
		m.logger.Debugw("buildModelFilterOpts - adding a tag list filter: ", filterBody.Tags)
		usingV1Filters = true
		filterOpts = append(filterOpts, query.ArrayContains{
			Column: query.JSONColumn{
				Path: []query.JSONPathStep{{Key: "Message", IsJsonString: true}, {Key: "tags"}, {Key: "tag"}},
				Type: query.String,
			},
			Values: convertToAny(filterBody.Tags),
		})
	}

	if usingV1Filters {
		m.logger.Debugw("GetMetadataList - Using v1 filters, adding SerializedVersion clause (=1)")
		filterOpts = append(filterOpts, query.ValueEquals{
			Column: query.JSONColumn{
				Path: []query.JSONPathStep{{Key: serializedVersion}},
				Type: query.String,
			},
			Value: serializedV1,
		})
	}
	return filterOpts
}

func (m *MetadataServer) GetMetadataList(c *gin.Context) {
	typeParam := c.Param("type")
	pageSize, offset, err := m.parsePaginationParams(c)
	if err != nil {
		fetchError := &FetchError{StatusCode: http.StatusInternalServerError, Type: "GetMetadataList"}
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}

	switch typeParam {
	case "features":
		m.getFeatureMetadataList(c, pageSize, offset)
	case "training-sets":
		m.getTrainingSetMetadataList(c, pageSize, offset)
	case "sources":
		m.getSourceMetadataList(c, pageSize, offset)
	case "labels":
		m.getLabelMetadataList(c, pageSize, offset)
	case "entities":
		m.getEntityMetadataList(c, pageSize, offset)
	case "models":
		m.getModelMetadataList(c, pageSize, offset)
	case "users":
		m.getUserMetadataList(c, pageSize, offset)
	case "providers":
		m.getProviderMetadataList(c, pageSize, offset)
	default:
		m.logger.Errorw("Not a valid data type", "Error", typeParam)
		fetchError := &FetchError{StatusCode: http.StatusBadRequest, Type: typeParam}
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}
}

func (m *MetadataServer) parsePaginationParams(c *gin.Context) (int, int, error) {
	pageSize := 500 // default
	offset := 0
	pageSizeQuery := c.Query("pageSize")
	offsetQuery := c.Query("offset")

	if pageSizeQuery != "" && offsetQuery != "" {
		ps, err := strconv.Atoi(pageSizeQuery)
		if err != nil || ps <= 0 {
			m.logger.Errorw("Invalid pageSize value:", ps, err)
			return 0, 0, err
		}
		off, err := strconv.Atoi(offsetQuery)
		if err != nil || off < 0 {
			m.logger.Errorw("Invalid offset value:", off, err)
			return 0, 0, err
		}
		pageSize = ps
		offset = off
	}
	return pageSize, offset, nil
}

func (m *MetadataServer) getFeatureMetadataList(c *gin.Context, pageSize, offset int) {
	resourceType := metadata.FEATURE
	count, mResources, err := m.getCountAndResources(resourceType, pageSize, offset)
	if err != nil {
		fetchError := m.GetRequestError(http.StatusInternalServerError, err, c, "getCountAndResources - Failed to get count or list")
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}

	resourceList := make([]FeatureResource, len(mResources))
	for i, parsedResource := range mResources {
		deserialized := parsedResource.Proto()
		feature, ok := deserialized.(*pb.Feature)
		if !ok {
			m.logger.Errorw("Could not deserialize resource with ID: %s", parsedResource.ID().String())
			continue
		}
		wrappedResource := metadata.WrapProtoFeature(feature)
		variantList, fetchError := m.readFromFeature(wrappedResource, false)
		if fetchError != nil {
			m.logger.Errorw(fetchError.Error())
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		resourceList[i] = FeatureResource{
			AllVariants:    wrappedResource.Variants(),
			Type:           "Feature",
			DefaultVariant: wrappedResource.DefaultVariant(),
			Name:           wrappedResource.Name(),
			Variants:       variantList,
		}
	}
	resp := GetMetadataListResp{
		Count:        count,
		ResourceList: resourceList,
	}
	c.JSON(http.StatusOK, resp)
}

func (m *MetadataServer) buildFeatureVariantFilterOpts(filterBody FeatureVariantFilters) []query.Query {
	filterOpts := []query.Query{}
	usingV1Filters := false

	if filterBody.SearchTxt != "" {
		m.logger.Debugw("buildFeatureFilterOpts - adding a search txt filter: ", filterBody.SearchTxt)
		filterOpts = append(filterOpts, query.ValueLike{
			Column: query.JSONColumn{
				Path: []query.JSONPathStep{{Key: "Message", IsJsonString: true}, {Key: "name"}},
				Type: query.String,
			},
			Value: filterBody.SearchTxt,
		})
	}

	if len(filterBody.Owners) > 0 {
		m.logger.Debugw("buildFeatureFilterOpts - adding a feature owner filter: ", filterBody.Owners)
		usingV1Filters = true
		filterOpts = append(filterOpts, query.ValueIn{
			Column: query.JSONColumn{
				Path: []query.JSONPathStep{{Key: "Message", IsJsonString: true}, {Key: "owner"}},
				Type: query.String,
			},
			Values: convertToAny(filterBody.Owners),
		})
	}

	if len(filterBody.Statuses) > 0 {
		m.logger.Debugw("buildFeatureFilterOpts - adding a feature status filter: ", filterBody.Statuses)
		usingV1Filters = true
		filterOpts = append(filterOpts, query.ValueIn{
			Column: query.JSONColumn{
				Path: []query.JSONPathStep{{Key: "Message", IsJsonString: true}, {Key: "status"}, {Key: "status"}},
				Type: query.String,
			},
			Values: convertToAny(filterBody.Statuses),
		})
	}

	if len(filterBody.Tags) > 0 {
		m.logger.Debugw("buildFeatureFilterOpts - adding a tag list filter: ", filterBody.Tags)
		usingV1Filters = true
		filterOpts = append(filterOpts, query.ArrayContains{
			Column: query.JSONColumn{
				Path: []query.JSONPathStep{{Key: "Message", IsJsonString: true}, {Key: "tags"}, {Key: "tag"}},
				Type: query.String,
			},
			Values: convertToAny(filterBody.Tags),
		})
	}

	if usingV1Filters {
		m.logger.Debugw("GetMetadataList - Using v1 filters, adding SerializedVersion clause (=1)")
		filterOpts = append(filterOpts, query.ValueEquals{
			Column: query.JSONColumn{
				Path: []query.JSONPathStep{{Key: serializedVersion}},
				Type: query.String,
			},
			Value: serializedV1,
		})
	}
	return filterOpts
}

func (m *MetadataServer) buildLabelVariantFilterOpts(filterBody LabelVariantFilters) []query.Query {
	filterOpts := []query.Query{}
	usingV1Filters := false

	if filterBody.SearchTxt != "" {
		m.logger.Debugw("buildLabelFilterOpts - adding a search txt filter: ", filterBody.SearchTxt)
		filterOpts = append(filterOpts, query.ValueLike{
			Column: query.JSONColumn{
				Path: []query.JSONPathStep{{Key: "Message", IsJsonString: true}, {Key: "name"}},
				Type: query.String,
			},
			Value: filterBody.SearchTxt,
		})
	}

	if len(filterBody.Owners) > 0 {
		m.logger.Debugw("buildLabelFilterOpts - adding a label owner filter: ", filterBody.Owners)
		usingV1Filters = true
		filterOpts = append(filterOpts, query.ValueIn{
			Column: query.JSONColumn{
				Path: []query.JSONPathStep{{Key: "Message", IsJsonString: true}, {Key: "owner"}},
				Type: query.String,
			},
			Values: convertToAny(filterBody.Owners),
		})
	}

	if len(filterBody.Statuses) > 0 {
		m.logger.Debugw("buildLabelFilterOpts - adding a label status filter: ", filterBody.Statuses)
		usingV1Filters = true
		filterOpts = append(filterOpts, query.ValueIn{
			Column: query.JSONColumn{
				Path: []query.JSONPathStep{{Key: "Message", IsJsonString: true}, {Key: "status"}, {Key: "status"}},
				Type: query.String,
			},
			Values: convertToAny(filterBody.Statuses),
		})
	}

	if len(filterBody.Tags) > 0 {
		m.logger.Debugw("buildLabelFilterOpts - adding a tag list filter: ", filterBody.Tags)
		usingV1Filters = true
		filterOpts = append(filterOpts, query.ArrayContains{
			Column: query.JSONColumn{
				Path: []query.JSONPathStep{{Key: "Message", IsJsonString: true}, {Key: "tags"}, {Key: "tag"}},
				Type: query.String,
			},
			Values: convertToAny(filterBody.Tags),
		})
	}

	if usingV1Filters {
		m.logger.Debugw("GetMetadataList - Using v1 filters, adding SerializedVersion clause (=1)")
		filterOpts = append(filterOpts, query.ValueEquals{
			Column: query.JSONColumn{
				Path: []query.JSONPathStep{{Key: serializedVersion}},
				Type: query.String,
			},
			Value: serializedV1,
		})
	}
	return filterOpts
}

func (m *MetadataServer) buildSourceVariantFilterOpts(filterBody SourceVariantFilters) []query.Query {
	filterOpts := []query.Query{}
	usingV1Filters := false

	if filterBody.SearchTxt != "" {
		m.logger.Debugw("buildSourceVariantFilterOpts - adding a search txt filter: ", filterBody.SearchTxt)
		filterOpts = append(filterOpts, query.ValueLike{
			Column: query.JSONColumn{
				Path: []query.JSONPathStep{{Key: "Message", IsJsonString: true}, {Key: "name"}},
				Type: query.String,
			},
			Value: filterBody.SearchTxt,
		})
	}

	if len(filterBody.Types) > 0 {
		usingV1Filters = true
		m.logger.Debugw("buildSourceVariantFilterOpts - adding a source type filter: ", filterBody.Modes)
		datasetType := filterBody.Types[0] //only support 1 at a time for now

		if datasetType == "Primary Table" {
			filterOpts = append(filterOpts, query.ValueEquals{
				Not: true,
				Column: query.JSONColumn{
					Path: []query.JSONPathStep{{Key: "Message", IsJsonString: true}, {Key: "primaryData"}},
					Type: query.String,
				},
				Value: "NULL",
			})
		}

		if datasetType == "SQL Transformation" {
			filterOpts = append(filterOpts, query.ValueEquals{
				Not: true,
				Column: query.JSONColumn{
					Path: []query.JSONPathStep{{Key: "Message", IsJsonString: true}, {Key: "transformation", IsJsonString: true}, {Key: "SQLTransformation", IsJsonString: true}},
					Type: query.String,
				},
				Value: "NULL",
			})
		}

		if datasetType == "Dataframe Transformation" {
			filterOpts = append(filterOpts, query.ValueEquals{
				Not: true,
				Column: query.JSONColumn{
					Path: []query.JSONPathStep{{Key: "Message", IsJsonString: true}, {Key: "transformation", IsJsonString: true}, {Key: "DFTransformation", IsJsonString: true}},
					Type: query.String,
				},
				Value: "NULL",
			})
		}
	}

	if len(filterBody.Modes) > 0 {
		m.logger.Debugw("buildSourceVariantFilterOpts - adding a source mode filter: ", filterBody.Modes)
		usingV1Filters = true
		filterOpts = append(filterOpts, query.ValueIn{
			Column: query.JSONColumn{
				Path: []query.JSONPathStep{{Key: "Message", IsJsonString: true}, {Key: "mode"}},
				Type: query.String,
			},
			Values: convertToAny(filterBody.Modes),
		})
	}

	if len(filterBody.Statuses) > 0 {
		m.logger.Debugw("buildSourceVariantFilterOpts - adding a source status filter: ", filterBody.Statuses)
		usingV1Filters = true
		filterOpts = append(filterOpts, query.ValueIn{
			Column: query.JSONColumn{
				Path: []query.JSONPathStep{{Key: "Message", IsJsonString: true}, {Key: "status"}, {Key: "status"}},
				Type: query.String,
			},
			Values: convertToAny(filterBody.Statuses),
		})
	}

	if len(filterBody.Tags) > 0 {
		m.logger.Debugw("buildSourceVariantFilterOpts - adding a tag list filter: ", filterBody.Tags)
		usingV1Filters = true
		filterOpts = append(filterOpts, query.ArrayContains{
			Column: query.JSONColumn{
				Path: []query.JSONPathStep{{Key: "Message", IsJsonString: true}, {Key: "tags"}, {Key: "tag"}},
				Type: query.String,
			},
			Values: convertToAny(filterBody.Tags),
		})
	}

	if len(filterBody.Owners) > 0 {
		m.logger.Debugw("buildSourceVariantFilterOpts - adding a feature owner filter: ", filterBody.Owners)
		usingV1Filters = true
		filterOpts = append(filterOpts, query.ValueIn{
			Column: query.JSONColumn{
				Path: []query.JSONPathStep{{Key: "Message", IsJsonString: true}, {Key: "owner"}},
				Type: query.String,
			},
			Values: convertToAny(filterBody.Owners),
		})
	}

	if usingV1Filters {
		m.logger.Debugw("GetMetadataList - Using v1 filters, adding SerializedVersion clause (=1)")
		filterOpts = append(filterOpts, query.ValueEquals{
			Column: query.JSONColumn{
				Path: []query.JSONPathStep{{Key: serializedVersion}},
				Type: query.String,
			},
			Value: serializedV1,
		})
	}
	return filterOpts
}

func (m *MetadataServer) buildEntityFilterOpts(filterBody EntityFilters) []query.Query {
	filterOpts := []query.Query{}
	usingV1Filters := false

	if filterBody.SearchTxt != "" {
		m.logger.Debugw("buildEntityFilterOpts - adding a search txt filter: ", filterBody.SearchTxt)
		filterOpts = append(filterOpts, query.ValueLike{
			Column: query.JSONColumn{
				Path: []query.JSONPathStep{{Key: "Message", IsJsonString: true}, {Key: "name"}},
				Type: query.String,
			},
			Value: filterBody.SearchTxt,
		})
	}

	if len(filterBody.Statuses) > 0 {
		m.logger.Debugw("buildEntityFilterOpts - adding a feature status filter: ", filterBody.Statuses)
		usingV1Filters = true
		filterOpts = append(filterOpts, query.ValueIn{
			Column: query.JSONColumn{
				Path: []query.JSONPathStep{{Key: "Message", IsJsonString: true}, {Key: "status"}, {Key: "status"}},
				Type: query.String,
			},
			Values: convertToAny(filterBody.Statuses),
		})
	}

	if len(filterBody.Tags) > 0 {
		m.logger.Debugw("buildEntityFilterOpts - adding a tag list filter: ", filterBody.Tags)
		usingV1Filters = true
		filterOpts = append(filterOpts, query.ArrayContains{
			Column: query.JSONColumn{
				Path: []query.JSONPathStep{{Key: "Message", IsJsonString: true}, {Key: "tags"}, {Key: "tag"}},
				Type: query.String,
			},
			Values: convertToAny(filterBody.Tags),
		})
	}

	if len(filterBody.Labels) > 0 {
		m.logger.Debugw("buildEntityFilterOpts - adding a label list filter: ", filterBody.Labels)
		usingV1Filters = true
		filterOpts = append(filterOpts, query.ObjectArrayContains{
			Column: query.JSONColumn{
				Path: []query.JSONPathStep{{Key: "Message", IsJsonString: true}, {Key: "labels"}},
				Type: query.Object,
			},
			Values:      convertToAny(filterBody.Labels),
			SearchField: "name",
		})
	}

	if len(filterBody.Features) > 0 {
		m.logger.Debugw("buildEntityFilterOpts - adding a feature list filter: ", filterBody.Features)
		usingV1Filters = true
		filterOpts = append(filterOpts, query.ObjectArrayContains{
			Column: query.JSONColumn{
				Path: []query.JSONPathStep{{Key: "Message", IsJsonString: true}, {Key: "features"}},
				Type: query.Object,
			},
			Values:      convertToAny(filterBody.Features),
			SearchField: "name",
		})
	}

	if len(filterBody.Owners) > 0 {
		m.logger.Debugw("buildEntityFilterOpts - adding a owner filter: ", filterBody.Owners)
		filterOpts = append(filterOpts, query.ValueIn{
			Column: query.JSONColumn{
				Path: []query.JSONPathStep{{Key: "Message", IsJsonString: true}, {Key: "owner"}},
				Type: query.String,
			},
			Values: convertToAny(filterBody.Owners),
		})
	}

	if usingV1Filters {
		m.logger.Debugw("GetMetadataList - Using v1 filters, adding SerializedVersion clause (=1)")
		filterOpts = append(filterOpts, query.ValueEquals{
			Column: query.JSONColumn{
				Path: []query.JSONPathStep{{Key: serializedVersion}},
				Type: query.String,
			},
			Value: serializedV1,
		})
	}
	return filterOpts
}

func (m *MetadataServer) buildTrainingSetVariantFilterOpts(filterBody TrainingSetVariantFilters) []query.Query {
	filterOpts := []query.Query{}
	usingV1Filters := false

	if filterBody.SearchTxt != "" {
		m.logger.Debugw("buildTrainingSetFilterOpts - adding a search txt filter: ", filterBody.SearchTxt)
		filterOpts = append(filterOpts, query.ValueLike{
			Column: query.JSONColumn{
				Path: []query.JSONPathStep{{Key: "Message", IsJsonString: true}, {Key: "name"}},
				Type: query.String,
			},
			Value: filterBody.SearchTxt,
		})
	}

	if len(filterBody.Owners) > 0 {
		m.logger.Debugw("buildTrainingSetFilterOpts - adding a feature owner filter: ", filterBody.Owners)
		usingV1Filters = true
		filterOpts = append(filterOpts, query.ValueIn{
			Column: query.JSONColumn{
				Path: []query.JSONPathStep{{Key: "Message", IsJsonString: true}, {Key: "owner"}},
				Type: query.String,
			},
			Values: convertToAny(filterBody.Owners),
		})
	}

	if len(filterBody.Statuses) > 0 {
		m.logger.Debugw("buildTrainingSetFilterOpts - adding a feature status filter: ", filterBody.Statuses)
		usingV1Filters = true
		filterOpts = append(filterOpts, query.ValueIn{
			Column: query.JSONColumn{
				Path: []query.JSONPathStep{{Key: "Message", IsJsonString: true}, {Key: "status"}, {Key: "status"}},
				Type: query.String,
			},
			Values: convertToAny(filterBody.Statuses),
		})
	}

	if len(filterBody.Tags) > 0 {
		m.logger.Debugw("buildTrainingSetFilterOpts - adding a tag list filter: ", filterBody.Tags)
		usingV1Filters = true
		filterOpts = append(filterOpts, query.ArrayContains{
			Column: query.JSONColumn{
				Path: []query.JSONPathStep{{Key: "Message", IsJsonString: true}, {Key: "tags"}, {Key: "tag"}},
				Type: query.String,
			},
			Values: convertToAny(filterBody.Tags),
		})
	}

	if len(filterBody.Labels) > 0 {
		m.logger.Debugw("buildTrainingSetFilterOpts - adding a label list filter: ", filterBody.Labels)
		usingV1Filters = true
		filterOpts = append(filterOpts, query.ValueIn{
			Column: query.JSONColumn{
				Path: []query.JSONPathStep{{Key: "Message", IsJsonString: true}, {Key: "label"}, {Key: "name"}},
				Type: query.String,
			},
			Values: convertToAny(filterBody.Labels),
		})
	}

	if len(filterBody.Providers) > 0 {
		m.logger.Debugw("buildTrainingSetFilterOpts - adding a provider list filter: ", filterBody.Providers)
		usingV1Filters = true
		filterOpts = append(filterOpts, query.ValueIn{
			Column: query.JSONColumn{
				Path: []query.JSONPathStep{{Key: "Message", IsJsonString: true}, {Key: "provider"}},
				Type: query.String,
			},
			Values: convertToAny(filterBody.Providers),
		})
	}

	if usingV1Filters {
		m.logger.Debugw("GetMetadataList - Using v1 filters, adding SerializedVersion clause (=1)")
		filterOpts = append(filterOpts, query.ValueEquals{
			Column: query.JSONColumn{
				Path: []query.JSONPathStep{{Key: serializedVersion}},
				Type: query.String,
			},
			Value: serializedV1,
		})
	}
	return filterOpts
}

func (m *MetadataServer) getTrainingSetMetadataList(c *gin.Context, pageSize, offset int) {
	resourceType := metadata.TRAINING_SET
	count, mResources, err := m.getCountAndResources(resourceType, pageSize, offset)
	if err != nil {
		fetchError := m.GetRequestError(http.StatusInternalServerError, err, c, "getCountAndResources - Failed to get count or list")
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}
	resourceList := make([]TrainingSetResource, len(mResources))
	for i, parsedResource := range mResources {
		deserialized := parsedResource.Proto()
		trainingSet, ok := deserialized.(*pb.TrainingSet)
		if !ok {
			m.logger.Errorw("Could not deserialize resource with ID: %s", parsedResource.ID().String())
			continue
		}
		wrappedResource := metadata.WrapProtoTrainingSet(trainingSet)
		variantList, fetchError := m.readFromTrainingSet(wrappedResource, false)
		if fetchError != nil {
			m.logger.Errorw(fetchError.Error())
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		resourceList[i] = TrainingSetResource{
			AllVariants:    wrappedResource.Variants(),
			Type:           "TrainingSet",
			DefaultVariant: wrappedResource.DefaultVariant(),
			Name:           wrappedResource.Name(),
			Variants:       variantList,
		}
	}
	resp := GetMetadataListResp{
		Count:        count,
		ResourceList: resourceList,
	}
	c.JSON(http.StatusOK, resp)
}

func (m *MetadataServer) getSourceMetadataList(c *gin.Context, pageSize, offset int) {
	resourceType := metadata.SOURCE
	count, mResources, err := m.getCountAndResources(resourceType, pageSize, offset)
	if err != nil {
		fetchError := m.GetRequestError(http.StatusInternalServerError, err, c, "getCountAndResources - Failed to get count or list")
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}
	resourceList := make([]SourceResource, len(mResources))
	for i, parsedResource := range mResources {
		deserialized := parsedResource.Proto()
		source, ok := deserialized.(*pb.Source)
		if !ok {
			m.logger.Errorw("Could not deserialize resource with ID: %s", parsedResource.ID().String())
			continue
		}
		wrappedResource := metadata.WrapProtoSource(source)
		variantList, fetchError := m.readFromSource(wrappedResource, false)
		if fetchError != nil {
			m.logger.Errorw(fetchError.Error())
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		resourceList[i] = SourceResource{
			AllVariants:    wrappedResource.Variants(),
			Type:           "Source",
			DefaultVariant: wrappedResource.DefaultVariant(),
			Name:           wrappedResource.Name(),
			Variants:       variantList,
		}

	}
	resp := GetMetadataListResp{
		Count:        count,
		ResourceList: resourceList,
	}
	c.JSON(http.StatusOK, resp)
}

func (m *MetadataServer) getLabelMetadataList(c *gin.Context, pageSize, offset int) {
	resourceType := metadata.LABEL
	count, mResources, err := m.getCountAndResources(resourceType, pageSize, offset)
	if err != nil {
		fetchError := m.GetRequestError(http.StatusInternalServerError, err, c, "getCountAndResources - Failed to get count or list")
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}
	resourceList := make([]LabelResource, len(mResources))
	for i, parsedResource := range mResources {
		deserialized := parsedResource.Proto()
		label, ok := deserialized.(*pb.Label)
		if !ok {
			m.logger.Errorw("Could not deserialize resource with ID: %s", parsedResource.ID().String())
			continue
		}
		wrappedResource := metadata.WrapProtoLabel(label)
		variantList, fetchError := m.readFromLabel(wrappedResource, false)
		if fetchError != nil {
			m.logger.Errorw(fetchError.Error())
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		resourceList[i] = LabelResource{
			AllVariants:    wrappedResource.Variants(),
			Type:           "Label",
			DefaultVariant: wrappedResource.DefaultVariant(),
			Name:           wrappedResource.Name(),
			Variants:       variantList,
		}
	}
	resp := GetMetadataListResp{
		Count:        count,
		ResourceList: resourceList,
	}
	c.JSON(http.StatusOK, resp)
}

func (m *MetadataServer) getEntityMetadataList(c *gin.Context, pageSize, offset int) {
	resourceType := metadata.ENTITY
	count, mResources, err := m.getCountAndResources(resourceType, pageSize, offset)
	if err != nil {
		fetchError := m.GetRequestError(http.StatusInternalServerError, err, c, "getCountAndResources - Failed to get count or list")
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}
	resourceList := make([]metadata.EntityResource, len(mResources))
	for i, parsedResource := range mResources {
		deserialized := parsedResource.Proto()
		entity, ok := deserialized.(*pb.Entity)
		if !ok {
			m.logger.Errorw("Could not deserialize resource with ID: %s", parsedResource.ID().String())
			continue
		}
		wrappedResource := metadata.WrapProtoEntity(entity)
		resourceList[i] = metadata.EntityResource{
			Name:        wrappedResource.Name(),
			Type:        "Entity",
			Description: wrappedResource.Description(),
			Status:      wrappedResource.Status().String(),
			Tags:        wrappedResource.Tags(),
			Properties:  wrappedResource.Properties(),
		}
	}
	resp := GetMetadataListResp{
		Count:        count,
		ResourceList: resourceList,
	}
	c.JSON(http.StatusOK, resp)
}

func (m *MetadataServer) getModelMetadataList(c *gin.Context, pageSize, offset int) {
	resourceType := metadata.MODEL
	count, mResources, err := m.getCountAndResources(resourceType, pageSize, offset)
	if err != nil {
		fetchError := m.GetRequestError(http.StatusInternalServerError, err, c, "getCountAndResources - Failed to get count or list")
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}
	resourceList := make([]ModelResource, len(mResources))
	for i, parsedResource := range mResources {
		deserialized := parsedResource.Proto()
		model, ok := deserialized.(*pb.Model)
		if !ok {
			m.logger.Errorw("Could not deserialize resource with ID: %s", parsedResource.ID().String())
			continue
		}
		wrappedResource := metadata.WrapProtoModel(model)
		resourceList[i] = ModelResource{
			Name:        wrappedResource.Name(),
			Type:        "Model",
			Description: wrappedResource.Description(),
			Status:      wrappedResource.Status().String(),
			Tags:        wrappedResource.Tags(),
			Properties:  wrappedResource.Properties(),
		}
	}
	resp := GetMetadataListResp{
		Count:        count,
		ResourceList: resourceList,
	}
	c.JSON(http.StatusOK, resp)
}

func (m *MetadataServer) getUserMetadataList(c *gin.Context, pageSize, offset int) {
	resourceType := metadata.USER
	count, mResources, err := m.getCountAndResources(resourceType, pageSize, offset)
	if err != nil {
		fetchError := m.GetRequestError(http.StatusInternalServerError, err, c, "getCountAndResources - Failed to get count or list")
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}
	resourceList := make([]UserResource, len(mResources))
	for i, parsedResource := range mResources {
		deserialized := parsedResource.Proto()
		user, ok := deserialized.(*pb.User)
		if !ok {
			m.logger.Errorw("Could not deserialize resource with ID: %s", parsedResource.ID().String())
			continue
		}
		wrappedResource := metadata.WrapProtoUser(user)
		resourceList[i] = UserResource{
			Name:       wrappedResource.Name(),
			Type:       "User",
			Status:     wrappedResource.Status().String(),
			Tags:       wrappedResource.Tags(),
			Properties: wrappedResource.Properties(),
		}
	}
	resp := GetMetadataListResp{
		Count:        count,
		ResourceList: resourceList,
	}
	c.JSON(http.StatusOK, resp)
}

func (m *MetadataServer) getProviderMetadataList(c *gin.Context, pageSize, offset int) {
	var filterBody ProviderFilters
	if err := c.BindJSON(&filterBody); err != nil {
		fetchError := m.GetRequestError(http.StatusInternalServerError, err, c, "ProviderFilters - Error binding the request body")
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}

	filterOpts := m.buildProviderFilterOpts(filterBody)
	resourceType := metadata.PROVIDER
	count, mResources, err := m.getCountAndResources(resourceType, pageSize, offset, filterOpts...)
	if err != nil {
		fetchError := m.GetRequestError(http.StatusInternalServerError, err, c, "getCountAndResources - Failed to get count or list")
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}

	resourceList := make([]metadata.ProviderResource, len(mResources))
	for i, parsedResource := range mResources {
		deserialized := parsedResource.Proto()
		provider, ok := deserialized.(*pb.Provider)
		if !ok {
			m.logger.Errorw("Could not deserialize resource with ID: %s", parsedResource.ID().String())
			continue
		}
		wrappedResource := metadata.WrapProtoProvider(provider)

		//log only, don't want to crash the response if a periphery record returns an error.
		sources, sourcesErr := m.getSources(wrappedResource.Sources())
		if sourcesErr != nil {
			m.logger.Errorw("getSources() returned an error for:", "Provider", provider.Name, sourcesErr)
		}
		features, featuresErr := m.getFeatures(wrappedResource.Features())
		if featuresErr != nil {
			m.logger.Errorw("getFeatures() returned an error for:", "Provider", provider.Name, featuresErr)
		}
		labels, labelsErr := m.getLabels(wrappedResource.Labels())
		if labelsErr != nil {
			m.logger.Errorw("getLabels() returned an error for:", "Provider", provider.Name, labelsErr)
		}
		trainingSets, tsErr := m.getTrainingSets(wrappedResource.TrainingSets())
		if tsErr != nil {
			m.logger.Errorw("getTrainingSets() returned an error for:", "Provider", provider.Name, tsErr)
		}

		resourceList[i] = metadata.ProviderResource{
			Name:         wrappedResource.Name(),
			Description:  wrappedResource.Description(),
			Type:         "Provider",
			Software:     wrappedResource.Software(),
			Team:         wrappedResource.Team(),
			Sources:      sources,
			Features:     features,
			Labels:       labels,
			TrainingSets: trainingSets,
			ProviderType: wrappedResource.Type(),
			Status:       wrappedResource.Status().String(),
			Tags:         wrappedResource.Tags(),
			Properties:   wrappedResource.Properties(),
		}
	}
	resp := GetMetadataListResp{
		Count:        count,
		ResourceList: resourceList,
	}
	c.JSON(http.StatusOK, resp)
}

func (m *MetadataServer) buildProviderFilterOpts(filterBody ProviderFilters) []query.Query {
	filterOpts := []query.Query{}
	usingV1Filters := false

	if filterBody.SearchTxt != "" {
		m.logger.Debugw("buildProviderFilterOpts - adding a search txt filter: ", filterBody.SearchTxt)
		filterOpts = append(filterOpts, query.ValueLike{
			Column: query.JSONColumn{
				Path: []query.JSONPathStep{{Key: "Message", IsJsonString: true}, {Key: "name"}},
				Type: query.String,
			},
			Value: filterBody.SearchTxt,
		})
	}

	if len(filterBody.ProviderType) > 0 {
		usingV1Filters = true
		var typeList []pt.Type
		if slices.Contains(filterBody.ProviderType, Online) {
			typeList = append(typeList, pt.GetOnlineTypes()...)
		}

		if slices.Contains(filterBody.ProviderType, Offline) {
			typeList = append(typeList, pt.GetOfflineTypes()...)
		}

		if slices.Contains(filterBody.ProviderType, File) {
			typeList = append(typeList, pt.GetFileTypes()...)
		}

		filterOpts = append(filterOpts, query.ValueIn{
			Column: query.JSONColumn{
				Path: []query.JSONPathStep{{Key: "Message", IsJsonString: true}, {Key: "type"}},
				Type: query.String,
			},
			Values: convertToAny(typeList),
		})
	}

	if len(filterBody.Status) > 0 {
		usingV1Filters = true
		var statusList []string
		if slices.Contains(filterBody.Status, Connected) {
			statusList = append(statusList, pb.ResourceStatus_READY.String())
		}
		if slices.Contains(filterBody.Status, Disconnected) {
			statusList = append(statusList, []string{pb.ResourceStatus_RUNNING.String(), pb.ResourceStatus_NO_STATUS.String(),
				pb.ResourceStatus_PENDING.String(), pb.ResourceStatus_CREATED.String(), pb.ResourceStatus_FAILED.String()}...)
		}

		filterOpts = append(filterOpts, query.ValueIn{
			Column: query.JSONColumn{
				Path: []query.JSONPathStep{{Key: "Message", IsJsonString: true}, {Key: "status"}, {Key: "status"}},
				Type: query.String,
			},
			Values: convertToAny(statusList),
		})
	}

	if usingV1Filters {
		m.logger.Debugw("GetMetadataList - Using v1 filters, adding SerializedVersion clause (=1)")
		filterOpts = append(filterOpts, query.ValueEquals{
			Column: query.JSONColumn{
				Path: []query.JSONPathStep{{Key: serializedVersion}},
				Type: query.String,
			},
			Value: serializedV1,
		})
	}
	return filterOpts
}

func (m *MetadataServer) FailRunningJobs(c *gin.Context) {
	config := postgres.Config{
		Host:     help.GetEnv("PSQL_HOST", "localhost"),
		Port:     help.GetEnv("PSQL_PORT", "5432"),
		User:     help.GetEnv("PSQL_USER", "postgres"),
		Password: help.GetEnv("PSQL_PASSWORD", "password"),
		DBName:   help.GetEnv("PSQL_DB", "postgres"),
		SSLMode:  help.GetEnv("PSQL_SSLMODE", "disable"),
	}
	db, err := postgres.NewPool(c, config)
	if err != nil {
		c.JSON(400, fmt.Sprintf("could not create database connection: %s", err.Error()))
		return
	}

	connection, err := db.Acquire(c)
	if err != nil {
		c.JSON(400, fferr.NewInternalError(fmt.Errorf("failed to acquire connection from the database pool: %w", err)))
		return
	}
	defer connection.Release()

	err = connection.Ping(c)
	if err != nil {
		c.JSON(400, fferr.NewInternalErrorf("failed to ping the database: %w", err))
		return
	}
	_, err = connection.Exec(c, "UPDATE ff_task_metadata SET value = jsonb_set(value::jsonb, '{status}', '4', false) WHERE key LIKE '/tasks/runs/metadata/%'  AND (value::jsonb ->> 'status')::int = 5;")
	if err != nil {
		c.JSON(400, fmt.Sprintf("failed to run query: %s", err.Error()))
		return
	}
	c.JSON(200, "Status change complete")
}

func (m *MetadataServer) GetSearch(c *gin.Context) {
	query, ok := c.GetQuery("q")
	if !ok {
		c.JSON(http.StatusInternalServerError, "Missing query")
		return
	}

	result, err := SearchClient.RunSearch(query)
	if err != nil {
		m.logger.Errorw("Failed to fetch resources", "error", err)
		c.JSON(http.StatusInternalServerError, "Failed to fetch resources")
		return
	}
	c.JSON(http.StatusOK, result)
}

func (m *MetadataServer) GetVersionMap(c *gin.Context) {
	versionMap := map[string]string{
		"version": help.GetEnv("FEATUREFORM_VERSION", ""),
	}
	c.JSON(http.StatusOK, versionMap)
}

type SourceDataResponse struct {
	Columns []string   `json:"columns"`
	Rows    [][]string `json:"rows"`
}

const MaxPreviewCols = 15

func (m *MetadataServer) GetSourceData(c *gin.Context) {
	name := c.Query("name")
	variant := c.Query("variant")

	if name == "" || variant == "" {
		fetchError := &FetchError{StatusCode: http.StatusBadRequest, Type: "GetSourceData - Could not find the name or variant query parameters"}
		m.logger.Errorw(fetchError.Error(), "Metadata error")
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}

	var limit int64 = 150
	response := SourceDataResponse{}

	iter, err := m.getSourceDataIterator(name, variant, limit)
	if err != nil {
		fetchError := &FetchError{StatusCode: http.StatusInternalServerError, Type: fmt.Sprintf("GetSourceData - %s", err.Error())}
		m.logger.Errorw(fetchError.Error(), "Metadata error", err)
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}

	for iter.Next() {
		sRow, err := serving.SerializedSourceRow(iter.Values())
		if err != nil {
			fetchError := &FetchError{StatusCode: http.StatusInternalServerError, Type: "GetSourceData"}
			m.logger.Errorw(fetchError.Error(), "Metadata error", err)
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		dataRow := []string{}
		for i, rowElement := range sRow.Rows {
			dataRow = append(dataRow, extractElementValue(rowElement))
			if i == MaxPreviewCols {
				dataRow = append(dataRow, "")
				break
			}
		}
		response.Rows = append(response.Rows, dataRow)
	}

	for i, columnName := range iter.Columns() {
		cleanName := strings.ReplaceAll(columnName, "\"", "")
		response.Columns = append(response.Columns, cleanName)
		if i == MaxPreviewCols {
			response.Columns = append(response.Columns, fmt.Sprintf("%d More Columns...", len(iter.Columns())-MaxPreviewCols))
			break
		}
	}

	if err := iter.Err(); err != nil {
		fetchError := &FetchError{StatusCode: http.StatusInternalServerError, Type: "GetSourceData"}
		m.logger.Errorw(fetchError.Error(), "Metadata error", err)
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}
	c.JSON(http.StatusOK, response)
}

/*
example proto.value args:
double_value:2544
str_value:"C7332112"
*/
func extractElementValue(rowString *proto.Value) string {
	split := strings.Split(rowString.String(), ":")
	result := strings.ReplaceAll(split[1], "\"", "")
	return result
}

func (m *MetadataServer) getSourceDataIterator(name, variant string, limit int64) (provider.GenericTableIterator, error) {
	ctx := context.TODO()
	m.logger.Infow("Getting Source Variant Iterator", "name", name, "variant", variant)
	sv, err := m.client.GetSourceVariant(ctx, metadata.NameVariant{Name: name, Variant: variant})
	if err != nil {
		return nil, err
	}
	providerEntry, err := sv.FetchProvider(m.client, ctx)
	m.logger.Debugw("Fetched Source Variant Provider", "name", providerEntry.Name(), "type", providerEntry.Type())
	if err != nil {
		return nil, err
	}
	p, err := provider.Get(pt.Type(providerEntry.Type()), providerEntry.SerializedConfig())
	if err != nil {
		return nil, err
	}
	store, err := p.AsOfflineStore()
	if err != nil {
		return nil, err
	}
	var primary provider.PrimaryTable
	var providerErr error
	if sv.IsTransformation() {
		t, err := store.GetTransformationTable(provider.ResourceID{Name: name, Variant: variant, Type: provider.Transformation})
		if err != nil {
			providerErr = err
		} else {
			providerErr = nil
			primary = t.(provider.PrimaryTable)
		}
	} else {
		primaryNameVariant := metadata.NameVariant{
			Name:    name,
			Variant: variant,
		}
		source, err := m.client.GetSourceVariant(context.Background(), primaryNameVariant)
		if err != nil {
			return nil, err
		}
		primary, providerErr = store.GetPrimaryTable(provider.ResourceID{Name: name, Variant: variant, Type: provider.Primary}, *source)
	}
	if providerErr != nil {
		return nil, providerErr
	}
	return primary.IterateSegment(limit)
}

type VariantResult interface {
	Name() string
	Variant() string
	Tags() metadata.Tags
}

type TagResult struct {
	Name    string   `json:"name"`
	Variant string   `json:"variant"`
	Tags    []string `json:"tags"`
}

func GetTagResult(param VariantResult) TagResult {
	return TagResult{
		Name:    param.Name(),
		Variant: param.Variant(),
		Tags:    param.Tags(),
	}
}

func (m *MetadataServer) GetRequestError(code int, err error, c *gin.Context, resourceType string) *FetchError {
	fetchError := &FetchError{StatusCode: code, Type: resourceType}
	m.logger.Errorw(fetchError.Error(), "Metadata error", err)
	return fetchError
}

func (m *MetadataServer) SetFoundVariantJSON(foundVariant VariantResult, err error, c *gin.Context, resourceType string) {
	if err != nil {
		fetchError := m.GetRequestError(http.StatusInternalServerError, err, c, resourceType)
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}
	c.JSON(http.StatusOK, GetTagResult(foundVariant))
}

type TagGetBody struct {
	Variant string `json:"variant"`
}

func (m *MetadataServer) GetTags(c *gin.Context) {
	name := c.Param("resource")
	resourceType := c.Param("type")
	var requestBody TagGetBody
	if err := c.BindJSON(&requestBody); err != nil {
		fetchError := m.GetRequestError(http.StatusInternalServerError, err, c, "GetTags - Error binding the request body")
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}
	nameVariant := metadata.NameVariant{Name: name, Variant: requestBody.Variant}
	switch resourceType {
	case "features":
		foundVariant, err := m.client.GetFeatureVariant(context.Background(), nameVariant)
		m.SetFoundVariantJSON(foundVariant, err, c, resourceType)
	case "labels":
		foundVariant, err := m.client.GetLabelVariant(context.Background(), nameVariant)
		m.SetFoundVariantJSON(foundVariant, err, c, resourceType)
	case "training-sets":
		foundVariant, err := m.client.GetTrainingSetVariant(context.Background(), nameVariant)
		m.SetFoundVariantJSON(foundVariant, err, c, resourceType)
	case "sources":
		foundVariant, err := m.client.GetSourceVariant(context.Background(), nameVariant)
		m.SetFoundVariantJSON(foundVariant, err, c, resourceType)
	case "entities":
		foundVariant, err := m.client.GetEntity(context.Background(), name)
		m.SetFoundVariantJSON(foundVariant, err, c, resourceType)
	case "users":
		foundVariant, err := m.client.GetUser(context.Background(), name)
		m.SetFoundVariantJSON(foundVariant, err, c, resourceType)
	case "models":
		foundVariant, err := m.client.GetModel(context.Background(), name)
		m.SetFoundVariantJSON(foundVariant, err, c, resourceType)
	case "providers":
		foundVariant, err := m.client.GetProvider(context.Background(), name)
		m.SetFoundVariantJSON(foundVariant, err, c, resourceType)
	}
}

type TypeList struct {
	Name string `json:"name"`
}

type FetchTypeListParams struct {
	ResourceType metadata.ResourceType
	Columns      []query.Column
	KeyName      string
	OrderBy      string
	Limit        int
}

func (m *MetadataServer) fetchTypeList(params FetchTypeListParams) ([]TypeList, error) {

	m.logger.Debugw("Using v1 filters, adding SerializedVersion clause (=\"1\")")
	queryOpts := []query.Query{
		query.ValueSort{
			Column: query.SQLColumn{Column: params.KeyName},
			Dir:    query.Desc,
		},
		query.GroupBy{Name: params.KeyName},
		query.ValueEquals{
			Column: query.JSONColumn{
				Path: []query.JSONPathStep{{Key: serializedVersion}},
				Type: query.String,
			},
			Value: serializedV1,
		},
		query.Limit{Limit: params.Limit},
	}

	records, err := m.StorageProvider.ListColumn(params.ResourceType.String(), params.Columns, queryOpts...)
	if err != nil {
		return nil, err
	}

	results := []TypeList{}
	for _, recordMap := range records {
		name, ok := recordMap[params.KeyName].(string)
		if !ok {
			m.logger.Debugw("Failed to cast key name", params.KeyName, recordMap[params.KeyName], params.OrderBy, recordMap[params.OrderBy])
			continue
		}
		name = strings.Trim(name, "\"")
		results = append(results, TypeList{Name: name})
	}

	return results, nil
}

func (m *MetadataServer) GetTypeTags(c *gin.Context) {
	resourceType := c.Param("type")

	resourceTypeMap := map[string]metadata.ResourceType{
		"features":      metadata.FEATURE,
		"labels":        metadata.LABEL,
		"training-sets": metadata.TRAINING_SET,
		"sources":       metadata.SOURCE,
		"entities":      metadata.ENTITY,
		"users":         metadata.USER,
		"models":        metadata.MODEL,
		"providers":     metadata.PROVIDER,
	}

	rType, ok := resourceTypeMap[resourceType]
	if !ok {
		fetchError := m.GetRequestError(http.StatusInternalServerError, errors.Errorf("Type param matches no types"), c, resourceType)
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}

	// setup 2 columns: tag, and total_count
	// data will return in this format: [map[tag:"v1" total_count:1], map[tag:"testing" total_count:2], ...]
	// each map represents a tabular "row"
	tagKey, orderBy := "tag", "total_count"
	columns := []query.Column{
		query.SQLColumn{
			Column: "(json_array_elements((VALUE::json->>'Message')::json->'tags'->'tag'))::text",
			Alias:  tagKey,
		},
		query.SQLColumn{
			Column: "COUNT(*)",
			Alias:  orderBy,
		},
	}

	params := FetchTypeListParams{
		ResourceType: rType,
		Columns:      columns,
		KeyName:      tagKey,
		OrderBy:      orderBy,
		Limit:        typeListLimit,
	}

	results, err := m.fetchTypeList(params)
	if err != nil {
		fetchError := m.GetRequestError(http.StatusInternalServerError, err, c, err.Error())
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}

	c.JSON(http.StatusOK, results)
}

// could explore making a property fetch generic for all callers
func (m *MetadataServer) GetTypeOwners(c *gin.Context) {
	resourceType := c.Param("type")

	resourceTypeMap := map[string]metadata.ResourceType{
		"features":      metadata.FEATURE,
		"labels":        metadata.LABEL,
		"training-sets": metadata.TRAINING_SET,
		"sources":       metadata.SOURCE,
		"entities":      metadata.ENTITY,
	}

	rType, ok := resourceTypeMap[resourceType]
	if !ok {
		fetchError := m.GetRequestError(http.StatusInternalServerError, errors.Errorf("Type param matches no types"), c, resourceType)
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}

	ownerKey, orderBy := "owner", "total_count"
	columns := []query.Column{
		query.SQLColumn{
			Column: "(VALUE::JSON ->> 'Message')::JSON ->> 'owner'",
			Alias:  ownerKey,
		},
		query.SQLColumn{
			Column: "COUNT(*)",
			Alias:  orderBy,
		},
	}

	params := FetchTypeListParams{
		ResourceType: rType,
		Columns:      columns,
		KeyName:      ownerKey,
		OrderBy:      orderBy,
		Limit:        typeListLimit,
	}

	results, err := m.fetchTypeList(params)
	if err != nil {
		fetchError := m.GetRequestError(http.StatusInternalServerError, err, c, err.Error())
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}

	c.JSON(http.StatusOK, results)
}

func (m *MetadataServer) getProviderNameTypeMap() (map[string]string, error) {
	providerName, providerType := "provider_name", "provider_type"
	columns := []query.Column{
		query.SQLColumn{
			Column: "(value::json->>'Message')::Json->>'name'",
			Alias:  providerName,
		},
		query.SQLColumn{
			Column: "(value::json->>'Message')::Json->>'type'",
			Alias:  providerType,
		},
	}

	queryOpts := []query.Query{
		query.ValueEquals{
			Column: query.JSONColumn{
				Path: []query.JSONPathStep{{Key: serializedVersion}},
				Type: query.String,
			},
			Value: serializedV1,
		},
	}

	records, err := m.StorageProvider.ListColumn(metadata.PROVIDER.String(), columns, queryOpts...)
	if err != nil {
		return nil, err
	}

	results := map[string]string{}
	for _, recordMap := range records {
		nameString, nameOk := recordMap[providerName].(string)
		if !nameOk {
			m.logger.Debugw("Failed to cast provider name", providerName, recordMap[providerName], recordMap[providerType])
			continue
		}

		typeString, typeOk := recordMap[providerType].(string)
		if !typeOk {
			m.logger.Debugw("Failed to cast provider type", providerName, recordMap[providerName], recordMap[providerType])
			continue
		}

		if nameOk && typeOk {
			nameString = strings.Trim(nameString, "\"")
			typeString = strings.Trim(typeString, "\"")
			results[nameString] = typeString
		}
	}
	return results, nil
}

type TagPostBody struct {
	Tags    []string `json:"tags"`
	Variant string   `json:"variant"`
}

func getResourceType(resourceTypeString string) metadata.ResourceType {
	var resourceType metadata.ResourceType
	switch resourceTypeString {
	case "features":
		resourceType = metadata.FEATURE_VARIANT
	case "labels":
		resourceType = metadata.LABEL_VARIANT
	case "training-sets":
		resourceType = metadata.TRAINING_SET_VARIANT
	case "sources":
		resourceType = metadata.SOURCE_VARIANT
	case "entities":
		resourceType = metadata.ENTITY
	case "users":
		resourceType = metadata.USER
	case "models":
		resourceType = metadata.MODEL
	case "providers":
		resourceType = metadata.PROVIDER
	}
	return resourceType
}

func (m *MetadataServer) PostTags(c *gin.Context) {
	var requestBody TagPostBody
	resourceTypeParam := c.Param("type")
	if err := c.BindJSON(&requestBody); err != nil {
		fetchError := m.GetRequestError(http.StatusInternalServerError, err, c, "PostTags - Error binding the request body")
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}
	resourceType := getResourceType(resourceTypeParam)
	name := c.Param("resource")
	variant := requestBody.Variant

	objID := metadata.ResourceID{
		Name:    name,
		Variant: variant,
		Type:    resourceType,
	}
	foundResource, err := m.lookup.Lookup(c, objID)

	if err != nil {
		fetchError := m.GetRequestError(http.StatusBadRequest, err, c, "PostTags - Error finding the resource with resourceID")
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}

	replaceTags(resourceTypeParam, foundResource, &pb.Tags{Tag: requestBody.Tags})

	m.lookup.Set(c, objID, foundResource)

	updatedResource := search.ResourceDoc{
		Name:    name,
		Variant: variant,
		Type:    resourceType.String(),
		Tags:    requestBody.Tags,
	}
	// Update search index for Meilisearch
	err = SearchClient.Upsert(updatedResource)
	if err != nil {
		m.logger.Error(err.Error())
	}

	c.JSON(http.StatusOK, TagResult{
		Name:    name,
		Variant: variant,
		Tags:    requestBody.Tags,
	})
}

func replaceTags(resourceTypeParam string, currentResource metadata.Resource, newTagList *pb.Tags) error {
	deserialized := currentResource.Proto()
	switch resourceTypeParam {
	case "features":
		variantUpdate, ok := deserialized.(*pb.FeatureVariant)
		if !ok {
			return errors.New("replaceTags - Failed to deserialize variant")
		}
		variantUpdate.Tags.Reset()
		variantUpdate.Tags = newTagList
	case "labels":
		variantUpdate, ok := deserialized.(*pb.LabelVariant)
		if !ok {
			return errors.New("replaceTags - Failed to deserialize variant")
		}
		variantUpdate.Tags.Reset()
		variantUpdate.Tags = newTagList
	case "training-sets":
		variantUpdate, ok := deserialized.(*pb.TrainingSetVariant)
		if !ok {
			return errors.New("replaceTags - Failed to deserialize variant")
		}
		variantUpdate.Tags.Reset()
		variantUpdate.Tags = newTagList
	case "sources":
		variantUpdate, ok := deserialized.(*pb.SourceVariant)
		if !ok {
			return errors.New("replaceTags - Failed to deserialize variant")
		}
		variantUpdate.Tags.Reset()
		variantUpdate.Tags = newTagList
	case "entities":
		variantUpdate, ok := deserialized.(*pb.Entity)
		if !ok {
			return errors.New("replaceTags - Failed to deserialize variant")
		}
		variantUpdate.Tags.Reset()
		variantUpdate.Tags = newTagList
	case "users":
		variantUpdate, ok := deserialized.(*pb.User)
		if !ok {
			return errors.New("replaceTags - Failed to deserialize variant")
		}
		variantUpdate.Tags.Reset()
		variantUpdate.Tags = newTagList
	case "models":
		variantUpdate, ok := deserialized.(*pb.Model)
		if !ok {
			return errors.New("replaceTags - Failed to deserialize variant")
		}
		variantUpdate.Tags.Reset()
		variantUpdate.Tags = newTagList
	case "providers":
		variantUpdate, ok := deserialized.(*pb.Provider)
		if !ok {
			return errors.New("replaceTags - Failed to deserialize variant")
		}
		variantUpdate.Tags.Reset()
		variantUpdate.Tags = newTagList
	}
	return nil
}

type TaskRunListResponse struct {
	TaskRunList []TaskRunItem `json:"list"`
	Count       int           `json:"count"`
}

type TaskRunItem struct {
	Task    sc.TaskMetadata    `json:"task"`
	TaskRun sc.TaskRunMetadata `json:"taskRun"`
}

type PaginateRequest struct {
	Status        string `json:"status"`
	SearchText    string `json:"searchText"`
	VariantSearch string `json:"variantSearch"`
	SortBy        string `json:"sortBy"`
	PageSize      int    `json:"pageSize"`
	Offset        int    `json:"offset"`
}

func (m MetadataServer) getResourceQuery(prefix string) []query.Query {
	queryList := []query.Query{
		query.KeyPrefix{Not: true, Prefix: prefix + "_VARIANT"},
	}
	return queryList
}

func (m MetadataServer) getTaskRunsQuery(requestBody PaginateRequest, isCount bool) []query.Query {
	queryList := []query.Query{}

	if requestBody.Status != "ALL" {
		var statusList []any
		switch requestBody.Status {
		case "ACTIVE":
			statusList = []any{pb.ResourceStatus_RUNNING}
		case "COMPLETE":
			statusList = []any{pb.ResourceStatus_CREATED, pb.ResourceStatus_READY, pb.ResourceStatus_FAILED}
		default:
			m.logger.Warnf("the request status (%s) did not match any cases", requestBody.Status)
		}
		if statusList != nil {
			queryList = append(queryList, query.ValueIn{
				Column: query.JSONColumn{
					Path: []query.JSONPathStep{{Key: "status"}},
					Type: query.Int,
				},
				Values: statusList,
			})
		}
	}

	if requestBody.VariantSearch != "" && requestBody.SearchText != "" {
		nameSearch := query.ValueEquals{
			Column: query.JSONColumn{
				Path: []query.JSONPathStep{{Key: "target", IsJsonString: true}, {Key: "name"}},
				Type: query.String,
			},
			Value: requestBody.SearchText,
		}
		queryList = append(queryList, nameSearch)

		variantSearch := query.ValueEquals{
			Column: query.JSONColumn{
				Path: []query.JSONPathStep{{Key: "target", IsJsonString: true}, {Key: "variant"}},
				Type: query.String,
			},
			Value: requestBody.VariantSearch,
		}
		queryList = append(queryList, variantSearch)

	} else if requestBody.SearchText != "" {
		search := query.ValueLike{
			Column: query.JSONColumn{
				Path: []query.JSONPathStep{{Key: "target", IsJsonString: true}, {Key: "name"}},
				Type: query.String,
			},
			Value: requestBody.SearchText,
		}
		queryList = append(queryList, search)
	}

	if !isCount {
		pagination := query.Limit{
			Limit:  requestBody.PageSize,
			Offset: requestBody.Offset * requestBody.PageSize,
		}
		queryList = append(queryList, pagination)

		switch requestBody.SortBy {
		case "DATE":
			queryList = append(queryList, query.ValueSort{
				Column: query.JSONColumn{
					Path: []query.JSONPathStep{{Key: "startTime"}},
					Type: query.Timestamp,
				},
				Dir: query.Desc,
			})
		case "STATUS":
			queryList = append(queryList, query.ValueSort{
				Column: query.JSONColumn{
					Path: []query.JSONPathStep{{Key: "status"}},
					Type: query.Int,
				},
				Dir: query.Desc,
			})
		case "RUN_ID":
			queryList = append(queryList, query.ValueSort{
				Column: query.JSONColumn{
					Path: []query.JSONPathStep{{Key: "runId"}},
					Type: query.Int,
				},
				Dir: query.Desc,
			})
		default:
			queryList = append(queryList, query.ValueSort{
				Column: query.JSONColumn{
					Path: []query.JSONPathStep{{Key: "runId"}},
					Type: query.Int,
				},
				Dir: query.Desc,
			})
		}
	}
	return queryList
}

func (m *MetadataServer) GetTaskRuns(c *gin.Context) {
	var requestBody PaginateRequest

	if err := c.BindJSON(&requestBody); err != nil {
		fetchError := m.GetRequestError(http.StatusBadRequest, err, c, "GetTaskRuns - Error binding the request body")
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}

	if requestBody.PageSize <= 0 {
		fetchError := &FetchError{StatusCode: http.StatusBadRequest, Type: fmt.Sprintf("GetTaskRuns - Error invalid pageSize value: %d", requestBody.PageSize)}
		m.logger.Errorw(fetchError.Error())
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}

	if requestBody.Offset < 0 {
		fetchError := &FetchError{StatusCode: http.StatusBadRequest, Type: fmt.Sprintf("GetTaskRuns - Error invalid offset value: %d", requestBody.Offset)}
		m.logger.Errorw(fetchError.Error(), "Metadata Error invalid offset value:", requestBody.Offset)
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}

	countQuery := m.getTaskRunsQuery(requestBody, true)
	count, err := m.StorageProvider.Count(sc.TaskRunMetadataKey{}.String(), countQuery...)
	if err != nil {
		fetchError := m.GetRequestError(http.StatusInternalServerError, err, c, "GetTaskRuns - Failed to get count")
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}

	listQuery := m.getTaskRunsQuery(requestBody, false)
	records, err := m.StorageProvider.List(sc.TaskRunMetadataKey{}.String(), listQuery...)
	if err != nil {
		fetchError := m.GetRequestError(http.StatusInternalServerError, err, c, "GetTaskRuns - Failed to fetch task runs")
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}

	var runs []sc.TaskRunMetadata
	for _, record := range records {
		taskRun := sc.TaskRunMetadata{}
		err = taskRun.Unmarshal([]byte(record))
		if err != nil {
			fetchError := m.GetRequestError(http.StatusInternalServerError, err, c, "GetTaskRuns - Failed to fetch task runs")
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		runs = append(runs, taskRun)
	}

	taskRunItems := make([]TaskRunItem, 0)
	for _, run := range runs {
		task, err := m.client.Tasks.GetTaskByID(run.TaskId)
		if err != nil {
			fetchError := m.GetRequestError(http.StatusInternalServerError, err, c, "GetTaskRuns - Failed to fetch task")
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}

		taskRunItems = append(taskRunItems, TaskRunItem{Task: task, TaskRun: run})
	}

	c.JSON(http.StatusOK, TaskRunListResponse{TaskRunList: taskRunItems, Count: count})
}

type OtherRun struct {
	ID        sc.TaskRunID `json:"runId"`
	StartTime time.Time    `json:"startTime"`
	Status    sc.Status    `json:"status"`
	Link      string       `json:"link"`
}

type TaskRunDetailResponse struct {
	TaskRun   sc.TaskRunMetadata `json:"taskRun"`
	OtherRuns []OtherRun         `json:"otherRuns"`
}

func (m *MetadataServer) GetTaskRunDetails(c *gin.Context) {
	strTaskID := c.Param("taskId")
	strTaskRunId := c.Param("taskRunId")

	taskID, err := sc.ParseTaskID(strTaskID)
	if err != nil {
		fetchError := &FetchError{StatusCode: http.StatusBadRequest, Type: "GetTaskRunDetails - Could not find the taskRunId parameter"}
		m.logger.Errorw(fetchError.Error(), "Metadata error")
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}

	taskRunID, err := sc.ParseTaskRunID(strTaskRunId)
	if err != nil {
		fetchError := &FetchError{StatusCode: 400, Type: "GetTaskRunDetails - Could not convert the given taskRunId"}
		m.logger.Errorw(fetchError.Error(), "Metadata error")
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}

	runs, err := m.client.Tasks.GetRuns(taskID)
	if err != nil {
		fetchError := m.GetRequestError(http.StatusInternalServerError, err, c, "GetTaskRuns - Failed to fetch task")
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}

	var otherRuns []OtherRun
	var selectedRun sc.TaskRunMetadata
	for _, run := range runs {
		if !run.ID.Equals(taskRunID) {
			otherRuns = append(otherRuns, OtherRun{ID: run.ID, StartTime: run.StartTime, Status: run.Status, Link: ""})
		} else {
			selectedRun = run
		}
	}

	resp := TaskRunDetailResponse{
		TaskRun:   selectedRun,
		OtherRuns: otherRuns,
	}

	c.JSON(http.StatusOK, resp)
}

func (m *MetadataServer) Start(port string, local bool) error {
	router := gin.Default()
	if local {
		conf := cors.DefaultConfig()
		conf.AllowHeaders = []string{"Authorization", "Content-Type,access-control-allow-origin, access-control-allow-headers"}
		conf.AllowAllOrigins = true
		router.Use(cors.New(conf))
	} else {
		router.Use(cors.Default())
	}
	router.POST("/data/:type", m.GetMetadataList)
	router.GET("/data/failrunning", m.FailRunningJobs)
	router.GET("/data/:type/:resource", m.GetMetadata)
	router.GET("/data/search", m.GetSearch)
	router.GET("/data/version", m.GetVersionMap)
	router.GET("/data/sourcedata", m.GetSourceData)
	router.POST("/data/:type/:resource/gettags", m.GetTags)
	router.POST("/data/:type/:resource/tags", m.PostTags)
	router.POST("/data/taskruns", m.GetTaskRuns)
	router.GET("/data/taskruns/taskrundetail/:taskId/:taskRunId", m.GetTaskRunDetails)
	router.GET("/data/:type/prop/tags", m.GetTypeTags)
	router.POST("/data/feature/variants", m.GetFeatureVariantResources)
	router.POST("/data/label/variants", m.GetLabelVariantResources)
	router.POST("/data/datasets/variants", m.GetSourceVariantResources)
	router.POST("/data/training-sets/variants", m.GetTrainingSetVariantResources)
	router.POST("/data/entities", m.GetEntityResources)
	router.POST("/data/providers", m.GetProviderResources)
	router.POST("/data/models", m.GetModelResources)
	router.GET("/data/:type/prop/owners", m.GetTypeOwners)

	return router.Run(port)
}
