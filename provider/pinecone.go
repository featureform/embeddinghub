// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	pl "github.com/featureform/provider/location"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/featureform/fferr"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/provider/types"
	"github.com/google/uuid"
)

const (
	/*
		Pinecone index names can only be 45 characters long and can only contain
		lowercase letters, numbers, and hyphens. To save space, we use a shorter
		prefix than those used by other online providers.
	*/
	prefixTemplate    = "ff-idx--%s"
	namespaceTemplate = "ff-namespace--%s-%s"
	/*
		The subdomain for index operations only need the environment.
	*/
	indexOperationURLTemplate = "controller.%s"
	/*
		The subdomain for vector operations is composed of the following, in order:
		1. The index name
		2. The project ID
		3. The environment
	*/
	vectorOperationURLTemplate = "%s-%s.svc.%s"
	nameVariantTemplate        = "%s-%s"
)

type pineconeOnlineStore struct {
	client *pineconeAPI
	prefix string
	BaseProvider
}

func pineconeOnlineStoreFactory(serialized pc.SerializedConfig) (Provider, error) {
	pineconeConfig := &pc.PineconeConfig{}
	if err := pineconeConfig.Deserialize(serialized); err != nil {
		return nil, err
	}
	return NewPineconeOnlineStore(pineconeConfig)
}

func NewPineconeOnlineStore(options *pc.PineconeConfig) (*pineconeOnlineStore, error) {
	return &pineconeOnlineStore{
		client: NewPineconeAPI(options),
		prefix: prefixTemplate,
		BaseProvider: BaseProvider{
			ProviderType:   pt.PineconeOnline,
			ProviderConfig: options.Serialize(),
		},
	}, nil
}

func (store *pineconeOnlineStore) AsOnlineStore() (OnlineStore, error) {
	return store, nil
}

func (store *pineconeOnlineStore) Close() error {
	// implementation of Close method
	return nil
}

func (store *pineconeOnlineStore) CreateTable(feature, variant string, valueType types.ValueType) (OnlineStoreTable, error) {
	return &pineconeOnlineTable{
		api:       store.client,
		indexName: store.createIndexName(feature, variant),
		namespace: fmt.Sprintf(namespaceTemplate, feature, variant),
		valueType: valueType,
	}, nil
}

func (store *pineconeOnlineStore) DeleteTable(feature, variant string) error {
	// implementation of DeleteTable method
	return nil
}

func (store *pineconeOnlineStore) CheckHealth() (bool, error) {
	return false, fferr.NewInternalError(fmt.Errorf("not implemented"))
}

func (store *pineconeOnlineStore) Delete(location pl.Location) error {
	return fferr.NewInternalErrorf("delete not implemented")
}

func (store *pineconeOnlineStore) CreateIndex(feature, variant string, vectorType types.VectorType) (VectorStoreTable, error) {
	indexName := store.createIndexName(feature, variant)
	if err := store.client.createIndex(indexName, vectorType.Dimension); err != nil {
		return nil, err
	}
	// Given Pinecone indexes are cloud-based clusters of compute resources, they take
	// some time to spin up and become fully available. To ensure users don't encounter
	// unnecessary errors while registering a new index, we wait for the index to be
	// ready.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("timed out waiting for pinecone index %s to be ready", indexName)
		case <-ticker.C:
			table, err := store.getTableForReadyIndex(indexName, feature, variant)
			if err != nil {
				return nil, err
			}
			if table != nil {
				return table, nil
			}
		}
	}
}

func (store *pineconeOnlineStore) getTableForReadyIndex(indexName, feature, variant string) (VectorStoreTable, error) {
	dimension, state, err := store.client.describeIndex(indexName)
	if err != nil {
		return nil, err
	}
	if state == Ready {
		return pineconeOnlineTable{
			api:       store.client,
			indexName: indexName,
			namespace: fmt.Sprintf(namespaceTemplate, feature, variant),
			valueType: types.VectorType{
				Dimension:   dimension,
				ScalarType:  types.Float32,
				IsEmbedding: true,
			},
		}, nil
	} else {
		return nil, nil
	}
}

func (store *pineconeOnlineStore) DeleteIndex(feature, variant string) error {
	indexName := store.createIndexName(feature, variant)
	return store.client.deleteIndex(indexName)
}

func (store *pineconeOnlineStore) GetTable(feature, variant string) (OnlineStoreTable, error) {
	indexName := store.createIndexName(feature, variant)
	dimension, state, err := store.client.describeIndex(indexName)
	if err != nil {
		return nil, err
	}
	if state != Ready {
		wrapped := fferr.NewConnectionError(pt.PineconeOnline.String(), fmt.Errorf("pinecone index not ready"))
		wrapped.AddDetail("index_name", indexName)
		wrapped.AddDetail("current_state", string(state))
		return nil, wrapped
	}
	return pineconeOnlineTable{
		api:       store.client,
		indexName: indexName,
		namespace: fmt.Sprintf(namespaceTemplate, feature, variant),
		valueType: types.VectorType{
			Dimension:   dimension,
			ScalarType:  types.Float32,
			IsEmbedding: true,
		},
	}, nil
}

func (store *pineconeOnlineStore) createIndexName(feature, variant string) string {
	uuid := uuid.NewSHA1(uuid.NameSpaceDNS, []byte(fmt.Sprintf(nameVariantTemplate, feature, variant)))
	return fmt.Sprintf(store.prefix, uuid.String())
}

type pineconeOnlineTable struct {
	api       *pineconeAPI
	indexName string
	namespace string
	valueType types.ValueType
}

func (table pineconeOnlineTable) Set(entity string, value interface{}) error {
	vector, isVector := value.([]float32)
	if !isVector {
		wrapped := fferr.NewInvalidArgumentError(fmt.Errorf("expected value to be of type []float32, got %T", value))
		wrapped.AddDetail("provider", pt.PineconeOnline.String())
		wrapped.AddDetail("entity", entity)
		wrapped.AddDetail("index_name", table.indexName)
		return wrapped
	}
	err := table.api.upsert(table.indexName, table.namespace, entity, vector)
	if err != nil {
		return err
	}
	return nil
}

func (table pineconeOnlineTable) Get(entity string) (interface{}, error) {
	vector, err := table.api.fetch(table.indexName, table.namespace, entity)
	if err != nil {
		return nil, err
	}
	return vector, nil
}

func (table pineconeOnlineTable) Nearest(feature, variant string, vector []float32, k int32) ([]string, error) {
	entities, err := table.api.query(table.indexName, table.namespace, vector, int64(k))
	if err != nil {
		return nil, err
	}
	return entities, nil
}

type pineconeAPI struct {
	client          *http.Client
	baseURLTemplate string
	config          *pc.PineconeConfig
}

func NewPineconeAPI(config *pc.PineconeConfig) *pineconeAPI {
	return &pineconeAPI{
		client:          &http.Client{},
		baseURLTemplate: "https://%s.pinecone.io/%s",
		config:          config,
	}
}

// https://docs.pinecone.io/reference/create_index
func (api pineconeAPI) createIndex(name string, dimension int32) error {
	base := api.getIndexOperationURL("databases")
	payload := &createIndexRequest{
		Name:      name,
		Dimension: dimension,
		Metric:    "cosine",
	}
	_, err := api.request(http.MethodPost, base, payload, http.StatusCreated)
	if err != nil {
		return err
	}
	return nil
}

// https://docs.pinecone.io/reference/describe_index
func (api pineconeAPI) describeIndex(name string) (dimension int32, state PineconeIndexState, err error) {
	base := api.getIndexOperationURL(fmt.Sprintf("databases/%s", name))
	body, err := api.request(http.MethodGet, base, nil, http.StatusOK)
	if err != nil {
		return
	}
	var response describeIndexResponse
	err = json.Unmarshal(body, &response)
	if err != nil {
		err = fferr.NewInternalError(err)
		return
	}
	dimension = int32(response.Database.Dimension)
	state = response.Status.State
	return
}

// https://docs.pinecone.io/reference/delete_index
func (api pineconeAPI) deleteIndex(name string) error {
	base := api.getIndexOperationURL(fmt.Sprintf("databases/%s", name))
	_, err := api.request("DELETE", base, nil, http.StatusAccepted)
	return err
}

// https://docs.pinecone.io/reference/upsert
func (api pineconeAPI) upsert(indexName, namespace, id string, vector []float32) error {
	base := api.getVectorOperationURL(indexName, "vectors/upsert")
	payload := &upsertRequest{
		Vectors: []vectorElement{
			{
				ID:     api.generateDeterministicID(id),
				Values: vector,
				Metadata: metadataElement{
					"id": id,
				},
			},
		},
		Namespace: namespace,
	}
	body, err := api.request(http.MethodPost, base, payload, http.StatusOK)
	if err != nil {
		return err
	}
	var response upsertResponse
	err = json.Unmarshal(body, &response)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	if response.UpsertedCount != 1 {
		wrapped := fferr.NewExecutionError(pt.PineconeOnline.String(), fmt.Errorf("expected 1 upserted count, got %d", response.UpsertedCount))
		return wrapped
	}
	return nil
}

// https://docs.pinecone.io/reference/fetch
func (api pineconeAPI) fetch(indexName, namespace, id string) ([]float32, error) {
	base, err := url.Parse(api.getVectorOperationURL(indexName, "vectors/fetch"))
	if err != nil {
		return nil, fferr.NewInternalError(err)
	}
	vectorID := api.generateDeterministicID(id)
	params := url.Values{}
	params.Add("ids", vectorID)
	params.Add("namespace", namespace)
	base.RawQuery = params.Encode()
	body, err := api.request(http.MethodGet, base.String(), nil, http.StatusOK)
	if err != nil {
		return nil, err
	}
	var response fetchResponse
	err = json.Unmarshal(body, &response)
	if err != nil {
		return nil, fferr.NewInternalError(err)
	}
	vector, ok := response.Vectors[vectorID]
	if !ok {
		wrapped := fferr.NewDatasetNotFoundError(vectorID, "", fmt.Errorf("vector not found"))
		wrapped.AddDetail("id", id)
		wrapped.AddDetail("index_name", indexName)
		wrapped.AddDetail("namespace", namespace)
		return nil, wrapped
	}
	return vector.Values, nil
}

// https://docs.pinecone.io/reference/query
func (api pineconeAPI) query(indexName, namespace string, vector []float32, k int64) ([]string, error) {
	base := api.getVectorOperationURL(indexName, "query")
	payload := &queryRequest{
		Vector:    vector,
		Namespace: namespace,
		TopK:      k,
		// Given the original/raw id for the vector is stored in the vector's metadata map,
		// it's necessary to return the metadata for each result so that the original id can be
		// returned to the user as the UUID5 representation has no meaning outside of Pinecone.
		IncludeMetadata: true,
	}
	body, err := api.request(http.MethodPost, base, payload, http.StatusOK)
	if err != nil {
		return nil, err
	}
	var response queryResponse
	err = json.Unmarshal(body, &response)
	if err != nil {
		return nil, fferr.NewInternalError(err)
	}
	results := make([]string, k)
	for i, result := range response.Matches {
		results[i] = result.Metadata["id"]
	}
	return results, nil
}

func (api pineconeAPI) getIndexOperationURL(operation string) string {
	subdomain := fmt.Sprintf(indexOperationURLTemplate, api.config.Environment)
	return fmt.Sprintf(api.baseURLTemplate, subdomain, operation)
}

func (api pineconeAPI) getVectorOperationURL(indexName, operation string) string {
	subdomain := fmt.Sprintf(vectorOperationURLTemplate, indexName, api.config.ProjectID, api.config.Environment)
	return fmt.Sprintf(api.baseURLTemplate, subdomain, operation)
}

func (api pineconeAPI) request(method, url string, payload interface{}, expectedStatus int) ([]byte, error) {
	var reader io.Reader
	switch method {
	case http.MethodPost:
		jsonPayload, err := json.Marshal(payload)
		if err != nil {
			return nil, fferr.NewInternalError(err)
		}
		reader = bytes.NewBuffer(jsonPayload)
	case http.MethodGet, http.MethodDelete:
		reader = nil
	default:
		return nil, fferr.NewInternalError(fmt.Errorf("unsupported method %s", method))
	}
	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		return nil, fferr.NewConnectionError(pt.PineconeOnline.String(), err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Api-Key", api.config.ApiKey)
	resp, err := api.client.Do(req)
	if err != nil {
		return nil, fferr.NewConnectionError(pt.PineconeOnline.String(), err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fferr.NewInternalError(err)
	}
	if resp.StatusCode != expectedStatus {
		wrapped := fferr.NewConnectionError(pt.PineconeOnline.String(), err)
		wrapped.AddDetail("status_code", fmt.Sprintf("%d", resp.StatusCode))
		return nil, wrapped
	}
	return body, nil
}

// Generate a deterministic UUID for an ID of unknown format and composition using the SHA-1 hash of the ID
// and returns the string representation of the UUID to serve as the vector ID in Pinecone.
// This avoids URL encoding issues that may arise from using the raw, user-supplied ID.
func (api pineconeAPI) generateDeterministicID(id string) string {
	uuid := uuid.NewSHA1(uuid.NameSpaceDNS, []byte(id))
	return uuid.String()
}

type createIndexRequest struct {
	Name      string `json:"name"`
	Dimension int32  `json:"dimension"`
	Metric    string `json:"metric"`
}

type describeIndexResponse struct {
	Database struct {
		Name      string `json:"name"`
		Metric    string `json:"metric"`
		Dimension int    `json:"dimension"`
		Replicas  int    `json:"replicas"`
		Shards    int    `json:"shards"`
		Pods      int    `json:"pods"`
	} `json:"database"`
	Status struct {
		Waiting []interface{}      `json:"waiting"`
		Crashed []interface{}      `json:"crashed"`
		Host    string             `json:"host"`
		Port    int                `json:"port"`
		State   PineconeIndexState `json:"state"`
		Ready   bool               `json:"ready"`
	} `json:"status"`
}

type PineconeIndexState string

const (
	Initializing         PineconeIndexState = "Initializing"
	ScalingUp            PineconeIndexState = "ScalingUp"
	ScalingDown          PineconeIndexState = "ScalingDown"
	Terminating          PineconeIndexState = "Terminating"
	Ready                PineconeIndexState = "Ready"
	InitializationFailed PineconeIndexState = "InitializationFailed"
)

type metadataElement map[string]string

type vectorElement struct {
	ID       string          `json:"id"`
	Values   []float32       `json:"values"`
	Metadata metadataElement `json:"metadata"`
}

type upsertRequest struct {
	Vectors   []vectorElement `json:"vectors"`
	Namespace string          `json:"namespace"`
}

type upsertResponse struct {
	UpsertedCount int64 `json:"upsertedCount"`
}

type fetchResponse struct {
	Vectors   map[string]vectorElement `json:"vectors"`
	Namespace string                   `json:"namespace"`
}

type queryRequest struct {
	Namespace       string    `json:"namespace"`
	TopK            int64     `json:"topK"`
	Vector          []float32 `json:"vector"`
	IncludeMetadata bool      `json:"includeMetadata"`
}

type match struct {
	ID       string          `json:"id"`
	Score    float32         `json:"score"`
	Metadata metadataElement `json:"metadata"`
}

type queryResponse struct {
	Matches   []match `json:"matches"`
	Namespace string  `json:"namespace"`
}
