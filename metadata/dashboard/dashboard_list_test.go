// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package dashboard_metadata

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/featureform/ffsync"
	"github.com/featureform/logging"
	"github.com/featureform/metadata"
	pb "github.com/featureform/metadata/proto"
	"github.com/featureform/metadata/search"
	"github.com/featureform/provider"
	"github.com/featureform/provider/provider_type"
	ss "github.com/featureform/storage"
	"github.com/featureform/storage/query"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zaptest"
)

func GetMetadataServer(t *testing.T) MetadataServer {
	logger := logging.WrapZapLogger(zaptest.NewLogger(t).Sugar())
	client := &metadata.Client{}
	serv := MetadataServer{
		logger: logger,
		client: client,
	}
	return serv
}

func GetTestGinContext(mockRecorder *httptest.ResponseRecorder) *gin.Context {
	SearchClient = search.SearchMock{}
	gin.SetMode(gin.TestMode)
	ctx, _ := gin.CreateTestContext(mockRecorder)
	ctx.Request = &http.Request{
		Header: make(http.Header),
		URL:    &url.URL{},
	}
	return ctx
}

func MockJsonGet(c *gin.Context, params gin.Params) {
	c.Request.Method = "GET"
	c.Request.Header.Set("Content-Type", "application/json")
	c.Params = params
	getBody := TagGetBody{
		Variant: "default",
	}
	jsonValue, _ := json.Marshal(getBody)
	c.Request.Body = io.NopCloser(bytes.NewBuffer(jsonValue))
}

func MockJsonPost(c *gin.Context, params gin.Params, tagList []string) {
	c.Request.Method = "POST"
	c.Request.Header.Set("Content-Type", "application/json")
	postBody := TagPostBody{
		Tags:    tagList,
		Variant: "default",
	}
	jsonValue, _ := json.Marshal(postBody)
	c.Request.Body = io.NopCloser(bytes.NewBuffer(jsonValue))
	c.Params = params
}

func MockGet(c *gin.Context, urlValues url.Values, params gin.Params, userName string) {
	c.Request.Method = "GET"
	c.Params = params
	c.Request.Header.Set("Content-Type", "application/json")
	c.Request.URL.RawQuery = urlValues.Encode()
	c.Set("username", userName)
}

func MockPost(c *gin.Context, params gin.Params, postBody map[string]interface{}, userName string) {
	c.Request.Method = "POST"
	c.Request.Header.Set("Content-Type", "application/json")
	jsonValue, _ := json.Marshal(postBody)
	c.Request.Body = io.NopCloser(bytes.NewBuffer(jsonValue))
	c.Params = params
	c.Set("username", userName)
}

func TestGetVersionMap(t *testing.T) {
	mockRecorder := httptest.NewRecorder()
	ctx := GetTestGinContext(mockRecorder)
	MockJsonGet(ctx, nil)
	serv := MetadataServer{}

	version := "stable"
	t.Setenv("FEATUREFORM_VERSION", version)
	serv.GetVersionMap(ctx)

	var data map[string]interface{}
	json.Unmarshal(mockRecorder.Body.Bytes(), &data)

	assert.Equal(t, http.StatusOK, mockRecorder.Code)
	assert.Equal(t, version, data["version"])
}

func TestPostTags(t *testing.T) {
	name := "transactions"
	variant := "default"
	resourceType := "sources"
	tagList := []string{"test tag 40", "test tag 66"}

	mockRecorder := httptest.NewRecorder()
	ctx := GetTestGinContext(mockRecorder)
	params := []gin.Param{
		{
			Key:   "resource",
			Value: name,
		},
		{
			Key:   "type",
			Value: resourceType,
		},
	}

	MockJsonPost(ctx, params, tagList)

	res := metadata.ResourceID{
		Name:    name,
		Variant: variant,
		Type:    metadata.SOURCE_VARIANT,
	}

	resource, err := metadata.CreateEmptyResource(metadata.SOURCE_VARIANT)
	if err != nil {
		t.Fatalf(err.Error())
	}
	variantUpdate, ok := resource.Proto().(*pb.SourceVariant)
	if !ok {
		t.Fatal("The source_variant resource could not be cast")
	}
	variantUpdate.Tags = &pb.Tags{Tag: []string{}}

	locker, err := ffsync.NewMemoryLocker()
	if err != nil {
		t.Fatalf(err.Error())
	}
	mstorage, err := ss.NewMemoryStorageImplementation()
	if err != nil {
		panic(err.Error())
	}
	logger := logging.WrapZapLogger(zaptest.NewLogger(t).Sugar())
	storage := ss.MetadataStorage{
		Locker:          &locker,
		Storage:         &mstorage,
		SkipListLocking: true,
		Logger:          logger,
	}
	lookup := metadata.MemoryResourceLookup{Connection: storage}
	lookup.Set(context.Background(), res, resource)

	client := &metadata.Client{}
	serv := MetadataServer{
		lookup:          &lookup,
		client:          client,
		logger:          logger,
		StorageProvider: storage,
	}
	serv.PostTags(ctx)

	var data TagResult
	json.Unmarshal(mockRecorder.Body.Bytes(), &data)

	assert.Equal(t, http.StatusOK, mockRecorder.Code)
	assert.Equal(t, name, data.Name)
	assert.Equal(t, variant, data.Variant)
	assert.Equal(t, tagList, data.Tags)
}

func TestGetSourceDataReturnsData(t *testing.T) {
	mockRecorder := httptest.NewRecorder()
	ctx := GetTestGinContext(mockRecorder)
	u := url.Values{}
	u.Add("name", "nameParamValue")
	u.Add("variant", "variantParamValue")
	MockGet(ctx, u, nil, "default")

	logger := logging.WrapZapLogger(zaptest.NewLogger(t).Sugar())
	client := &metadata.Client{
		GrpcConn: metadata.MetadataServerMock{},
	}
	serv := MetadataServer{
		client: client,
		logger: logger,
	}

	serv.GetSourceData(ctx)

	iterator := provider.UnitTestIterator{}
	var data SourceDataResponse
	rowValues := []string{"row string value", "true", "10"}
	expectedRows := [][]string{rowValues, rowValues, rowValues}

	_ = json.Unmarshal(mockRecorder.Body.Bytes(), &data)
	assert.Equal(t, http.StatusOK, mockRecorder.Code)
	assert.Equal(t, iterator.Columns(), data.Columns)
	assert.Equal(t, expectedRows, data.Rows)
}

func TestGetSourceMissingNameOrVariantParamErrors(t *testing.T) {
	mockRecorder := httptest.NewRecorder()
	ctx := GetTestGinContext(mockRecorder)
	u := url.Values{}
	u.Add("name", "")    //intentionally blank
	u.Add("variant", "") //intentionally blank
	MockGet(ctx, u, nil, "default")

	logger := logging.WrapZapLogger(zaptest.NewLogger(t).Sugar())
	serv := MetadataServer{
		logger: logger,
	}

	serv.GetSourceData(ctx)

	var actualErrorMsg string
	expectedMsg := "Error 400: Failed to fetch GetSourceData - Could not find the name or variant query parameters"
	_ = json.Unmarshal(mockRecorder.Body.Bytes(), &actualErrorMsg)

	assert.Equal(t, http.StatusBadRequest, mockRecorder.Code)
	assert.Equal(t, expectedMsg, actualErrorMsg)
}

func TestGetSourceFaultyOrNilGrpcClientPanic(t *testing.T) {
	mockRecorder := httptest.NewRecorder()
	ctx := GetTestGinContext(mockRecorder)
	u := url.Values{}
	u.Add("name", "nameParamValue")
	u.Add("variant", "variantParamValue")
	MockGet(ctx, u, nil, "default")

	logger := logging.WrapZapLogger(zaptest.NewLogger(t).Sugar())
	serv := MetadataServer{
		logger: logger,
	}

	didPanic := func() {
		serv.GetSourceData(ctx)
	}

	assert.Panics(t, didPanic)
}

func getPaginateRequestBody(status, searchTxt, sortBy string, pageSize, offset int) map[string]interface{} {
	body := map[string]interface{}{"Status": status,
		"SearchText": searchTxt,
		"SortBy":     sortBy,
		"PageSize":   pageSize,
		"Offset":     offset,
	}
	return body
}

func TestGetTaskRunsErrors(t *testing.T) {
	tests := []struct {
		name         string
		body         map[string]interface{}
		expectedMsg  string
		expectedCode int
	}{
		{
			name:         "PageSize below or equal to zero",
			body:         getPaginateRequestBody("", "", "", 0, 15),
			expectedMsg:  "Error 400: Failed to fetch GetTaskRuns - Error invalid pageSize value: 0",
			expectedCode: http.StatusBadRequest,
		},
		{
			name:         "Offset below 0",
			body:         getPaginateRequestBody("", "", "", 152, -1),
			expectedMsg:  "Error 400: Failed to fetch GetTaskRuns - Error invalid offset value: -1",
			expectedCode: http.StatusBadRequest,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockRecorder := httptest.NewRecorder()
			ctx := GetTestGinContext(mockRecorder)
			MockPost(ctx, nil, tt.body, "default")

			logger := logging.WrapZapLogger(zaptest.NewLogger(t).Sugar())
			client := &metadata.Client{}
			serv := MetadataServer{
				client: client,
				logger: logger,
			}
			serv.GetTaskRuns(ctx)

			var actualErrorMsg string
			_ = json.Unmarshal(mockRecorder.Body.Bytes(), &actualErrorMsg)

			assert.Equal(t, tt.expectedCode, mockRecorder.Code)
			assert.Equal(t, tt.expectedMsg, actualErrorMsg)
		})
	}
}

func TestGetTaskRunsZeroResults(t *testing.T) {
	mockRecorder := httptest.NewRecorder()
	ctx := GetTestGinContext(mockRecorder)
	body := getPaginateRequestBody("ALL", "hahaha", "Nope", 7, 11)
	MockPost(ctx, nil, body, "default")

	locker, err := ffsync.NewMemoryLocker()
	if err != nil {
		t.Fatalf(err.Error())
	}
	mstorage, err := ss.NewMemoryStorageImplementation()
	if err != nil {
		panic(err.Error())
	}
	logger := logging.WrapZapLogger(zaptest.NewLogger(t).Sugar())
	storage := ss.MetadataStorage{
		Locker:          &locker,
		Storage:         &mstorage,
		SkipListLocking: true,
		Logger:          logger,
	}
	client := &metadata.Client{}
	serv := MetadataServer{
		client:          client,
		logger:          logger,
		StorageProvider: storage,
	}
	serv.GetTaskRuns(ctx)

	var taskRunResp TaskRunListResponse
	json.Unmarshal(mockRecorder.Body.Bytes(), &taskRunResp)

	assert.Equal(t, http.StatusOK, mockRecorder.Code)
	assert.Equal(t, 0, len(taskRunResp.TaskRunList))
	assert.Equal(t, 0, taskRunResp.Count)
}

func getFeatureVariantRequestBody(searchTxt string, statuses, owners, tags []string, pageSize, offset int) map[string]interface{} {
	body := map[string]interface{}{
		"SearchTxt": searchTxt,
		"Owners":    owners,
		"Statuses":  statuses,
		"Tags":      tags,
		"PageSize":  pageSize,
		"Offset":    offset,
	}
	return body
}

func getSourceVariantRequestBody(searchTxt string, modes, types, statuses, tags, owners []string, pageSize, offset int) map[string]interface{} {
	body := map[string]interface{}{
		"SearchTxt": searchTxt,
		"Types":     types,
		"Modes":     modes,
		"Statuses":  statuses,
		"tags":      tags,
		"Owners":    owners,
		"PageSize":  pageSize,
		"Offset":    offset,
	}
	return body
}

func getLabelVariantRequestBody(searchTxt string, statuses, owners, tags []string, pageSize, offset int) map[string]interface{} {
	body := map[string]interface{}{
		"SearchTxt": searchTxt,
		"Owners":    owners,
		"Statuses":  statuses,
		"Tags":      tags,
		"PageSize":  pageSize,
		"Offset":    offset,
	}
	return body
}

func getProviderRequestBody(searchText string, providerType []string, status []string, pageSize, offset int) map[string]interface{} {
	body := map[string]interface{}{
		"SearchTxt":    searchText,
		"ProviderType": providerType,
		"Status":       status,
		"pageSize":     pageSize,
		"offset":       offset,
	}
	return body
}

func GetTrainingSetVariantRequestBody(searchText string, statuses, owners, labels, providers, tags []string, pageSize, offset int) map[string]interface{} {
	body := map[string]interface{}{
		"SearchTxt": searchText,
		"Owners":    owners,
		"Statuses":  statuses,
		"Labels":    labels,
		"Providers": providers,
		"Tags":      tags,
		"pageSize":  pageSize,
		"offset":    offset,
	}
	return body
}

type MockVariantsStore struct {
	ListData   map[string]string
	ColumnData []map[string]interface{}
	Opts       []query.Query
}

func (m *MockVariantsStore) Set(key, value string) error {
	return nil
}

func (m *MockVariantsStore) Get(key string, opts ...query.Query) (string, error) {
	return "", nil
}

func (m *MockVariantsStore) List(prefix string, opts ...query.Query) (map[string]string, error) {
	//intercept the opts param
	m.Opts = opts
	return m.ListData, nil
}

func (m *MockVariantsStore) ListColumn(prefix string, columns []query.Column, opts ...query.Query) ([]map[string]interface{}, error) {
	return m.ColumnData, nil
}

func (m *MockVariantsStore) Count(prefix string, opts ...query.Query) (int, error) {
	return len(m.ListData), nil
}

func (m *MockVariantsStore) Delete(key string) (string, error) {
	return "", nil
}

func (m *MockVariantsStore) Close() {
}

func (m *MockVariantsStore) Type() ss.MetadataStorageType {
	return ss.MemoryMetadataStorage
}

func TestGetFeatureVariants(t *testing.T) {
	mockRecorder := httptest.NewRecorder()
	ctx := GetTestGinContext(mockRecorder)
	//prefix, searchTxt, owners providers, tags, serializedV1, paging, sort
	expectedQueryOpts := 8

	searchTxt := "searchTxt"
	statuses := []string{pb.ResourceStatus_FAILED.String()}
	owners := []string{"anthony@featureform.com"}
	tags := []string{"dummyTag"}
	body := getFeatureVariantRequestBody(searchTxt, statuses, owners, tags, 12, 0)
	MockPost(ctx, nil, body, "default")

	locker, err := ffsync.NewMemoryLocker()
	if err != nil {
		t.Fatalf(err.Error())
	}

	mockStore := MockVariantsStore{
		ListData: map[string]string{
			"FEATURE_VARIANT__avg_transactions__2024-08-21t18-16-06": `{"ResourceType":4,"StorageType":"Resource","Message":` +
				`"{\"name\":\"avg_transactions\",\"variant\":\"2024-08-21t18-16-06\",\"source\":{\"name\":\"average_user_transaction\",\"variant\":\"2024-08-21t18-16-06\"},` +
				`\"entity\":\"user\",\"created\":\"2024-08-21T23:16:09.892267302Z\",\"owner\":\"anthony@featureform.com\",\"provider\":\"latestv1test-redis\",` +
				`\"status\":{\"status\":\"FAILED\",\"errorMessage\":\"Resource Failed: required dataset is in a failed state\\n\u003e\u003e\u003e resource_name: ` +
				`average_user_transaction\\n\u003e\u003e\u003e resource_variant: 2024-08-21t18-16-06\\n\u003e\u003e\u003e resource_type: TRANSFORMATION\"},` +
				`\"trainingsets\":[{\"name\":\"fraud_training\",\"variant\":\"2024-08-21t18-16-06\"}],\"columns\":{\"entity\":\"user_id\",\"value\":\"avg_transaction_amt\"},` +
				`\"tags\":{\"tag\":[\"testV1\",\"testV1-READY\"]},\"properties\":{},\"taskIdList\":[\"12\"],\"type\":{\"scalar\":\"FLOAT32\"}}","SerializedVersion":1}`,
		},
		ColumnData: []map[string]interface{}{
			{"provider_name": "latestv1test-redis", "provider_type": string(provider_type.RedisOnline)},
		},
	}

	logger := logging.WrapZapLogger(zaptest.NewLogger(t).Sugar())
	storage := ss.MetadataStorage{
		Locker:          &locker,
		Storage:         &mockStore,
		SkipListLocking: true,
		Logger:          logger,
	}
	client := &metadata.Client{}
	serv := MetadataServer{
		client:          client,
		logger:          logger,
		StorageProvider: storage,
	}

	serv.GetFeatureVariantResources(ctx)

	var resp GetFeatureVariantListResp
	json.Unmarshal(mockRecorder.Body.Bytes(), &resp)

	//the response is valid
	assert.Equal(t, http.StatusOK, mockRecorder.Code)
	assert.Len(t, resp.Data, len(mockStore.ListData))
	//the data should parse
	assert.Equal(t, "anthony@featureform.com", resp.Data[0].Owner)
	assert.Equal(t, "avg_transactions", resp.Data[0].Name)
	assert.Equal(t, "2024-08-21t18-16-06", resp.Data[0].Variant)
	assert.Equal(t, "latestv1test-redis", resp.Data[0].Provider)
	assert.Equal(t, "FAILED", resp.Data[0].Status)
	assert.Equal(t, metadata.Tags{"testV1", "testV1-READY"}, resp.Data[0].Tags)
	assert.Len(t, mockStore.Opts, expectedQueryOpts)
}

func TestGetProviderNameTypeMap(t *testing.T) {
	const (
		myRedis      = "my_redis"
		yourPostgres = "your_postgres"
	)
	locker, err := ffsync.NewMemoryLocker()
	if err != nil {
		t.Fatalf(err.Error())
	}

	mockStore := MockVariantsStore{
		ColumnData: []map[string]interface{}{
			{"provider_name": myRedis, "provider_type": string(provider_type.RedisOnline)},
			{"provider_name": yourPostgres, "provider_type": string(provider_type.PostgresOffline)},
		},
	}

	logger := logging.WrapZapLogger(zaptest.NewLogger(t).Sugar())
	storage := ss.MetadataStorage{
		Locker:          &locker,
		Storage:         &mockStore,
		SkipListLocking: true,
		Logger:          logger,
	}
	client := &metadata.Client{}
	serv := MetadataServer{
		client:          client,
		logger:          logger,
		StorageProvider: storage,
	}

	result, err := serv.getProviderNameTypeMap()
	assert.NoError(t, err, "getProviderNameTypeMap returned an error!")

	assert.Equal(t, result[myRedis], string(provider_type.RedisOnline))
	assert.Equal(t, result[yourPostgres], string(provider_type.PostgresOffline))
	assert.Len(t, result, len(mockStore.ColumnData))
}

func TestGetTypeOwners(t *testing.T) {
	mockRecorder := httptest.NewRecorder()
	ctx := GetTestGinContext(mockRecorder)

	params := []gin.Param{
		{
			Key:   "type",
			Value: "features",
		},
	}
	MockPost(ctx, params, nil, "default")

	locker, err := ffsync.NewMemoryLocker()
	if err != nil {
		t.Fatalf(err.Error())
	}

	mockStore := MockVariantsStore{
		ColumnData: []map[string]interface{}{
			{"owner": "anthony@featureform.com", "total_count": 3},
			{"owner": "neo@featureform.com", "total_count": 2},
		},
	}

	logger := logging.WrapZapLogger(zaptest.NewLogger(t).Sugar())
	storage := ss.MetadataStorage{
		Locker:          &locker,
		Storage:         &mockStore,
		SkipListLocking: true,
		Logger:          logger,
	}
	client := &metadata.Client{}
	serv := MetadataServer{
		client:          client,
		logger:          logger,
		StorageProvider: storage,
	}

	serv.GetTypeOwners(ctx)

	var resp []TypeList
	json.Unmarshal(mockRecorder.Body.Bytes(), &resp)

	//the response is valid
	assert.Equal(t, http.StatusOK, mockRecorder.Code)
	assert.Len(t, resp, 2)
}

func TestGetSourceVariants(t *testing.T) {
	mockRecorder := httptest.NewRecorder()
	ctx := GetTestGinContext(mockRecorder)
	//searchTxt, modes, owners, types serializedV1, paging, sort
	expectedQueryOpts := 10

	searchTxt := "searchTxt"
	modes := []string{"Batch", "Incremental", "Streaming"}
	statuses := []string{"READY"}
	tags := []string{"v1"}
	owners := []string{"anthony@featureform.com"}
	types := []string{"Primary Table", "SQL Transformation"}
	body := getSourceVariantRequestBody(searchTxt, modes, types, statuses, tags, owners, 12, 0)
	MockPost(ctx, nil, body, "default")

	locker, err := ffsync.NewMemoryLocker()
	if err != nil {
		t.Fatalf(err.Error())
	}

	mockStore := MockVariantsStore{
		ListData: map[string]string{
			"SOURCE_VARIANT__transactions__2024-09-06t21-07-40": `{"ResourceType":7,"StorageType":"Resource","Message":` +
				`"{\"name\":\"transactions\",\"variant\":\"2024-09-06t21-07-40\",\"primaryData\":{\"table\":{\"name\":\"transactions\"}},\"owner\":` +
				`\"anthony@featureform.com\",\"provider\":\"postgres\",\"created\":\"2024-09-07T02:07:42.257152631Z\",\"status\":{\"status\":` +
				`\"READY\"},\"trainingsets\":[{\"name\":\"fraud_training\",\"variant\":\"2024-09-06t21-07-40\"}],\"labels\":[{\"name\":` +
				`\"fraudulent\",\"variant\":\"2024-09-06t21-07-40\"}],\"tags\":{\"tag\":[\"my_data\"]},\"properties\":{},\"maxJobDuration\":` +
				`\"172800s\",\"taskIdList\":[\"1\"]}","SerializedVersion":1}`,
			"SOURCE_VARIANT__average_user_transaction__2024-09-06t21-07-40": `{"ResourceType":7,"StorageType":"Resource","Message":"{\"name\":` +
				`\"average_user_transaction\",\"variant\":\"2024-09-06t21-07-40\",\"transformation\":{\"SQLTransformation\":{\"query\":` +
				`\"SELECT CustomerID as user_id, avg(TransactionAmount) as avg_transaction_amt from {{ transactions.2024-09-06t21-07-40 }} GROUP BY user_id\",\"source\":` +
				`[{\"name\":\"transactions\",\"variant\":\"2024-09-06t21-07-40\"}]},\"table\":{\"name\":` +
				`\"featureform_transformation__average_user_transaction__2024-09-06t21-07-40\"}},\"owner\":\"anthony@featureform.com\",\"provider\":` +
				`\"postgres\",\"created\":\"2024-09-07T02:07:42.921672215Z\",\"status\":{\"status\":\"READY\"},\"trainingsets\":[{\"name\":` +
				`\"fraud_training\",\"variant\":\"2024-09-06t21-07-40\"}],\"features\":[{\"name\":\"avg_transactions\",\"variant\":` +
				`\"2024-09-06t21-07-40\"}],\"tags\":{\"tag\":[\"avg\"]},\"properties\":{},\"maxJobDuration\":\"172800s\",` +
				`\"taskIdList\":[\"2\"]}","SerializedVersion":1}`,
		},
	}

	logger := logging.WrapZapLogger(zaptest.NewLogger(t).Sugar())
	storage := ss.MetadataStorage{
		Locker:          &locker,
		Storage:         &mockStore,
		SkipListLocking: true,
		Logger:          logger,
	}
	client := &metadata.Client{}
	serv := MetadataServer{
		client:          client,
		logger:          logger,
		StorageProvider: storage,
	}

	serv.GetSourceVariantResources(ctx)

	var resp GetSourceVariantListResp
	json.Unmarshal(mockRecorder.Body.Bytes(), &resp)

	//the response is valid
	assert.Equal(t, http.StatusOK, mockRecorder.Code)
	assert.Len(t, resp.Data, len(mockStore.ListData))

	//the data should parse, first record
	record1 := findSourceVariant("transactions", resp.Data)
	assert.NotNil(t, record1)
	assert.Equal(t, "anthony@featureform.com", record1.Owner)
	assert.Equal(t, "transactions", record1.Name)
	assert.Equal(t, "2024-09-06t21-07-40", record1.Variant)
	assert.Equal(t, "postgres", record1.Provider)
	assert.Equal(t, "READY", record1.Status)
	assert.Equal(t, "Primary Table", record1.SourceType)
	assert.Len(t, mockStore.Opts, expectedQueryOpts)

	//second record
	record2 := findSourceVariant("average_user_transaction", resp.Data)
	assert.NotNil(t, record2)
	assert.Equal(t, "anthony@featureform.com", record2.Owner)
	assert.Equal(t, "average_user_transaction", record2.Name)
	assert.Equal(t, "2024-09-06t21-07-40", record2.Variant)
	assert.Equal(t, "postgres", record2.Provider)
	assert.Equal(t, "READY", record2.Status)
	assert.Equal(t, "SQL Transformation", record2.SourceType)
	assert.Len(t, mockStore.Opts, expectedQueryOpts)
}

func findSourceVariant(searchName string, variantsList []metadata.SourceVariantResource) *metadata.SourceVariantResource {
	var foundRecord metadata.SourceVariantResource
	for _, variant := range variantsList {
		if variant.Name == searchName {
			foundRecord = variant
		}
	}
	return &foundRecord
}

func TestGetLabelVariants(t *testing.T) {
	mockRecorder := httptest.NewRecorder()
	ctx := GetTestGinContext(mockRecorder)
	//prefix, searchTxt, owners, providers, tags, serializedV1, paging, sort
	expectedQueryOpts := 8

	searchTxt := "searchTxt"
	statuses := []string{pb.ResourceStatus_READY.String()}
	owners := []string{"riddhi@featureform.com"}
	tags := []string{"dummyTag"}
	body := getLabelVariantRequestBody(searchTxt, statuses, owners, tags, 12, 0)
	MockPost(ctx, nil, body, "default")

	locker, err := ffsync.NewMemoryLocker()
	if err != nil {
		t.Fatalf(err.Error())
	}

	mockStore := MockVariantsStore{
		ListData: map[string]string{
			"LABEL_VARIANT__trans_label__2024-09-27t15-58-54": `{"ResourceType":5,"StorageType":"Resource",` +
				`"Message":"{\"name\":\"trans_label\",\"variant\":\"2024-09-27t15-58-54\",\"source\":` +
				`{\"name\":\"transaction\",\"variant\":\"variant_447335\"},\"entity\":\"user\",\"created\":` +
				`\"2024-09-27T19:58:59.696273631Z\",\"owner\":\"riddhi@featureform.com\",\"provider\":` +
				`\"postgres-quickstart\",\"status\":{\"status\":\"READY\"},\"columns\":{\"entity\":\"customerid\",` +
				`\"value\":\"custlocation\",\"ts\":\"timestamp\"},\"tags\":{},\"properties\":{},\"type\":{\"scalar\":` +
				`\"STRING\"},\"taskIdList\":[\"3\"]}","SerializedVersion":1}`,
		},
	}

	logger := logging.WrapZapLogger(zaptest.NewLogger(t).Sugar())
	storage := ss.MetadataStorage{
		Locker:          &locker,
		Storage:         &mockStore,
		SkipListLocking: true,
		Logger:          logger,
	}
	client := &metadata.Client{}
	serv := MetadataServer{
		client:          client,
		logger:          logger,
		StorageProvider: storage,
	}

	serv.GetLabelVariantResources(ctx)

	var resp GetLabelVariantListResp
	json.Unmarshal(mockRecorder.Body.Bytes(), &resp)

	//the response is valid
	assert.Equal(t, http.StatusOK, mockRecorder.Code)
	assert.Len(t, resp.Data, len(mockStore.ListData))
	//the data should parse
	assert.Equal(t, "riddhi@featureform.com", resp.Data[0].Owner)
	assert.Equal(t, "trans_label", resp.Data[0].Name)
	assert.Equal(t, "2024-09-27t15-58-54", resp.Data[0].Variant)
	assert.Equal(t, "postgres-quickstart", resp.Data[0].Provider)
	assert.Equal(t, "READY", resp.Data[0].Status)
	assert.Equal(t, metadata.Tags{}, resp.Data[0].Tags)
	assert.Len(t, mockStore.Opts, expectedQueryOpts)
}

func TestGetProviders(t *testing.T) {
	mockRecorder := httptest.NewRecorder()
	ctx := GetTestGinContext(mockRecorder)
	//searchTxt, provider types, status
	expectedQueryOpts := 7

	searchTxt := "searchTxt"
	statuses := []string{"READY"}
	providerType := []string{string(provider_type.PostgresOffline)}
	body := getProviderRequestBody(searchTxt, providerType, statuses, 12, 0)
	MockPost(ctx, nil, body, "default")

	locker, err := ffsync.NewMemoryLocker()
	if err != nil {
		t.Fatalf(err.Error())
	}

	mockStore := MockVariantsStore{
		ListData: map[string]string{
			"PROVIDER__postgres-quickstart__": `{"ResourceType":8,` +
				`"StorageType":"Resource","Message":"{\"name\":\"postgres-quickstart\",` +
				`\"type\":\"POSTGRES_OFFLINE\",\"software\":\"postgres\",` +
				`\"serializedConfig\":\"eyJIb3N0IjogImhvc3QuZG9ja2VyLmludGVybmFsIiwgIlBvcnQiOiAiNTQzMiIsICJVc2VybmFtZSI6ICJwb3N0Z3JlcyIsICJQYXNzd29yZCI6ICJwYXNzd29yZCIsICJEYXRhYmFzZSI6ICJwb3N0Z3JlcyIsICJTU0xNb2RlIjogImRpc2FibGUifQ==\",` +
				`\"status\":{\"status\":\"READY\"},\"sources\":[{\"name\":\"transaction\",\"variant\":\"variant_447335\"}],` +
				`\"labels\":[{\"name\":\"f2_label\",\"variant\":\"2024-09-27t15-58-54\"}],\"tags\":{},\"properties\":{}}",` +
				`"SerializedVersion":1}`,
		},
	}

	logger := logging.WrapZapLogger(zaptest.NewLogger(t).Sugar())
	storage := ss.MetadataStorage{
		Locker:          &locker,
		Storage:         &mockStore,
		SkipListLocking: true,
		Logger:          logger,
	}
	client := &metadata.Client{}
	serv := MetadataServer{
		client:          client,
		logger:          logger,
		StorageProvider: storage,
	}

	serv.GetProviderResources(ctx)

	var resp GetProviderListResp
	json.Unmarshal(mockRecorder.Body.Bytes(), &resp)

	//the response is valid
	assert.Equal(t, http.StatusOK, mockRecorder.Code)
	assert.Len(t, resp.Data, len(mockStore.ListData))
	//the data should parse
	assert.Equal(t, "postgres-quickstart", resp.Data[0].Name)
	assert.Equal(t, "POSTGRES_OFFLINE", resp.Data[0].ProviderType)
	assert.Equal(t, "READY", resp.Data[0].Status)
	assert.Len(t, mockStore.Opts, expectedQueryOpts)
}

func TestGetTrainingSetVariant(t *testing.T) {
	mockRecorder := httptest.NewRecorder()
	ctx := GetTestGinContext(mockRecorder)
	//prefix, searchTxt, owners, providers, tags, serializedV1, paging, sort
	expectedQueryOpts := 10

	searchTxt := "searchTxt"
	statuses := []string{pb.ResourceStatus_READY.String()}
	owners := []string{"riddhi@featureform.com"}
	tags := []string{"dummyTag"}
	labels := []string{"f7_label"}
	providers := []string{"postgres-quickstart"}
	body := GetTrainingSetVariantRequestBody(searchTxt, statuses, owners, labels, providers, tags, 12, 0)
	MockPost(ctx, nil, body, "default")

	locker, err := ffsync.NewMemoryLocker()
	if err != nil {
		t.Fatalf(err.Error())
	}

	mockStore := MockVariantsStore{
		ListData: map[string]string{
			"TRAINING_SET_VARIANT__my_training_set__2024-10-23t17-36-17": `{"ResourceType":6,` +
				`"StorageType":"Resource","Message":"{\"name\":\"my_training_set\",\"variant\":\"2024-10-23t17-36-17\",` +
				`\"owner\":\"riddhi@featureform.com\",\"created\":\"2024-10-24T00:36:47.029200085Z\",` +
				`\"provider\":\"postgres-quickstart\",\"status\":{\"status\":\"READY\"},\"features\":` +
				`[{\"name\":\"table7_feature\",\"variant\":\"default-1\"},{\"name\":\"table2_feature\",` +
				`\"variant\":\"default-1\"}],\"label\":{\"name\":\"f7_label\",\"variant\":\"label_variant\"}` +
				`,\"tags\":{\"tag\":[\"dummyTag\"]},\"properties\":{},\"taskIdList\":[\"31\"],\"resourceSnowflakeConfig\":{\"dynamicTableConfig\"` +
				`:{\"targetLag\":\"1 days\",\"refreshMode\":\"REFRESH_MODE_AUTO\",\"initialize\":\"INITIALIZE_ON_CREATE\"}}}"` +
				`,"SerializedVersion":1}`,
		},
	}

	logger := logging.WrapZapLogger(zaptest.NewLogger(t).Sugar())
	storage := ss.MetadataStorage{
		Locker:          &locker,
		Storage:         &mockStore,
		SkipListLocking: true,
		Logger:          logger,
	}
	client := &metadata.Client{}
	serv := MetadataServer{
		client:          client,
		logger:          logger,
		StorageProvider: storage,
	}

	serv.GetTrainingSetVariantResources(ctx)

	var resp GetTrainingSetVariantListResp
	json.Unmarshal(mockRecorder.Body.Bytes(), &resp)

	//the response is valid
	assert.Equal(t, http.StatusOK, mockRecorder.Code)
	assert.Len(t, resp.Data, len(mockStore.ListData))
	//the data should parse
	assert.Equal(t, "riddhi@featureform.com", resp.Data[0].Owner)
	assert.Equal(t, "my_training_set", resp.Data[0].Name)
	assert.Equal(t, "2024-10-23t17-36-17", resp.Data[0].Variant)
	assert.Equal(t, "postgres-quickstart", resp.Data[0].Provider)
	assert.Equal(t, "READY", resp.Data[0].Status)
	assert.Equal(t, metadata.Tags{"dummyTag"}, resp.Data[0].Tags)
	assert.Len(t, mockStore.Opts, expectedQueryOpts)
}
