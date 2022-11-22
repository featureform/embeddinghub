//go:build k8s
// +build k8s

package provider

import (
	"bytes"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/featureform/helpers"

	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"github.com/mitchellh/mapstructure"
	"github.com/segmentio/parquet-go"
)

func uuidWithoutDashes() string {
	return fmt.Sprintf("a%s", strings.ReplaceAll(uuid.New().String(), "-", ""))
}

func TestBlobInterfaces(t *testing.T) {
	fileStoreTests := map[string]func(*testing.T, FileStore){
		"Test Filestore Read and Write":  testFilestoreReadAndWrite,
		"Test Exists":                    testExists,
		"Test Not Exists":                testNotExists,
		"Test Serve":                     testServe,
		"Test Serve Directory":           testServeDirectory,
		"Test Delete":                    testDelete,
		"Test Delete All":                testDeleteAll,
		"Test Newest file":               testNewestFile,
		"Test Path with prefix":          testPathWithPrefix,
		"Test Num Rows":                  testNumRows,
		"Test Databricks Initialization": testDatabricksInitialization,
	}

	err := godotenv.Load("../.env")

	mydir, err := os.Getwd()
	if err != nil {
		t.Fatalf("could not get working directory")
	}

	directoryPath := fmt.Sprintf("%s/scripts/k8s/tests/test_files/output/go_tests", mydir)
	_ = os.MkdirAll(directoryPath, os.ModePerm)

	fileStoreConfig := FileFileStoreConfig{DirPath: fmt.Sprintf(`file:///%s`, directoryPath)}
	serializedFileConfig, err := fileStoreConfig.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize file store config: %v", err)
	}
	fileFileStore, err := NewFileFileStore(serializedFileConfig)
	if err != nil {
		t.Fatalf("failed to create new file blob store: %v", err)
	}
	azureStoreConfig := AzureFileStoreConfig{
		AccountName:   helpers.GetEnv("AZURE_ACCOUNT_NAME", ""),
		AccountKey:    helpers.GetEnv("AZURE_ACCOUNT_KEY", ""),
		ContainerName: helpers.GetEnv("AZURE_CONTAINER_NAME", ""),
		Path:          "testdirectory/testpath",
	}
	serializedAzureConfig, err := azureStoreConfig.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize azure store config: %v", err)
	}
	azureFileStore, err := NewAzureFileStore(serializedAzureConfig)
	if err != nil {
		t.Fatalf("failed to create new azure blob store: %v", err)
	}

	blobProviders := map[string]FileStore{
		"File":  fileFileStore,
		"Azure": azureFileStore,
	}
	for testName, fileTest := range fileStoreTests {
		fileTest = fileTest
		testName = testName
		for blobName, blobProvider := range blobProviders {
			blobName = blobName
			blobProvider = blobProvider
			t.Run(fmt.Sprintf("%s: %s", testName, blobName), func(t *testing.T) {
				fileTest(t, blobProvider)
			})
		}
	}
	for _, blobProvider := range blobProviders {
		blobProvider.Close()
	}
}

func testFilestoreReadAndWrite(t *testing.T, store FileStore) {
	testWrite := []byte("example data")
	testKey := uuidWithoutDashes()
	exists, err := store.Exists(testKey)
	if exists {
		t.Fatalf("Exists when not yet written")
	}
	if err := store.Write(testKey, testWrite); err != nil {
		t.Fatalf("Failure writing data %s to key %s: %v", string(testWrite), testKey, err)
	}
	exists, err = store.Exists(testKey)
	if err != nil {
		t.Fatalf("Failure checking existence of key %s: %v", testKey, err)
	}
	if !exists {
		t.Fatalf("Test key %s does not exist: %v", testKey, err)
	}
	readData, err := store.Read(testKey)
	if err != nil {
		t.Fatalf("Could not read key %s from store: %v", testKey, err)
	}
	if string(readData) != string(testWrite) {
		t.Fatalf("Read data does not match written data: %s != %s", readData, testWrite)
	}
	if err := store.Delete(testKey); err != nil {
		t.Fatalf("Failed to delete test file with key %s: %v", testKey, err)
	}
}

func TestExecutorRunLocal(t *testing.T) {
	localConfig := LocalExecutorConfig{
		ScriptPath: "./scripts/k8s/offline_store_pandas_runner.py",
	}
	serialized, err := localConfig.Serialize()
	if err != nil {
		t.Fatalf("Error serializing local executor configuration: %v", err)
	}
	executor, err := NewLocalExecutor(serialized)
	if err != nil {
		t.Fatalf("Error creating new Local Executor: %v", err)
	}
	mydir, err := os.Getwd()
	if err != nil {
		t.Fatalf("could not get working directory")
	}

	sqlEnvVars := map[string]string{
		"MODE":                "local",
		"OUTPUT_URI":          fmt.Sprintf(`%s/scripts/k8s/tests/test_files/output/local_test`, mydir),
		"SOURCES":             fmt.Sprintf("%s/scripts/k8s/tests/test_files/inputs/transaction_short/part-00000-9d3cb5a3-4b9c-4109-afa3-a75759bfcf89-c000.snappy.parquet", mydir),
		"TRANSFORMATION_TYPE": "sql",
		"TRANSFORMATION":      "SELECT * FROM source_0 LIMIT 1",
	}
	if err := executor.ExecuteScript(sqlEnvVars); err != nil {
		t.Fatalf("Failed to execute pandas script: %v", err)
	}
}

func TestNewConfig(t *testing.T) {
	err := godotenv.Load("../.env")

	k8sConfig := K8sAzureConfig{
		ExecutorType:   K8s,
		ExecutorConfig: KubernetesExecutorConfig{},
		StoreType:      Azure,
		StoreConfig: AzureFileStoreConfig{
			AccountName:   helpers.GetEnv("AZURE_ACCOUNT_NAME", ""),
			AccountKey:    helpers.GetEnv("AZURE_ACCOUNT_KEY", ""),
			ContainerName: helpers.GetEnv("AZURE_CONTAINER_NAME", ""),
			Path:          "",
		},
	}
	serialized, err := k8sConfig.Serialize()
	if err != nil {
		t.Fatalf("could not serialize: %v", err)
	}
	provider, err := k8sAzureOfflineStoreFactory(SerializedConfig(serialized))
	if err != nil {
		t.Fatalf("could not get provider")
	}
	_, err = provider.AsOfflineStore()
	if err != nil {
		t.Fatalf("failed to convert store to offline store: %v", err)
	}
}

func Test_parquetIteratorFromReader(t *testing.T) {
	rows := 1000000
	type RowType struct {
		Index  int
		SIndex string
	}

	var buf bytes.Buffer
	w := parquet.NewWriter(&buf)
	var testRows []RowType
	for i := 1; i < rows; i++ {
		row := RowType{
			i,
			fmt.Sprintf("%d", i),
		}
		testRows = append(testRows, row)
		w.Write(row)
	}
	w.Close()
	iter, err := parquetIteratorFromBytes(buf.Bytes())
	if err != nil {
		t.Fatalf(err.Error())
	}
	index := 0
	for {
		value, err := iter.Next()
		if err != nil {
			break
		} else if value == nil && err == nil {
			break
		}
		var result RowType
		mapstructure.Decode(value, &result)
		if result != testRows[index] {
			t.Errorf("Rows not equal %v!=%v\n", value, testRows[index])
		}
		index += 1
	}
}

func testExists(t *testing.T, store FileStore) {
	randomKey := uuid.New().String()
	randomData := []byte(uuid.New().String())
	if err := store.Write(randomKey, randomData); err != nil {
		t.Fatalf("Could not write key to filestore: %v", err)
	}
	exists, err := store.Exists(randomKey)
	if err != nil {
		t.Fatalf("Could not check that key exists in filestore: %v", err)
	}
	if !exists {
		t.Fatalf("Key written to file store does not exist")
	}
	// cleanup test
	if store.Delete(randomKey); err != nil {
		t.Fatalf("error deleting random key: %v", err)
	}
}

func testNotExists(t *testing.T, store FileStore) {
	randomKey := uuid.New().String()
	exists, err := store.Exists(randomKey)
	if err != nil {
		t.Fatalf("Could not check that key exists in filestore: %v", err)
	}
	if exists {
		t.Fatalf("Key not written to file store exists")
	}
}

func randomStructList(length int64) []any {
	type PersonEntry struct {
		ID         int64
		Name       string
		Points     float32
		Score      float64
		Registered bool
		Created    int64 `parquet:"," parquet-key:",timestamp"`
	}
	personList := make([]any, length)
	for i := int64(0); i < length; i++ {
		personList[i] = PersonEntry{
			ID:         i,
			Name:       uuid.New().String(),
			Points:     float32(i) + 0.1,
			Score:      float64(i) + 0.1,
			Registered: false,
			Created:    time.Now().UnixMilli(),
		}
	}
	return personList
}

func compareStructWithInterface(compareStruct any, compareInterface map[string]interface{}) (bool, error) {
	structValue := reflect.ValueOf(compareStruct)
	structType := structValue.Type()

	for i := 0; i < structValue.NumField(); i++ {
		val, ok := compareInterface[structType.Field(i).Name]
		if !ok {
			return false, fmt.Errorf("submitted struct contains field not in interface: %s", structType.Field(i).Name)
		}
		if val != structValue.Field(i).Interface() {
			return false, fmt.Errorf("submitted struct field value not same as in interface. Expected %v %T, got %v %T", structValue.Field(i).Interface(), structValue.Field(i).Interface(), val, val)
		}
	}
	return true, nil
}

func testServe(t *testing.T, store FileStore) {
	parquetNumRows := int64(5)
	randomStructs := randomStructList(parquetNumRows)
	parquetBytes, err := convertToParquetBytes(randomStructs)
	if err != nil {
		t.Fatalf("could not convert struct list to parquet bytes: %v", err)
	}
	randomParquetKey := fmt.Sprintf("%s.parquet", uuid.New().String())
	if err := store.Write(randomParquetKey, parquetBytes); err != nil {
		t.Fatalf("Could not write parquet bytes to random key: %v", err)
	}
	iterator, err := store.Serve(randomParquetKey)
	if err != nil {
		t.Fatalf("Could not get parquet iterator: %v", err)
	}
	idx := int64(0)
	for {
		parquetRow, err := iterator.Next()
		idx += 1
		if err != nil {
			t.Fatalf("Error iterating through parquet file: %v", err)
		}
		if parquetRow == nil {
			if idx-1 != parquetNumRows {
				t.Fatalf("Incorrect number of rows in parquet file. Expected %d, got %d", parquetNumRows, idx-1)
			}
			break
		}
		if idx-1 > parquetNumRows {
			t.Fatalf("iterating over more rows than given")
		}
		identical, err := compareStructWithInterface(randomStructs[idx-1], parquetRow)
		if err != nil {
			t.Fatalf("Error comparing struct with interface: %v", err)
		}
		if !identical {
			t.Fatalf("Submitted row and returned struct not identical. Got %v, expected %v", parquetRow, randomStructs[idx-1])
		}
	}
	// cleanup test
	if err := store.Delete(randomParquetKey); err != nil {
		t.Fatalf("Could not delete parquet file: %v", err)
	}
}

func testServeDirectory(t *testing.T, store FileStore) {
	parquetNumRows := int64(5)
	parquetNumFiles := int64(5)
	randomDirectory := uuid.New().String()
	randomStructs := make([][]any, parquetNumFiles)
	for i := int64(0); i < parquetNumFiles; i++ {
		randomStructs[i] = randomStructList(parquetNumRows)
		parquetBytes, err := convertToParquetBytes(randomStructs[i])
		if err != nil {
			t.Fatalf("error converting struct to parquet bytes")
		}
		randomKey := fmt.Sprintf("part000%d%s.parquet", i, uuid.New().String())
		randomPath := fmt.Sprintf("%s/%s", randomDirectory, randomKey)
		if err := store.Write(randomPath, parquetBytes); err != nil {
			t.Fatalf("Could not write parquet bytes to path: %v", err)
		}
	}
	iterator, err := store.Serve(randomDirectory)
	if err != nil {
		t.Fatalf("Could not get parquet iterator: %v", err)
	}
	totalRows := int64(parquetNumFiles * parquetNumRows)
	idx := int64(0)
	for {
		parquetRow, err := iterator.Next()
		idx += 1
		if parquetRow == nil {
			if idx-1 != totalRows {
				t.Fatalf("Incorrect number of rows in parquet file. Expected %d, got %d", totalRows, idx-1)
			}
			break
		}
		if idx-1 > totalRows {
			t.Fatalf("iterating over more rows than given")
		}
		numFile := int((idx - 1) / 5)
		numRow := (idx - 1) % 5
		identical, err := compareStructWithInterface(randomStructs[numFile][numRow], parquetRow)
		if err != nil {
			t.Fatalf("Error comparing struct with interface: %v", err)
		}
		if !identical {
			t.Fatalf("Submitted row and returned struct not identical. Got %v, expected %v", parquetRow, randomStructs[numFile][numRow])
		}
	}
	// cleanup test
	if err := store.DeleteAll(randomDirectory); err != nil {
		t.Fatalf("Could not delete parquet directory: %v", err)
	}
}

func testDelete(t *testing.T, store FileStore) {
	randomKey := uuid.New().String()
	randomData := []byte(uuid.New().String())
	if err := store.Write(randomKey, randomData); err != nil {
		t.Fatalf("Could not write key to filestore: %v", err)
	}
	exists, err := store.Exists(randomKey)
	if err != nil {
		t.Fatalf("Could not check that key exists in filestore: %v", err)
	}
	if !exists {
		t.Fatalf("Key written to file store does not exist")
	}
	if err := store.Delete(randomKey); err != nil {
		t.Fatalf("Could not delete key from filestore: %v", err)
	}
	exists, err = store.Exists(randomKey)
	if err != nil {
		t.Fatalf("Could not check that key exists in filestore: %v", err)
	}
	if exists {
		t.Fatalf("Key deleted from file store exists")
	}

}

func testDeleteAll(t *testing.T, store FileStore) {
	randomListLength := 5
	randomDirectory := uuid.New().String()
	randomKeyList := make([]string, randomListLength)
	for i := 0; i < randomListLength; i++ {
		randomKeyList[i] = uuid.New().String()
		randomPath := fmt.Sprintf("%s/%s", randomDirectory, randomKeyList[i])
		randomData := []byte(uuid.New().String())
		if err := store.Write(randomPath, randomData); err != nil {
			t.Fatalf("Could not write key to filestore: %v", err)
		}
	}
	for i := 0; i < randomListLength; i++ {
		randomPath := fmt.Sprintf("%s/%s", randomDirectory, randomKeyList[i])
		exists, err := store.Exists(randomPath)
		if err != nil {
			t.Fatalf("Could not check that key exists in filestore: %v", err)
		}
		if !exists {
			t.Fatalf("Key written to file store does not exist")
		}
	}
	if err := store.DeleteAll(randomDirectory); err != nil {
		t.Fatalf("Could not delete directory: %v", err)
	}
	for i := 0; i < randomListLength; i++ {
		randomPath := fmt.Sprintf("%s/%s", randomDirectory, randomKeyList[i])
		exists, err := store.Exists(randomPath)
		if err != nil {
			t.Fatalf("Could not check that key exists in filestore: %v", err)
		}
		if exists {
			t.Fatalf("Key deleted from filestore does exist")
		}
	}

}

func testNewestFile(t *testing.T, store FileStore) {
	// write a bunch of blobs with different timestamps
	randomListLength := 5
	randomDirectory := uuid.New().String()
	randomKeyList := make([]string, randomListLength)
	for i := 0; i < randomListLength; i++ {
		randomKeyList[i] = uuid.New().String()
		randomPath := fmt.Sprintf("%s/%s.parquet", randomDirectory, randomKeyList[i])
		randomData := []byte(uuid.New().String())
		if err := store.Write(randomPath, randomData); err != nil {
			t.Fatalf("Could not write key to filestore: %v", err)
		}
		time.Sleep(1 * time.Second) // To guarantee ordering of created in metadata follows struct ordering
	}
	newestFile, err := store.NewestFile(randomDirectory)
	if err != nil {
		t.Fatalf("Error getting newest file from directory: %v", err)
	}
	expectedNewestFile := fmt.Sprintf("%s/%s.parquet", randomDirectory, randomKeyList[randomListLength-1])
	if newestFile != expectedNewestFile {
		t.Fatalf("Newest file did not retrieve actual newest file. Expected '%s', got '%s'", expectedNewestFile, newestFile)
	}
	// cleanup test
	if err := store.DeleteAll(randomDirectory); err != nil {
		t.Fatalf("Could not delete directory: %v", err)
	}
}

func testPathWithPrefix(t *testing.T, store FileStore) {
	randomKey := uuid.New().String()
	azureStore, ok := store.(AzureFileStore)
	if ok {
		azurePathWithPrefix := azureStore.PathWithPrefix(randomKey, false)
		if azurePathWithPrefix != fmt.Sprintf("%s/%s", azureStore.Path, randomKey) {
			t.Fatalf("Incorrect path with prefix. Expected %s, got %s", fmt.Sprintf("%s/%s", azureStore.Path, randomKey), azurePathWithPrefix)
		}
	}
	fileFileStore, ok := store.(FileFileStore)
	if ok {
		filePathWithPrefix := fileFileStore.PathWithPrefix(randomKey, false)
		if filePathWithPrefix != fmt.Sprintf("%s%s", fileFileStore.DirPath, randomKey) {
			t.Fatalf("Incorrect path with prefix. Expected %s, got %s", fmt.Sprintf("%s%s", fileFileStore.DirPath, randomKey), filePathWithPrefix)
		}
	}
}

func testNumRows(t *testing.T, store FileStore) {
	parquetNumRows := int64(5)
	randomStructList := randomStructList(parquetNumRows)
	parquetBytes, err := convertToParquetBytes(randomStructList)
	randomParquetPath := fmt.Sprintf("%s.parquet", uuid.New().String())
	if err != nil {
		t.Fatalf("Could not convert struct list to parquet bytes: %v", err)
	}
	if err := store.Write(randomParquetPath, parquetBytes); err != nil {
		t.Fatalf("Could not write parquet bytes to path: %v", err)
	}
	numRows, err := store.NumRows(randomParquetPath)
	if err != nil {
		t.Fatalf("Could not get num rows from parquet file: %v", err)
	}
	if numRows != parquetNumRows {
		t.Fatalf("Incorrect retrieved num rows from parquet file. Expected %d, got %d", numRows, parquetNumRows)
	}
	// cleanup test
	if err := store.Delete(randomParquetPath); err != nil {
		t.Fatalf("Could not delete parquet file: %v", err)
	}
}

func testDatabricksInitialization(t *testing.T, store FileStore) {
	host := helpers.GetEnv("DATABRICKS_HOST", "")
	token := helpers.GetEnv("DATABRICKS_ACCESS_TOKEN", "")
	cluster := helpers.GetEnv("DATABRICKS_CLUSTER", "")
	databricksConfig := DatabricksConfig{
		Host:    host,
		Token:   token,
		Cluster: cluster,
	}
	executor, err := NewDatabricksExecutor(databricksConfig)
	if err != nil {
		t.Fatalf("Could not create new databricks client: %v", err)
	}
	if err := executor.InitializeExecutor(store); err != nil {
		t.Fatalf("Error initializing executor: %v", err)
	}
	// sparkArgs := []string{}
	// if err := executor.RunSparkJob(&sparkArgs); err != nil {
	// 	t.Fatalf("could not run spark job: %v", err)
	// }
}

//tests for spark executor
// RunSparkJob(args *[]string) error
// InitializeExecutor(store FileStore) error
// PythonFileURI() string
// SparkSubmitArgs(destPath string, cleanQuery string, sourceList []string, jobType JobType) []string
// GetDFArgs(outputURI string, code string, mapping []SourceMapping) ([]string, error)
