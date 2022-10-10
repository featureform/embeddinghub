//go:build k8s
// +build k8s

package provider

import (
	"fmt"
	"testing"
	"reflect"
	"os"

	"github.com/google/uuid"
	// parquet "github.com/segmentio/parquet-go"
)

func TestBlobInterfaces(t *testing.T) {
	blobTests := map[string]func(*testing.T, BlobStore){
		"Test Blob Read and Write": testBlobReadAndWrite,
		// "Test Blob CSV Serve":      testBlobCSVServe,
		"Test Blob Parquet Serve": testBlobParquetServe,
	}
	localBlobStore, err := NewMemoryBlobStore(Config([]byte("")))
	if err != nil {
		t.Fatalf("Failed to create memory blob store")
	}
	mydir, err := os.Getwd()
    if err != nil {
        t.Fatalf("could not get working directory")
    }
	fmt.Println(mydir)


	fileStoreConfig := FileBlobStoreConfig{DirPath: fmt.Sprintf(`file:////%s/tests/file_tests`, mydir)}
	serializedFileConfig, err := fileStoreConfig.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize file store config: %v", err)
	}
	fileBlobStore, err := NewFileBlobStore(serializedFileConfig)
	if err != nil {
		t.Fatalf("failed to create new file blob store: %v", err)
	}
	azureStoreConfig := AzureBlobStoreConfig{
		AccountName: "featureformtesting",
		AccountKey: os.Getenv("AZURE_ACCOUNT_KEY"),
		BucketName: "testcontainer",
	}
	serializedAzureConfig, err := azureStoreConfig.Serialize()
	if err != nil {
		t.Fatalf("dailed to serialize azure store config: %v", err)
	}
	azureBlobStore, err := NewAzureBlobStore(serializedAzureConfig)
	if err != nil {
		t.Fatalf("failed to create new azure blob store: %v", err)
	}
	fmt.Println(azureBlobStore)
	fmt.Println(localBlobStore)
	

	blobProviders := map[string]BlobStore{
		// "Local": localBlobStore,
		"File": fileBlobStore,
		// "Azure": azureBlobStore,
	}
	for testName, blobTest := range blobTests {
		blobTest = blobTest
		testName = testName
		for blobName, blobProvider := range blobProviders {
			blobName = blobName
			blobProvider = blobProvider
			t.Run(fmt.Sprintf("%s: %s", testName, blobName), func(t *testing.T) {
				blobTest(t, blobProvider)
			})
		}
	}
	for blobName, blobProvider := range blobProviders {
		fmt.Printf("Closing %s blob store\n", blobName)
		blobProvider.Close()
	}
}

func testBlobReadAndWrite(t *testing.T, store BlobStore) {
	testWrite := []byte("example data")
	testKey := uuid.New().String()
	if err := store.Write(testKey, testWrite); err != nil {
		t.Fatalf("Failure writing data %s to key %s: %v", string(testWrite), testKey, err)
	}
	exists, err := store.Exists(testKey)
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

// func testBlobCSVServe(t *testing.T, store BlobStore) {
// 	//write csv file, then iterate all data types
// 	csvBytes := []byte(`1,2,3,4,5
// 	6,7,8,9,10`)
// 	testKey := fmt.Sprintf("%s.csv", uuid.New().String())
// 	if err := store.Write(testKey, csvBytes); err != nil {
// 		t.Fatalf("Failure writing csv data %s to key %s: %v", string(csvBytes), testKey, err)
// 	}
// 	iterator, err := store.Serve(testKey)
// 	if err != nil {
// 		t.Fatalf("Failure getting serving iterator with key %s: %v", testKey, err)
// 	}
// 	for row, err := iterator.Next(); err != nil; row, err = iterator.Next() {
// 		fmt.Println(row)
// 	}
// 	fmt.Println(err)
// }


func testBlobParquetServe(t *testing.T, store BlobStore) {
	testKey := fmt.Sprintf("input/transactions.snappy.parquet")
	iterator, err := store.Serve(testKey)
	if err != nil {
		t.Fatalf("Failure getting serving iterator with key %s: %v", testKey, err)
	}
	for row, err := iterator.Next(); err == nil; row, err = iterator.Next() {
		if err != nil {
			break
		}
		fmt.Printf("%v, %T\n", reflect.ValueOf(row), row)
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
	fmt.Println(mydir)
	sqlEnvVars := map[string]string{
		"MODE": "local",
		"OUTPUT_URI": fmt.Sprintf(`%s/scripts/k8s/tests/test_files/output/local_test/`, mydir),
		"SOURCES": fmt.Sprintf("%s/scripts/k8s/tests/test_files/inputs/transaction", mydir),
		"TRANSFORMATION_TYPE": "sql",
		"TRANSFORMATION": "SELECT * FROM source_0 LIMIT 1",
	}
	if err := executor.ExecuteScript(sqlEnvVars); err != nil {
		t.Fatalf("Failed to execute pandas script: %v", err)
	}
}