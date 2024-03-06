package provider_config

import (
	"encoding/json"
	"fmt"
	"github.com/featureform/fferr"

	filestore "github.com/featureform/filestore"
	ss "github.com/featureform/helpers/string_set"
	"github.com/mitchellh/mapstructure"
)

type K8sConfig struct {
	ExecutorType   ExecutorType
	ExecutorConfig interface{}
	StoreType      filestore.FileStoreType
	StoreConfig    FileStoreConfig
}

func (k8s *K8sConfig) Serialize() ([]byte, error) {
	data, err := json.Marshal(k8s)
	if err != nil {
		return nil, fferr.NewInternalError(err)
	}
	return data, nil
}

func (k8s *K8sConfig) Deserialize(data SerializedConfig) error {
	err := json.Unmarshal(data, k8s)
	if err != nil {
		return err
	}
	return nil
}

func (k8s *K8sConfig) UnmarshalJSON(data []byte) error {
	type tempConfig struct {
		ExecutorType   ExecutorType
		ExecutorConfig interface{}
		StoreType      filestore.FileStoreType
		StoreConfig    map[string]interface{}
	}

	var temp tempConfig
	err := json.Unmarshal(data, &temp)
	if err != nil {
		return fferr.NewInternalError(err)
	}

	k8s.ExecutorType = temp.ExecutorType
	k8s.StoreType = temp.StoreType

	if temp.ExecutorConfig == "" {
		k8s.ExecutorConfig = ExecutorConfig{}
	} else {
		err = k8s.decodeExecutor(temp.ExecutorType, temp.ExecutorConfig)
		if err != nil {
			return err
		}
	}

	err = k8s.decodeFileStore(temp.StoreType, temp.StoreConfig)
	if err != nil {
		return err
	}

	return nil
}

func (k8s *K8sConfig) decodeExecutor(executorType ExecutorType, configMap interface{}) error {
	serializedExecutor, err := json.Marshal(configMap)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	excConfig := ExecutorConfig{}
	err = excConfig.Deserialize(serializedExecutor)
	if err != nil {
		return fferr.NewInternalError(err)
	}

	k8s.ExecutorConfig = excConfig
	return nil
}

func (k8s *K8sConfig) decodeFileStore(fileStoreType filestore.FileStoreType, configMap map[string]interface{}) error {
	var fileStoreConfig FileStoreConfig
	switch fileStoreType {
	case filestore.Azure:
		fileStoreConfig = &AzureFileStoreConfig{}
	case filestore.S3:
		fileStoreConfig = &S3FileStoreConfig{}
	default:
		return fferr.NewProviderConfigError("Kubernetes", fmt.Errorf("the file store type '%s' is not supported for k8s", fileStoreType))
	}

	err := mapstructure.Decode(configMap, fileStoreConfig)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	k8s.StoreConfig = fileStoreConfig
	return nil
}

func (k8s K8sConfig) MutableFields() ss.StringSet {
	result := ss.StringSet{
		"ExecutorConfig": true,
	}

	var storeFields ss.StringSet
	switch k8s.StoreType {
	case filestore.Azure:
		storeFields = k8s.StoreConfig.(*AzureFileStoreConfig).MutableFields()
	case filestore.S3:
		storeFields = k8s.StoreConfig.(*S3FileStoreConfig).MutableFields()
	}

	for field, val := range storeFields {
		result["Store."+field] = val
	}

	return result
}

func (a K8sConfig) DifferingFields(b K8sConfig) (ss.StringSet, error) {
	result := ss.StringSet{}

	if a.StoreType != b.StoreType {
		return result, fferr.NewInternalError(fmt.Errorf("store config mismatch: a = %v; b = %v", a.StoreType, b.StoreType))
	}

	executorFields, err := differingFields(a.ExecutorConfig, b.ExecutorConfig)
	if err != nil {
		return result, err
	}

	if len(executorFields) > 0 {
		result["ExecutorConfig"] = true
	}

	var storeFields ss.StringSet
	switch a.StoreType {
	case filestore.Azure:
		storeFields, err = a.StoreConfig.(*AzureFileStoreConfig).DifferingFields(*b.StoreConfig.(*AzureFileStoreConfig))
	case filestore.S3:
		storeFields, err = a.StoreConfig.(*S3FileStoreConfig).DifferingFields(*b.StoreConfig.(*S3FileStoreConfig))
	default:
		return nil, fferr.NewProviderConfigError("Kubernetes", fmt.Errorf("unsupported store type: %v", a.StoreType))
	}

	if err != nil {
		return result, err
	}

	for field, val := range storeFields {
		result["Store."+field] = val
	}

	return result, err
}

const (
	GoProc ExecutorType = "GO_PROCESS"
	K8s    ExecutorType = "K8S"
)
