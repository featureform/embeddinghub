package provider_config

import "encoding/json"

type EMRConfig struct {
	Credentials   AWSCredentials
	ClusterRegion string
	ClusterName   string
}

func (e *EMRConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, e)
	if err != nil {
		return err
	}
	return nil
}

func (e *EMRConfig) Serialize() ([]byte, error) {
	conf, err := json.Marshal(e)
	if err != nil {
		return nil, err
	}
	return conf, nil
}

func (e *EMRConfig) IsExecutorConfig() bool {
	return true
}
