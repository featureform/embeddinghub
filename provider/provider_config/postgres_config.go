package provider_config

import (
	"encoding/json"
	"github.com/featureform/fferr"

	ss "github.com/featureform/helpers/string_set"
)

type PostgresConfig struct {
	Host     string `json:"Host"`
	Port     string `json:"Port"`
	Username string `json:"Username"`
	Password string `json:"Password"`
	Database string `json:"Database"`
	SSLMode  string `json:"SSLMode"`
}

func (pg *PostgresConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, pg)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	return nil
}

func (pg *PostgresConfig) Serialize() []byte {
	conf, err := json.Marshal(pg)
	if err != nil {
		panic(err)
	}
	return conf
}

func (pg PostgresConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Username": true,
		"Password": true,
		"Port":     true,
		"SSLMode":  true,
	}
}

func (a PostgresConfig) DifferingFields(b PostgresConfig) (ss.StringSet, error) {
	return differingFields(a, b)
}
