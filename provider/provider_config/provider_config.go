package provider_config

import (
	ss "github.com/featureform/helpers/string_set"
	si "github.com/featureform/helpers/struct_iterator"
	sm "github.com/featureform/helpers/struct_map"
)

const EXAMPLE_GCP_CREDENTIALS = `{
	"type": "service_account",
	"project_id": "",
	"private_key_id": "",
	"private_key": "-----BEGIN PRIVATE KEY----------END PRIVATE KEY-----\n",
	"client_email": "",
	"client_id": "",
	"auth_uri": "https://accounts.google.com/o/oauth2/auth",
	"token_uri": "https://oauth2.googleapis.com/token",
	"auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
	"client_x509_cert_url": ""
}`

type ProviderConfig interface {
	Serialize() []byte
	Deserialize(config SerializedConfig) error
}

type MutableConfig interface {
	MutableFields() ss.StringSet
	DifferingFields(b MutableConfig) (ss.StringSet, error)
}

type SerializedConfig []byte

func differingFields(a, b interface{}) (ss.StringSet, error) {
	diff := ss.StringSet{}
	aIter, err := si.NewStructIterator(a)
	if err != nil {
		return nil, err
	}

	bMap, err := sm.NewStructMap(b)

	if err != nil {
		return nil, err
	}

	for aIter.Next() {
		key := aIter.Key()
		aVal := aIter.Value()
		if !bMap.Has(key, aVal) {
			diff[key] = true
		}
	}

	return diff, nil
}
