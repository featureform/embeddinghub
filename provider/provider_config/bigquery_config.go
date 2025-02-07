// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider_config

import (
	"encoding/json"

	"github.com/featureform/fferr"
	ss "github.com/featureform/helpers/stringset"
	"github.com/featureform/logging/redacted"
)

type BigQueryConfig struct {
	ProjectId   string
	DatasetId   string
	Credentials map[string]interface{}
}

func (bq *BigQueryConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, bq)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	return nil
}

func (bq *BigQueryConfig) Serialize() []byte {
	conf, err := json.Marshal(bq)
	if err != nil {
		panic(err)
	}
	return conf
}

func (bq BigQueryConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Credentials": true,
	}
}

func (a BigQueryConfig) DifferingFields(b BigQueryConfig) (ss.StringSet, error) {
	return differingFields(a, b)
}

func (bq *BigQueryConfig) Redacted() *BigQueryConfig {
	if bq == nil {
		return nil
	}
	redactedCreds := make(map[string]interface{})
	for k, _ := range bq.Credentials {
		redactedCreds[k] = redacted.String
	}
	return &BigQueryConfig{
		ProjectId:   bq.ProjectId,
		DatasetId:   bq.DatasetId,
		Credentials: redactedCreds,
	}
}
