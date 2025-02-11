// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider_config

import (
	"encoding/json"
	"io/ioutil"
	"reflect"
	"testing"

	ss "github.com/featureform/helpers/stringset"
)

func TestFirestoreConfigMutableFields(t *testing.T) {
	expected := ss.StringSet{
		"Credentials": true,
	}

	config := FirestoreConfig{
		ProjectID:   "ff-gcp-proj-id",
		Collection:  "transactions-ds",
		Credentials: map[string]interface{}{},
	}
	actual := config.MutableFields()

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected %v but received %v", expected, actual)
	}
}

func TestFirestoreConfigDifferingFields(t *testing.T) {
	type args struct {
		a FirestoreConfig
		b FirestoreConfig
	}

	gcpCredsBytes, err := ioutil.ReadFile("../test_files/gcp_creds.json")
	if err != nil {
		t.Errorf("failed to read gcp_creds.json due to %v", err)
	}

	var credentialsDictA map[string]interface{}
	err = json.Unmarshal(gcpCredsBytes, &credentialsDictA)
	if err != nil {
		t.Errorf("failed to unmarshal GCP credentials: %v", err)
	}
	var credentialsDictB map[string]interface{}
	err = json.Unmarshal([]byte(gcpCredsBytes), &credentialsDictB)
	if err != nil {
		t.Errorf("failed to unmarshal GCP credentials: %v", err)
	}
	credentialsDictB["client_email"] = "test@featureform.com"

	tests := []struct {
		name     string
		args     args
		expected ss.StringSet
	}{
		{"No Differing Fields", args{
			a: FirestoreConfig{
				ProjectID:   "ff-gcp-proj-id",
				Collection:  "transactions-ds",
				Credentials: map[string]interface{}{},
			},
			b: FirestoreConfig{
				ProjectID:   "ff-gcp-proj-id",
				Collection:  "transactions-ds",
				Credentials: map[string]interface{}{},
			},
		}, ss.StringSet{}},
		{"Differing Fields", args{
			a: FirestoreConfig{
				ProjectID:   "ff-gcp-proj-id",
				Collection:  "transactions-ds",
				Credentials: credentialsDictA,
			},
			b: FirestoreConfig{
				ProjectID:   "ff-gcp-proj-v2-id",
				Collection:  "transactions-ds",
				Credentials: credentialsDictB,
			},
		}, ss.StringSet{
			"ProjectID":   true,
			"Credentials": true,
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := tt.args.a.DifferingFields(tt.args.b)

			if err != nil {
				t.Errorf("Failed to get differing fields due to error: %v", err)
			}

			if !reflect.DeepEqual(actual, tt.expected) {
				t.Errorf("Expected %v, but instead found %v", tt.expected, actual)
			}

		})
	}

}
