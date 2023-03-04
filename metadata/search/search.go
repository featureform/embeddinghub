// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package search

import (
	"fmt"
	re "github.com/avast/retry-go/v4"
	"github.com/typesense/typesense-go/typesense"
	"github.com/typesense/typesense-go/typesense/api"
	"time"
)

type Searcher interface {
	Upsert(ResourceDoc) error
	RunSearch(q string) ([]ResourceDoc, error)
	DeleteAll() error
}

type TypeSenseParams struct {
	Host   string
	Port   string
	ApiKey string
}

type Search struct {
	client *typesense.Client
}

func NewTypesenseSearch(params *TypeSenseParams) (Searcher, error) {
	client := typesense.NewClient(
		typesense.WithServer(fmt.Sprintf("http://%s:%s", params.Host, params.Port)),
		typesense.WithAPIKey(params.ApiKey))

	// Retries connection to typesense. If there is an error creating the schema
	// it stops attempting, otherwise uses a backoff delay and retries.
	err := re.Do(
		func() error {
			if _, errRetr := client.Collection("resource").Retrieve(); errRetr != nil {
				errHttp, isHttpErr := errRetr.(*typesense.HTTPError)
				schemaNotFound := isHttpErr && errHttp.Status == 404
				if schemaNotFound {
					if err := makeSchema(client); err != nil {
						return re.Unrecoverable(err)
					}
				} else {
					fmt.Printf("could not connect to typesense. retrying...\n")
				}
				return errRetr
			}
			return nil
		},
		re.DelayType(func(n uint, err error, config *re.Config) time.Duration {
			return re.BackOffDelay(n, err, config)
		}),
		re.Attempts(10),
	)
	if err != nil {
		return nil, err
	}
	if err := initializeCollection(client); err != nil {
		return nil, err
	}
	return &Search{
		client: client,
	}, nil
}

type ResourceDoc struct {
	Name    string
	Variant string
	Type    string
}

func makeSchema(client *typesense.Client) error {
	schema := &api.CollectionSchema{
		Name: "resource",
		Fields: []api.Field{
			{
				Name: "Name",
				Type: "string",
			},
			{
				Name: "Variant",
				Type: "string",
			},
			{
				Name: "Type",
				Type: "string",
			},
		},
		TokenSeparators: &[]string{
			"-",
			"_",
		},
	}
	_, err := client.Collections().Create(schema)
	return err
}

func initializeCollection(client *typesense.Client) error {
	var resourceinitial []interface{}
	var resourceempty ResourceDoc
	resourceinitial = append(resourceinitial, resourceempty)
	action := "create"
	batchnum := 40
	params := &api.ImportDocumentsParams{
		Action:    &action,
		BatchSize: &batchnum,
	}
	//initializing resource collection with empty struct so we can use upsert function
	_, err := client.Collection("resource").Documents().Import(resourceinitial, params)
	return err
}

func (s Search) Upsert(doc ResourceDoc) error {
	_, err := s.client.Collection("resource").Documents().Upsert(doc)
	return err
}

func (s Search) DeleteAll() error {
	if _, err := s.client.Collection("resource").Delete(); err != nil {
		return err
	}
	return makeSchema(s.client)
}

func (s Search) RunSearch(q string) ([]ResourceDoc, error) {
	searchParameters := &api.SearchCollectionParams{
		Q:       q,
		QueryBy: "Name",
	}
	results, errGetResults := s.client.Collection("resource").Documents().Search(searchParameters)
	if errGetResults != nil {
		return nil, errGetResults
	}
	var searchresults []ResourceDoc
	for _, hit := range *results.Hits {
		doc := *hit.Document
		searchresults = append(searchresults, ResourceDoc{
			Name:    doc["Name"].(string),
			Type:    doc["Type"].(string),
			Variant: doc["Variant"].(string),
		})
	}
	return searchresults, nil
}
