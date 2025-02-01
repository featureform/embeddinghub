// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package location

import (
	"encoding/json"
	"fmt"
	pb "github.com/featureform/metadata/proto"
	"strings"

	"github.com/featureform/fferr"
	"github.com/featureform/filestore"
	"github.com/featureform/logging"
)

type LocationType string

const (
	NilLocationType       LocationType = "nil_location_type"
	SQLLocationType       LocationType = "sql"
	FileStoreLocationType LocationType = "filestore"
	CatalogLocationType   LocationType = "catalog"
	KafkaLocationType     LocationType = "kafka"
)

type Location interface {
	Location() string
	Type() LocationType
	Serialize() (string, error)
	Deserialize(config []byte) error
	Proto() *pb.Location
}

type NilLocation struct{}

func (loc NilLocation) Location() string {
	return "<nil location>"
}

func (loc NilLocation) Type() LocationType {
	return NilLocationType
}

func (loc NilLocation) Serialize() (string, error) {
	// Serializes into valid JSON
	return "{}", nil
}

func (loc NilLocation) Deserialize(config []byte) error {
	confStr := string(config)
	if confStr == "{}" {
		return nil
	}
	errMsg := fmt.Sprintf("Cannot deserialize into nil location\n%s\n", confStr)
	logging.GlobalLogger.Error(errMsg)
	return fferr.NewInternalErrorf(errMsg)
}

func (loc NilLocation) Proto() *pb.Location {
	return nil
}

type JSONLocation struct {
	OutputLocation string  `json:"outputLocation"`
	LocationType   string  `json:"locationType"`
	TableFormat    *string `json:"tableFormat,omitempty"`
}

func NewSQLLocation(table string) Location {
	return &SQLLocation{table: table}
}

func NewFullyQualifiedSQLLocation(database, schema, table string) Location {
	return &SQLLocation{database: database, schema: schema, table: table}
}

type FullyQualifiedObject struct {
	Database string
	Schema   string
	Table    string
}

func (f FullyQualifiedObject) String() string {
	parts := []string{}

	// Only add database and schema if they are not empty
	if f.Database != "" && f.Schema != "" {
		parts = append(parts, f.Database)
	}

	if f.Schema != "" {
		parts = append(parts, f.Schema)
	}

	parts = append(parts, f.Table)

	return strings.Join(parts, ".")
}

type SQLLocation struct {
	database string
	schema   string
	table    string
}

func (l *SQLLocation) GetDatabase() string {
	return l.database
}

func (l *SQLLocation) GetSchema() string {
	return l.schema
}

func (l *SQLLocation) GetTable() string {
	return l.table
}

func (l *SQLLocation) Location() string {
	return l.table
}

func (l *SQLLocation) TableLocation() FullyQualifiedObject {
	return FullyQualifiedObject{
		Database: l.database,
		Schema:   l.schema,
		Table:    l.table,
	}
}

func (l *SQLLocation) Type() LocationType {
	return SQLLocationType
}

func (l *SQLLocation) MarshalJSON() ([]byte, error) {
	return json.Marshal(JSONLocation{
		OutputLocation: l.Location(),
		LocationType:   "sql",
	})
}

func (l *SQLLocation) Serialize() (string, error) {
	data, err := json.Marshal(l)
	if err != nil {
		return "", fferr.NewInternalErrorf("failed to serialize SQLLocation: %v", err)
	}

	return string(data), nil
}

func (l *SQLLocation) Deserialize(config []byte) error {
	var jsonLoc JSONLocation
	if err := json.Unmarshal(config, &jsonLoc); err != nil {
		return fferr.NewInternalErrorf("failed to deserialize SQLLocation: %v", err)
	}
	if jsonLoc.LocationType != string(SQLLocationType) {
		return fferr.NewInternalErrorf("invalid location type for SQLLocation: %s", jsonLoc.LocationType)
	}
	l.table = jsonLoc.OutputLocation
	return nil
}

func (l *SQLLocation) Proto() *pb.Location {
	return &pb.Location{
		Location: &pb.Location_Table{
			Table: &pb.SQLTable{
				Database: l.database,
				Schema:   l.schema,
				Name:     l.table,
			},
		},
	}
}

func NewFileLocation(path filestore.Filepath) Location {
	return &FileStoreLocation{path: path}
}

type FileStoreLocation struct {
	path filestore.Filepath
}

func (l FileStoreLocation) Location() string {
	return l.path.ToURI()
}

func (l FileStoreLocation) Type() LocationType {
	return FileStoreLocationType
}

func (l FileStoreLocation) Serialize() (string, error) {
	data, err := json.Marshal(l)
	if err != nil {
		return "", fferr.NewInternalErrorf("failed to serialize FileStoreLocation: %v", err)
	}

	return string(data), nil
}

func (l FileStoreLocation) Filepath() filestore.Filepath {
	return l.path
}

func (l FileStoreLocation) MarshalJSON() ([]byte, error) {
	return json.Marshal(JSONLocation{
		OutputLocation: l.Location(),
		LocationType:   "filestore",
	})
}

func (l *FileStoreLocation) Deserialize(config []byte) error {
	var jsonLoc JSONLocation
	if err := json.Unmarshal(config, &jsonLoc); err != nil {
		return fferr.NewInternalErrorf("failed to deserialize FileStoreLocation: %v", err)
	}
	if jsonLoc.LocationType != string(FileStoreLocationType) {
		return fferr.NewInternalErrorf("invalid location type for FileStoreLocation: %s", jsonLoc.LocationType)
	}
	fp := filestore.FilePath{}
	if err := fp.ParseFilePath(jsonLoc.OutputLocation); err != nil {
		return err
	}
	l.path = &fp
	return nil
}

func (l *FileStoreLocation) Proto() *pb.Location {
	return &pb.Location{
		Location: &pb.Location_Filestore{
			Filestore: &pb.FileStoreTable{
				Path: l.path.ToURI(),
			},
		},
	}
}

func NewCatalogLocation(database, table, tableFormat string) Location {
	return &CatalogLocation{database: database, table: table, tableFormat: tableFormat}
}

type CatalogLocation struct {
	database    string
	table       string
	tableFormat string
}

func (l CatalogLocation) Location() string {
	return fmt.Sprintf("%s.%s", l.database, l.table)
}

func (l CatalogLocation) Type() LocationType {
	return CatalogLocationType
}

func (l CatalogLocation) Serialize() (string, error) {
	data, err := json.Marshal(l)
	if err != nil {
		return "", fferr.NewInternalErrorf("failed to serialize CatalogLocation: %v", err)
	}

	return string(data), nil
}

func (l CatalogLocation) Database() string {
	return l.database
}

func (l CatalogLocation) Table() string {
	return l.table
}

func (l CatalogLocation) TableFormat() string {
	return l.tableFormat
}

func (l CatalogLocation) MarshalJSON() ([]byte, error) {
	return json.Marshal(JSONLocation{
		OutputLocation: l.Location(),
		LocationType:   "catalog",
		TableFormat:    &l.tableFormat,
	})
}

func (l *CatalogLocation) Deserialize(config []byte) error {
	var jsonLoc JSONLocation
	if err := json.Unmarshal(config, &jsonLoc); err != nil {
		return fferr.NewInternalErrorf("failed to deserialize CatalogLocation: %v", err)
	}
	if jsonLoc.LocationType != string(CatalogLocationType) {
		return fferr.NewInternalErrorf("invalid location type for CatalogLocation: %s", jsonLoc.LocationType)
	}
	l.tableFormat = *jsonLoc.TableFormat
	locationParts := strings.Split(jsonLoc.OutputLocation, ".")
	if len(locationParts) != 2 {
		return fferr.NewInternalErrorf("invalid location format for CatalogLocation: %s; expected the pattern '<database_name>.<table_name>'", jsonLoc.OutputLocation)
	}
	l.database = locationParts[0]
	l.table = locationParts[1]
	return nil
}

func (l *CatalogLocation) Proto() *pb.Location {
	return &pb.Location{
		Location: &pb.Location_Catalog{
			Catalog: &pb.CatalogTable{
				Database:    l.database,
				Table:       l.table,
				TableFormat: l.tableFormat,
			},
		},
	}
}

type KafkaLocation struct {
	Topic string
}

func (l *KafkaLocation) Location() string {
	return l.Topic
}

// Type returns the type of the location as Kafka.
func (l *KafkaLocation) Type() LocationType {
	return KafkaLocationType
}

func (l *KafkaLocation) Serialize() (string, error) {
	data, err := json.Marshal(l)
	if err != nil {
		return "", fferr.NewInternalErrorf("failed to serialize KafkaLocation: %v", err)
	}
	return string(data), nil
}

func (l *KafkaLocation) MarshalJSON() ([]byte, error) {
	return json.Marshal(JSONLocation{
		OutputLocation: l.Location(),
		LocationType:   "kafka",
	})
}

func (l *KafkaLocation) Deserialize(config []byte) error {
	var jsonLoc JSONLocation
	if err := json.Unmarshal(config, &jsonLoc); err != nil {
		return fferr.NewInternalErrorf("failed to deserialize KafkaLocation: %v", err)
	}
	if jsonLoc.LocationType != string(KafkaLocationType) {
		return fferr.NewInternalErrorf("invalid location type for KafkaLocation: %s", jsonLoc.LocationType)
	}
	l.Topic = jsonLoc.OutputLocation
	return nil
}

func (l *KafkaLocation) Proto() *pb.Location {
	return &pb.Location{
		Location: &pb.Location_Kafka{
			Kafka: &pb.Kafka{
				Topic: l.Topic,
			},
		},
	}
}

func NewKafkaLocation(topic string) Location {
	return &KafkaLocation{Topic: topic}
}

func FromProto(pbLocation *pb.Location) (Location, error) {
	if pbLocation == nil {
		return nil, fferr.NewInternalErrorf("nil Location protobuf provided")
	}

	switch loc := pbLocation.Location.(type) {
	case *pb.Location_Table:
		return NewFullyQualifiedSQLLocation(loc.Table.Database, loc.Table.Schema, loc.Table.Name), nil

	case *pb.Location_Filestore:
		// Handle FileStoreTable case
		fp := filestore.FilePath{}
		err := fp.ParseFilePath(loc.Filestore.Path)
		if err != nil {
			return nil, fferr.NewInternalErrorf("invalid filestore path: %v", err)
		}
		return NewFileLocation(&fp), nil

	case *pb.Location_Catalog:
		return NewCatalogLocation(loc.Catalog.Database, loc.Catalog.Table, loc.Catalog.TableFormat), nil

	case *pb.Location_Kafka:
		return NewKafkaLocation(loc.Kafka.Topic), nil

	default:
		// Handle unknown or nil location
		return nil, fferr.NewInternalErrorf("unknown or unsupported location type in protobuf: %T", loc)
	}
}
