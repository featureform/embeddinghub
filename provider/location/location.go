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
)

type Location interface {
	Location() string
	Type() LocationType
	Serialize() (string, error)
	Deserialize(config []byte) error
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
	// In some cases, only the schema is empty.
	// One example is BigQuery, where we map a database to a dataset,
	// but it has no concept of a schema, so we ignore it.
	if f.Database != "" {
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
