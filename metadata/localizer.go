// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package metadata

import (
	"github.com/featureform/fferr"
	"github.com/featureform/filestore"
	"github.com/featureform/logging"
	pb "github.com/featureform/metadata/proto"
	pl "github.com/featureform/provider/location"
	pc "github.com/featureform/provider/provider_config"
	ps "github.com/featureform/provider/provider_schema"
	pt "github.com/featureform/provider/provider_type"
)

const DEFAULT_GLUE_DATABASE = "featureform"

type Localizer interface {
	LocalizeSourceVariant(sv *pb.SourceVariant) (pl.Location, error)
	LocalizeFeatureVariant(fv *pb.FeatureVariant) (pl.Location, error)
}

type SparkLocalizer struct {
	providerConfig *pc.SparkConfig
	logger         logging.Logger
}

func NewSparkLocalizer(config pc.SerializedConfig) (*SparkLocalizer, error) {
	providerConfig := &pc.SparkConfig{}
	if err := providerConfig.Deserialize(config); err != nil {
		return nil, err
	}
	return &SparkLocalizer{
		providerConfig: providerConfig,
		logger:         logging.NewLogger("localizer"),
	}, nil
}

func (sl *SparkLocalizer) LocalizeSourceVariant(sv *pb.SourceVariant) (pl.Location, error) {
	logger := sl.logger.With("source_name", sv.Name, "source_variant", sv.Variant)
	var location pl.Location
	if sl.providerConfig.UsesCatalog() {
		database := sl.providerConfig.GlueConfig.Database
		// Previously, the database was hardcoded as "featureform" in a path-like format
		// (e.g. "glue://featureform/resourcetype__name__variant"); however, the Glue config
		// now has fields for database and table format. In the event database is empty, the
		// assumption is that the provider was registered prior to PR #1109, and therefore
		// uses the hardcoded database name `featureform`
		if database == "" {
			logger.Warnw("Glue database not set; using default database", "default_database", DEFAULT_GLUE_DATABASE)
			database = DEFAULT_GLUE_DATABASE
		}
		tableFormat := sl.providerConfig.GlueConfig.TableFormat
		// Similarly, any GlueConfig that lacks a table format is assumed to be using Iceberg
		// as this was the only supported table format prior to PR #1109
		if tableFormat == "" {
			logger.Warnw("Glue table format not set; using default table format", "default_table_format", pc.Iceberg)
			tableFormat = pc.Iceberg
		}
		tableName, err := ps.ResourceToCatalogTableName("Transformation", sv.Name, sv.Variant)
		if err != nil {
			logger.Error("failed to get catalog name for source variant", err)
			return nil, err
		}
		location = pl.NewCatalogLocation(database, tableName, string(tableFormat))
	} else {
		var fp filestore.Filepath
		switch storeConfig := sl.providerConfig.StoreConfig.(type) {
		case *pc.S3FileStoreConfig:
			logger.Debugw("Provider config is S3", "bucket", storeConfig.BucketPath)
			s3fp, err := filestore.NewEmptyDirpath(filestore.S3)
			if err != nil {
				logger.Errorw("failed to create S3 filepath", "err", err)
				return nil, err
			}
			if err := s3fp.SetScheme(filestore.S3APrefix); err != nil {
				logger.Errorw("failed to set S3 scheme", "err", err)
				return nil, err
			}
			if err := s3fp.SetBucket(storeConfig.BucketPath); err != nil {
				logger.Errorw("failed to set S3 bucket", "err", err)
				return nil, err
			}
			var key string
			if storeConfig.Path != "" {
				key = storeConfig.Path + "/" + ps.ResourceToDirectoryPath("Transformation", sv.Name, sv.Variant)
			} else {
				key = ps.ResourceToDirectoryPath("Transformation", sv.Name, sv.Variant)
			}
			logger.Debugw("Setting key", "key", key)
			if err := s3fp.SetKey(key); err != nil {
				logger.Errorw("failed to set S3 key", "err", err)
				return nil, err
			}
			fp = s3fp
		default:
			logger.Errorw("unsupported store config type", "store_config_type", storeConfig)
			return nil, fferr.NewInternalErrorf("unsupported store config type %T", storeConfig)
		}
		location = pl.NewFileLocation(fp)
	}
	return location, nil
}

func (sl *SparkLocalizer) LocalizeFeatureVariant(fv *pb.FeatureVariant) (pl.Location, error) {
	sl.logger.Warn("FeatureVariant localization not implemented for Spark")
	return pl.NilLocation{}, nil
}

type SqlLocalizer struct {
}

func (sl *SqlLocalizer) LocalizeSourceVariant(sv *pb.SourceVariant) (pl.Location, error) {
	var resourceType string
	switch sv.GetDefinition().(type) {
	case *pb.SourceVariant_PrimaryData:
		resourceType = "Primary"
	case *pb.SourceVariant_Transformation:
		resourceType = "Transformation"
	default:
		return nil, fferr.NewInternalErrorf("unsupported source variant definition type %T", sv.GetDefinition())
	}

	tableName, err := ps.ResourceToTableName(resourceType, sv.Name, sv.Variant)
	if err != nil {
		return nil, err
	}
	return pl.NewSQLLocation(tableName), nil
}

func (sl *SqlLocalizer) LocalizeFeatureVariant(sv *pb.FeatureVariant) (pl.Location, error) {
	materializationName, err := ps.ResourceToTableName("Materialization", sv.Name, sv.Variant)
	if err != nil {
		return nil, err
	}
	return pl.NewSQLLocation(materializationName), nil
}

func GetLocalizer(providerType pt.Type, config pc.SerializedConfig) (Localizer, error) {
	switch providerType {
	case pt.SparkOffline:
		return NewSparkLocalizer(config)
	case pt.SnowflakeOffline, pt.BigQueryOffline, pt.RedshiftOffline, pt.ClickHouseOffline, pt.PostgresOffline:
		return &SqlLocalizer{}, nil
	default:
		return nil, fferr.NewInternalErrorf("provider type %s does not support localizer interface", providerType)
	}
}
