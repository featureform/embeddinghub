// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package storage

import (
	"context"
	"fmt"
	"strings"

	"github.com/featureform/fferr"
	"github.com/featureform/helpers/postgres"
	"github.com/featureform/logging"
	"github.com/featureform/storage/query"
	"github.com/featureform/storage/sqlgen"
)

func NewPSQLStorageImplementation(ctx context.Context, db *postgres.Pool, tableName string) (metadataStorageImplementation, error) {
	logger := logging.GetLoggerFromContext(ctx)
	indexName := "ff_key_pattern"
	sanitizedName := postgres.Sanitize(tableName)
	// Create a table to store the key-value pairs
	tableCreationSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (key VARCHAR(2048) PRIMARY KEY, value TEXT)", sanitizedName)
	if _, err := db.Exec(ctx, tableCreationSQL); err != nil {
		logger.Errorw("Failed to create table", "table-name", sanitizedName, "err", err)
		return nil, fferr.NewInternalErrorf("failed to create table %s: %w", sanitizedName, err)
	}

	// This column is used in deletion
	addClm := fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS marked_for_deletion_at TIMESTAMP DEFAULT null", sanitizedName)
	if _, err := db.Exec(ctx, addClm); err != nil {
		logger.Errorw("Failed to add deletion column", "table-name", sanitizedName, "err", err)
		return nil, fferr.NewInternalErrorf("failed to add deletion column to %s: %w", sanitizedName, err)
	}

	// Add a text index to use for LIKE queries
	indexCreationSQL := fmt.Sprintf("CREATE INDEX %s ON %s (key text_pattern_ops);", indexName, sanitizedName)
	if _, err := db.Exec(ctx, indexCreationSQL); err != nil {
		// Index probably aleady exists, ignore the error
		logger.Warnf("failed to create index %s on %s: %v", indexName, sanitizedName, err)
	}

	return &PSQLStorageImplementation{
		Db:        db,
		tableName: tableName,
		logger:    logger,
	}, nil
}

type PSQLStorageImplementation struct {
	Db        *postgres.Pool
	tableName string
	logger    logging.Logger // TODO remove and pass in ctx
}

func (psql *PSQLStorageImplementation) Set(key string, value string) error {
	if key == "" {
		return fferr.NewInvalidArgumentError(fmt.Errorf("cannot set an empty key"))
	}

	insertSQL := psql.setQuery()
	psql.logger.Debugw("Setting key with query", "query", insertSQL, "key", key, "value", value)
	_, err := psql.Db.Exec(context.Background(), insertSQL, key, value)
	if err != nil {
		return fferr.NewInternalErrorf("failed to set key %s: %w", key, err)
	}

	return nil
}

func (psql *PSQLStorageImplementation) GetWithOpts(key string, opts ...query.Query) (string, error) {
	if key == "" {
		psql.logger.Errorw("Cannot get an empty key")
		return "", fferr.NewInvalidArgumentErrorf("cannot get an empty key")
	}

	opts = append(opts, query.ValueEquals{
		Column: query.SQLColumn{
			Column: "key",
		},
		Value: key,
	})

	logger := psql.logger.With("key", key, "options", opts, "table", psql.tableName)
	logger.Infow("Getting key with options")
	qry, err := sqlgen.NewListQuery(psql.tableName, opts, query.SQLColumn{Column: "value"})
	if err != nil {
		logger.Errorw("qry builder failed", "error", err)
		return "", err
	}

	qryStr, args, err := qry.Compile()
	logger = logger.With("query", qryStr, "args", args)
	logger.Debugw("get: compiled query", "query", qryStr, "args", args)
	if err != nil {
		logger.Errorw("Failed to compile get query", "error", err)
		return "", err
	}
	row := psql.Db.QueryRow(context.Background(), qryStr, args...)

	var value string
	if err := row.Scan(&value); err != nil {
		psql.logger.Errorw("Failed to scan row", "error", err)
		return "", fferr.NewInternalErrorf("failed to get key %s: %v", key, err)
	}

	return value, nil
}

func (psql *PSQLStorageImplementation) Get(key string) (string, error) {
	return psql.GetWithOpts(key)
}

func (psql *PSQLStorageImplementation) List(prefix string, opts ...query.Query) (map[string]string, error) {
	opts = append(opts, query.KeyPrefix{Prefix: prefix})
	logger := psql.logger.With("prefix", prefix, "options", opts, "table", psql.tableName)
	logger.Infow("Listing keys with options")
	qry, err := sqlgen.NewListQuery(psql.tableName, opts)
	if err != nil {
		psql.logger.Errorw("List failed", "error", err)
		return nil, err
	}
	qryStr, args, err := qry.Compile()
	logger = logger.With("query", qryStr, "args", args)
	logger.Debugw("List: Compiled query")
	if err != nil {
		logger.Errorw("Failed to compile list query", "error", err)
		return nil, err
	}
	rows, err := psql.Db.Query(context.TODO(), qryStr, args...)
	if err != nil {
		logger.Errorw("List failed", "error", err)
		return nil, fferr.NewInternalErrorf("failed to list keys with prefix %s: %v", prefix, err)
	}

	result := make(map[string]string)
	for rows.Next() {
		var key, value string
		err := rows.Scan(&key, &value)
		if err != nil {
			return nil, fferr.NewInternalErrorf("failed to scan key-value pair: %w", err)
		}
		result[key] = value
	}

	return result, nil
}

func (psql *PSQLStorageImplementation) Count(prefix string, opts ...query.Query) (int, error) {
	opts = append(opts, query.KeyPrefix{Prefix: prefix})
	psql.logger.Infow("Counting keys with options", "options", opts, "table", psql.tableName)
	qry, err := sqlgen.NewListQuery(psql.tableName, opts)
	if err != nil {
		psql.logger.Errorw("List failed", "error", err)
		return 0, err
	}
	qryStr, args, err := qry.CompileCount()
	if err != nil {
		psql.logger.Errorw("Failed to compile count query", "error", err)
		return 0, err
	}
	row := psql.Db.QueryRow(context.TODO(), qryStr, args...)
	var cnt int
	if err := row.Scan(&cnt); err != nil {
		return 0, err
	}
	return cnt, nil
}

func (psql *PSQLStorageImplementation) ListColumn(prefix string, columns []query.Column, opts ...query.Query) ([]map[string]interface{}, error) {
	opts = append(opts, query.KeyPrefix{Prefix: prefix})
	psql.logger.Infow("Listing computed columns with options", "options", opts, "table", psql.tableName)
	qry, err := sqlgen.NewListQuery(psql.tableName, opts, columns...)
	if err != nil {
		psql.logger.Errorw("List failed", "error", err)
		return nil, err
	}
	qryStr, args, err := qry.Compile()
	if err != nil {
		psql.logger.Errorw("Failed to compile list query", "error", err)
		return nil, err
	}
	rows, err := psql.Db.Query(context.TODO(), qryStr, args...)
	if err != nil {
		psql.logger.Errorw("List failed", "error", err)
		return nil, fferr.NewInternalErrorf("failed to list rows with prefix %s: %w", prefix, err)
	}
	defer rows.Close()

	fieldDescriptions := rows.FieldDescriptions()
	columnNames := make([]string, len(fieldDescriptions))
	for i, description := range fieldDescriptions {
		columnNames[i] = string(description.Name)
	}

	//rows of results (is dynamic, the caller will need to know or test for the value types)
	var results []map[string]interface{}

	values := make([]interface{}, len(columnNames))
	valuePtrs := make([]interface{}, len(columnNames))

	for rows.Next() {
		// assign pointers to the value slice (to scan into)
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		// scan the row
		err := rows.Scan(valuePtrs...)
		if err != nil {
			return nil, fferr.NewInternalErrorf("failed to scan row: %w", err)
		}

		rowMap := make(map[string]interface{})
		for i, colName := range columnNames {
			rowMap[colName] = values[i]
		}

		results = append(results, rowMap)
	}

	if err := rows.Err(); err != nil {
		return nil, fferr.NewInternalErrorf("row error occurred: %w", err)
	}

	return results, nil
}

func (psql *PSQLStorageImplementation) Delete(key string) (string, error) {
	if key == "" {
		return "", fferr.NewInvalidArgumentError(fmt.Errorf("cannot delete empty key"))
	}

	deleteSQL := psql.deleteQuery()
	row := psql.Db.QueryRow(context.Background(), deleteSQL, key)

	var value string
	err := row.Scan(&value)
	if err != nil && strings.Contains(err.Error(), "no rows in result set") {
		return "", fferr.NewKeyNotFoundError(key, nil)
	} else if err != nil {
		return "", fferr.NewInternalErrorf("failed to delete key %s: %v", key, err)
	}

	return value, nil
}

func (psql *PSQLStorageImplementation) Pool() *postgres.Pool {
	return psql.Db
}

func (psql *PSQLStorageImplementation) Close() {
	// No-op
}

// SQL Queries
func (psql *PSQLStorageImplementation) setQuery() string {
	return fmt.Sprintf("INSERT INTO %s (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = $2", postgres.Sanitize(psql.tableName))
}

func (psql *PSQLStorageImplementation) deleteQuery() string {
	return fmt.Sprintf("DELETE FROM %s WHERE key = $1 RETURNING value", postgres.Sanitize(psql.tableName))
}

func (psql *PSQLStorageImplementation) Type() MetadataStorageType {
	return PSQLMetadataStorage
}
