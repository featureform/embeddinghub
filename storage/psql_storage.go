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
	"github.com/featureform/fferr"
	"github.com/featureform/helpers"
	"github.com/featureform/logging"
	"github.com/featureform/storage/query"
	"github.com/featureform/storage/sqlgen"
	"github.com/jackc/pgx/v4/pgxpool"
	_ "github.com/lib/pq"
	"strings"
)

func NewPSQLStorageImplementation(config helpers.PSQLConfig, tableName string) (metadataStorageImplementation, error) {
	db, err := helpers.NewPSQLPoolConnection(config)
	if err != nil {
		return nil, err
	}

	connection, err := db.Acquire(context.Background())
	if err != nil {
		return nil, fferr.NewInternalErrorf("failed to acquire connection from the database pool: %w", err)
	}

	err = connection.Ping(context.Background())
	if err != nil {
		return nil, fferr.NewInternalErrorf("failed to ping the database: %w", err)
	}

	indexName := "ff_key_pattern"
	sanitizedName := helpers.SanitizePostgres(tableName)
	// Create a table to store the key-value pairs.
	tableCreationSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (key VARCHAR(2048) PRIMARY KEY, value TEXT, marked_for_deletion_at TIMESTAMP default null)", sanitizedName)
	_, err = db.Exec(context.Background(), tableCreationSQL)
	if err != nil {
		return nil, fferr.NewInternalErrorf("failed to create table %s: %w", sanitizedName, err)
	}

	// Add a text index to use for LIKE queries
	indexCreationSQL := fmt.Sprintf("CREATE INDEX %s ON %s (key text_pattern_ops);", indexName, sanitizedName)
	_, err = db.Exec(context.Background(), indexCreationSQL)
	if err != nil {
		// Index probably aleady exists, ignore the error
		fmt.Printf("failed to create index %s on %s: %v", indexName, sanitizedName, err)
	}

	return &psqlStorageImplementation{
		db:         db,
		tableName:  tableName,
		connection: connection,
		logger:     logging.NewLogger("psql_storage"),
	}, nil
}

type psqlStorageImplementation struct {
	db         *pgxpool.Pool
	tableName  string
	connection *pgxpool.Conn
	logger     logging.Logger
}

func (psql *psqlStorageImplementation) Set(key string, value string) error {
	if key == "" {
		return fferr.NewInvalidArgumentError(fmt.Errorf("cannot set an empty key"))
	}

	insertSQL := psql.setQuery()
	psql.logger.Debugw("Setting key with query", "query", insertSQL, "key", key, "value", value)
	_, err := psql.db.Exec(context.Background(), insertSQL, key, value)
	if err != nil {
		return fferr.NewInternalErrorf("failed to set key %s: %w", key, err)
	}

	return nil
}

func (psql *psqlStorageImplementation) Get(key string, opts ...query.Query) (string, error) {
	if key == "" {
		return "", fferr.NewInvalidArgumentErrorf("cannot get an empty key")
	}

	opts = append(opts, query.ValueEquals{
		Column: query.SQLColumn{
			Column: "key",
		},
		Value: key,
	})

	psql.logger.Infow("Getting key with options", "options", opts, "table", psql.tableName)
	qry, err := sqlgen.NewListQuery(psql.tableName, opts, query.SQLColumn{Column: "value"})
	if err != nil {
		return "", err
	}

	qryStr, args, err := qry.Compile()
	psql.logger.Debugw("get: compiled query", "query", qryStr, "args", args)
	if err != nil {
		return "", err
	}
	row := psql.db.QueryRow(context.Background(), qryStr, args...)

	var value string
	err = row.Scan(&value)
	if err != nil {
		return "", fferr.NewInternalErrorf("failed to get key %s: %w", key, err)
	}

	return value, nil
}

func (psql *psqlStorageImplementation) List(prefix string, opts ...query.Query) (map[string]string, error) {
	opts = append(opts, query.KeyPrefix{Prefix: prefix})
	psql.logger.Infow("Listing keys with options", "options", opts, "table", psql.tableName)
	qry, err := sqlgen.NewListQuery(psql.tableName, opts)
	if err != nil {
		psql.logger.Errorw("List failed", "error", err)
		return nil, err
	}
	qryStr, args, err := qry.Compile()
	psql.logger.Debugw("List: Compiled query", "query", qryStr, "args", args)
	if err != nil {
		psql.logger.Errorw("Failed to compile list query", "error", err, "query", qry)
		return nil, err
	}
	rows, err := psql.db.Query(context.TODO(), qryStr, args...)
	if err != nil {
		psql.logger.Errorw("List failed", "error", err)
		return nil, fferr.NewInternalErrorf("failed to list keys with prefix %s: %w", prefix, err)
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

func (psql *psqlStorageImplementation) Count(prefix string, opts ...query.Query) (int, error) {
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
	row := psql.db.QueryRow(context.TODO(), qryStr, args...)
	var cnt int
	if err := row.Scan(&cnt); err != nil {
		return 0, err
	}
	return cnt, nil
}

func (psql *psqlStorageImplementation) ListColumn(prefix string, columns []query.Column, opts ...query.Query) ([]map[string]interface{}, error) {
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
	rows, err := psql.db.Query(context.TODO(), qryStr, args...)
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

func (psql *psqlStorageImplementation) Delete(key string) (string, error) {
	if key == "" {
		return "", fferr.NewInvalidArgumentError(fmt.Errorf("cannot delete empty key"))
	}

	deleteSQL := psql.deleteQuery()
	row := psql.db.QueryRow(context.Background(), deleteSQL, key)

	var value string
	err := row.Scan(&value)
	if err != nil && strings.Contains(err.Error(), "no rows in result set") {
		return "", fferr.NewKeyNotFoundError(key, nil)
	} else if err != nil {
		return "", fferr.NewInternalErrorf("failed to delete key %s: %v", key, err)
	}

	return value, nil
}

func (psql *psqlStorageImplementation) Close() {
	psql.connection.Release()
	psql.db.Close()
}

// SQL Queries
func (psql *psqlStorageImplementation) setQuery() string {
	return fmt.Sprintf("INSERT INTO %s (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = $2", helpers.SanitizePostgres(psql.tableName))
}

func (psql *psqlStorageImplementation) deleteQuery() string {
	return fmt.Sprintf("DELETE FROM %s WHERE key = $1 RETURNING value", helpers.SanitizePostgres(psql.tableName))
}

func (psql *psqlStorageImplementation) Type() MetadataStorageType {
	return PSQLMetadataStorage
}
