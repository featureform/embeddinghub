// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package helpers

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/featureform/fferr"
	psql "github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

// Postgres
func getPostgresConfig(connectionString string) (*pgxpool.Config, error) {
	const defaultMaxConns = 10
	const defaultMinConns = 0
	const defaultMaxConnLifetime = time.Hour
	const defaultMaxConnIdleTime = time.Minute
	const defaultHealthCheckPeriod = time.Minute
	const defaultConnectTimeout = time.Minute * 5

	dbConfig, err := pgxpool.ParseConfig(connectionString)
	if err != nil {
		return nil, fferr.NewConnectionError(connectionString, err)
	}

	dbConfig.MaxConns = defaultMaxConns
	dbConfig.MinConns = defaultMinConns
	dbConfig.MaxConnLifetime = defaultMaxConnLifetime
	dbConfig.MaxConnIdleTime = defaultMaxConnIdleTime
	dbConfig.HealthCheckPeriod = defaultHealthCheckPeriod
	dbConfig.ConnConfig.ConnectTimeout = defaultConnectTimeout

	return dbConfig, nil
}

func NewPSQLPoolConnection(config PSQLConfig) (*pgxpool.Pool, error) {
	poolConfig, err := getPostgresConfig(config.ConnectionString())
	if err != nil {
		return nil, err
	}

	db, err := pgxpool.ConnectConfig(context.Background(), poolConfig)
	if err != nil {
		return nil, fferr.NewInternalError(fmt.Errorf("failed to open connection to Postgres: %w", err))
	}

	return db, nil
}

func NewMetadataPSQLConfigFromEnv() PSQLConfig {
	config := PSQLConfig{
		Host:     GetEnv("PSQL_HOST", "localhost"),
		Port:     GetEnv("PSQL_PORT", "5432"),
		User:     GetEnv("PSQL_USER", "postgres"),
		Password: GetEnv("PSQL_PASSWORD", "password"),
		DBName:   GetEnv("PSQL_DB", "postgres"),
		SSLMode:  GetEnv("PSQL_SSLMODE", "disable"),
	}
	return config
}

func NewMetadataPSQLConfigForTesting() PSQLConfig {
	config := PSQLConfig{
		Host:     "localhost",
		User:     "postgres",
		Password: "password",
		Port:     "5432",
		DBName:   "postgres",
		SSLMode:  "disable",
	}
	return config
}

type PSQLConfig struct {
	Host     string
	Port     string
	User     string
	Password string
	DBName   string
	SSLMode  string
}

func (c PSQLConfig) ConnectionString() string {
	u := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(c.User, c.Password),
		Host:   fmt.Sprintf("%s:%s", c.Host, c.Port),
		Path:   c.DBName,
		RawQuery: (url.Values{
			"sslmode": []string{c.SSLMode},
		}).Encode(),
	}

	return u.String()
}

func SanitizePostgres(ident string) string {
	return psql.Identifier{ident}.Sanitize()
}

// ETCD
type ETCDConfig struct {
	Host        string
	Port        string
	Username    string
	Password    string
	DialTimeout time.Duration
}

func (c ETCDConfig) URL() string {
	u := &url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:%s", c.Host, c.Port),
	}
	return u.String()
}
