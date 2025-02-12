// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2025 FeatureForm Inc.
//

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"time"

	"github.com/jackc/pgx/v4/stdlib"

	"github.com/featureform/fferr"
	"github.com/featureform/logging"
	"github.com/featureform/logging/redacted"
	pl "github.com/featureform/provider/location"

	"github.com/avast/retry-go/v4"
	psql "github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

type Pool struct {
	*pgxpool.Pool
}

func getConfig(connectionString string, logger logging.Logger) (*pgxpool.Config, error) {
	const defaultMaxConns = 10
	const defaultMinConns = 0
	const defaultMaxConnLifetime = time.Hour
	const defaultMaxConnIdleTime = time.Minute
	const defaultHealthCheckPeriod = time.Minute
	const defaultConnectTimeout = time.Minute * 5

	dbConfig, err := pgxpool.ParseConfig(connectionString)
	if err != nil {
		logger.Errorw("Connecting to postgres failed", "err", err)
		return nil, fferr.NewConnectionError(connectionString, err)
	}

	// TODO properly handle defaults
	dbConfig.MaxConns = defaultMaxConns
	dbConfig.MinConns = defaultMinConns
	dbConfig.MaxConnLifetime = defaultMaxConnLifetime
	dbConfig.MaxConnIdleTime = defaultMaxConnIdleTime
	dbConfig.HealthCheckPeriod = defaultHealthCheckPeriod
	dbConfig.ConnConfig.ConnectTimeout = defaultConnectTimeout
	logger.Debugw(
		"Set postgres pool connection options",
		"MaxConns", dbConfig.MaxConns,
		"MinConns", dbConfig.MinConns,
		"MaxConnLifetime", dbConfig.MaxConnLifetime,
		"MaxConnIdleTime", dbConfig.MaxConnIdleTime,
		"HealthCheckPeriod", dbConfig.HealthCheckPeriod,
		"Connection timeout", dbConfig.ConnConfig.ConnectTimeout,
	)
	return dbConfig, nil
}

func NewPool(ctx context.Context, config Config) (*Pool, error) {
	logger := logging.GetLoggerFromContext(ctx)
	logger.With("psql-connect-config", config.Redacted())
	logger.Info("Creating postgres pool connection")
	poolConfig, err := getConfig(config.ConnectionString(), logger)
	if err != nil {
		logger.Errorw("Failed to parse postgres config", "err", err)
		return nil, err
	}
	// Ensure the context has a timeout
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		defaultTimeout := 2 * time.Second
		logger.Infof(
			"Connecting to Postgres pool has no deadline, defaulting to %v",
			defaultTimeout,
		)
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultTimeout)
		defer cancel()
	}
	connectAttempt := 0
	var pool *Pool
	retryErr := retry.Do(
		func() error {
			connectAttempt++
			logger = logger.With("psql-connect-attempt", connectAttempt)
			var err error
			pool, err = newPool(ctx, logger, poolConfig)
			return err
		},
		retry.Context(ctx),                // Respect the context's timeout/cancellation
		retry.Delay(100*time.Millisecond), // Wait time between retries
		retry.MaxDelay(1*time.Second),     // Maximum delay (optional)
		retry.Attempts(0),                 // Unlimited retries (until the context times out)
		retry.LastErrorOnly(true),         // Only return the last error
	)
	ctxTimedOutBeforeFirstAttempt := pool == nil
	if retryErr != nil || ctxTimedOutBeforeFirstAttempt {
		logger.Errorw("Timed out connecting to Postgres", "err", retryErr)
		return nil, retryErr
	}
	return pool, nil
}

func newPool(ctx context.Context, logger logging.Logger, poolConfig *pgxpool.Config) (*Pool, error) {
	logger.Debug("Attempting to connect to pool")
	db, err := pgxpool.ConnectConfig(ctx, poolConfig)
	if err != nil {
		logger.Errorw("Failed to connect to postgres pool", "err", err)
		return nil, fferr.NewInternalErrorf("failed to open connection to Postgres: %w", err)
	}

	logger.Debug("Acquiring connection")
	conn, err := db.Acquire(ctx)
	if err != nil {
		logger.Errorw("Failed to acquire connection to postgres", "err", err)
		return nil, fferr.NewInternalErrorf("failed to open connection to Postgres: %w", err)
	}
	defer conn.Release()

	logger.Debug("Pinging postgres")
	if err := conn.Ping(ctx); err != nil {
		logger.Errorw("Failed to ping to postgres", "err", err)
		return nil, fferr.NewInternalErrorf("failed to ping Postgres: %w", err)
	}

	logger.Info("Created postgres pool connection")
	return &Pool{db}, nil
}

// ToSqlDB returns a *sql.DB from the pgx pool, for use with libraries that require it.
// IMPORTANT:
// 1. If possible, use the pgx pool directly as this method creates an entirely new connection pool
// 2. The caller MUST call Close() on the returned *sql.DB when it's no longer needed to prevent connection leaks
// 3. This method may return an error if the connection cannot be established
func (p *Pool) ToSqlDB() (*sql.DB, error) {
	db := stdlib.OpenDB(*p.Config().ConnConfig)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fferr.NewInternalErrorf("failed to ping Postgres: %v", err)
	}
	return db, nil
}

type Config struct {
	Host     string
	Port     string
	User     string
	Password string
	DBName   string
	SSLMode  string
}

func (c Config) Redacted() map[string]any {
	return map[string]any{
		"Host":     c.Host,
		"Port":     c.Port,
		"User":     c.User,
		"Password": redacted.String,
		"DBName":   c.DBName,
		"SSLMode":  c.SSLMode,
	}
}

func (c Config) ConnectionString() string {
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

func Sanitize(ident string) string {
	return psql.Identifier{ident}.Sanitize()
}

func SanitizeLocation(obj pl.SQLLocation) string {
	var parts []string
	if obj.GetDatabase() != "" && obj.GetSchema() != "" {
		parts = append(parts, obj.GetDatabase())
		parts = append(parts, obj.GetSchema())
	}
	parts = append(parts, obj.GetTable())
	return psql.Identifier(parts).Sanitize()
}
