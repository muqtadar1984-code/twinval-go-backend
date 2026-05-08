// Package db owns Postgres connectivity for the live ingestion pipeline.
//
// It uses pgx/v5 with connection pooling. The same database holds the
// Intern Observation Portal's tables (managed by alembic) — this package
// must NEVER alter portal-owned tables. Migrations here are tracked in
// ingestion_schema_migrations, separate from alembic_version.
package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// NewPool returns a pgxpool.Pool wired for the ingestion service.
// The pool is safe for concurrent use across goroutines.
//
// Defaults: max 16 connections, 5-minute idle timeout, 30-second connect
// timeout. Override via the connection string's pool params if needed.
func NewPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	if dsn == "" {
		return nil, fmt.Errorf("DSN is empty")
	}

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse DSN: %w", err)
	}

	if cfg.MaxConns < 4 {
		cfg.MaxConns = 16
	}
	if cfg.MaxConnIdleTime == 0 {
		cfg.MaxConnIdleTime = 5 * time.Minute
	}
	if cfg.ConnConfig.ConnectTimeout == 0 {
		cfg.ConnConfig.ConnectTimeout = 30 * time.Second
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("new pool: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	return pool, nil
}
