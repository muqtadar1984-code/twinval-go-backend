package db

import (
	"context"
	"embed"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// MigrationFile is one .sql file in the migrations/ directory.
type MigrationFile struct {
	Version string
	SQL     string
}

// loadMigrations reads every embedded .sql file and returns them sorted
// by filename (which encodes the version number).
func loadMigrations() ([]MigrationFile, error) {
	entries, err := migrationFS.ReadDir("migrations")
	if err != nil {
		return nil, fmt.Errorf("read embedded migrations: %w", err)
	}
	files := make([]MigrationFile, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		raw, err := migrationFS.ReadFile("migrations/" + e.Name())
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", e.Name(), err)
		}
		version := strings.TrimSuffix(e.Name(), ".sql")
		files = append(files, MigrationFile{Version: version, SQL: string(raw)})
	}
	sort.Slice(files, func(i, j int) bool {
		return files[i].Version < files[j].Version
	})
	return files, nil
}

// MigrateUp applies every embedded migration that has not yet been
// recorded in ingestion_schema_migrations. Idempotent — safe to run on
// every container boot. The migrations themselves are wrapped in
// IF NOT EXISTS so a partially-applied state is recoverable.
//
// Migrations are tracked in a separate table from alembic_version so
// the portal's Python migration tool and this Go runner can coexist on
// the same database without stepping on each other.
func MigrateUp(ctx context.Context, pool *pgxpool.Pool) error {
	files, err := loadMigrations()
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return nil
	}

	// Bootstrap the tracking table itself — the first migration creates
	// it, but we need to read it before applying anything. Run the
	// CREATE TABLE separately so the read query never fails.
	const bootstrap = `
CREATE TABLE IF NOT EXISTS ingestion_schema_migrations (
    version     TEXT PRIMARY KEY,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);`
	if _, err := pool.Exec(ctx, bootstrap); err != nil {
		return fmt.Errorf("bootstrap migration table: %w", err)
	}

	rows, err := pool.Query(ctx, `SELECT version FROM ingestion_schema_migrations`)
	if err != nil {
		return fmt.Errorf("read applied migrations: %w", err)
	}
	applied := map[string]struct{}{}
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			rows.Close()
			return err
		}
		applied[v] = struct{}{}
	}
	rows.Close()

	for _, m := range files {
		if _, ok := applied[m.Version]; ok {
			continue
		}
		if _, err := pool.Exec(ctx, m.SQL); err != nil {
			return fmt.Errorf("apply migration %s: %w", m.Version, err)
		}
	}
	return nil
}
