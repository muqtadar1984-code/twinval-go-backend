package humanobs

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PgFetcher reads severity counts from the Intern Observation Portal's
// `observation_entries` table over a shared Railway Postgres connection.
//
// READ-ONLY by design. The portal's alembic migrations own the schema;
// this package never writes to portal tables.
//
// Schema reference (portal-owned):
//
//	observation_entries(
//	  id, intern_id, property_id, sensor_zone_id, owner_profile_id,
//	  submitted_at TIMESTAMPTZ,
//	  building_label TEXT,    -- frozen snapshot at submit time
//	  zone_label    TEXT,     -- frozen snapshot at submit time
//	  severity      ENUM('Normal','Watch','Alert'),
//	  voided        BOOLEAN,
//	  ...
//	)
//
// Building/zone matching uses the snapshot fields so observations stay
// addressable forever — even after a property is renamed or archived.
type PgFetcher struct {
	pool *pgxpool.Pool
}

// NewPgFetcher wires a fetcher to a pgx pool. The pool is owned by the
// caller; PgFetcher does not Close() it on shutdown.
func NewPgFetcher(pool *pgxpool.Pool) *PgFetcher {
	return &PgFetcher{pool: pool}
}

// FetchSeverityCounts implements Fetcher. Returns counts of non-voided
// observations submitted at or after `since`, matching the supplied
// building+zone snapshot fields.
func (f *PgFetcher) FetchSeverityCounts(ctx context.Context, building, zone string, since time.Time) (SeverityCounts, error) {
	const stmt = `
SELECT severity, COUNT(*)
FROM observation_entries
WHERE building_label = $1
  AND zone_label = $2
  AND voided = false
  AND submitted_at >= $3
GROUP BY severity`
	rows, err := f.pool.Query(ctx, stmt, building, zone, since)
	if err != nil {
		return SeverityCounts{}, fmt.Errorf("query observation_entries: %w", err)
	}
	defer rows.Close()

	var out SeverityCounts
	for rows.Next() {
		var sev string
		var n int
		if err := rows.Scan(&sev, &n); err != nil {
			return SeverityCounts{}, fmt.Errorf("scan: %w", err)
		}
		switch sev {
		case "Normal":
			out.Normal = n
		case "Watch":
			out.Watch = n
		case "Alert":
			out.Alert = n
		}
	}
	if err := rows.Err(); err != nil {
		return SeverityCounts{}, err
	}
	return out, nil
}
