-- TwinVal live ingestion pipeline schema.
--
-- This migration adds two tables to the SHARED Railway Postgres
-- database used by the Intern Observation Portal. It does NOT touch
-- portal-owned tables (alembic-managed): users, owner_profiles,
-- properties, property_stakeholders, sensor_zones, observation_entries,
-- access_grants, alembic_version.
--
-- Migrations for this side are tracked in ingestion_schema_migrations
-- to keep them isolated from alembic_version.

CREATE TABLE IF NOT EXISTS ingestion_schema_migrations (
    version     TEXT PRIMARY KEY,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Raw sensor readings — one row per validated reading, before denoising.
-- High-volume table; partition / archive policy is operational, not
-- enforced at schema level.
CREATE TABLE IF NOT EXISTS sensor_readings_raw (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    sensor_id     TEXT NOT NULL,
    building      TEXT NOT NULL,
    zone          TEXT NOT NULL,
    sensor_type   TEXT NOT NULL,
    unit          TEXT NOT NULL,
    raw_value     DOUBLE PRECISION NOT NULL,
    quality       DOUBLE PRECISION NOT NULL,
    source        TEXT NOT NULL,           -- "mqtt" | "webhook" | "modbus"
    received_at   TIMESTAMPTZ NOT NULL,
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_readings_sensor_id      ON sensor_readings_raw (sensor_id);
CREATE INDEX IF NOT EXISTS ix_readings_zone           ON sensor_readings_raw (building, zone);
CREATE INDEX IF NOT EXISTS ix_readings_received_at    ON sensor_readings_raw (received_at DESC);

-- Aggregated zone snapshots with their computed indicators + RTPMV.
-- One row per zone per aggregation window. data_source distinguishes
-- live readings from simulated runs that may share the same database.
CREATE TABLE IF NOT EXISTS zone_snapshots (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    building        TEXT NOT NULL,
    zone            TEXT NOT NULL,
    snapshot_at     TIMESTAMPTZ NOT NULL,
    sensor_values   JSONB NOT NULL,        -- map of sensor_type -> normalised value
    shf             DOUBLE PRECISION,
    esf             DOUBLE PRECISION,
    uss             DOUBLE PRECISION,
    pdp             DOUBLE PRECISION,
    ci              DOUBLE PRECISION,
    health_factor   DOUBLE PRECISION,
    rtpmv           DOUBLE PRECISION,
    land_value      DOUBLE PRECISION,
    structure_value DOUBLE PRECISION,
    data_source     TEXT NOT NULL          -- "live" | "simulated"
);

CREATE INDEX IF NOT EXISTS ix_snapshots_zone        ON zone_snapshots (building, zone);
CREATE INDEX IF NOT EXISTS ix_snapshots_at          ON zone_snapshots (snapshot_at DESC);
CREATE INDEX IF NOT EXISTS ix_snapshots_data_source ON zone_snapshots (data_source);

INSERT INTO ingestion_schema_migrations (version) VALUES ('0001_create_ingestion_tables')
ON CONFLICT (version) DO NOTHING;
