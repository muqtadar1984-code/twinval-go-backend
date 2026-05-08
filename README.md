# internal/compute — Patent Module 340

## What this package does

This is the mathematical core of TwinVal. It implements **Patent Module 340
(Computation Module)** from the Complete Specification filed 13 March 2026,
Application No. 202641030498.

It computes:
1. Five technical indicators (SHF, ESF, USS, PDP, CI)
2. Health Factor (composite of all five)
3. Real-Time Property Market Value (RTPMV)

## Files

| File | Purpose |
|------|---------|
| `types.go` | All data structures — `ConditionedData`, `TechnicalIndicators`, `RTPMV`, `BaselineMarketValue` |
| `indicators.go` | All computation functions — one function per indicator + `ComputeHealthFactor` + `ComputeRTPMV` |
| `indicators_test.go` | Unit tests + parity fixtures for validation against Python POC |

## Key formulas (from patent specification)

```
RTPMV = Land_Value + (Structure_Value × Health_Factor)

Health_Factor = SHF × ESF × (1 − USS) × PDP × CI
```

## Rules for Claude Code

1. **Do not change default config values** in `DefaultSHFConfig()`,
   `DefaultESFConfig()`, etc. without also updating `indicators_test.go`
   parity fixtures. These values must match the Python POC exactly.

2. **Do not reorder the Health Factor formula**. The `(1 − USS)` inversion
   is intentional — USS measures stress (higher = worse), so it must be
   inverted before multiplication.

3. **All indicator functions must return values in [0.0, 1.0]**. The
   `clamp()` helper enforces this. Never remove clamp calls.

4. **The Land_Value in RTPMV must never be multiplied by Health Factor**.
   Only Structure_Value is adjusted. This is a core patent claim.

5. **Wöhler S-N curve in ComputeSHF** — the `wohlerPenalty()` function
   implements a non-linear fatigue model. Low readings below the normal
   ceiling produce zero penalty. Do not linearise this.

6. **Effective age in ComputePDP** can decrease below chronological age
   when `ConditionQuality > 0.5`. This is the maintenance incentive
   mechanism described in patent paragraph [0070]. Do not clamp it
   to chronological age as a floor.

## How to run tests

```bash
cd internal/compute
go test ./... -v
```

## Parity validation against Python POC

Before deploying any changes:

1. Run the Python POC simulator with `degradedPropertyData()` inputs
2. Record the output for SHF, ESF, USS, PDP, CI, Health Factor
3. Paste those values into the `parityFixtures` map in `indicators_test.go`
4. Run `go test ./... -v` — all parity tests must pass

Any difference beyond `1e-9` is a computation error that must be fixed
before the Go backend can replace the Python POC.

## Dependencies

None beyond the Go standard library (`math`). This package must remain
dependency-free to ensure portability across deployment environments.

---

# Live Ingestion Pipeline

## What this is

A pluggable layer that accepts **real sensor data** from physical IoT devices,
normalises it into the same internal data structures the existing simulator
produces, and feeds the existing indicator + RTPMV computation engine.

A single environment variable toggles between modes:

```
TWINVAL_DATA_SOURCE=simulated   # default — existing ASHRAE GEPIII path, untouched
TWINVAL_DATA_SOURCE=live        # new ingestion pipeline; simulator + legacy webhook are NOT started
```

The simulated path is byte-identical when `TWINVAL_DATA_SOURCE` is unset
or `simulated` — every existing test passes regardless of whether the
live components are compiled in.

## Architecture

```
adapters --rawCh--> validator -> denoiser -> normaliser -> aggregator
                                                                  |
                                                              snapCh
                                                                  |
                                                              computer
                                                                  |
                                                          (existing internal/compute)
                                                                  |
                                                            sinks: hub + db
```

Three protocols, one pipeline:

| Protocol | Path | When to use |
|---|---|---|
| **MQTT** | gateways publish to `twinval/{building}/{zone}/{sensor_type}/{sensor_id}` | Default for IoT gateways |
| **HTTP webhook** | `POST /ingest/webhook` with `X-TwinVal-Signature: <hmac>` | Gateways that push, not subscribe |
| **Modbus TCP** | poll `modbus_map.yaml` register list | Legacy BMS systems |

## Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `TWINVAL_DATA_SOURCE` | `simulated` | Toggle. `live` activates the ingestion pipeline. |
| `MQTT_BROKER_URL` | _(unset = MQTT off)_ | e.g. `mqtts://broker.iium.twinval:8883` |
| `MQTT_CLIENT_ID` | `twinval-ingest` | Unique per ingestion replica |
| `MQTT_TLS` | `true` | `false` only for local dev brokers |
| `MQTT_USERNAME` / `MQTT_PASSWORD` | _(empty)_ | Optional broker credentials |
| `WEBHOOK_HMAC_SECRET` | _(empty)_ | **Required**; pipeline fails closed without it |
| `MODBUS_MAP_PATH` | `./config/modbus_map.yaml` | Override to deploy a custom register map |
| `SENSOR_BOUNDS_PATH` | `./config/sensor_bounds.yaml` | Override to deploy custom bounds |
| `AGGREGATION_WINDOW_SECONDS` | `10` | Zone snapshot window |
| `EMA_ALPHA` | `0.3` | Per-sensor exponential moving average weight |
| `DB_URL` | _(empty)_ | **Required in live mode**; shared portal Postgres |
| `DB_WRITE_WORKERS` | `4` | Async DB writer pool size |
| `HUMAN_OBS_WINDOW_HOURS` | `4` | Lookback for portal observations contributing to CI |
| `LOG_LEVEL` | `info` | `debug` / `info` / `warn` / `error` |
| `METRICS_PORT` | `9090` | Prometheus `/metrics` endpoint port (separate from the API port) |

## sensor_bounds.yaml

Per-type physical envelope. Readings outside `min`/`max` are dropped by
the validator with reason `out_of_range`. `normal_min`/`normal_max`
(optional) are forwarded to the indicator stage as comfort bands.

```yaml
sensor_types:
  temperature:
    unit: "°C"
    min: -10.0
    max: 60.0
    normal_min: 18.0
    normal_max: 28.0
  vibration:
    unit: "mm/s²"
    min: 0.0
    max: 50.0
    normal_min: 0.0
    normal_max: 5.0
  # ... humidity, strain, air_quality_pm25, co2, occupancy,
  #     electrical_load, water_consumption — see config/sensor_bounds.yaml
```

## modbus_map.yaml

```yaml
poll_interval_seconds: 30
devices:
  - host: "10.0.0.10"
    port: 502
    unit_id: 1
    name: "IIUM Block A — HVAC controller"
    sensors:
      - register: 100
        register_type: "input"        # input | holding
        sensor_id: "IIUM-BLK-A-TEMP-01"
        sensor_type: "temperature"
        building: "Block A"
        zone: "Roof - HVAC"
        unit: "°C"
        scale: 0.1                    # raw_value × scale = physical
        quality: 1.0                  # static; Modbus has no signal field
```

`scale` handles the typical BMS pattern of storing 22.5°C as register
`225` with `scale: 0.1`. Negative register values are decoded as `int16`
(uint16 65486 → int16 -50 → −5.0°C with `scale: 0.1`).

## Prometheus metrics

The live pipeline exposes the following metrics on
`http://0.0.0.0:${METRICS_PORT}/metrics`:

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `twinval_readings_received_total` | counter | `source, building, zone, sensor_type` | Validated readings entering the pipeline |
| `twinval_readings_dropped_total` | counter | `reason` | `quality_below_threshold` / `out_of_range` / `malformed` / `unknown_sensor_type` |
| `twinval_zone_snapshots_total` | counter | `building, zone` | ZoneSnapshots emitted by the aggregator |
| `twinval_rtpmv_current` | gauge | `building, zone` | Most recent RTPMV per zone |
| `twinval_health_factor_current` | gauge | `building, zone` | Most recent Health Factor per zone |
| `twinval_pipeline_latency_seconds` | histogram | _(none)_ | Reading-received → broadcast end-to-end |
| `twinval_db_write_errors_total` | counter | _(none)_ | Async DB writes that failed |
| `twinval_mqtt_reconnections_total` | counter | _(none)_ | Broker reconnection events |
| `twinval_human_obs_applied_total` | counter | `building, zone, severity` | Portal observations contributing to the CI delta |

## Confidence Index — human observations

The `CI` indicator incorporates intern observation records from the
Intern Observation Portal (`observation_entries` table on the same
Railway Postgres). On each compute cycle, observations submitted within
`HUMAN_OBS_WINDOW_HOURS` for that building+zone contribute:

```
Normal  -> +0.02
Watch   -> -0.05
Alert   -> -0.15
```

The sum is clamped to `[-1, +1]` before the modifier returns; the final
CI is then clamped to `[0, 1]` by the existing compute layer. Voided
observations are excluded — they are the audit-correct equivalent of
delete in the portal, and their CI signal is retracted.

The connection is **read-only** — the live pipeline never writes to
portal-owned tables.

## Database schema

Two tables are added to the shared Railway Postgres in a separate
migration namespace (`ingestion_schema_migrations`) so this Go runner
and the portal's Python alembic migrations coexist safely:

```sql
sensor_readings_raw (
  id, sensor_id, building, zone, sensor_type, unit,
  raw_value, quality, source, received_at, ingested_at
)

zone_snapshots (
  id, building, zone, snapshot_at,
  sensor_values (JSONB),
  shf, esf, uss, pdp, ci,
  health_factor, rtpmv,
  land_value, structure_value,
  data_source       -- "live" | "simulated"
)
```

Migrations run automatically on container boot via embedded `.sql` files
in `internal/ingestion/db/migrations/`.

## Graceful shutdown

On `SIGINT` / `SIGTERM` the live pipeline shuts down in this order,
bounded by a single 30-second context:

1. **Adapters** stop accepting new data (MQTT disconnects, Modbus stops
   polling, webhook returns 5xx for new POSTs)
2. **Pipeline** drains its raw + snapshot channels so in-flight readings
   reach broadcast
3. **DB writer pool** flushes queued jobs
4. **DB pool** closes
5. **Metrics server** shuts down

Anything still running past 30 seconds is force-exited.

## Local dev

```bash
# Simulated mode (default) — no extra setup needed
go run ./cmd/api

# Live mode against a local broker + Postgres
export TWINVAL_DATA_SOURCE=live
export MQTT_BROKER_URL=mqtt://localhost:1883
export MQTT_TLS=false
export WEBHOOK_HMAC_SECRET=dev-secret
export DB_URL=postgres://twinval:twinval@localhost:5432/twinval
go run ./cmd/api
```

Then on a separate terminal:

```bash
# Send a test webhook
BODY='{"sensor_id":"X-1","building":"Block A","zone":"Roof","sensor_type":"temperature","value":22.5,"quality":1.0,"ts":"2026-05-08T10:00:00Z"}'
SIG=$(printf "%s" "$BODY" | openssl dgst -sha256 -hmac "dev-secret" -hex | awk '{print $2}')
curl -X POST http://localhost:8080/ingest/webhook \
  -H "Content-Type: application/json" \
  -H "X-TwinVal-Signature: $SIG" \
  -d "$BODY"
# expect: 202 Accepted

curl http://localhost:9090/metrics | grep twinval_
```

## Integration tests

Two end-to-end paths are covered by Docker-backed tests under
`internal/ingestion/integration/` (build tag `integration`):

| Test | Components exercised |
|---|---|
| `TestIntegration_WebhookToDB` | signed POST → webhook adapter → pipeline → compute → writer pool → real Postgres → asserts `zone_snapshots` row |
| `TestIntegration_WebhookToDB_BadSignatureNoDBRow` | same path with bad HMAC → asserts ZERO rows persisted |
| `TestIntegration_MQTTToBroadcast` | publisher → real Mosquitto → MQTT adapter → pipeline → compute → recording sink |

Run with Docker available (Docker Desktop on Windows/macOS, native
daemon on Linux):

```bash
go test -tags integration ./internal/ingestion/integration/...
```

These tests are **excluded** from the default `go test ./...` run via
the `integration` build tag — the unit-test loop stays fast and works
without Docker. If Docker is unreachable, each test calls `t.Skip()`
with a clear message rather than failing.

**Modbus integration test is intentionally omitted.** The
`grid-x/modbus` library is the trust boundary for wire-protocol
correctness; our adapter's contract surface (decoding, polling,
reconnect, scale handling) is already fully covered by the in-process
fake-client unit tests in `internal/ingestion/adapters/modbus_test.go`.
Adding a real Modbus slave container would only verify that the third-
party library calls the protocol correctly — not a bug surface we own.
