//go:build integration

// Package integration runs end-to-end tests of the live ingestion
// pipeline against real external dependencies (Postgres + Mosquitto)
// via testcontainers-go. Build tag `integration` keeps these out of
// the default `go test ./...` run since they require Docker and take
// 10–30 seconds each.
//
// Run with:
//
//	go test -tags integration ./internal/ingestion/integration/...
//
// Skip cleanly if Docker is unreachable.
package integration

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/twinval/internal/ingestion/config"
)

// startPostgres launches a fresh Postgres 16 container, returns its
// connection DSN and a cleanup function. Calls t.Skip if Docker is
// not reachable so the test gracefully skips on dev machines without
// Docker Desktop running.
func startPostgres(t *testing.T, ctx context.Context) (dsn string, cleanup func()) {
	t.Helper()
	c, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("twinval"),
		postgres.WithUsername("twinval"),
		postgres.WithPassword("twinval"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Skipf("postgres testcontainer unavailable (Docker?) — skipping: %v", err)
	}
	dsn, err = c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		_ = c.Terminate(ctx)
		t.Fatalf("connection string: %v", err)
	}
	return dsn, func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = c.Terminate(stopCtx)
	}
}

// startMosquitto launches a fresh Eclipse Mosquitto v2 container with
// anonymous-allow config, returns its host:port and a cleanup. Calls
// t.Skip on Docker errors.
func startMosquitto(t *testing.T, ctx context.Context) (brokerURL string, cleanup func()) {
	t.Helper()
	const cfg = "listener 1883\nallow_anonymous true\n"

	req := testcontainers.ContainerRequest{
		Image:        "eclipse-mosquitto:2",
		ExposedPorts: []string{"1883/tcp"},
		Files: []testcontainers.ContainerFile{{
			Reader:            bytes.NewReader([]byte(cfg)),
			ContainerFilePath: "/mosquitto/config/mosquitto.conf",
			FileMode:          0o644,
		}},
		WaitingFor: wait.ForListeningPort("1883/tcp").WithStartupTimeout(30 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Skipf("mosquitto testcontainer unavailable (Docker?) — skipping: %v", err)
	}
	host, err := c.Host(ctx)
	if err != nil {
		_ = c.Terminate(ctx)
		t.Fatalf("container host: %v", err)
	}
	port, err := c.MappedPort(ctx, "1883/tcp")
	if err != nil {
		_ = c.Terminate(ctx)
		t.Fatalf("container port: %v", err)
	}
	brokerURL = "tcp://" + host + ":" + port.Port()
	return brokerURL, func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = c.Terminate(stopCtx)
	}
}

// signHMAC produces the hex SHA-256 signature for a webhook body.
func signHMAC(body []byte, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

// testBounds returns the minimal sensor_bounds shape used by every
// integration test. Matches the spec's defaults.
func testBounds() config.SensorBoundsFile {
	return config.SensorBoundsFile{
		SensorTypes: map[string]config.SensorBound{
			"temperature":      {Unit: "°C", Min: -10, Max: 60, NormalMin: 18, NormalMax: 28},
			"humidity":         {Unit: "%RH", Min: 0, Max: 100, NormalMin: 30, NormalMax: 70},
			"vibration":        {Unit: "mm/s²", Min: 0, Max: 50, NormalMin: 0, NormalMax: 5},
			"strain":           {Unit: "με", Min: -500, Max: 500},
			"air_quality_pm25": {Unit: "µg/m³", Min: 0, Max: 500},
			"co2":              {Unit: "ppm", Min: 300, Max: 5000},
			"occupancy":        {Unit: "%", Min: 0, Max: 100},
			"electrical_load":  {Unit: "kW", Min: 0, Max: 500},
			"water_consumption":{Unit: "L/hr", Min: 0, Max: 1000},
		},
	}
}
