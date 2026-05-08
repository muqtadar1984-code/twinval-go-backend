package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoad_DefaultsToSimulated(t *testing.T) {
	os.Unsetenv("TWINVAL_DATA_SOURCE")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("expected no error in default mode, got %v", err)
	}
	if cfg.DataSource != DataSourceSimulated {
		t.Fatalf("expected simulated default, got %s", cfg.DataSource)
	}
	if cfg.IsLive() {
		t.Fatal("IsLive should be false in default mode")
	}
}

func TestLoad_RejectsUnknownDataSource(t *testing.T) {
	t.Setenv("TWINVAL_DATA_SOURCE", "garbage")
	_, err := Load()
	if err == nil {
		t.Fatal("expected error for unknown data source")
	}
}

func TestLoad_LiveRequiresDBURL(t *testing.T) {
	t.Setenv("TWINVAL_DATA_SOURCE", "live")
	t.Setenv("DB_URL", "")
	t.Setenv("SENSOR_BOUNDS_PATH", writeTempBounds(t))
	t.Setenv("MODBUS_MAP_PATH", "/nonexistent/modbus.yaml")
	_, err := Load()
	if err == nil {
		t.Fatal("expected error when DB_URL missing in live mode")
	}
}

func TestLoad_LiveSucceedsWithMinimumEnv(t *testing.T) {
	t.Setenv("TWINVAL_DATA_SOURCE", "live")
	t.Setenv("DB_URL", "postgres://x")
	t.Setenv("SENSOR_BOUNDS_PATH", writeTempBounds(t))
	t.Setenv("MODBUS_MAP_PATH", "/nonexistent/modbus.yaml")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if !cfg.IsLive() {
		t.Fatal("expected live mode")
	}
	if cfg.AggregationWindow.Seconds() != 10 {
		t.Fatalf("default aggregation window should be 10s, got %v", cfg.AggregationWindow)
	}
	if cfg.EMAAlpha != 0.3 {
		t.Fatalf("default EMA alpha should be 0.3, got %v", cfg.EMAAlpha)
	}
	if len(cfg.SensorBounds.SensorTypes) == 0 {
		t.Fatal("sensor bounds should be loaded")
	}
}

func TestLoadSensorBounds_RejectsMinAboveMax(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.yaml")
	contents := `sensor_types:
  bogus:
    unit: "x"
    min: 10
    max: 5
`
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadSensorBounds(path); err == nil {
		t.Fatal("expected error for min >= max")
	}
}

func TestLoadModbusMap_ParsesExample(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "modbus.yaml")
	contents := `poll_interval_seconds: 15
devices:
  - host: "10.0.0.1"
    port: 502
    unit_id: 1
    sensors:
      - register: 100
        register_type: "input"
        sensor_id: "X-01"
        sensor_type: "temperature"
        building: "B"
        zone: "Z"
        unit: "°C"
        scale: 0.1
        quality: 1.0
`
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	m, err := LoadModbusMap(path)
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if m.PollIntervalSeconds != 15 {
		t.Fatalf("expected poll interval 15, got %d", m.PollIntervalSeconds)
	}
	if len(m.Devices) != 1 || len(m.Devices[0].Sensors) != 1 {
		t.Fatalf("unexpected shape: %+v", m)
	}
}

func TestLoadModbusMap_RejectsBadRegisterType(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "modbus.yaml")
	contents := `devices:
  - host: "10.0.0.1"
    port: 502
    unit_id: 1
    sensors:
      - register: 100
        register_type: "wrong"
        sensor_id: "X"
        sensor_type: "temperature"
        building: "B"
        zone: "Z"
        unit: "°C"
        scale: 1.0
        quality: 1.0
`
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadModbusMap(path); err == nil {
		t.Fatal("expected error for invalid register_type")
	}
}

// writeTempBounds writes a minimal valid sensor_bounds.yaml to a temp
// dir and returns its path.
func writeTempBounds(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "bounds.yaml")
	contents := `sensor_types:
  temperature:
    unit: "°C"
    min: -10
    max: 60
`
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
