// Package config loads runtime configuration for the live ingestion
// pipeline from environment variables and YAML files.
//
// The pipeline runs only when TWINVAL_DATA_SOURCE=live. In simulated
// mode the existing ASHRAE-driven path is unchanged; the live config is
// not loaded and adapter goroutines are not started.
//
// Validation is fail-fast: any malformed YAML or missing required value
// returns an error from Load(). Callers should log the error and exit
// rather than starting a partially-configured pipeline.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// DataSource selects between the existing ASHRAE simulation and the new
// live ingestion pipeline. Default is Simulated to preserve backwards
// compatibility — adding the new pipeline cannot regress existing
// deployments unless an operator opts in.
type DataSource string

const (
	DataSourceSimulated DataSource = "simulated"
	DataSourceLive      DataSource = "live"
)

// IngestionConfig is every parameter the live pipeline needs at startup.
// Fields are populated from environment variables and YAML files by Load.
type IngestionConfig struct {
	DataSource DataSource

	// MQTT adapter
	MQTTBrokerURL string
	MQTTClientID  string
	MQTTTLS       bool
	MQTTUsername  string
	MQTTPassword  string

	// Webhook adapter
	WebhookHMACSecret string

	// Modbus adapter
	ModbusMapPath string
	ModbusMap     ModbusMap

	// Sensor bounds (validator + normaliser)
	SensorBoundsPath string
	SensorBounds     SensorBoundsFile

	// Pipeline tuning
	AggregationWindow time.Duration
	EMAAlpha          float64

	// Database (shared with Intern Observation Portal — read observation_entries
	// for the human-observation CI modifier; write own ingestion tables).
	DBURL          string
	DBWriteWorkers int

	// Human-observation CI modifier window
	HumanObsWindow time.Duration

	// HumanObsMaxPositiveDelta caps accumulated Normal-observation CI
	// credit per window. Critical when HumanObsWindow is widened (e.g.
	// 7 days for the bungalow pilot): uncapped Normals saturate CI and
	// mask Watch/Alert signals. Warnings are never capped.
	HumanObsMaxPositiveDelta float64

	// Observability
	LogLevel     string
	MetricsPort  int
}

// SensorBoundsFile mirrors the structure of config/sensor_bounds.yaml.
type SensorBoundsFile struct {
	SensorTypes map[string]SensorBound `yaml:"sensor_types"`
}

// SensorBound is the per-sensor-type physical envelope + comfort band.
// NormalMin and NormalMax are zero when omitted in YAML; downstream
// stages should treat zero-valued comfort bands as "not applicable".
type SensorBound struct {
	Unit      string  `yaml:"unit"`
	Min       float64 `yaml:"min"`
	Max       float64 `yaml:"max"`
	NormalMin float64 `yaml:"normal_min,omitempty"`
	NormalMax float64 `yaml:"normal_max,omitempty"`
}

// ModbusMap mirrors the structure of config/modbus_map.yaml.
type ModbusMap struct {
	PollIntervalSeconds int            `yaml:"poll_interval_seconds"`
	Devices             []ModbusDevice `yaml:"devices"`
}

// ModbusDevice is one polled slave + the registers to read from it.
type ModbusDevice struct {
	Host    string         `yaml:"host"`
	Port    int            `yaml:"port"`
	UnitID  uint8          `yaml:"unit_id"`
	Name    string         `yaml:"name,omitempty"`
	Sensors []ModbusSensor `yaml:"sensors"`
}

// ModbusSensor is one register on a Modbus device, with metadata that
// lets the pipeline route the reading to the right zone.
type ModbusSensor struct {
	Register     uint16  `yaml:"register"`
	RegisterType string  `yaml:"register_type"` // "input" | "holding"
	SensorID     string  `yaml:"sensor_id"`
	SensorType   string  `yaml:"sensor_type"`
	Building     string  `yaml:"building"`
	Zone         string  `yaml:"zone"`
	Unit         string  `yaml:"unit"`
	Scale        float64 `yaml:"scale"`
	Quality      float64 `yaml:"quality"`
}

// Load reads env vars, parses the YAML files, and validates the result.
// In simulated mode only DataSource is populated — the rest of the
// fields are zero values and YAML files are NOT read.
func Load() (IngestionConfig, error) {
	src := DataSource(strings.ToLower(getEnv("TWINVAL_DATA_SOURCE", string(DataSourceSimulated))))
	if src != DataSourceLive && src != DataSourceSimulated {
		return IngestionConfig{}, fmt.Errorf("TWINVAL_DATA_SOURCE must be 'live' or 'simulated', got %q", src)
	}

	cfg := IngestionConfig{DataSource: src}
	if src == DataSourceSimulated {
		return cfg, nil
	}

	cfg.MQTTBrokerURL = getEnv("MQTT_BROKER_URL", "")
	cfg.MQTTClientID = getEnv("MQTT_CLIENT_ID", "twinval-ingest")
	cfg.MQTTTLS = getEnvBool("MQTT_TLS", true)
	cfg.MQTTUsername = getEnv("MQTT_USERNAME", "")
	cfg.MQTTPassword = getEnv("MQTT_PASSWORD", "")

	cfg.WebhookHMACSecret = getEnv("WEBHOOK_HMAC_SECRET", "")

	cfg.ModbusMapPath = getEnv("MODBUS_MAP_PATH", "./config/modbus_map.yaml")
	cfg.SensorBoundsPath = getEnv("SENSOR_BOUNDS_PATH", "./config/sensor_bounds.yaml")

	cfg.AggregationWindow = time.Duration(getEnvInt("AGGREGATION_WINDOW_SECONDS", 10)) * time.Second
	cfg.EMAAlpha = getEnvFloat("EMA_ALPHA", 0.3)

	cfg.DBURL = getEnv("DB_URL", "")
	cfg.DBWriteWorkers = getEnvInt("DB_WRITE_WORKERS", 4)

	cfg.HumanObsWindow = time.Duration(getEnvInt("HUMAN_OBS_WINDOW_HOURS", 4)) * time.Hour
	cfg.HumanObsMaxPositiveDelta = getEnvFloat("HUMAN_OBS_MAX_POSITIVE_DELTA", 0.10)

	cfg.LogLevel = strings.ToLower(getEnv("LOG_LEVEL", "info"))
	cfg.MetricsPort = getEnvInt("METRICS_PORT", 9090)

	// Load YAML configs.
	bounds, err := LoadSensorBounds(cfg.SensorBoundsPath)
	if err != nil {
		return cfg, fmt.Errorf("loading sensor bounds: %w", err)
	}
	cfg.SensorBounds = bounds

	modbus, err := LoadModbusMap(cfg.ModbusMapPath)
	if err != nil {
		// Modbus is optional — if no devices are deployed, the file may not exist.
		// Distinguish "file missing" (allow, no devices) from "file malformed" (fail).
		if !errors.Is(err, os.ErrNotExist) {
			return cfg, fmt.Errorf("loading modbus map: %w", err)
		}
		modbus = ModbusMap{}
	}
	cfg.ModbusMap = modbus

	if err := cfg.validateLive(); err != nil {
		return cfg, err
	}
	return cfg, nil
}

// LoadSensorBounds parses a sensor_bounds.yaml file from disk.
// Exposed so tests + adapters can reuse the parser.
func LoadSensorBounds(path string) (SensorBoundsFile, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return SensorBoundsFile{}, err
	}
	var out SensorBoundsFile
	if err := yaml.Unmarshal(raw, &out); err != nil {
		return SensorBoundsFile{}, fmt.Errorf("parsing %s: %w", path, err)
	}
	if len(out.SensorTypes) == 0 {
		return out, fmt.Errorf("%s: no sensor_types defined", path)
	}
	for name, b := range out.SensorTypes {
		if b.Min >= b.Max {
			return out, fmt.Errorf("%s: sensor type %q has min >= max", path, name)
		}
	}
	return out, nil
}

// LoadModbusMap parses a modbus_map.yaml file from disk.
func LoadModbusMap(path string) (ModbusMap, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return ModbusMap{}, err
	}
	var out ModbusMap
	if err := yaml.Unmarshal(raw, &out); err != nil {
		return ModbusMap{}, fmt.Errorf("parsing %s: %w", path, err)
	}
	if out.PollIntervalSeconds <= 0 {
		out.PollIntervalSeconds = 30
	}
	for di, d := range out.Devices {
		if d.Host == "" {
			return out, fmt.Errorf("%s: device[%d] missing host", path, di)
		}
		if d.Port == 0 {
			return out, fmt.Errorf("%s: device[%d] missing port", path, di)
		}
		for si, s := range d.Sensors {
			if s.SensorID == "" {
				return out, fmt.Errorf("%s: device[%d].sensors[%d] missing sensor_id", path, di, si)
			}
			if s.SensorType == "" {
				return out, fmt.Errorf("%s: device[%d].sensors[%d] missing sensor_type", path, di, si)
			}
			if s.RegisterType != "" && s.RegisterType != "input" && s.RegisterType != "holding" {
				return out, fmt.Errorf("%s: device[%d].sensors[%d] register_type must be 'input' or 'holding'", path, di, si)
			}
			if s.Quality < 0 || s.Quality > 1 {
				return out, fmt.Errorf("%s: device[%d].sensors[%d] quality must be in [0,1]", path, di, si)
			}
		}
	}
	return out, nil
}

// validateLive enforces the env-var requirements for live mode.
func (c IngestionConfig) validateLive() error {
	var missing []string
	if c.DBURL == "" {
		missing = append(missing, "DB_URL")
	}
	if c.AggregationWindow <= 0 {
		return fmt.Errorf("AGGREGATION_WINDOW_SECONDS must be > 0")
	}
	if c.EMAAlpha <= 0 || c.EMAAlpha > 1 {
		return fmt.Errorf("EMA_ALPHA must be in (0, 1]")
	}
	if c.DBWriteWorkers <= 0 {
		return fmt.Errorf("DB_WRITE_WORKERS must be > 0")
	}
	// MQTT / Webhook secrets are warnings rather than fatal — an operator
	// may run with only Modbus, or only Webhook, or only MQTT.
	if len(missing) > 0 {
		return fmt.Errorf("missing required env vars in live mode: %s", strings.Join(missing, ", "))
	}
	return nil
}

// IsLive returns true when the configured DataSource is "live".
func (c IngestionConfig) IsLive() bool { return c.DataSource == DataSourceLive }

// ---------------------------------------------------------------------
// env helpers — separate from the existing config/ package so the live
// pipeline doesn't take a dependency on it.
// ---------------------------------------------------------------------

func getEnv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func getEnvInt(key string, def int) int {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func getEnvFloat(key string, def float64) float64 {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func getEnvBool(key string, def bool) bool {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		switch strings.ToLower(v) {
		case "1", "true", "yes", "on":
			return true
		case "0", "false", "no", "off":
			return false
		}
	}
	return def
}
