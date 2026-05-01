// Package config loads runtime configuration from environment variables.
//
// Every value has a sensible production default; environment variables
// override only when set to a non-empty string. Invalid numeric values
// fall back to defaults rather than panicking — config errors are
// logged at startup, not turned into runtime crashes.
package config

import (
	"os"
	"strconv"
	"strings"
)

// AppConfig is the full configuration struct used by cmd/api.
type AppConfig struct {
	Port       string
	PropertyID string
	CORSOrigin string
	ChainLimit int

	LandValue      float64
	StructureValue float64
	Currency       string

	// PropertyMode selects single-property mode ("single", default) or
	// the catalogued ASHRAE 5-building mode ("ashrae"). In ashrae mode,
	// LandValue/StructureValue/Currency from env are ignored — each
	// building carries its own valuation from internal/ashrae.Buildings.
	PropertyMode string

	// SimEnabled spawns the embedded simulator goroutine. In ashrae mode
	// it drives all 5 buildings; in single mode it drives the configured
	// PropertyID.
	SimEnabled bool

	// SimIntervalMs is the wall-clock interval between simulated ticks.
	// Each tick advances the simulator by one simulated hour.
	SimIntervalMs int
}

// LoadFromEnv reads every supported environment variable and falls back
// to defaults when a variable is unset, empty, or unparseable.
//
// Port resolution order: TWINVAL_PORT → PORT → "8080". The PORT fallback
// keeps the binary compatible with PaaS hosts (Railway, Fly, Render) that
// inject a dynamic PORT into the container.
func LoadFromEnv() AppConfig {
	return AppConfig{
		Port:           resolvePort(),
		PropertyID:     getEnv("TWINVAL_PROPERTY_ID", "PROP-ASHRAE-001"),
		CORSOrigin:     getEnv("TWINVAL_CORS_ORIGIN", "*"),
		ChainLimit:     getEnvInt("TWINVAL_CHAIN_LIMIT", 10),
		LandValue:      getEnvFloat("TWINVAL_LAND_VALUE", 500_000.0),
		StructureValue: getEnvFloat("TWINVAL_STRUCTURE_VALUE", 1_000_000.0),
		Currency:       getEnv("TWINVAL_CURRENCY", "MYR"),
		PropertyMode:   getEnv("TWINVAL_PROPERTY_MODE", "single"),
		SimEnabled:     getEnvBool("TWINVAL_SIM_ENABLED", false),
		SimIntervalMs:  getEnvInt("TWINVAL_SIM_INTERVAL_MS", 1000),
	}
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

func resolvePort() string {
	if v := os.Getenv("TWINVAL_PORT"); v != "" {
		return v
	}
	if v := os.Getenv("PORT"); v != "" {
		return v
	}
	return "8080"
}

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
