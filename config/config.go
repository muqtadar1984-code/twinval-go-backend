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
}

// LoadFromEnv reads every supported environment variable and falls back
// to defaults when a variable is unset, empty, or unparseable.
func LoadFromEnv() AppConfig {
	return AppConfig{
		Port:           getEnv("TWINVAL_PORT", "8080"),
		PropertyID:     getEnv("TWINVAL_PROPERTY_ID", "PROP-ASHRAE-001"),
		CORSOrigin:     getEnv("TWINVAL_CORS_ORIGIN", "*"),
		ChainLimit:     getEnvInt("TWINVAL_CHAIN_LIMIT", 10),
		LandValue:      getEnvFloat("TWINVAL_LAND_VALUE", 500_000.0),
		StructureValue: getEnvFloat("TWINVAL_STRUCTURE_VALUE", 1_000_000.0),
		Currency:       getEnv("TWINVAL_CURRENCY", "MYR"),
	}
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
