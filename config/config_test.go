package config

import (
	"os"
	"testing"
)

var allKeys = []string{
	"TWINVAL_PORT",
	"TWINVAL_PROPERTY_ID",
	"TWINVAL_CORS_ORIGIN",
	"TWINVAL_CHAIN_LIMIT",
	"TWINVAL_LAND_VALUE",
	"TWINVAL_STRUCTURE_VALUE",
	"TWINVAL_CURRENCY",
	"PORT",
}

func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range allKeys {
		os.Unsetenv(k)
	}
}

func TestLoadFromEnv_ReturnsDefaultsWhenEnvAbsent(t *testing.T) {
	clearEnv(t)
	cfg := LoadFromEnv()

	if cfg.Port != "8080" {
		t.Errorf("Port: got %q, want 8080", cfg.Port)
	}
	if cfg.PropertyID != "PROP-ASHRAE-001" {
		t.Errorf("PropertyID: got %q, want PROP-ASHRAE-001", cfg.PropertyID)
	}
	if cfg.CORSOrigin != "*" {
		t.Errorf("CORSOrigin: got %q, want *", cfg.CORSOrigin)
	}
	if cfg.ChainLimit != 10 {
		t.Errorf("ChainLimit: got %d, want 10", cfg.ChainLimit)
	}
	if cfg.LandValue != 500_000.0 {
		t.Errorf("LandValue: got %v, want 500000", cfg.LandValue)
	}
	if cfg.StructureValue != 1_000_000.0 {
		t.Errorf("StructureValue: got %v, want 1000000", cfg.StructureValue)
	}
	if cfg.Currency != "MYR" {
		t.Errorf("Currency: got %q, want MYR", cfg.Currency)
	}
}

func TestLoadFromEnv_HonoursOverrides(t *testing.T) {
	clearEnv(t)
	t.Setenv("TWINVAL_PORT", "9090")
	t.Setenv("TWINVAL_PROPERTY_ID", "PROP-XYZ")
	t.Setenv("TWINVAL_CORS_ORIGIN", "https://twinval.com")
	t.Setenv("TWINVAL_CHAIN_LIMIT", "25")
	t.Setenv("TWINVAL_LAND_VALUE", "750000")
	t.Setenv("TWINVAL_STRUCTURE_VALUE", "2000000")
	t.Setenv("TWINVAL_CURRENCY", "USD")

	cfg := LoadFromEnv()

	if cfg.Port != "9090" {
		t.Errorf("Port: got %q, want 9090", cfg.Port)
	}
	if cfg.PropertyID != "PROP-XYZ" {
		t.Errorf("PropertyID: got %q, want PROP-XYZ", cfg.PropertyID)
	}
	if cfg.CORSOrigin != "https://twinval.com" {
		t.Errorf("CORSOrigin: got %q, want https://twinval.com", cfg.CORSOrigin)
	}
	if cfg.ChainLimit != 25 {
		t.Errorf("ChainLimit: got %d, want 25", cfg.ChainLimit)
	}
	if cfg.LandValue != 750_000.0 {
		t.Errorf("LandValue: got %v, want 750000", cfg.LandValue)
	}
	if cfg.StructureValue != 2_000_000.0 {
		t.Errorf("StructureValue: got %v, want 2000000", cfg.StructureValue)
	}
	if cfg.Currency != "USD" {
		t.Errorf("Currency: got %q, want USD", cfg.Currency)
	}
}

func TestLoadFromEnv_PortFallsBackToPORTWhenTWINVAL_PORTAbsent(t *testing.T) {
	clearEnv(t)
	t.Setenv("PORT", "7777")
	cfg := LoadFromEnv()
	if cfg.Port != "7777" {
		t.Errorf("Port: got %q, want 7777 (PORT fallback)", cfg.Port)
	}
}

func TestLoadFromEnv_TWINVAL_PORTBeatsPORT(t *testing.T) {
	clearEnv(t)
	t.Setenv("PORT", "7777")
	t.Setenv("TWINVAL_PORT", "9090")
	cfg := LoadFromEnv()
	if cfg.Port != "9090" {
		t.Errorf("Port: got %q, want 9090 (TWINVAL_PORT wins over PORT)", cfg.Port)
	}
}

func TestLoadFromEnv_InvalidNumericFallsBackToDefault(t *testing.T) {
	clearEnv(t)
	t.Setenv("TWINVAL_CHAIN_LIMIT", "not-a-number")
	t.Setenv("TWINVAL_LAND_VALUE", "garbage")

	cfg := LoadFromEnv()
	if cfg.ChainLimit != 10 {
		t.Errorf("ChainLimit: got %d, want 10 (fallback)", cfg.ChainLimit)
	}
	if cfg.LandValue != 500_000.0 {
		t.Errorf("LandValue: got %v, want 500000 (fallback)", cfg.LandValue)
	}
}
