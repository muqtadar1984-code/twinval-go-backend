// Package compute implements Patent Module 340 — the computation module.
//
// This package is the mathematical heart of TwinVal. It computes:
//   - Five technical indicators (SHF, ESF, USS, PDP, CI)
//   - Health Factor (composite of all five indicators)
//   - Real-Time Property Market Value (RTPMV)
//
// Formula (from patent specification paragraph [0054]):
//
//	RTPMV = Land_Value + (Structure_Value × Health_Factor)
//
// Health Factor (paragraph [0055]):
//
//	Health_Factor = SHF × ESF × (1 − USS) × PDP × CI
//
// All indicators are dimensionless, normalised to [0.0, 1.0].
// A value of 1.0 represents the real estate property in perfect condition.
//
// VALIDATION NOTE:
// Every function in this package must produce numerically identical output
// to the Python POC when given identical inputs. Run validation/parity_test.go
// against the Python POC before any production deployment.
package compute

// ConditionedData is the cleaned, normalised sensor output from the
// condition package (Patent Module 320). It is the input to all
// technical indicator computations.
type ConditionedData struct {
	// Structural readings
	VibrationMagnitude float64 // metres/second² — from vibration sensor
	StrainMagnitude    float64 // microstrain — from strain sensor

	// Environmental readings (per zone; averaged across zones for ESF)
	Temperature  float64 // degrees Celsius
	Humidity     float64 // relative humidity, 0–100
	AirQualityPM float64 // PM2.5 µg/m³

	// Usage readings
	OccupancyRatio    float64 // current occupancy / design capacity, 0–1
	ElectricalLoad    float64 // kW — normalised against rated capacity
	WaterConsumption  float64 // litres/day — normalised against baseline

	// Sensor metadata (used by CI computation)
	UptimeContinuity     float64 // fraction of expected readings received, 0–1
	CrossSensorConsistency float64 // inter-sensor agreement score, 0–1
	CalibrationRecency   float64 // days since last calibration (lower = better)
	TamperScore          float64 // 1.0 = no tamper detected; lower = suspected

	// Property metadata
	ChronologicalAge    float64 // years since construction
	MaintenanceSensitivity float64 // 0–1; higher = more sensitive to maintenance history
	ConditionQuality    float64 // sensor-derived overall quality score, 0–1
}

// TechnicalIndicators holds all five computed indicators.
// Each field is dimensionless and normalised to [0.0, 1.0].
// These map directly to patent specification paragraph [0047].
type TechnicalIndicators struct {
	// SHF — Structural Health Factor (paragraph [0048])
	// Quantifies structural integrity from vibration and strain sensors.
	// Uses Wöhler S-N fatigue curve model for non-linear penalty.
	SHF float64

	// ESF — Environmental Stability Factor (paragraph [0049])
	// Multi-zone assessment of temperature, humidity, air quality.
	ESF float64

	// USS — Usage Stress Score (paragraph [0050])
	// Cumulative usage load relative to design capacity.
	// NOTE: Applied as (1 − USS) in the Health Factor formula.
	USS float64

	// PDP — Predictive Deterioration Penalty (paragraph [0051])
	// Age-related deterioration adjusted for maintenance and condition.
	// Uses effective age (not chronological age).
	PDP float64

	// CI — Confidence Index (paragraph [0052])
	// Reliability of the conditioned data feeding the digital twin.
	// Derived from uptime, cross-sensor consistency, calibration recency,
	// tamper detection, and market impact weighting.
	CI float64
}

// HealthFactor is the composite measure derived from all five
// technical indicators (patent paragraph [0055]).
// Range: [0.0, 1.0] — 1.0 = perfect condition.
type HealthFactor float64

// BaselineMarketValue holds the land and structure split required
// by the RTPMV formula (patent paragraph [0054]).
// The plurality of technical indicators applies ONLY to Structure_Value.
// Land_Value does not physically deteriorate and remains unchanged.
type BaselineMarketValue struct {
	LandValue      float64 // from government assessment, third-party appraisal, or user override
	StructureValue float64 // from government assessment, third-party appraisal, or user override
	Currency       string  // ISO 4217 — e.g. "MYR", "USD", "AED"
	Source         string  // e.g. "JPPH Malaysia", "Third-party appraisal", "User override"
}

// RTPMV is the Real-Time Property Market Value — the primary output
// of the TwinVal system (patent paragraph [0054]).
type RTPMV struct {
	Value          float64             // Land_Value + (Structure_Value × Health_Factor)
	Currency       string              // ISO 4217
	HealthFactor   HealthFactor        // composite health factor used in this computation
	Indicators     TechnicalIndicators // snapshot of indicators used
	Baseline       BaselineMarketValue // baseline used in this computation
	ComputedAt     int64               // Unix timestamp nanoseconds
}

// PropertyType determines which sensor set is relevant for a property.
// Different property types use different subsets of ConditionedData.
// Defined in patent specification paragraph [0041].
type PropertyType int

const (
	PropertyTypeCommercialOffice PropertyType = iota
	PropertyTypeResidential
	PropertyTypeRetailMall
	PropertyTypeHealthcareFacility
	PropertyTypeLandedBungalow
)
