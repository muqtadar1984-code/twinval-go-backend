package compute

import "math"

// =============================================================================
// SHF — Structural Health Factor
// Patent specification paragraph [0048]
// =============================================================================

// SHFConfig holds the calibration constants for the Wöhler S-N fatigue model.
// These values must match the Python POC exactly for parity validation.
// Adjust via config/ package — never hardcode in calling code.
type SHFConfig struct {
	// Wöhler curve exponent — controls how sharply penalty accelerates
	// at high-magnitude readings. Typical range: 3.0–6.0.
	WohlerExponent float64

	// Normal operation ceiling — readings below this threshold produce
	// negligible penalty (flat region of S-N curve).
	VibrationNormalCeiling float64 // metres/second²
	StrainNormalCeiling    float64 // microstrain

	// Vibration and strain weighting in combined SHF
	VibrationWeight float64
	StrainWeight    float64
}

// DefaultSHFConfig returns the config values used in the Python POC.
// VALIDATION: changing these values breaks numerical parity — update
// validation/parity_test.go fixtures if you change them.
func DefaultSHFConfig() SHFConfig {
	return SHFConfig{
		WohlerExponent:         4.0,
		VibrationNormalCeiling: 0.05,
		StrainNormalCeiling:    200.0,
		VibrationWeight:        0.6,
		StrainWeight:           0.4,
	}
}

// ComputeSHF computes the Structural Health Factor.
//
// Non-linear Wöhler S-N fatigue model: low-magnitude readings produce
// negligible penalty; high-magnitude readings produce accelerating penalty.
//
// Returns a value in [0.0, 1.0] where 1.0 = perfect structural health.
func ComputeSHF(data ConditionedData, cfg SHFConfig) float64 {
	vibPenalty := wohlerPenalty(data.VibrationMagnitude, cfg.VibrationNormalCeiling, cfg.WohlerExponent)
	strainPenalty := wohlerPenalty(data.StrainMagnitude, cfg.StrainNormalCeiling, cfg.WohlerExponent)

	combined := cfg.VibrationWeight*vibPenalty + cfg.StrainWeight*strainPenalty
	shf := 1.0 - clamp(combined, 0.0, 1.0)
	return shf
}

// wohlerPenalty computes the S-N fatigue penalty for a single sensor reading.
// Returns 0.0 when reading <= normalCeiling (flat region).
// Returns an accelerating value when reading > normalCeiling.
func wohlerPenalty(reading, normalCeiling, exponent float64) float64 {
	if reading <= normalCeiling {
		return 0.0
	}
	// Normalised excess above the safe threshold
	excess := (reading - normalCeiling) / normalCeiling
	return math.Pow(excess, exponent) / (1.0 + math.Pow(excess, exponent))
}

// =============================================================================
// ESF — Environmental Stability Factor
// Patent specification paragraph [0049]
// =============================================================================

// ESFConfig holds thresholds for environmental stability assessment.
type ESFConfig struct {
	// Optimal ranges — readings within these ranges produce no penalty
	TempOptimalMin float64 // °C
	TempOptimalMax float64 // °C
	HumidityOptMin float64 // %RH
	HumidityOptMax float64 // %RH
	PM25SafeLevel  float64 // µg/m³ — WHO guideline

	// Weights for each environmental parameter
	TempWeight     float64
	HumidityWeight float64
	AirQualWeight  float64
}

// DefaultESFConfig returns the config used in the Python POC.
func DefaultESFConfig() ESFConfig {
	return ESFConfig{
		TempOptimalMin: 18.0,
		TempOptimalMax: 26.0,
		HumidityOptMin: 40.0,
		HumidityOptMax: 60.0,
		PM25SafeLevel:  15.0,
		TempWeight:     0.4,
		HumidityWeight: 0.35,
		AirQualWeight:  0.25,
	}
}

// ComputeESF computes the Environmental Stability Factor.
// Returns a value in [0.0, 1.0] where 1.0 = perfect environmental stability.
func ComputeESF(data ConditionedData, cfg ESFConfig) float64 {
	tempScore := rangeScore(data.Temperature, cfg.TempOptimalMin, cfg.TempOptimalMax)
	humidScore := rangeScore(data.Humidity, cfg.HumidityOptMin, cfg.HumidityOptMax)
	airScore := 1.0 - clamp(data.AirQualityPM/cfg.PM25SafeLevel-1.0, 0.0, 1.0)

	esf := cfg.TempWeight*tempScore + cfg.HumidityWeight*humidScore + cfg.AirQualWeight*airScore
	return clamp(esf, 0.0, 1.0)
}

// rangeScore returns 1.0 if value is within [min, max], decays linearly outside.
func rangeScore(value, min, max float64) float64 {
	if value >= min && value <= max {
		return 1.0
	}
	mid := (min + max) / 2.0
	halfRange := (max - min) / 2.0
	deviation := math.Abs(value-mid) - halfRange
	penalty := deviation / halfRange
	return clamp(1.0-penalty, 0.0, 1.0)
}

// =============================================================================
// USS — Usage Stress Score
// Patent specification paragraph [0050]
// =============================================================================

// USSConfig holds weighting for usage stress sources.
//
// The NormalUse thresholds define a no-penalty deadband per source:
// usage at or below the threshold produces zero stress, mirroring the
// flat region every other indicator already has (SHF's vibration
// ceiling, ESF's comfort bands). Without a deadband, ANY occupancy or
// load registers as stress — which over-discounts normally occupied
// residential property (a building operating as designed is not being
// stressed). Above the threshold, stress scales linearly to 1.0 at
// full design capacity.
type USSConfig struct {
	OccupancyWeight  float64
	ElectricalWeight float64
	WaterWeight      float64

	// No-penalty thresholds, each in the same normalised [0, 1] units
	// as the corresponding ConditionedData input. Zero = no deadband
	// (legacy behaviour). Residential calibration (bungalow pilot):
	// occupancy 0.40, electrical 0.30, water 0.30 — keep in lockstep
	// with the Excel workbooks' Config sheet.
	OccupancyNormalUse  float64
	ElectricalNormalUse float64
	WaterNormalUse      float64
}

// DefaultUSSConfig returns the config used in the Python POC.
// NormalUse thresholds default to 0 so numerical parity with the
// Python POC is preserved (validation/parity_test.go); residential
// deployments override them via configuration.
func DefaultUSSConfig() USSConfig {
	return USSConfig{
		OccupancyWeight:  0.5,
		ElectricalWeight: 0.3,
		WaterWeight:      0.2,
	}
}

// ComputeUSS computes the Usage Stress Score.
//
// IMPORTANT: USS is applied as (1 − USS) in the Health Factor formula.
// This function returns the raw stress value [0.0, 1.0] where:
//   - 1.0 = property operating at or above design capacity (maximum stress)
//   - 0.0 = property idle, or operating within its normal-use deadband
func ComputeUSS(data ConditionedData, cfg USSConfig) float64 {
	uss := cfg.OccupancyWeight*usageStress(data.OccupancyRatio, cfg.OccupancyNormalUse) +
		cfg.ElectricalWeight*usageStress(data.ElectricalLoad, cfg.ElectricalNormalUse) +
		cfg.WaterWeight*usageStress(data.WaterConsumption, cfg.WaterNormalUse)
	return clamp(uss, 0.0, 1.0)
}

// usageStress maps a normalised usage reading to [0, 1] stress with a
// no-penalty deadband: 0 at or below normalUse, then linear to 1.0 at
// full capacity. A normalUse >= 1 disables the source entirely.
func usageStress(reading, normalUse float64) float64 {
	r := clamp(reading, 0.0, 1.0)
	if r <= normalUse {
		return 0.0
	}
	if normalUse >= 1.0 {
		return 0.0
	}
	return (r - normalUse) / (1.0 - normalUse)
}

// =============================================================================
// PDP — Predictive Deterioration Penalty
// Patent specification paragraph [0051]
// =============================================================================

// PDPConfig holds constants for effective age computation.
type PDPConfig struct {
	// DesignLifeYears is the expected service life of the property type.
	// Used as the denominator in deterioration rate computation.
	DesignLifeYears float64

	// EffectiveAgeReductionRate controls how much sustained good sensor
	// readings can reduce the effective age below chronological age.
	// This is TwinVal's maintenance incentive mechanism
	// (patent paragraph [0070]).
	EffectiveAgeReductionRate float64
}

// DefaultPDPConfig returns the config used in the Python POC.
func DefaultPDPConfig() PDPConfig {
	return PDPConfig{
		DesignLifeYears:           50.0,
		EffectiveAgeReductionRate: 0.3,
	}
}

// ComputePDP computes the Predictive Deterioration Penalty.
//
// Effective age accounts for chronological age, sensor-derived condition
// quality, and maintenance sensitivity. When sustained favourable sensor
// readings indicate consistent maintenance, effective age is REDUCED
// relative to actual age, lowering the deterioration penalty and increasing
// RTPMV — creating a financial incentive for property owners to maintain
// their buildings (patent paragraph [0070]).
//
// Returns a value in [0.0, 1.0] where 1.0 = no deterioration penalty.
func ComputePDP(data ConditionedData, cfg PDPConfig) float64 {
	effectiveAge := computeEffectiveAge(data, cfg)
	deteriorationRate := effectiveAge / cfg.DesignLifeYears
	pdp := 1.0 - clamp(deteriorationRate, 0.0, 1.0)
	return pdp
}

// computeEffectiveAge derives the effective age of the property.
// Good condition quality and maintenance reduce effective age below
// chronological age. Poor condition or maintenance increase it.
func computeEffectiveAge(data ConditionedData, cfg PDPConfig) float64 {
	// conditionQuality = 1.0 means perfect condition — reduce effective age
	// conditionQuality = 0.0 means poor condition — effective age = chronological age
	conditionAdjustment := data.MaintenanceSensitivity *
		cfg.EffectiveAgeReductionRate *
		(data.ConditionQuality - 0.5) * 2.0 // normalise to [-1, 1]

	effectiveAge := data.ChronologicalAge * (1.0 - conditionAdjustment)
	return math.Max(0.0, effectiveAge)
}

// =============================================================================
// CI — Confidence Index
// Patent specification paragraph [0052]
// =============================================================================

// CIConfig holds weights for each component of data confidence.
type CIConfig struct {
	UptimeWeight      float64
	ConsistencyWeight float64
	CalibrationWeight float64
	TamperWeight      float64

	// CalibrationDecayDays: calibration recency beyond this many days
	// starts reducing the CI score
	CalibrationDecayDays float64
}

// DefaultCIConfig returns the config used in the Python POC.
func DefaultCIConfig() CIConfig {
	return CIConfig{
		UptimeWeight:         0.30,
		ConsistencyWeight:    0.30,
		CalibrationWeight:    0.20,
		TamperWeight:         0.20,
		CalibrationDecayDays: 90.0,
	}
}

// ComputeCI computes the Confidence Index.
//
// Quantifies reliability of the conditioned data used by the digital twin.
// A low CI widens the bid/ask spread on the exchange platform and may
// trigger trading restrictions (patent paragraph [0060]).
//
// Returns a value in [0.0, 1.0] where 1.0 = full data confidence.
func ComputeCI(data ConditionedData, cfg CIConfig) float64 {
	// Calibration score: decays linearly after CalibrationDecayDays
	calibScore := 1.0 - clamp(data.CalibrationRecency/cfg.CalibrationDecayDays-1.0, 0.0, 1.0)

	ci := cfg.UptimeWeight*clamp(data.UptimeContinuity, 0.0, 1.0) +
		cfg.ConsistencyWeight*clamp(data.CrossSensorConsistency, 0.0, 1.0) +
		cfg.CalibrationWeight*calibScore +
		cfg.TamperWeight*clamp(data.TamperScore, 0.0, 1.0)

	return clamp(ci, 0.0, 1.0)
}

// =============================================================================
// Health Factor
// Patent specification paragraph [0055]
// =============================================================================

// ComputeHealthFactor computes the composite Health Factor from all five
// technical indicators.
//
// Formula: Health_Factor = SHF × ESF × (1 − USS) × PDP × CI
//
// NOTE: USS is inverted — high usage stress REDUCES the health factor.
func ComputeHealthFactor(ind TechnicalIndicators) HealthFactor {
	hf := ind.SHF * ind.ESF * (1.0 - ind.USS) * ind.PDP * ind.CI
	return HealthFactor(clamp(hf, 0.0, 1.0))
}

// =============================================================================
// RTPMV — Real-Time Property Market Value
// Patent specification paragraph [0054]
// =============================================================================

// ComputeRTPMV computes the Real-Time Property Market Value.
//
// Formula: RTPMV = Land_Value + (Structure_Value × Health_Factor)
//
// The technical indicators apply ONLY to Structure_Value. Land_Value does
// not physically deteriorate and remains unchanged. This separation ensures
// RTPMV accurately reflects changes in physical condition (patent [0054]).
func ComputeRTPMV(baseline BaselineMarketValue, hf HealthFactor) float64 {
	return baseline.LandValue + (baseline.StructureValue * float64(hf))
}

// =============================================================================
// All indicators in one call — convenience function for the pipeline
// =============================================================================

// AllIndicatorConfigs bundles all five indicator configs.
type AllIndicatorConfigs struct {
	SHF SHFConfig
	ESF ESFConfig
	USS USSConfig
	PDP PDPConfig
	CI  CIConfig
}

// DefaultAllConfigs returns all default configs matching the Python POC.
func DefaultAllConfigs() AllIndicatorConfigs {
	return AllIndicatorConfigs{
		SHF: DefaultSHFConfig(),
		ESF: DefaultESFConfig(),
		USS: DefaultUSSConfig(),
		PDP: DefaultPDPConfig(),
		CI:  DefaultCIConfig(),
	}
}

// ComputeAllIndicators computes all five technical indicators from
// conditioned sensor data in a single call.
func ComputeAllIndicators(data ConditionedData, cfgs AllIndicatorConfigs) TechnicalIndicators {
	return TechnicalIndicators{
		SHF: ComputeSHF(data, cfgs.SHF),
		ESF: ComputeESF(data, cfgs.ESF),
		USS: ComputeUSS(data, cfgs.USS),
		PDP: ComputePDP(data, cfgs.PDP),
		CI:  ComputeCI(data, cfgs.CI),
	}
}

// =============================================================================
// Helpers
// =============================================================================

func clamp(v, min, max float64) float64 {
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}
