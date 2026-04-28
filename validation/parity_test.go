// Package validation holds the cross-implementation parity tests.
//
// These tests compare Go-computed values against the Python POC's
// outputs for the same inputs. The Python POC is the patent's reference
// implementation — Go must produce numerically identical results
// (within 1e-9 for floats; byte-identical for SHA-256 hashes) before
// the Go backend is allowed to replace it in production.
//
// Workflow:
//
//  1. Inputs are pre-populated in parity_fixtures.json.
//  2. Run the Python POC with the same inputs.
//  3. Record Python's outputs in the "expected" sections of the JSON.
//  4. Run `go test ./validation/...`. Parity passes when every assertion
//     succeeds; mismatches are reported with both Go and Python values
//     so the discrepancy is obvious.
//
// Until the JSON file is populated, expected fields can be left as
// `null` (or omitted). The test logs which fields need populating and
// continues — it does not fail. Once a field is non-null, it is treated
// as authoritative and any mismatch is a hard failure.
//
// Tolerance: 1e-9 for float comparisons. SHA-256 hashes must match
// exactly (string equality).
package validation

import (
	"encoding/json"
	"math"
	"os"
	"testing"

	"github.com/twinval/internal/compute"
	"github.com/twinval/internal/cryptochain"
)

// fixturesPath is relative to the test's working directory, which Go
// sets to the directory containing the test file.
const fixturesPath = "parity_fixtures.json"

// tolerance for float comparisons. A discrepancy larger than this is a
// computation error — almost certainly a formula bug or a config drift.
const tolerance = 1e-9

// =============================================================================
// Fixture file types
// =============================================================================

type Fixtures struct {
	IndicatorScenarios []IndicatorScenario `json:"indicator_scenarios"`
	HashScenarios      []HashScenario      `json:"hash_scenarios"`
}

type IndicatorScenario struct {
	Name            string            `json:"name"`
	ConditionedData ConditionedDataIn `json:"conditioned_data"`
	Baseline        BaselineIn        `json:"baseline"`
	Expected        IndicatorExpected `json:"expected"`
}

// ConditionedDataIn mirrors compute.ConditionedData with snake_case JSON
// tags so the fixture file is human-friendly.
type ConditionedDataIn struct {
	VibrationMagnitude     float64 `json:"vibration_magnitude"`
	StrainMagnitude        float64 `json:"strain_magnitude"`
	Temperature            float64 `json:"temperature"`
	Humidity               float64 `json:"humidity"`
	AirQualityPM           float64 `json:"air_quality_pm"`
	OccupancyRatio         float64 `json:"occupancy_ratio"`
	ElectricalLoad         float64 `json:"electrical_load"`
	WaterConsumption       float64 `json:"water_consumption"`
	UptimeContinuity       float64 `json:"uptime_continuity"`
	CrossSensorConsistency float64 `json:"cross_sensor_consistency"`
	CalibrationRecency     float64 `json:"calibration_recency"`
	TamperScore            float64 `json:"tamper_score"`
	ChronologicalAge       float64 `json:"chronological_age"`
	MaintenanceSensitivity float64 `json:"maintenance_sensitivity"`
	ConditionQuality       float64 `json:"condition_quality"`
}

func (c ConditionedDataIn) toCompute() compute.ConditionedData {
	return compute.ConditionedData{
		VibrationMagnitude:     c.VibrationMagnitude,
		StrainMagnitude:        c.StrainMagnitude,
		Temperature:            c.Temperature,
		Humidity:               c.Humidity,
		AirQualityPM:           c.AirQualityPM,
		OccupancyRatio:         c.OccupancyRatio,
		ElectricalLoad:         c.ElectricalLoad,
		WaterConsumption:       c.WaterConsumption,
		UptimeContinuity:       c.UptimeContinuity,
		CrossSensorConsistency: c.CrossSensorConsistency,
		CalibrationRecency:     c.CalibrationRecency,
		TamperScore:            c.TamperScore,
		ChronologicalAge:       c.ChronologicalAge,
		MaintenanceSensitivity: c.MaintenanceSensitivity,
		ConditionQuality:       c.ConditionQuality,
	}
}

type BaselineIn struct {
	LandValue      float64 `json:"land_value"`
	StructureValue float64 `json:"structure_value"`
	Currency       string  `json:"currency"`
}

// IndicatorExpected uses *float64 so JSON `null` (or an omitted field)
// signals "not yet populated from Python" — distinct from a populated
// value that happens to be 0.0 (e.g. ESF on a degraded property).
type IndicatorExpected struct {
	SHF          *float64 `json:"shf"`
	ESF          *float64 `json:"esf"`
	USS          *float64 `json:"uss"`
	PDP          *float64 `json:"pdp"`
	CI           *float64 `json:"ci"`
	HealthFactor *float64 `json:"health_factor"`
	RTPMV        *float64 `json:"rtpmv"`
}

type HashScenario struct {
	Name            string             `json:"name"`
	ConditionedData *ConditionedDataIn `json:"conditioned_data,omitempty"`
	Indicators      *IndicatorsIn      `json:"indicators,omitempty"`
	Token           *TokenIn           `json:"token,omitempty"`
	Expected        HashExpected       `json:"expected"`
}

type IndicatorsIn struct {
	SHF float64 `json:"shf"`
	ESF float64 `json:"esf"`
	USS float64 `json:"uss"`
	PDP float64 `json:"pdp"`
	CI  float64 `json:"ci"`
}

func (i IndicatorsIn) toCompute() compute.TechnicalIndicators {
	return compute.TechnicalIndicators{
		SHF: i.SHF, ESF: i.ESF, USS: i.USS, PDP: i.PDP, CI: i.CI,
	}
}

type TokenIn struct {
	TokenID             uint64  `json:"token_id"`
	PropertyID          string  `json:"property_id"`
	Timestamp           int64   `json:"timestamp"`
	ConditionedDataHash string  `json:"conditioned_data_hash"`
	IndicatorsHash      string  `json:"indicators_hash"`
	RTPMV               float64 `json:"rtpmv"`
	Currency            string  `json:"currency"`
	PrecedingHash       string  `json:"preceding_hash"`
}

func (t TokenIn) toCryptochain() cryptochain.ValuationToken {
	return cryptochain.ValuationToken{
		TokenID:             t.TokenID,
		PropertyID:          t.PropertyID,
		Timestamp:           t.Timestamp,
		ConditionedDataHash: t.ConditionedDataHash,
		IndicatorsHash:      t.IndicatorsHash,
		RTPMV:               t.RTPMV,
		Currency:            t.Currency,
		PrecedingHash:       t.PrecedingHash,
	}
}

// HashExpected uses empty string as the "not yet populated" sentinel —
// no real SHA-256 hex output is empty.
type HashExpected struct {
	ConditionedDataHash string `json:"conditioned_data_hash"`
	IndicatorsHash      string `json:"indicators_hash"`
	TokenHash           string `json:"token_hash"`
}

// =============================================================================
// Fixture loading
// =============================================================================

// loadFixtures reads parity_fixtures.json. If the file is absent the
// caller test is skipped — parity is opt-in until the Python POC has
// been run and outputs recorded.
func loadFixtures(t *testing.T) *Fixtures {
	t.Helper()
	data, err := os.ReadFile(fixturesPath)
	if err != nil {
		if os.IsNotExist(err) {
			t.Skipf("%s not present — populate it from Python POC to enable parity checks", fixturesPath)
			return nil
		}
		t.Fatalf("read %s: %v", fixturesPath, err)
	}
	var f Fixtures
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatalf("parse %s: %v", fixturesPath, err)
	}
	return &f
}

// =============================================================================
// Indicator parity
// =============================================================================

func TestParity_Indicators(t *testing.T) {
	fix := loadFixtures(t)
	if fix == nil {
		return
	}
	if len(fix.IndicatorScenarios) == 0 {
		t.Skip("no indicator_scenarios in parity_fixtures.json")
		return
	}

	cfgs := compute.DefaultAllConfigs()

	for _, scn := range fix.IndicatorScenarios {
		scn := scn
		t.Run(scn.Name, func(t *testing.T) {
			data := scn.ConditionedData.toCompute()
			ind := compute.ComputeAllIndicators(data, cfgs)
			hf := compute.ComputeHealthFactor(ind)
			rtpmv := compute.ComputeRTPMV(compute.BaselineMarketValue{
				LandValue:      scn.Baseline.LandValue,
				StructureValue: scn.Baseline.StructureValue,
				Currency:       scn.Baseline.Currency,
			}, hf)

			compareFloat(t, "SHF", ind.SHF, scn.Expected.SHF)
			compareFloat(t, "ESF", ind.ESF, scn.Expected.ESF)
			compareFloat(t, "USS", ind.USS, scn.Expected.USS)
			compareFloat(t, "PDP", ind.PDP, scn.Expected.PDP)
			compareFloat(t, "CI", ind.CI, scn.Expected.CI)
			compareFloat(t, "HealthFactor", float64(hf), scn.Expected.HealthFactor)
			compareFloat(t, "RTPMV", rtpmv, scn.Expected.RTPMV)
		})
	}
}

// compareFloat asserts goVal ≈ pyVal within tolerance, or logs a
// "needs populating" notice when pyVal is nil (JSON null).
func compareFloat(t *testing.T, name string, goVal float64, pyVal *float64) {
	t.Helper()
	if pyVal == nil {
		t.Logf("PARITY %s: not populated yet — Go=%.15g (record this in parity_fixtures.json after running Python POC)", name, goVal)
		return
	}
	diff := math.Abs(goVal - *pyVal)
	if diff > tolerance {
		t.Errorf("PARITY %s mismatch:\n  Go     = %.15g\n  Python = %.15g\n  diff   = %.2e (tolerance %.0e)",
			name, goVal, *pyVal, diff, tolerance)
		return
	}
	t.Logf("PARITY %s OK: Go=%.15g Python=%.15g diff=%.2e", name, goVal, *pyVal, diff)
}

// =============================================================================
// Hash parity
// =============================================================================

func TestParity_Hashes(t *testing.T) {
	fix := loadFixtures(t)
	if fix == nil {
		return
	}
	if len(fix.HashScenarios) == 0 {
		t.Skip("no hash_scenarios in parity_fixtures.json")
		return
	}

	for _, scn := range fix.HashScenarios {
		scn := scn
		t.Run(scn.Name, func(t *testing.T) {
			if scn.ConditionedData != nil {
				goHash := cryptochain.HashConditionedData(scn.ConditionedData.toCompute())
				compareHash(t, "ConditionedDataHash", goHash, scn.Expected.ConditionedDataHash)
			}
			if scn.Indicators != nil {
				goHash := cryptochain.HashIndicators(scn.Indicators.toCompute())
				compareHash(t, "IndicatorsHash", goHash, scn.Expected.IndicatorsHash)
			}
			if scn.Token != nil {
				goHash := cryptochain.HashToken(scn.Token.toCryptochain())
				compareHash(t, "TokenHash", goHash, scn.Expected.TokenHash)
			}
		})
	}
}

// compareHash asserts byte-identical hex strings, or logs a
// "needs populating" notice when pyHash is empty.
func compareHash(t *testing.T, name, goHash, pyHash string) {
	t.Helper()
	if pyHash == "" {
		t.Logf("PARITY %s: not populated yet — Go=%s (record this in parity_fixtures.json after running Python POC)", name, goHash)
		return
	}
	if goHash != pyHash {
		t.Errorf("PARITY %s mismatch:\n  Go     = %s\n  Python = %s", name, goHash, pyHash)
		return
	}
	t.Logf("PARITY %s OK: %s", name, goHash)
}
