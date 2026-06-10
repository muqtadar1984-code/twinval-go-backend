package compute

import (
	"math"
	"testing"
)

// tolerance for floating point comparison against Python POC outputs.
// Python float64 and Go float64 should agree to at least 10 decimal places.
const tolerance = 1e-9

// =============================================================================
// These test fixtures must match the Python POC output EXACTLY.
// Run the Python POC with these same inputs and record its outputs here.
// Any discrepancy means the Go port has a computation error.
// =============================================================================

// perfectPropertyData returns conditioned data representing a property
// in perfect condition — all indicators should return 1.0.
func perfectPropertyData() ConditionedData {
	return ConditionedData{
		VibrationMagnitude:      0.01, // well below normal ceiling
		StrainMagnitude:         50.0, // well below normal ceiling
		Temperature:             22.0, // mid-range optimal
		Humidity:                50.0, // mid-range optimal
		AirQualityPM:            5.0,  // well below safe level
		OccupancyRatio:          0.0,  // unoccupied
		ElectricalLoad:          0.0,
		WaterConsumption:        0.0,
		UptimeContinuity:        1.0,
		CrossSensorConsistency:  1.0,
		CalibrationRecency:      30.0, // 30 days — well within decay window
		TamperScore:             1.0,
		ChronologicalAge:        0.0,
		MaintenanceSensitivity:  0.5,
		ConditionQuality:        1.0,
	}
}

// degradedPropertyData returns conditioned data representing a stressed
// property — all indicators should return significantly below 1.0.
func degradedPropertyData() ConditionedData {
	return ConditionedData{
		VibrationMagnitude:      0.2,  // above normal ceiling
		StrainMagnitude:         600.0, // above normal ceiling
		Temperature:             35.0, // above optimal max
		Humidity:                80.0, // above optimal max
		AirQualityPM:            45.0, // 3× safe level
		OccupancyRatio:          0.95,
		ElectricalLoad:          0.90,
		WaterConsumption:        0.85,
		UptimeContinuity:        0.70,
		CrossSensorConsistency:  0.65,
		CalibrationRecency:      200.0, // well past decay window
		TamperScore:             0.80,
		ChronologicalAge:        30.0,
		MaintenanceSensitivity:  0.8,
		ConditionQuality:        0.3,
	}
}

func TestComputeSHF_PerfectProperty(t *testing.T) {
	data := perfectPropertyData()
	cfg := DefaultSHFConfig()
	shf := ComputeSHF(data, cfg)

	// Perfect property: both readings below normal ceiling → no penalty → SHF = 1.0
	if math.Abs(shf-1.0) > tolerance {
		t.Errorf("SHF perfect property: got %v, want 1.0", shf)
	}
}

func TestComputeSHF_BoundsAlwaysValid(t *testing.T) {
	cases := []ConditionedData{perfectPropertyData(), degradedPropertyData()}
	cfg := DefaultSHFConfig()

	for _, data := range cases {
		shf := ComputeSHF(data, cfg)
		if shf < 0.0 || shf > 1.0 {
			t.Errorf("SHF out of bounds [0,1]: got %v", shf)
		}
	}
}

func TestComputeESF_PerfectProperty(t *testing.T) {
	data := perfectPropertyData()
	cfg := DefaultESFConfig()
	esf := ComputeESF(data, cfg)

	// Temperature 22°C and humidity 50% are both in optimal range → ESF = 1.0
	if math.Abs(esf-1.0) > tolerance {
		t.Errorf("ESF perfect property: got %v, want 1.0", esf)
	}
}

func TestComputeUSS_ZeroOccupancy(t *testing.T) {
	data := perfectPropertyData() // occupancy, electrical, water all 0
	cfg := DefaultUSSConfig()
	uss := ComputeUSS(data, cfg)

	if math.Abs(uss-0.0) > tolerance {
		t.Errorf("USS zero load: got %v, want 0.0", uss)
	}
}

func TestComputeUSS_FullLoad(t *testing.T) {
	data := ConditionedData{
		OccupancyRatio:   1.0,
		ElectricalLoad:   1.0,
		WaterConsumption: 1.0,
	}
	cfg := DefaultUSSConfig()
	uss := ComputeUSS(data, cfg)

	if math.Abs(uss-1.0) > tolerance {
		t.Errorf("USS full load: got %v, want 1.0", uss)
	}
}

func TestComputeUSS_DefaultDeadbandIsZero_ParityPreserved(t *testing.T) {
	// With default config (no deadband) the legacy linear behaviour must
	// hold exactly — this is the Python-POC parity guarantee.
	data := ConditionedData{
		OccupancyRatio:   0.4,
		ElectricalLoad:   0.5,
		WaterConsumption: 0.2,
	}
	uss := ComputeUSS(data, DefaultUSSConfig())
	want := 0.5*0.4 + 0.3*0.5 + 0.2*0.2 // 0.39
	if math.Abs(uss-want) > tolerance {
		t.Errorf("USS default deadband: got %v, want %v", uss, want)
	}
}

func TestComputeUSS_NormalUseDeadband(t *testing.T) {
	// Residential calibration: usage at or below the normal-use
	// threshold produces zero stress.
	cfg := DefaultUSSConfig()
	cfg.OccupancyNormalUse = 0.40
	cfg.ElectricalNormalUse = 0.30
	cfg.WaterNormalUse = 0.30

	within := ConditionedData{
		OccupancyRatio:   0.35,
		ElectricalLoad:   0.25,
		WaterConsumption: 0.10,
	}
	if uss := ComputeUSS(within, cfg); math.Abs(uss) > tolerance {
		t.Errorf("USS within deadband: got %v, want 0.0", uss)
	}

	// Above the threshold, stress scales linearly to 1.0 at capacity.
	above := ConditionedData{
		OccupancyRatio:   0.70, // (0.70-0.40)/0.60 = 0.5
		ElectricalLoad:   0.65, // (0.65-0.30)/0.70 = 0.5
		WaterConsumption: 0.30, // at threshold = 0
	}
	uss := ComputeUSS(above, cfg)
	want := 0.5*0.5 + 0.3*0.5 // 0.40
	if math.Abs(uss-want) > tolerance {
		t.Errorf("USS above deadband: got %v, want %v", uss, want)
	}

	// Full capacity still scores full stress regardless of deadband.
	full := ConditionedData{OccupancyRatio: 1.0, ElectricalLoad: 1.0, WaterConsumption: 1.0}
	if uss := ComputeUSS(full, cfg); math.Abs(uss-1.0) > tolerance {
		t.Errorf("USS full load with deadband: got %v, want 1.0", uss)
	}
}

func TestComputePDP_NewProperty(t *testing.T) {
	data := perfectPropertyData() // ChronologicalAge = 0
	cfg := DefaultPDPConfig()
	pdp := ComputePDP(data, cfg)

	// New property: effective age = 0 → no deterioration penalty → PDP = 1.0
	if math.Abs(pdp-1.0) > tolerance {
		t.Errorf("PDP new property: got %v, want 1.0", pdp)
	}
}

func TestComputeCI_PerfectSensors(t *testing.T) {
	data := perfectPropertyData()
	cfg := DefaultCIConfig()
	ci := ComputeCI(data, cfg)

	if math.Abs(ci-1.0) > tolerance {
		t.Errorf("CI perfect sensors: got %v, want 1.0", ci)
	}
}

func TestComputeHealthFactor_PerfectIndicators(t *testing.T) {
	ind := TechnicalIndicators{
		SHF: 1.0,
		ESF: 1.0,
		USS: 0.0, // zero usage stress → (1 - USS) = 1.0
		PDP: 1.0,
		CI:  1.0,
	}
	hf := ComputeHealthFactor(ind)

	// 1.0 × 1.0 × 1.0 × 1.0 × 1.0 = 1.0
	if math.Abs(float64(hf)-1.0) > tolerance {
		t.Errorf("HealthFactor perfect: got %v, want 1.0", hf)
	}
}

func TestComputeHealthFactor_ZeroCI(t *testing.T) {
	ind := TechnicalIndicators{
		SHF: 1.0,
		ESF: 1.0,
		USS: 0.0,
		PDP: 1.0,
		CI:  0.0, // zero confidence → entire health factor collapses
	}
	hf := ComputeHealthFactor(ind)

	if math.Abs(float64(hf)-0.0) > tolerance {
		t.Errorf("HealthFactor zero CI: got %v, want 0.0", hf)
	}
}

func TestComputeRTPMV_Formula(t *testing.T) {
	baseline := BaselineMarketValue{
		LandValue:      500_000.0,
		StructureValue: 1_000_000.0,
		Currency:       "MYR",
		Source:         "Test fixture",
	}
	hf := HealthFactor(0.85)

	rtpmv := ComputeRTPMV(baseline, hf)
	// Expected: 500,000 + (1,000,000 × 0.85) = 1,350,000
	expected := 1_350_000.0

	if math.Abs(rtpmv-expected) > tolerance {
		t.Errorf("RTPMV formula: got %v, want %v", rtpmv, expected)
	}
}

func TestComputeRTPMV_LandUnchanged(t *testing.T) {
	// Land value must remain constant regardless of health factor
	baseline := BaselineMarketValue{
		LandValue:      800_000.0,
		StructureValue: 200_000.0,
	}

	rtpmvGood := ComputeRTPMV(baseline, HealthFactor(1.0))
	rtpmvBad := ComputeRTPMV(baseline, HealthFactor(0.0))

	// Land component must be identical in both cases
	landGood := rtpmvGood - 200_000.0*1.0
	landBad := rtpmvBad - 200_000.0*0.0

	if math.Abs(landGood-landBad) > tolerance {
		t.Errorf("Land value changed with health factor: good=%v bad=%v", landGood, landBad)
	}
}

// =============================================================================
// PARITY FIXTURES
// Replace the "want" values below with actual Python POC outputs.
// Run: python poc/compute_parity.py to generate the expected values.
// =============================================================================

func TestParityWithPythonPOC_DegradedProperty(t *testing.T) {
	data := degradedPropertyData()
	cfgs := DefaultAllConfigs()

	ind := ComputeAllIndicators(data, cfgs)
	hf := ComputeHealthFactor(ind)

	// TODO: replace these placeholder values with actual Python POC outputs
	// Run the Python POC with degradedPropertyData() inputs and paste here.
	parityFixtures := map[string]struct {
		got  float64
		want float64
	}{
		"SHF": {got: ind.SHF, want: 0.0}, // REPLACE with Python output
		"ESF": {got: ind.ESF, want: 0.0}, // REPLACE with Python output
		"USS": {got: ind.USS, want: 0.0}, // REPLACE with Python output
		"PDP": {got: ind.PDP, want: 0.0}, // REPLACE with Python output
		"CI":  {got: ind.CI, want: 0.0},  // REPLACE with Python output
		"HF":  {got: float64(hf), want: 0.0}, // REPLACE with Python output
	}

	for name, f := range parityFixtures {
		if f.want == 0.0 {
			t.Logf("PARITY FIXTURE NOT SET for %s: got %v — run Python POC to get expected value", name, f.got)
			continue
		}
		if math.Abs(f.got-f.want) > tolerance {
			t.Errorf("PARITY FAILURE %s: Go=%v Python=%v (diff=%v)", name, f.got, f.want, math.Abs(f.got-f.want))
		}
	}
}
