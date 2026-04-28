package condition

import (
	"math"
	"testing"
)

const tolerance = 1e-9

// =============================================================================
// Denoise — exponential moving average
// =============================================================================

func TestDenoise_Empty(t *testing.T) {
	out := Denoise([]float64{}, 0.2)
	if len(out) != 0 {
		t.Errorf("empty input: got len %d, want 0", len(out))
	}
}

func TestDenoise_SingleValue(t *testing.T) {
	out := Denoise([]float64{42.0}, 0.2)
	if len(out) != 1 || out[0] != 42.0 {
		t.Errorf("single value: got %v, want [42.0]", out)
	}
}

func TestDenoise_KnownEMA(t *testing.T) {
	// alpha = 0.5, series = 1..5
	// EMA[0] = 1
	// EMA[1] = 0.5*2 + 0.5*1   = 1.5
	// EMA[2] = 0.5*3 + 0.5*1.5 = 2.25
	// EMA[3] = 0.5*4 + 0.5*2.25 = 3.125
	// EMA[4] = 0.5*5 + 0.5*3.125 = 4.0625
	series := []float64{1, 2, 3, 4, 5}
	want := []float64{1.0, 1.5, 2.25, 3.125, 4.0625}
	got := Denoise(series, 0.5)
	if len(got) != len(want) {
		t.Fatalf("length: got %d, want %d", len(got), len(want))
	}
	for i := range want {
		if math.Abs(got[i]-want[i]) > tolerance {
			t.Errorf("EMA[%d]: got %v, want %v", i, got[i], want[i])
		}
	}
}

func TestDenoise_ConstantSeriesUnchanged(t *testing.T) {
	got := Denoise([]float64{7, 7, 7, 7}, 0.3)
	for i, v := range got {
		if math.Abs(v-7.0) > tolerance {
			t.Errorf("EMA of constant series at %d: got %v, want 7.0", i, v)
		}
	}
}

// =============================================================================
// Normalise — bounds and clamping
// =============================================================================

func TestNormalise_InRange(t *testing.T) {
	if got := Normalise(50, 0, 100); math.Abs(got-0.5) > tolerance {
		t.Errorf("got %v, want 0.5", got)
	}
}

func TestNormalise_BelowMinClampsToZero(t *testing.T) {
	if got := Normalise(-5, 0, 100); got != 0.0 {
		t.Errorf("got %v, want 0.0", got)
	}
}

func TestNormalise_AboveMaxClampsToOne(t *testing.T) {
	if got := Normalise(150, 0, 100); got != 1.0 {
		t.Errorf("got %v, want 1.0", got)
	}
}

func TestNormalise_DegenerateRange(t *testing.T) {
	if got := Normalise(50, 100, 100); got != 0.0 {
		t.Errorf("max <= min: got %v, want 0.0", got)
	}
}

// =============================================================================
// AlignTimestamps — forward-fill snapshot
// =============================================================================

func TestAlignTimestamps_TakesMostRecentBeforeAsOf(t *testing.T) {
	readings := []RawSensorReading{
		{SensorID: "A", TimestampNs: 100, Value: 10},
		{SensorID: "A", TimestampNs: 200, Value: 20},
		{SensorID: "B", TimestampNs: 150, Value: 50},
	}
	snap := AlignTimestamps(readings, 250)
	if snap["A"] != 20 {
		t.Errorf("A: got %v, want 20", snap["A"])
	}
	if snap["B"] != 50 {
		t.Errorf("B: got %v, want 50", snap["B"])
	}
}

func TestAlignTimestamps_ExcludesFutureReadings(t *testing.T) {
	readings := []RawSensorReading{
		{SensorID: "A", TimestampNs: 100, Value: 10},
		{SensorID: "A", TimestampNs: 300, Value: 20},
	}
	snap := AlignTimestamps(readings, 200)
	if snap["A"] != 10 {
		t.Errorf("future reading should be excluded: got %v, want 10", snap["A"])
	}
}

// =============================================================================
// CheckUptimeContinuity
// =============================================================================

func TestCheckUptimeContinuity_Partial(t *testing.T) {
	if got := CheckUptimeContinuity(92, 100); math.Abs(got-0.92) > tolerance {
		t.Errorf("got %v, want 0.92", got)
	}
}

func TestCheckUptimeContinuity_OverDeliveryClampsToOne(t *testing.T) {
	if got := CheckUptimeContinuity(200, 100); got != 1.0 {
		t.Errorf("got %v, want 1.0", got)
	}
}

func TestCheckUptimeContinuity_NoExpectationIsOne(t *testing.T) {
	if got := CheckUptimeContinuity(0, 0); got != 1.0 {
		t.Errorf("got %v, want 1.0", got)
	}
}

// =============================================================================
// CheckTamper
// =============================================================================

func TestCheckTamper_NoJumpsScoresOne(t *testing.T) {
	readings := []RawSensorReading{
		{SensorID: "S1", TimestampNs: 1, Value: 20},
		{SensorID: "S1", TimestampNs: 2, Value: 21},
		{SensorID: "S1", TimestampNs: 3, Value: 22},
	}
	if got := CheckTamper(readings, 10.0); got != 1.0 {
		t.Errorf("got %v, want 1.0", got)
	}
}

func TestCheckTamper_OneJumpHalvesScore(t *testing.T) {
	// 2 pairs total; 1 pair (22 → 60) exceeds the 10.0 step limit.
	readings := []RawSensorReading{
		{SensorID: "S1", TimestampNs: 1, Value: 20},
		{SensorID: "S1", TimestampNs: 2, Value: 22},
		{SensorID: "S1", TimestampNs: 3, Value: 60},
	}
	if got := CheckTamper(readings, 10.0); math.Abs(got-0.5) > tolerance {
		t.Errorf("got %v, want 0.5", got)
	}
}

func TestCheckTamper_NoPairsScoresOne(t *testing.T) {
	readings := []RawSensorReading{
		{SensorID: "S1", TimestampNs: 1, Value: 20},
	}
	if got := CheckTamper(readings, 10.0); got != 1.0 {
		t.Errorf("got %v, want 1.0", got)
	}
}

// =============================================================================
// CheckCrossConsistency
// =============================================================================

func TestCheckCrossConsistency_TightAgreement(t *testing.T) {
	// Two sensors in zone "office", spread 0.5, tolerance 5.0 → penalty 0.1
	// Score = 1 - 0.1 = 0.9
	readings := []RawSensorReading{
		{SensorID: "T1", Zone: "office", Value: 22.0},
		{SensorID: "T2", Zone: "office", Value: 22.5},
	}
	if got := CheckCrossConsistency(readings, 5.0); math.Abs(got-0.9) > tolerance {
		t.Errorf("got %v, want 0.9", got)
	}
}

func TestCheckCrossConsistency_SingleSensorInZone(t *testing.T) {
	readings := []RawSensorReading{
		{SensorID: "T1", Zone: "office", Value: 22.0},
	}
	if got := CheckCrossConsistency(readings, 5.0); got != 1.0 {
		t.Errorf("got %v, want 1.0", got)
	}
}

func TestCheckCrossConsistency_LargeDisagreementClampsToZero(t *testing.T) {
	// Spread 10, tolerance 1 → penalty 10 → score clamps to 0
	readings := []RawSensorReading{
		{SensorID: "T1", Zone: "office", Value: 20.0},
		{SensorID: "T2", Zone: "office", Value: 30.0},
	}
	if got := CheckCrossConsistency(readings, 1.0); got != 0.0 {
		t.Errorf("got %v, want 0.0", got)
	}
}

// =============================================================================
// Process — end-to-end pipeline
// =============================================================================

func TestProcess_ProducesValidConditionedData(t *testing.T) {
	cfg := DefaultConditioningConfig()
	batch := RawSensorBatch{
		WindowStartNs:             0,
		WindowEndNs:               1000,
		ExpectedReadingsPerSensor: 3,
		Readings: []RawSensorReading{
			{SensorID: "vib1", SensorType: SensorVibration, Zone: "core", TimestampNs: 100, Value: 0.02, CalibrationDaysAgo: 30},
			{SensorID: "vib1", SensorType: SensorVibration, Zone: "core", TimestampNs: 200, Value: 0.03, CalibrationDaysAgo: 30},
			{SensorID: "vib1", SensorType: SensorVibration, Zone: "core", TimestampNs: 300, Value: 0.04, CalibrationDaysAgo: 30},

			{SensorID: "str1", SensorType: SensorStrain, Zone: "beam-A", TimestampNs: 100, Value: 100, CalibrationDaysAgo: 30},
			{SensorID: "str1", SensorType: SensorStrain, Zone: "beam-A", TimestampNs: 200, Value: 110, CalibrationDaysAgo: 30},
			{SensorID: "str1", SensorType: SensorStrain, Zone: "beam-A", TimestampNs: 300, Value: 120, CalibrationDaysAgo: 30},

			{SensorID: "tmp1", SensorType: SensorTemperature, Zone: "lobby", TimestampNs: 100, Value: 21, CalibrationDaysAgo: 30},
			{SensorID: "tmp1", SensorType: SensorTemperature, Zone: "lobby", TimestampNs: 200, Value: 22, CalibrationDaysAgo: 30},
			{SensorID: "tmp1", SensorType: SensorTemperature, Zone: "lobby", TimestampNs: 300, Value: 22, CalibrationDaysAgo: 30},

			{SensorID: "hum1", SensorType: SensorHumidity, Zone: "lobby", TimestampNs: 100, Value: 50, CalibrationDaysAgo: 30},
			{SensorID: "hum1", SensorType: SensorHumidity, Zone: "lobby", TimestampNs: 200, Value: 50, CalibrationDaysAgo: 30},
			{SensorID: "hum1", SensorType: SensorHumidity, Zone: "lobby", TimestampNs: 300, Value: 50, CalibrationDaysAgo: 30},

			{SensorID: "pm1", SensorType: SensorPM25, Zone: "lobby", TimestampNs: 100, Value: 8, CalibrationDaysAgo: 30},
			{SensorID: "pm1", SensorType: SensorPM25, Zone: "lobby", TimestampNs: 200, Value: 9, CalibrationDaysAgo: 30},
			{SensorID: "pm1", SensorType: SensorPM25, Zone: "lobby", TimestampNs: 300, Value: 10, CalibrationDaysAgo: 30},

			{SensorID: "occ1", SensorType: SensorOccupancy, Zone: "core", TimestampNs: 100, Value: 0.4, CalibrationDaysAgo: 30},
			{SensorID: "occ1", SensorType: SensorOccupancy, Zone: "core", TimestampNs: 200, Value: 0.5, CalibrationDaysAgo: 30},
			{SensorID: "occ1", SensorType: SensorOccupancy, Zone: "core", TimestampNs: 300, Value: 0.6, CalibrationDaysAgo: 30},

			{SensorID: "el1", SensorType: SensorElectrical, Zone: "core", TimestampNs: 100, Value: 0.5, CalibrationDaysAgo: 30},
			{SensorID: "el1", SensorType: SensorElectrical, Zone: "core", TimestampNs: 200, Value: 0.5, CalibrationDaysAgo: 30},
			{SensorID: "el1", SensorType: SensorElectrical, Zone: "core", TimestampNs: 300, Value: 0.5, CalibrationDaysAgo: 30},

			{SensorID: "wat1", SensorType: SensorWater, Zone: "core", TimestampNs: 100, Value: 0.3, CalibrationDaysAgo: 30},
			{SensorID: "wat1", SensorType: SensorWater, Zone: "core", TimestampNs: 200, Value: 0.3, CalibrationDaysAgo: 30},
			{SensorID: "wat1", SensorType: SensorWater, Zone: "core", TimestampNs: 300, Value: 0.3, CalibrationDaysAgo: 30},
		},
		PropertyMeta: PropertyMeta{
			ChronologicalAge:       10,
			MaintenanceSensitivity: 0.7,
			ConditionQuality:       0.8,
		},
	}

	cd, ir := Process(batch, cfg)

	// Property metadata should pass through unchanged
	if cd.ChronologicalAge != 10 {
		t.Errorf("ChronologicalAge: got %v, want 10", cd.ChronologicalAge)
	}
	if cd.MaintenanceSensitivity != 0.7 {
		t.Errorf("MaintenanceSensitivity: got %v, want 0.7", cd.MaintenanceSensitivity)
	}
	if cd.ConditionQuality != 0.8 {
		t.Errorf("ConditionQuality: got %v, want 0.8", cd.ConditionQuality)
	}

	// 8 sensors × 3 readings = 24 received vs 24 expected → uptime 1.0
	if math.Abs(cd.UptimeContinuity-1.0) > tolerance {
		t.Errorf("UptimeContinuity: got %v, want 1.0", cd.UptimeContinuity)
	}

	// Calibration mean is 30 days for every reading
	if math.Abs(cd.CalibrationRecency-30.0) > tolerance {
		t.Errorf("CalibrationRecency: got %v, want 30.0", cd.CalibrationRecency)
	}

	// Bounded fields stay in [0, 1]
	for name, v := range map[string]float64{
		"OccupancyRatio":         cd.OccupancyRatio,
		"ElectricalLoad":         cd.ElectricalLoad,
		"WaterConsumption":       cd.WaterConsumption,
		"UptimeContinuity":       cd.UptimeContinuity,
		"CrossSensorConsistency": cd.CrossSensorConsistency,
		"TamperScore":            cd.TamperScore,
	} {
		if v < 0.0 || v > 1.0 {
			t.Errorf("%s out of [0, 1]: %v", name, v)
		}
	}

	// IntegrityReport must match the corresponding ConditionedData fields
	if cd.UptimeContinuity != ir.UptimeContinuity ||
		cd.CrossSensorConsistency != ir.CrossSensorConsistency ||
		cd.CalibrationRecency != ir.CalibrationRecency ||
		cd.TamperScore != ir.TamperScore {
		t.Errorf("ConditionedData CI fields disagree with IntegrityReport: cd=%+v ir=%+v", cd, ir)
	}

	// Vibration EMA (alpha 0.2): 0.02 → 0.022 → 0.0256
	wantVib := 0.0256
	if math.Abs(cd.VibrationMagnitude-wantVib) > tolerance {
		t.Errorf("VibrationMagnitude smoothed: got %v, want %v", cd.VibrationMagnitude, wantVib)
	}
}

func TestProcess_NoReadingsProducesZeroValues(t *testing.T) {
	cfg := DefaultConditioningConfig()
	batch := RawSensorBatch{
		WindowStartNs:             0,
		WindowEndNs:               1000,
		ExpectedReadingsPerSensor: 0,
		PropertyMeta:              PropertyMeta{ChronologicalAge: 5},
	}
	cd, ir := Process(batch, cfg)

	if cd.VibrationMagnitude != 0 || cd.Temperature != 0 || cd.OccupancyRatio != 0 {
		t.Errorf("empty batch should leave physical fields at 0: %+v", cd)
	}
	if cd.ChronologicalAge != 5 {
		t.Errorf("ChronologicalAge passthrough: got %v, want 5", cd.ChronologicalAge)
	}
	// No readings, no expectation → uptime 1.0 (no signal)
	if ir.UptimeContinuity != 1.0 {
		t.Errorf("UptimeContinuity with empty batch: got %v, want 1.0", ir.UptimeContinuity)
	}
}
