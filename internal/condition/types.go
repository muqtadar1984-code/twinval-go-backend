// Package condition implements Patent Module 320 — the processing module.
//
// It transforms raw sensor batches into compute.ConditionedData via three
// preprocessing techniques described in patent paragraph [0043]:
//
//  1. Denoising — exponential moving average per sensor stream
//  2. Normalisation — bounded [0, 1] scaling for fractional sensors
//  3. Temporal alignment — forward-fill snapshot at a target timestamp
//
// It also runs the four sensor integrity checks from paragraph [0045]
// (uptime, cross-sensor consistency, calibration recency, tamper detection)
// that feed the Confidence Index in the compute package.
//
// VALIDATION NOTE:
// Process() is the single entry point. Its output must be numerically
// identical to the Python POC's preprocessing pipeline given identical
// inputs and identical config.
package condition

// SensorType identifies the kind of physical reading a sensor produces.
// These map to fields in compute.ConditionedData.
type SensorType string

const (
	SensorVibration   SensorType = "vibration"
	SensorStrain      SensorType = "strain"
	SensorTemperature SensorType = "temperature"
	SensorHumidity    SensorType = "humidity"
	SensorPM25        SensorType = "pm25"
	SensorOccupancy   SensorType = "occupancy"
	SensorElectrical  SensorType = "electrical"
	SensorWater       SensorType = "water"
)

// RawSensorReading is one timestamped data point from a single sensor.
// Multiple sensors of the same type may exist in the same zone (redundant
// deployment) — that is what enables the CrossSensorConsistency check.
type RawSensorReading struct {
	SensorID           string
	SensorType         SensorType
	Zone               string  // logical zone (e.g. "lobby", "beam-A")
	TimestampNs        int64   // Unix nanoseconds
	Value              float64 // physical reading in the sensor's native units
	CalibrationDaysAgo float64 // days since this sensor was last calibrated
}

// RawSensorBatch carries all raw readings for one conditioning window
// plus the property-level metadata that is forwarded directly to
// compute.ConditionedData (no preprocessing applied).
type RawSensorBatch struct {
	Readings []RawSensorReading

	WindowStartNs int64 // start of the time window covered by this batch
	WindowEndNs   int64 // end of the window — used as the snapshot timestamp

	// ExpectedReadingsPerSensor is the number of readings each sensor
	// was scheduled to produce in this window. Used for uptime computation.
	// Must be > 0 for uptime to be measurable.
	ExpectedReadingsPerSensor int

	PropertyMeta PropertyMeta
}

// PropertyMeta is the property-level data that bypasses sensor processing.
// These values come from the property registration record, not sensors.
type PropertyMeta struct {
	ChronologicalAge       float64 // years since construction
	MaintenanceSensitivity float64 // 0–1; higher = more PDP impact
	ConditionQuality       float64 // 0–1; sensor-derived overall condition
}

// Range holds an inclusive min/max pair used by Normalise.
type Range struct {
	Min, Max float64
}

// ConditioningConfig holds every calibration parameter for the condition
// pipeline. No defaults are baked into the processing functions — every
// call requires an explicit config so behaviour is reproducible.
type ConditioningConfig struct {
	// EMAAlpha is the smoothing factor for Denoise. Must be in (0, 1].
	// Higher = more weight on recent readings (less smoothing).
	EMAAlpha float64

	// Bounds defines the physical [Min, Max] used by Normalise per type.
	// Only the fractional sensors (occupancy, electrical, water) are
	// normalised by Process; physical-unit sensors (vibration, strain,
	// temperature, humidity, pm25) pass through to keep compute happy.
	Bounds map[SensorType]Range

	// TamperStepLimits is the largest physical change between two
	// consecutive readings of the same sensor that is considered plausible.
	// Steps larger than this contribute to a tamper flag.
	TamperStepLimits map[SensorType]float64

	// ConsistencyTolerances is the largest spread between same-zone
	// sensors of the same type that still scores 1.0. Spreads beyond
	// this point reduce the consistency score linearly.
	ConsistencyTolerances map[SensorType]float64
}

// IntegrityReport is the output of the four integrity checks. Its four
// fields map directly to the four CI input fields on compute.ConditionedData.
type IntegrityReport struct {
	UptimeContinuity       float64 // [0, 1]
	CrossSensorConsistency float64 // [0, 1]
	CalibrationRecency     float64 // mean days since calibration across batch
	TamperScore            float64 // [0, 1]; 1.0 = clean
}

// DefaultConditioningConfig returns a config with sensible production-
// representative defaults. Bounds for fractional sensors are [0, 1] so
// already-normalised inputs pass through unchanged. Physical sensors
// have wide bounds because Process does not normalise them.
func DefaultConditioningConfig() ConditioningConfig {
	return ConditioningConfig{
		EMAAlpha: 0.2,
		Bounds: map[SensorType]Range{
			SensorVibration:   {Min: 0.0, Max: 1.0},
			SensorStrain:      {Min: 0.0, Max: 1000.0},
			SensorTemperature: {Min: -10.0, Max: 50.0},
			SensorHumidity:    {Min: 0.0, Max: 100.0},
			SensorPM25:        {Min: 0.0, Max: 500.0},
			SensorOccupancy:   {Min: 0.0, Max: 1.0},
			SensorElectrical:  {Min: 0.0, Max: 1.0},
			SensorWater:       {Min: 0.0, Max: 1.0},
		},
		TamperStepLimits: map[SensorType]float64{
			SensorVibration:   0.5,
			SensorStrain:      300.0,
			SensorTemperature: 10.0,
			SensorHumidity:    20.0,
			SensorPM25:        100.0,
			SensorOccupancy:   0.5,
			SensorElectrical:  0.5,
			SensorWater:       0.5,
		},
		ConsistencyTolerances: map[SensorType]float64{
			SensorVibration:   0.02,
			SensorStrain:      50.0,
			SensorTemperature: 1.0,
			SensorHumidity:    5.0,
			SensorPM25:        5.0,
			SensorOccupancy:   0.1,
			SensorElectrical:  0.1,
			SensorWater:       0.1,
		},
	}
}
