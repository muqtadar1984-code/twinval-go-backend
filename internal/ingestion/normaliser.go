package ingestion

import (
	"github.com/twinval/internal/ingestion/config"
	"github.com/twinval/internal/ingestion/models"
)

// Normaliser scales a SensorReading.Value to the unit interval [0, 1]
// using the per-type Min/Max from sensor_bounds.yaml. The original
// Value is preserved on the embedded SensorReading so the raw physical
// value can still be persisted to sensor_readings_raw.
//
// Stateless. Safe for concurrent use.
type Normaliser struct {
	bounds map[string]config.SensorBound
}

// NewNormaliser builds a Normaliser from a loaded bounds file.
func NewNormaliser(bounds config.SensorBoundsFile) *Normaliser {
	return &Normaliser{bounds: bounds.SensorTypes}
}

// Normalise returns the scaled reading and ok=true if the sensor type
// is known. ok=false for unknown types — caller should drop those.
//
// Values below Min clamp to 0; values above Max clamp to 1. Degenerate
// ranges (Min == Max) produce NormalisedValue=0 rather than NaN.
func (n *Normaliser) Normalise(r models.SensorReading) (models.NormalisedReading, bool) {
	bound, ok := n.bounds[r.SensorType]
	if !ok {
		return models.NormalisedReading{}, false
	}
	rng := bound.Max - bound.Min
	if rng <= 0 {
		return models.NormalisedReading{SensorReading: r, NormalisedValue: 0}, true
	}
	norm := (r.Value - bound.Min) / rng
	if norm < 0 {
		norm = 0
	}
	if norm > 1 {
		norm = 1
	}
	return models.NormalisedReading{SensorReading: r, NormalisedValue: norm}, true
}
