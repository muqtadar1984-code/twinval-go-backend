package ingest

import (
	"fmt"
	"math"

	"github.com/twinval/internal/condition"
)

// validSensorTypes is the set of sensor type strings the pipeline accepts.
// Mirrors the constants in condition.types.go — keep in sync if a new
// sensor type is added there.
var validSensorTypes = map[condition.SensorType]struct{}{
	condition.SensorVibration:   {},
	condition.SensorStrain:      {},
	condition.SensorTemperature: {},
	condition.SensorHumidity:    {},
	condition.SensorPM25:        {},
	condition.SensorOccupancy:   {},
	condition.SensorElectrical:  {},
	condition.SensorWater:       {},
}

// ValidatePayload performs structural validation on a decoded WebhookPayload.
// Returns nil on success, or an IngestError (with Code 422) on failure.
//
// Checks:
//   - PropertyID is not empty
//   - At least one reading is present
//   - Every reading's SensorType is recognised
//   - Every reading's TimestampNs is non-zero
//   - Every reading's Value is finite (not NaN or Inf)
func ValidatePayload(p WebhookPayload) error {
	if p.PropertyID == "" {
		return newUnprocessable("property_id is required")
	}
	if len(p.Readings) == 0 {
		return newUnprocessable("at least one reading is required")
	}
	for i, r := range p.Readings {
		if _, ok := validSensorTypes[condition.SensorType(r.SensorType)]; !ok {
			return newUnprocessable(fmt.Sprintf("readings[%d]: unrecognised sensor_type %q", i, r.SensorType))
		}
		if r.TimestampNs == 0 {
			return newUnprocessable(fmt.Sprintf("readings[%d]: timestamp_ns must be non-zero", i))
		}
		if math.IsNaN(r.Value) || math.IsInf(r.Value, 0) {
			return newUnprocessable(fmt.Sprintf("readings[%d]: value must be finite", i))
		}
	}
	return nil
}
