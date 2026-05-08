// Package ingestion holds the live sensor pipeline stages and orchestrator.
//
// Stages, in order:
//
//	1. Validator   - drops malformed / out-of-range / low-quality readings
//	2. Denoiser    - per-sensor exponential moving average
//	3. Normaliser  - per-type [0,1] scaling using sensor_bounds.yaml
//	4. Aggregator  - buckets by zone, emits ZoneSnapshot per window
//	5. Computer    - ZoneSnapshot -> ComputedZoneSnapshot via existing
//	                 compute package (NEVER reimplements indicator math)
//	6. Broadcaster - emits ComputedZoneSnapshot to the WebSocket hub
//	                 and persists via the DB writer pool
package ingestion

import (
	"math"
	"sync/atomic"

	"github.com/twinval/internal/ingestion/config"
	"github.com/twinval/internal/ingestion/metrics"
	"github.com/twinval/internal/ingestion/models"
)

// DropReason is why the validator rejected a reading. Mirrors the
// `reason` label on the twinval_readings_dropped_total Prometheus
// counter — keep these strings stable, dashboards filter on them.
type DropReason string

const (
	DropMalformed             DropReason = "malformed"              // NaN / Inf / empty fields
	DropQualityBelowThreshold DropReason = "quality_below_threshold"
	DropUnknownSensorType     DropReason = "unknown_sensor_type"
	DropOutOfRange            DropReason = "out_of_range"
)

// QualityFloor is the minimum device-reported quality below which the
// validator drops the reading. Pulled from the spec (0.1).
const QualityFloor = 0.1

// Validator gates the pipeline against malformed / suspicious readings.
// Stateless beyond its bounds map; safe for concurrent use.
type Validator struct {
	bounds       map[string]config.SensorBound
	qualityFloor float64

	// counters — exposed via Stats; later wired to Prometheus.
	dropped struct {
		malformed atomic.Uint64
		quality   atomic.Uint64
		unknown   atomic.Uint64
		oob       atomic.Uint64
		passed    atomic.Uint64
	}
}

// NewValidator builds a Validator from the loaded bounds file.
// `qualityFloor` defaults to QualityFloor if <= 0 is supplied.
func NewValidator(bounds config.SensorBoundsFile, qualityFloor float64) *Validator {
	if qualityFloor <= 0 {
		qualityFloor = QualityFloor
	}
	return &Validator{
		bounds:       bounds.SensorTypes,
		qualityFloor: qualityFloor,
	}
}

// Validate returns (true, "") for an acceptable reading or
// (false, reason) for a rejected one. Counters are incremented either
// way so Stats() reports both passed and dropped totals.
func (v *Validator) Validate(r models.SensorReading) (bool, DropReason) {
	if r.SensorID == "" || r.SensorType == "" || r.Building == "" || r.Zone == "" {
		v.dropped.malformed.Add(1)
		metrics.ReadingsDropped.WithLabelValues(string(DropMalformed)).Inc()
		return false, DropMalformed
	}
	if math.IsNaN(r.Value) || math.IsInf(r.Value, 0) {
		v.dropped.malformed.Add(1)
		metrics.ReadingsDropped.WithLabelValues(string(DropMalformed)).Inc()
		return false, DropMalformed
	}
	if r.Quality < v.qualityFloor {
		v.dropped.quality.Add(1)
		metrics.ReadingsDropped.WithLabelValues(string(DropQualityBelowThreshold)).Inc()
		return false, DropQualityBelowThreshold
	}
	bound, ok := v.bounds[r.SensorType]
	if !ok {
		v.dropped.unknown.Add(1)
		metrics.ReadingsDropped.WithLabelValues(string(DropUnknownSensorType)).Inc()
		return false, DropUnknownSensorType
	}
	if r.Value < bound.Min || r.Value > bound.Max {
		v.dropped.oob.Add(1)
		metrics.ReadingsDropped.WithLabelValues(string(DropOutOfRange)).Inc()
		return false, DropOutOfRange
	}
	v.dropped.passed.Add(1)
	return true, ""
}

// ValidatorStats is a snapshot of Validator counters.
type ValidatorStats struct {
	Passed             uint64
	DroppedMalformed   uint64
	DroppedQuality     uint64
	DroppedUnknownType uint64
	DroppedOutOfRange  uint64
}

// Stats returns counter values. Cheap; safe to call concurrently.
func (v *Validator) Stats() ValidatorStats {
	return ValidatorStats{
		Passed:             v.dropped.passed.Load(),
		DroppedMalformed:   v.dropped.malformed.Load(),
		DroppedQuality:     v.dropped.quality.Load(),
		DroppedUnknownType: v.dropped.unknown.Load(),
		DroppedOutOfRange:  v.dropped.oob.Load(),
	}
}
