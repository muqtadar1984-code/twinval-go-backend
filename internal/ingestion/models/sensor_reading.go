// Package models holds the data structures shared across every stage of
// the live ingestion pipeline.
//
// The three structs flow through the pipeline in this order:
//
//   1. Adapters (MQTT / Webhook / Modbus) emit SensorReading.
//   2. Validator + Denoiser pass SensorReading through (state mutates
//      Value via EMA, but the struct shape is unchanged).
//   3. Normaliser converts SensorReading -> NormalisedReading by
//      computing NormalisedValue using the per-type bounds.
//   4. Aggregator groups NormalisedReadings by Building+Zone within a
//      time window and emits ZoneSnapshot — one snapshot per zone per
//      window, holding the latest normalised value per sensor type.
package models

import (
	"time"
)

// Source identifies which adapter produced a reading.
type Source string

const (
	SourceMQTT    Source = "mqtt"
	SourceWebhook Source = "webhook"
	SourceModbus  Source = "modbus"
)

// SensorReading is one timestamped data point from a single sensor in
// the field. It is the unit of work consumed by every pipeline stage
// up to (but not including) Normalise.
type SensorReading struct {
	SensorID   string
	Building   string
	Zone       string
	SensorType string
	Unit       string
	Value      float64
	Quality    float64 // 0.0 to 1.0; device-reported signal quality
	ReceivedAt time.Time
	Source     Source
}

// NormalisedReading is a SensorReading after the Normaliser has scaled
// Value to a unit interval [0, 1] using the per-type physical bounds.
// The original Value and Unit are preserved for storage and audit.
type NormalisedReading struct {
	SensorReading
	NormalisedValue float64 // [0, 1]
}

// ZoneKey identifies a zone for aggregator bucketing. Two readings with
// the same ZoneKey go into the same ZoneSnapshot bucket.
type ZoneKey struct {
	Building string
	Zone     string
}

// ZoneSnapshot is the aggregated view of one zone over one aggregation
// window. SensorValues maps sensor_type -> the latest normalised value
// in the window. Computed indicators are filled in by downstream stages.
type ZoneSnapshot struct {
	Building   string
	Zone       string
	SnapshotAt time.Time
	// SensorValues holds the most recent NormalisedValue per sensor_type
	// observed during the aggregation window.
	SensorValues map[string]float64
	// SensorRawValues holds the most recent raw Value per sensor_type
	// (post-EMA, pre-normalisation) so persistence can record physical
	// units even after the chain layer only sees [0,1] values.
	SensorRawValues map[string]float64
	// MeanQuality is the arithmetic mean of Quality across all readings
	// that contributed to this snapshot. Used by the CI computation to
	// down-weight zones with degraded signal quality.
	MeanQuality float64
	// ReadingCount is the total number of underlying readings aggregated.
	ReadingCount int
	// LatestReadingReceived is the largest ReceivedAt across the readings
	// that fed this bucket. Pipeline latency = ComputedAt - this value.
	// Zero when the bucket received nothing (impossible in practice —
	// kept as a defensive default so subtraction never overflows).
	LatestReadingReceived time.Time
}

// Key returns the ZoneKey for this snapshot.
func (z ZoneSnapshot) Key() ZoneKey {
	return ZoneKey{Building: z.Building, Zone: z.Zone}
}
