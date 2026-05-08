package ingestion

import (
	"github.com/twinval/internal/ingestion/models"
)

// Sink is anything that can absorb a ComputedZoneSnapshot. The pipeline
// hands a slice of Sinks each computed snapshot — typical sinks are:
//
//   - WebSocket broadcaster (writes to api.Hub)
//   - DB writer (queues a WriteJob into the writer pool)
//   - Metrics updater (sets gauges per zone)
//
// Sinks must be non-blocking. A slow sink must not back-pressure the
// pipeline; implement queueing or drop-on-overflow internally.
type Sink interface {
	Emit(s models.ComputedZoneSnapshot)
}

// SinkFunc is a convenience adapter for one-off sinks.
type SinkFunc func(models.ComputedZoneSnapshot)

// Emit implements Sink.
func (f SinkFunc) Emit(s models.ComputedZoneSnapshot) { f(s) }

// HubBroadcaster writes a per-zone payload to the supplied hub. The
// "property id" routing key is "<Building>/<Zone>" — the existing
// frontend subscribes by id, so live consumers subscribe to the
// zone-composite id.
//
// The Hub pointer type is left abstract so tests can inject a fake
// without importing api/. The shape match is enforced by the BroadcastJSON
// method signature on api.Hub: BroadcastJSON(propertyID string, payload any).
type HubBroadcaster struct {
	hub HubLike
}

// HubLike is the narrow contract HubBroadcaster needs from api.Hub.
type HubLike interface {
	BroadcastJSON(propertyID string, payload interface{})
}

// NewHubBroadcaster wires a hub into a Sink.
func NewHubBroadcaster(hub HubLike) *HubBroadcaster {
	return &HubBroadcaster{hub: hub}
}

// LivePayload is the JSON shape pushed for live zone-level updates.
// Distinct from api.WSMessage because it is per-zone, not per-property,
// and excludes the simulator-specific State / Exchange fields.
type LivePayload struct {
	Building       string             `json:"building"`
	Zone           string             `json:"zone"`
	Timestamp      int64              `json:"timestamp"`
	SensorValues   map[string]float64 `json:"sensor_values"`
	Indicators     IndicatorsPayload  `json:"indicators"`
	HealthFactor   float64            `json:"health_factor"`
	RTPMV          float64            `json:"rtpmv"`
	LandValue      float64            `json:"land_value"`
	StructureValue float64            `json:"structure_value"`
	DataSource     string             `json:"data_source"`
}

// IndicatorsPayload mirrors compute.TechnicalIndicators with stable
// JSON keys so the frontend can rely on the schema without importing
// the compute package.
type IndicatorsPayload struct {
	SHF float64 `json:"shf"`
	ESF float64 `json:"esf"`
	USS float64 `json:"uss"`
	PDP float64 `json:"pdp"`
	CI  float64 `json:"ci"`
}

// Emit pushes the snapshot to every subscriber of the zone-composite id.
func (b *HubBroadcaster) Emit(s models.ComputedZoneSnapshot) {
	id := s.Snapshot.Building + "/" + s.Snapshot.Zone
	payload := LivePayload{
		Building:     s.Snapshot.Building,
		Zone:         s.Snapshot.Zone,
		Timestamp:    s.ComputedAt.UnixMilli(),
		SensorValues: s.Snapshot.SensorValues,
		Indicators: IndicatorsPayload{
			SHF: s.Indicators.SHF,
			ESF: s.Indicators.ESF,
			USS: s.Indicators.USS,
			PDP: s.Indicators.PDP,
			CI:  s.Indicators.CI,
		},
		HealthFactor:   s.HealthFactor,
		RTPMV:          s.RTPMV,
		LandValue:      s.LandValue,
		StructureValue: s.StructureValue,
		DataSource:     s.DataSource,
	}
	b.hub.BroadcastJSON(id, payload)
}
