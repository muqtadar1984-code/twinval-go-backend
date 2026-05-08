package models

import (
	"time"

	"github.com/twinval/internal/compute"
)

// ComputedZoneSnapshot is what the indicator + RTPMV stages produce
// after a ZoneSnapshot has been processed through condition.Process and
// compute.ComputeAllIndicators. It carries everything needed to:
//   - persist to zone_snapshots
//   - broadcast over the existing WebSocket hub
type ComputedZoneSnapshot struct {
	Snapshot       ZoneSnapshot
	Indicators     compute.TechnicalIndicators
	HealthFactor   float64
	RTPMV          float64
	LandValue      float64
	StructureValue float64
	ComputedAt     time.Time
	DataSource     string // "live" | "simulated"
}
