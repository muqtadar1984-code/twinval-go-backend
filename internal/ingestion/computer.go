package ingestion

import (
	"time"

	"github.com/twinval/internal/compute"
	"github.com/twinval/internal/ingestion/models"
)

// PropertyValuation is the per-property baseline + age / sensitivity
// metadata the indicator math needs but a single ZoneSnapshot does not
// carry. In production these come from the portal Postgres (read-only).
// For Phase 2 we accept whatever the registry returns.
type PropertyValuation struct {
	LandValue              float64
	StructureValue         float64
	Currency               string
	ChronologicalAge       float64 // years
	MaintenanceSensitivity float64 // 0..1
}

// PropertyRegistry maps a building (and optionally zone) to its
// valuation + age metadata. Implementations can be in-memory (tests),
// portal-DB-backed (production), or static config.
type PropertyRegistry interface {
	Lookup(building, zone string) (PropertyValuation, bool)
}

// CIModifier supplies the human-observation-derived CI delta for a
// zone. nil-safe — Computer treats a nil modifier as "no delta".
//
// The delta is added to the sensor-derived CI; the result is clamped
// to [0,1] downstream.
type CIModifier interface {
	Delta(building, zone string) float64
}

// Computer turns a ZoneSnapshot into a ComputedZoneSnapshot using the
// existing internal/compute package. NEVER reimplements indicator
// math — every formula call goes to compute.* exports.
type Computer struct {
	registry PropertyRegistry
	modifier CIModifier
	cfgs     compute.AllIndicatorConfigs
	now      func() time.Time
}

// NewComputer constructs a Computer. `modifier` may be nil.
func NewComputer(registry PropertyRegistry, modifier CIModifier) *Computer {
	return &Computer{
		registry: registry,
		modifier: modifier,
		cfgs:     compute.DefaultAllConfigs(),
		now:      time.Now,
	}
}

// Compute runs the indicator chain on a single ZoneSnapshot. Returns
// (result, true) on success or (zero, false) if the registry has no
// entry for this zone — caller should drop those snapshots.
func (c *Computer) Compute(s models.ZoneSnapshot) (models.ComputedZoneSnapshot, bool) {
	val, ok := c.registry.Lookup(s.Building, s.Zone)
	if !ok {
		return models.ComputedZoneSnapshot{}, false
	}

	cd := buildConditionedData(s, val)
	indicators := compute.ComputeAllIndicators(cd, c.cfgs)

	if c.modifier != nil {
		delta := c.modifier.Delta(s.Building, s.Zone)
		ciAdjusted := indicators.CI + delta
		if ciAdjusted < 0 {
			ciAdjusted = 0
		}
		if ciAdjusted > 1 {
			ciAdjusted = 1
		}
		indicators.CI = ciAdjusted
	}

	hf := compute.ComputeHealthFactor(indicators)
	rtpmv := compute.ComputeRTPMV(compute.BaselineMarketValue{
		LandValue:      val.LandValue,
		StructureValue: val.StructureValue,
		Currency:       val.Currency,
	}, hf)

	return models.ComputedZoneSnapshot{
		Snapshot:       s,
		Indicators:     indicators,
		HealthFactor:   float64(hf),
		RTPMV:          rtpmv,
		LandValue:      val.LandValue,
		StructureValue: val.StructureValue,
		ComputedAt:     c.now(),
		DataSource:     "live",
	}, true
}

// buildConditionedData maps ZoneSnapshot sensor values + property
// metadata into a compute.ConditionedData. The mapping rules:
//
//   - Structural / Environmental sensors use RAW values (physical
//     units) — those compute formulas expect physical units, not
//     [0,1] fractions.
//   - Usage sensors (occupancy / electrical / water) use NORMALISED
//     values — those formulas expect 0..1 fractions.
//   - vibration (mm/s²) is divided by 1000 to convert to m/s² which
//     is what compute.ComputeSHF expects.
//
// Sensor-metadata fields default to "clean" values when no signal is
// available — Phase 2 wires MeanQuality into UptimeContinuity as a
// pragmatic proxy. Phase 4 (integrity report) will replace this with
// real cross-sensor / tamper / calibration tracking.
func buildConditionedData(s models.ZoneSnapshot, val PropertyValuation) compute.ConditionedData {
	raw := s.SensorRawValues
	norm := s.SensorValues

	cd := compute.ConditionedData{
		// Structural — raw physical values
		VibrationMagnitude: raw["vibration"] / 1000.0, // mm/s² → m/s²
		StrainMagnitude:    raw["strain"],

		// Environmental — raw physical values
		Temperature:  raw["temperature"],
		Humidity:     raw["humidity"],
		AirQualityPM: raw["air_quality_pm25"],

		// Usage — normalised 0..1
		OccupancyRatio:   norm["occupancy"],
		ElectricalLoad:   norm["electrical_load"],
		WaterConsumption: norm["water_consumption"],

		// Sensor metadata — pragmatic defaults until Phase 4
		UptimeContinuity:       s.MeanQuality,
		CrossSensorConsistency: 1.0,
		CalibrationRecency:     0,
		TamperScore:            1.0,

		// Property metadata
		ChronologicalAge:       val.ChronologicalAge,
		MaintenanceSensitivity: val.MaintenanceSensitivity,
		ConditionQuality:       s.MeanQuality,
	}
	return cd
}

// StaticRegistry is a trivial in-memory PropertyRegistry. Use for tests
// and as a placeholder until the portal-DB-backed registry is wired in
// Phase 4.
type StaticRegistry struct {
	entries map[models.ZoneKey]PropertyValuation
	defAult PropertyValuation
	hasDef  bool
}

// NewStaticRegistry returns an empty registry. Set entries via Put.
func NewStaticRegistry() *StaticRegistry {
	return &StaticRegistry{entries: make(map[models.ZoneKey]PropertyValuation)}
}

// Put registers a valuation for a building+zone.
func (r *StaticRegistry) Put(building, zone string, v PropertyValuation) {
	r.entries[models.ZoneKey{Building: building, Zone: zone}] = v
}

// SetDefault sets a fallback valuation returned when the exact zone is
// not registered. Without a default, Lookup returns ok=false.
func (r *StaticRegistry) SetDefault(v PropertyValuation) {
	r.defAult = v
	r.hasDef = true
}

// Lookup implements PropertyRegistry.
func (r *StaticRegistry) Lookup(building, zone string) (PropertyValuation, bool) {
	if v, ok := r.entries[models.ZoneKey{Building: building, Zone: zone}]; ok {
		return v, true
	}
	if r.hasDef {
		return r.defAult, true
	}
	return PropertyValuation{}, false
}
