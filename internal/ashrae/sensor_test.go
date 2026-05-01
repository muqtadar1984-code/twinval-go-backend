package ashrae

import (
	"testing"

	"github.com/twinval/internal/condition"
)

// =============================================================================
// Catalog
// =============================================================================

func TestBuildings_AllFiveCatalogued(t *testing.T) {
	if got := len(Buildings); got != 5 {
		t.Errorf("expected 5 buildings, got %d", got)
	}
	wantKeys := []string{
		"MY-KL-OFF-KLCC",
		"MY-SA-IDC-AXIS",
		"MY-KL-HC-ALAQAR",
		"MY-KL-RET-PAV",
		"MY-SJ-LOG-SUN",
	}
	for _, k := range wantKeys {
		if _, ok := ByKey(k); !ok {
			t.Errorf("missing building key %q", k)
		}
	}
	if _, ok := ByKey("DOES-NOT-EXIST"); ok {
		t.Errorf("ByKey returned ok=true for unknown key")
	}
}

func TestBuildings_StructureValueIsValuationMinusLand(t *testing.T) {
	for _, b := range Buildings {
		want := b.GovtValuation - b.LandValue
		if b.StructureValue != want {
			t.Errorf("%s: StructureValue=%v, want %v (govt=%v - land=%v)",
				b.Key, b.StructureValue, want, b.GovtValuation, b.LandValue)
		}
	}
}

// =============================================================================
// Engine determinism
// =============================================================================

func TestEngine_DeterministicGivenSameSeedAndOffset(t *testing.T) {
	b, _ := ByKey("MY-KL-OFF-KLCC")
	a := NewEngine(b, SITE1Weather, 0)
	c := NewEngine(b, SITE1Weather, 0)
	for i := 0; i < 24; i++ {
		ra := a.Next()
		rc := c.Next()
		// SensorReading contains a map, so == is illegal; compare scalars.
		if ra.Time != rc.Time ||
			ra.Vibration != rc.Vibration ||
			ra.Strain != rc.Strain ||
			ra.Moisture != rc.Moisture ||
			ra.Temperature != rc.Temperature ||
			ra.Occupancy != rc.Occupancy ||
			ra.ElectricalLoad != rc.ElectricalLoad ||
			ra.AirQuality != rc.AirQuality ||
			ra.Weather != rc.Weather {
			t.Errorf("hour %d: engines diverged\n  a=%+v\n  c=%+v", i, ra, rc)
			t.FailNow()
		}
	}
}

// =============================================================================
// Engine output ranges
// =============================================================================

func TestEngine_NormalisedFieldsStayInUnitInterval(t *testing.T) {
	b, _ := ByKey("MY-KL-OFF-KLCC")
	e := NewEngine(b, SITE1Weather, 0)
	for i := 0; i < 24*7; i++ { // simulate one week
		r := e.Next()
		for _, c := range []struct {
			name string
			v    float64
		}{
			{"Vibration", r.Vibration},
			{"Strain", r.Strain},
			{"Moisture", r.Moisture},
			{"Temperature(stress)", r.Temperature},
			{"Occupancy", r.Occupancy},
			{"ElectricalLoad", r.ElectricalLoad},
			{"AirQuality", r.AirQuality},
		} {
			if c.v < 0 || c.v > 1 {
				t.Errorf("hour %d %s out of [0,1]: %v", i, c.name, c.v)
			}
		}
	}
}

func TestEngine_HourCursorAdvances(t *testing.T) {
	b, _ := ByKey("MY-KL-OFF-KLCC")
	e := NewEngine(b, SITE1Weather, 0)
	r0 := e.Next()
	r1 := e.Next()
	if r1.Time.Sub(r0.Time).Hours() != 1 {
		t.Errorf("expected 1h advance per Next, got %v", r1.Time.Sub(r0.Time))
	}
}

// =============================================================================
// Diurnal pattern: occupancy at peak hour > occupancy at 03:00
// =============================================================================

func TestEngine_OccupancyPeaksAroundConfiguredHour(t *testing.T) {
	b, _ := ByKey("MY-KL-OFF-KLCC") // peak_hour = 14
	// Run a full week; collect mean occupancy by hour-of-day.
	bins := make([]float64, 24)
	counts := make([]int, 24)
	e := NewEngine(b, SITE1Weather, 0)
	for i := 0; i < 24*14; i++ {
		r := e.Next()
		bins[r.Hour] += r.Occupancy
		counts[r.Hour]++
	}
	for h := 0; h < 24; h++ {
		bins[h] /= float64(counts[h])
	}
	if bins[14] <= bins[3] {
		t.Errorf("occupancy at peak hour 14 (%.3f) should exceed 03:00 (%.3f)", bins[14], bins[3])
	}
}

// =============================================================================
// ToRawSensorBatch maps physical units correctly
// =============================================================================

func TestToRawSensorBatch_MapsPhysicalUnits(t *testing.T) {
	b, _ := ByKey("MY-KL-OFF-KLCC")
	e := NewEngine(b, SITE1Weather, 12) // start at noon — non-zero readings
	r := e.Next()
	batch := r.ToRawSensorBatch(b.Key, condition.PropertyMeta{
		ChronologicalAge:       float64(2026 - b.YearBuilt),
		MaintenanceSensitivity: 0.6,
		ConditionQuality:       0.85,
	}, 30)

	if batch.PropertyID != b.Key {
		t.Errorf("PropertyID: got %q, want %q", batch.PropertyID, b.Key)
	}
	if len(batch.Readings) != 8 {
		t.Errorf("expected 8 sensor readings (one per type), got %d", len(batch.Readings))
	}
	have := map[condition.SensorType]float64{}
	for _, rd := range batch.Readings {
		have[rd.SensorType] = rd.Value
	}
	// Vibration physical: r.Vibration * 0.5 → must be in [0, 0.5]
	if v := have[condition.SensorVibration]; v < 0 || v > 0.5 {
		t.Errorf("vibration physical out of [0, 0.5]: %v", v)
	}
	// Strain physical: r.Strain * 1000 → must be in [0, 1000]
	if v := have[condition.SensorStrain]; v < 0 || v > 1000 {
		t.Errorf("strain physical out of [0, 1000]: %v", v)
	}
	// Temperature physical = weather.AirTemperatureC — KL hot/humid range, never below 15 or above 40
	if v := have[condition.SensorTemperature]; v < 15 || v > 40 {
		t.Errorf("temperature physical out of plausible KL range: %v", v)
	}
	// Humidity physical = weather.HumidityPct — bounded [20, 100]
	if v := have[condition.SensorHumidity]; v < 20 || v > 100 {
		t.Errorf("humidity physical out of [20, 100]: %v", v)
	}
}
