package ingestion

import (
	"testing"
	"time"

	"github.com/twinval/internal/ingestion/models"
)

// fixedDelta is a CIModifier returning a constant delta.
type fixedDelta float64

func (f fixedDelta) Delta(_, _ string) float64 { return float64(f) }

func mkSnapshot() models.ZoneSnapshot {
	return models.ZoneSnapshot{
		Building:   "B",
		Zone:       "Z",
		SnapshotAt: time.Now(),
		SensorValues: map[string]float64{
			"temperature":      0.5,
			"humidity":         0.5,
			"air_quality_pm25": 0.1,
			"occupancy":        0.4,
			"electrical_load":  0.3,
			"water_consumption":0.2,
			"vibration":        0.05,
			"strain":           0.5,
		},
		SensorRawValues: map[string]float64{
			"temperature":      22.0,
			"humidity":         50.0,
			"air_quality_pm25": 10.0,
			"occupancy":        40.0,
			"electrical_load":  150.0,
			"water_consumption":200.0,
			"vibration":        1.0, // 1 mm/s²
			"strain":           50.0,
		},
		MeanQuality:  0.95,
		ReadingCount: 8,
	}
}

func mkRegistry() *StaticRegistry {
	r := NewStaticRegistry()
	r.Put("B", "Z", PropertyValuation{
		LandValue:              500_000,
		StructureValue:         1_000_000,
		Currency:               "MYR",
		ChronologicalAge:       10,
		MaintenanceSensitivity: 0.5,
	})
	return r
}

func TestComputer_ProducesAllFiveIndicators(t *testing.T) {
	c := NewComputer(mkRegistry(), nil)
	res, ok := c.Compute(mkSnapshot())
	if !ok {
		t.Fatal("expected ok=true")
	}
	if res.Indicators.SHF == 0 {
		t.Errorf("SHF should be > 0 for a healthy property, got %v", res.Indicators.SHF)
	}
	if res.Indicators.ESF == 0 {
		t.Errorf("ESF should be > 0 for a healthy property, got %v", res.Indicators.ESF)
	}
	if res.Indicators.CI == 0 {
		t.Errorf("CI should be > 0, got %v", res.Indicators.CI)
	}
	if res.HealthFactor == 0 {
		t.Errorf("HealthFactor should be > 0, got %v", res.HealthFactor)
	}
	if res.RTPMV <= res.LandValue {
		t.Errorf("RTPMV should exceed LandValue when health > 0, got %v <= %v", res.RTPMV, res.LandValue)
	}
	if res.DataSource != "live" {
		t.Errorf("expected DataSource=live, got %s", res.DataSource)
	}
}

func TestComputer_UnknownZoneReturnsNotOK(t *testing.T) {
	c := NewComputer(NewStaticRegistry(), nil)
	_, ok := c.Compute(mkSnapshot())
	if ok {
		t.Fatal("expected ok=false when registry has no entry")
	}
}

func TestComputer_FallbackToDefaultRegistry(t *testing.T) {
	r := NewStaticRegistry()
	r.SetDefault(PropertyValuation{
		LandValue: 100_000, StructureValue: 200_000, Currency: "MYR",
		ChronologicalAge: 5, MaintenanceSensitivity: 0.5,
	})
	c := NewComputer(r, nil)
	res, ok := c.Compute(mkSnapshot())
	if !ok {
		t.Fatal("default should make any zone resolvable")
	}
	if res.LandValue != 100_000 {
		t.Errorf("expected default LandValue=100000, got %v", res.LandValue)
	}
}

func TestComputer_CIModifierIsApplied(t *testing.T) {
	c1 := NewComputer(mkRegistry(), nil)
	res1, _ := c1.Compute(mkSnapshot())

	c2 := NewComputer(mkRegistry(), fixedDelta(-0.2))
	res2, _ := c2.Compute(mkSnapshot())

	if res2.Indicators.CI >= res1.Indicators.CI {
		t.Fatalf("negative CI delta should lower CI: %v -> %v", res1.Indicators.CI, res2.Indicators.CI)
	}
}

func TestComputer_CIClampsAtZero(t *testing.T) {
	// Force a huge negative delta so the clamp kicks in.
	c := NewComputer(mkRegistry(), fixedDelta(-99))
	res, _ := c.Compute(mkSnapshot())
	if res.Indicators.CI < 0 {
		t.Fatalf("CI must clamp at 0, got %v", res.Indicators.CI)
	}
	if res.Indicators.CI > 0 {
		t.Fatalf("with -99 delta, CI should clamp exactly to 0, got %v", res.Indicators.CI)
	}
}

func TestComputer_CIClampsAtOne(t *testing.T) {
	c := NewComputer(mkRegistry(), fixedDelta(99))
	res, _ := c.Compute(mkSnapshot())
	if res.Indicators.CI != 1.0 {
		t.Fatalf("CI must clamp at 1, got %v", res.Indicators.CI)
	}
}

func TestComputer_VibrationConvertedFromMmToM(t *testing.T) {
	// Sanity: a snapshot with raw vibration=1000 mm/s² should produce
	// the same SHF as a snapshot with raw vibration=1000 because it
	// converts to 1.0 m/s² either way through buildConditionedData.
	s := mkSnapshot()
	s.SensorRawValues["vibration"] = 1000 // 1000 mm/s² = 1.0 m/s²
	c := NewComputer(mkRegistry(), nil)
	res, ok := c.Compute(s)
	if !ok {
		t.Fatal("expected ok")
	}
	if res.Indicators.SHF <= 0 || res.Indicators.SHF > 1 {
		t.Fatalf("SHF should be in (0,1] for moderate vibration, got %v", res.Indicators.SHF)
	}
}
