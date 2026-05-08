package ingestion

import (
	"math"
	"testing"
	"time"

	"github.com/twinval/internal/ingestion/config"
	"github.com/twinval/internal/ingestion/models"
)

func testBounds() config.SensorBoundsFile {
	return config.SensorBoundsFile{
		SensorTypes: map[string]config.SensorBound{
			"temperature": {Unit: "°C", Min: -10, Max: 60},
			"humidity":    {Unit: "%RH", Min: 0, Max: 100},
		},
	}
}

func goodReading() models.SensorReading {
	return models.SensorReading{
		SensorID:   "X",
		Building:   "B",
		Zone:       "Z",
		SensorType: "temperature",
		Unit:       "°C",
		Value:      22.0,
		Quality:    1.0,
		ReceivedAt: time.Now(),
		Source:     models.SourceMQTT,
	}
}

func TestValidator_PassesGoodReading(t *testing.T) {
	v := NewValidator(testBounds(), 0)
	ok, reason := v.Validate(goodReading())
	if !ok || reason != "" {
		t.Fatalf("expected pass, got ok=%v reason=%s", ok, reason)
	}
	if v.Stats().Passed != 1 {
		t.Fatalf("expected Passed=1, got %d", v.Stats().Passed)
	}
}

func TestValidator_DropsLowQuality(t *testing.T) {
	v := NewValidator(testBounds(), 0)
	r := goodReading()
	r.Quality = 0.05
	ok, reason := v.Validate(r)
	if ok || reason != DropQualityBelowThreshold {
		t.Fatalf("expected quality drop, got %v / %s", ok, reason)
	}
}

func TestValidator_DropsOutOfRange(t *testing.T) {
	v := NewValidator(testBounds(), 0)
	r := goodReading()
	r.Value = 999.0
	ok, reason := v.Validate(r)
	if ok || reason != DropOutOfRange {
		t.Fatalf("expected out-of-range, got %v / %s", ok, reason)
	}
}

func TestValidator_DropsMalformedNaN(t *testing.T) {
	v := NewValidator(testBounds(), 0)
	r := goodReading()
	r.Value = math.NaN()
	ok, reason := v.Validate(r)
	if ok || reason != DropMalformed {
		t.Fatalf("expected malformed, got %v / %s", ok, reason)
	}
}

func TestValidator_DropsMalformedInf(t *testing.T) {
	v := NewValidator(testBounds(), 0)
	r := goodReading()
	r.Value = math.Inf(1)
	ok, _ := v.Validate(r)
	if ok {
		t.Fatal("expected drop for +Inf")
	}
}

func TestValidator_DropsMissingMandatoryFields(t *testing.T) {
	v := NewValidator(testBounds(), 0)
	cases := []struct {
		name string
		mut  func(*models.SensorReading)
	}{
		{"missing sensor_id", func(r *models.SensorReading) { r.SensorID = "" }},
		{"missing building", func(r *models.SensorReading) { r.Building = "" }},
		{"missing zone", func(r *models.SensorReading) { r.Zone = "" }},
		{"missing sensor_type", func(r *models.SensorReading) { r.SensorType = "" }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := goodReading()
			c.mut(&r)
			ok, reason := v.Validate(r)
			if ok || reason != DropMalformed {
				t.Fatalf("%s: expected malformed, got %v / %s", c.name, ok, reason)
			}
		})
	}
}

func TestValidator_DropsUnknownSensorType(t *testing.T) {
	v := NewValidator(testBounds(), 0)
	r := goodReading()
	r.SensorType = "made_up_sensor"
	ok, reason := v.Validate(r)
	if ok || reason != DropUnknownSensorType {
		t.Fatalf("expected unknown_sensor_type, got %v / %s", ok, reason)
	}
}

func TestValidator_AcceptsBoundaryValues(t *testing.T) {
	v := NewValidator(testBounds(), 0)
	for _, val := range []float64{-10.0, 60.0} {
		r := goodReading()
		r.Value = val
		ok, _ := v.Validate(r)
		if !ok {
			t.Fatalf("boundary value %v should pass", val)
		}
	}
}
