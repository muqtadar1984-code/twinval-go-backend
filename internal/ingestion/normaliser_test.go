package ingestion

import (
	"math"
	"testing"

	"github.com/twinval/internal/ingestion/config"
	"github.com/twinval/internal/ingestion/models"
)

func TestNormaliser_InRange(t *testing.T) {
	n := NewNormaliser(testBounds())
	r := models.SensorReading{SensorType: "temperature", Value: 25}
	out, ok := n.Normalise(r)
	if !ok {
		t.Fatal("expected ok")
	}
	// (25 - (-10)) / (60 - (-10)) = 35/70 = 0.5
	if math.Abs(out.NormalisedValue-0.5) > 1e-9 {
		t.Fatalf("expected 0.5, got %v", out.NormalisedValue)
	}
}

func TestNormaliser_BelowMinClampsToZero(t *testing.T) {
	n := NewNormaliser(testBounds())
	out, _ := n.Normalise(models.SensorReading{SensorType: "humidity", Value: -50})
	if out.NormalisedValue != 0 {
		t.Fatalf("expected clamp to 0, got %v", out.NormalisedValue)
	}
}

func TestNormaliser_AboveMaxClampsToOne(t *testing.T) {
	n := NewNormaliser(testBounds())
	out, _ := n.Normalise(models.SensorReading{SensorType: "humidity", Value: 1000})
	if out.NormalisedValue != 1 {
		t.Fatalf("expected clamp to 1, got %v", out.NormalisedValue)
	}
}

func TestNormaliser_PreservesRawValue(t *testing.T) {
	n := NewNormaliser(testBounds())
	r := models.SensorReading{SensorType: "temperature", Value: 22.5, SensorID: "X"}
	out, _ := n.Normalise(r)
	if out.SensorReading.Value != 22.5 {
		t.Fatalf("raw Value must be preserved on embedded struct, got %v", out.SensorReading.Value)
	}
	if out.SensorID != "X" {
		t.Fatalf("SensorID must be preserved, got %s", out.SensorID)
	}
}

func TestNormaliser_UnknownTypeReturnsNotOK(t *testing.T) {
	n := NewNormaliser(testBounds())
	_, ok := n.Normalise(models.SensorReading{SensorType: "imaginary"})
	if ok {
		t.Fatal("expected ok=false for unknown type")
	}
}

func TestNormaliser_DegenerateRange(t *testing.T) {
	bounds := config.SensorBoundsFile{
		SensorTypes: map[string]config.SensorBound{
			"flat": {Min: 5, Max: 5},
		},
	}
	n := NewNormaliser(bounds)
	out, ok := n.Normalise(models.SensorReading{SensorType: "flat", Value: 5})
	if !ok {
		t.Fatal("expected ok=true even for degenerate range")
	}
	if !math.IsNaN(out.NormalisedValue) && out.NormalisedValue != 0 {
		t.Fatalf("degenerate range should produce 0, got %v", out.NormalisedValue)
	}
}

func TestNormaliser_BoundaryValues(t *testing.T) {
	n := NewNormaliser(testBounds())
	low, _ := n.Normalise(models.SensorReading{SensorType: "humidity", Value: 0})
	high, _ := n.Normalise(models.SensorReading{SensorType: "humidity", Value: 100})
	if low.NormalisedValue != 0 || high.NormalisedValue != 1 {
		t.Fatalf("boundaries should be exactly 0 and 1, got %v / %v", low.NormalisedValue, high.NormalisedValue)
	}
}
