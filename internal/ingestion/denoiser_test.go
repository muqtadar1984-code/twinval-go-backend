package ingestion

import (
	"math"
	"sync"
	"testing"

	"github.com/twinval/internal/ingestion/models"
)

func TestDenoiser_FirstReadingPassesThrough(t *testing.T) {
	d := NewDenoiser(0.3)
	r := models.SensorReading{SensorID: "X", Value: 22.0}
	d.Apply(&r)
	if r.Value != 22.0 {
		t.Fatalf("first reading must pass through, got %v", r.Value)
	}
	if state := d.Snapshot(); state["X"] != 22.0 {
		t.Fatalf("state should seed at first value, got %v", state["X"])
	}
}

func TestDenoiser_KnownEMA(t *testing.T) {
	d := NewDenoiser(0.5)
	first := models.SensorReading{SensorID: "X", Value: 10}
	d.Apply(&first)
	second := models.SensorReading{SensorID: "X", Value: 20}
	d.Apply(&second)
	// EMA = 0.5*20 + 0.5*10 = 15
	if math.Abs(second.Value-15.0) > 1e-9 {
		t.Fatalf("expected EMA=15, got %v", second.Value)
	}
}

func TestDenoiser_ConstantSeriesUnchanged(t *testing.T) {
	d := NewDenoiser(0.3)
	for i := 0; i < 50; i++ {
		r := models.SensorReading{SensorID: "X", Value: 100.0}
		d.Apply(&r)
		if math.Abs(r.Value-100.0) > 1e-9 {
			t.Fatalf("constant series must stay constant, got %v at step %d", r.Value, i)
		}
	}
}

func TestDenoiser_ConvergesToTarget(t *testing.T) {
	d := NewDenoiser(0.3)
	first := models.SensorReading{SensorID: "X", Value: 0}
	d.Apply(&first)
	for i := 0; i < 200; i++ {
		r := models.SensorReading{SensorID: "X", Value: 50.0}
		d.Apply(&r)
	}
	// After 200 iterations the EMA should be very close to 50.
	state := d.Snapshot()
	if math.Abs(state["X"]-50.0) > 1e-3 {
		t.Fatalf("expected EMA to converge near 50, got %v", state["X"])
	}
}

func TestDenoiser_PerSensorIsolation(t *testing.T) {
	d := NewDenoiser(0.5)
	a := models.SensorReading{SensorID: "A", Value: 10}
	b := models.SensorReading{SensorID: "B", Value: 100}
	d.Apply(&a)
	d.Apply(&b)
	state := d.Snapshot()
	if state["A"] != 10 || state["B"] != 100 {
		t.Fatalf("per-sensor state leaked: %+v", state)
	}
}

func TestDenoiser_ConcurrentSafety(t *testing.T) {
	d := NewDenoiser(0.3)
	const writers = 32
	const perWriter = 500
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		id := i
		go func() {
			defer wg.Done()
			for j := 0; j < perWriter; j++ {
				r := models.SensorReading{
					SensorID: "S",
					Value:    float64(id*1000 + j),
				}
				d.Apply(&r)
			}
		}()
	}
	wg.Wait()
	// If we got here without a race detector failure (-race flag) and
	// no panics, the mutex protection works. Final state value is
	// non-deterministic but must exist.
	if state := d.Snapshot(); len(state) != 1 {
		t.Fatalf("expected one sensor in state, got %d", len(state))
	}
}

func TestDenoiser_ZeroAlphaFallsBackToDefault(t *testing.T) {
	d := NewDenoiser(0)
	if d.alpha != 0.3 {
		t.Fatalf("expected fallback to 0.3, got %v", d.alpha)
	}
}

func TestDenoiser_Reset(t *testing.T) {
	d := NewDenoiser(0.3)
	r := models.SensorReading{SensorID: "X", Value: 22}
	d.Apply(&r)
	d.Reset()
	if state := d.Snapshot(); len(state) != 0 {
		t.Fatalf("Reset should clear state, got %d entries", len(state))
	}
}
