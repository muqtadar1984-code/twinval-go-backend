package ingestion

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twinval/internal/ingestion/config"
	"github.com/twinval/internal/ingestion/models"
)

// recordingSink captures every snapshot emitted to it for assertions.
type recordingSink struct {
	mu    sync.Mutex
	items []models.ComputedZoneSnapshot
}

func (r *recordingSink) Emit(s models.ComputedZoneSnapshot) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.items = append(r.items, s)
}

func (r *recordingSink) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.items)
}

func newTestPipeline(t *testing.T, sink Sink) *Pipeline {
	t.Helper()
	bounds := config.SensorBoundsFile{
		SensorTypes: map[string]config.SensorBound{
			"temperature":      {Min: -10, Max: 60},
			"humidity":         {Min: 0, Max: 100},
			"vibration":        {Min: 0, Max: 50},
			"strain":           {Min: -500, Max: 500},
			"air_quality_pm25": {Min: 0, Max: 500},
			"occupancy":        {Min: 0, Max: 100},
			"electrical_load":  {Min: 0, Max: 500},
			"water_consumption":{Min: 0, Max: 1000},
		},
	}
	reg := NewStaticRegistry()
	reg.SetDefault(PropertyValuation{
		LandValue: 500_000, StructureValue: 1_000_000, Currency: "MYR",
		ChronologicalAge: 5, MaintenanceSensitivity: 0.5,
	})
	return NewPipeline(PipelineOptions{
		Validator:  NewValidator(bounds, 0),
		Denoiser:   NewDenoiser(0.3),
		Normaliser: NewNormaliser(bounds),
		Aggregator: NewAggregator(150*time.Millisecond, nil), // out is rebound by NewPipeline
		Computer:   NewComputer(reg, nil),
		Sinks:      []Sink{sink},
	})
}

func TestPipeline_EndToEnd_OneZone(t *testing.T) {
	sink := &recordingSink{}
	p := newTestPipeline(t, sink)
	p.Start()

	r := models.SensorReading{
		SensorID:   "T-1",
		Building:   "B",
		Zone:       "Z",
		SensorType: "temperature",
		Unit:       "°C",
		Value:      22.0,
		Quality:    1.0,
		ReceivedAt: time.Now(),
		Source:     models.SourceMQTT,
	}
	p.Submit(r)

	// Wait long enough for the aggregator window + compute stage to run.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if sink.count() > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	stop, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := p.Stop(stop); err != nil {
		t.Fatalf("Stop returned %v", err)
	}

	if sink.count() == 0 {
		t.Fatalf("expected at least 1 broadcast, got 0; stats=%+v", p.Snapshot())
	}
	stats := p.Snapshot()
	if stats.Submitted != 1 {
		t.Errorf("expected Submitted=1, got %d", stats.Submitted)
	}
	if stats.ValidatedPassed != 1 {
		t.Errorf("expected ValidatedPassed=1, got %d", stats.ValidatedPassed)
	}
	if stats.SnapshotsEmitted == 0 {
		t.Errorf("expected at least 1 snapshot, got 0")
	}
	if stats.BroadcastSent == 0 {
		t.Errorf("expected at least 1 broadcast, got 0")
	}
}

func TestPipeline_DropsBadReadings(t *testing.T) {
	sink := &recordingSink{}
	p := newTestPipeline(t, sink)
	p.Start()

	// One good, one bad (out of range), one bad (low quality).
	good := models.SensorReading{
		SensorID: "T-1", Building: "B", Zone: "Z",
		SensorType: "temperature", Value: 22, Quality: 1.0,
		ReceivedAt: time.Now(), Source: models.SourceMQTT,
	}
	oob := good
	oob.Value = 9999
	lowQ := good
	lowQ.Quality = 0.01

	p.Submit(good)
	p.Submit(oob)
	p.Submit(lowQ)

	time.Sleep(400 * time.Millisecond)

	stop, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = p.Stop(stop)

	stats := p.Snapshot()
	if stats.Submitted != 3 {
		t.Errorf("expected Submitted=3, got %d", stats.Submitted)
	}
	if stats.ValidatedPassed != 1 {
		t.Errorf("expected ValidatedPassed=1, got %d", stats.ValidatedPassed)
	}
}

func TestPipeline_SubmitAfterStopIsDropped(t *testing.T) {
	sink := &recordingSink{}
	p := newTestPipeline(t, sink)
	p.Start()

	stop, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = p.Stop(stop)

	p.Submit(models.SensorReading{
		SensorID: "T-1", Building: "B", Zone: "Z",
		SensorType: "temperature", Value: 22, Quality: 1.0,
		Source: models.SourceMQTT, ReceivedAt: time.Now(),
	})
	if p.Snapshot().SubmitDropped == 0 {
		t.Errorf("expected SubmitDropped >= 1 after Stop, got %d", p.Snapshot().SubmitDropped)
	}
}

func TestPipeline_MultipleZonesAreIndependent(t *testing.T) {
	sink := &recordingSink{}
	p := newTestPipeline(t, sink)
	p.Start()

	for _, z := range []string{"Z1", "Z2", "Z3"} {
		p.Submit(models.SensorReading{
			SensorID: "T-" + z, Building: "B", Zone: z,
			SensorType: "temperature", Value: 22, Quality: 1.0,
			Source: models.SourceMQTT, ReceivedAt: time.Now(),
		})
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if sink.count() >= 3 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	stop, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = p.Stop(stop)

	if sink.count() < 3 {
		t.Fatalf("expected one snapshot per zone, got %d", sink.count())
	}
}

func TestPipeline_HighThroughputSurvivesShutdown(t *testing.T) {
	sink := &recordingSink{}
	p := newTestPipeline(t, sink)
	p.Start()

	const N = 500
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < N; j++ {
				p.Submit(models.SensorReading{
					SensorID: "T-1", Building: "B", Zone: "Z",
					SensorType: "temperature", Value: 22, Quality: 1.0,
					Source: models.SourceMQTT, ReceivedAt: time.Now(),
				})
			}
		}()
	}
	wg.Wait()

	stop, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := p.Stop(stop); err != nil {
		t.Fatalf("Stop returned %v after burst", err)
	}

	stats := p.Snapshot()
	if got := stats.Submitted + stats.SubmitDropped; got != 4*N {
		t.Errorf("expected accounting to balance: Submitted+Dropped=%d, expected %d", got, 4*N)
	}
	// Compile-time reference to keep minDuration linked even when no test
	// uses it directly in a build mode.
	_ = atomic.LoadUint64
	_ = minDuration(time.Second, 2*time.Second)
}
