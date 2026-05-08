package ingestion

import (
	"sync"
	"testing"
	"time"

	"github.com/twinval/internal/compute"
	"github.com/twinval/internal/ingestion/models"
)

type fakeHub struct {
	mu     sync.Mutex
	calls  []hubCall
}

type hubCall struct {
	propertyID string
	payload    interface{}
}

func (f *fakeHub) BroadcastJSON(propertyID string, payload interface{}) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, hubCall{propertyID, payload})
}

func TestHubBroadcaster_RoutesZoneCompositeID(t *testing.T) {
	h := &fakeHub{}
	b := NewHubBroadcaster(h)
	b.Emit(models.ComputedZoneSnapshot{
		Snapshot: models.ZoneSnapshot{
			Building:     "Block A",
			Zone:         "Roof",
			SensorValues: map[string]float64{"temperature": 0.5},
		},
		Indicators:   compute.TechnicalIndicators{SHF: 0.9, ESF: 0.9, USS: 0.1, PDP: 0.95, CI: 0.99},
		HealthFactor: 0.7,
		RTPMV:        1_700_000,
		ComputedAt:   time.Now(),
		DataSource:   "live",
	})
	if len(h.calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(h.calls))
	}
	if h.calls[0].propertyID != "Block A/Roof" {
		t.Errorf("unexpected routing key: %q", h.calls[0].propertyID)
	}
	p, ok := h.calls[0].payload.(LivePayload)
	if !ok {
		t.Fatalf("payload should be LivePayload, got %T", h.calls[0].payload)
	}
	if p.Building != "Block A" || p.Zone != "Roof" {
		t.Errorf("payload zone fields wrong: %+v", p)
	}
	if p.Indicators.SHF != 0.9 {
		t.Errorf("SHF should pass through, got %v", p.Indicators.SHF)
	}
	if p.RTPMV != 1_700_000 {
		t.Errorf("RTPMV should pass through, got %v", p.RTPMV)
	}
	if p.DataSource != "live" {
		t.Errorf("DataSource should pass through, got %s", p.DataSource)
	}
}

func TestSinkFunc_AdaptsClosure(t *testing.T) {
	count := 0
	var s Sink = SinkFunc(func(_ models.ComputedZoneSnapshot) { count++ })
	s.Emit(models.ComputedZoneSnapshot{})
	s.Emit(models.ComputedZoneSnapshot{})
	if count != 2 {
		t.Fatalf("expected 2 emissions, got %d", count)
	}
}
