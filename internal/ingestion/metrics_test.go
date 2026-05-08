package ingestion

import (
	"context"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/twinval/internal/ingestion/metrics"
	"github.com/twinval/internal/ingestion/models"
)

// counterValue reads a counter or counter-vector child's current value
// without registering the metric on a registry. Returns 0 if the metric
// has not yet emitted any samples.
func counterValue(t *testing.T, c interface {
	Write(*dto.Metric) error
}) float64 {
	t.Helper()
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		t.Fatalf("read metric: %v", err)
	}
	if m.Counter != nil {
		return m.Counter.GetValue()
	}
	if m.Gauge != nil {
		return m.Gauge.GetValue()
	}
	return 0
}

func TestMetrics_ValidatorIncrementsDroppedReason(t *testing.T) {
	metrics.ResetForTest()
	v := NewValidator(testBounds(), 0)

	r := goodReading()
	r.Quality = 0.01 // below floor
	if ok, _ := v.Validate(r); ok {
		t.Fatal("expected drop")
	}

	got := counterValue(t,
		metrics.ReadingsDropped.WithLabelValues(string(DropQualityBelowThreshold)).(interface {
			Write(*dto.Metric) error
		}),
	)
	if got != 1 {
		t.Fatalf("expected dropped counter=1, got %v", got)
	}
}

func TestMetrics_PipelineEndToEnd_BumpsCountersAndGauges(t *testing.T) {
	metrics.ResetForTest()
	sink := &recordingSink{}
	p := newTestPipeline(t, sink)
	p.Start()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = p.Stop(ctx)
	}()

	p.Submit(models.SensorReading{
		SensorID:   "T-1",
		Building:   "B",
		Zone:       "Z",
		SensorType: "temperature",
		Value:      22,
		Quality:    1.0,
		ReceivedAt: time.Now(),
		Source:     models.SourceMQTT,
	})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if sink.count() > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if sink.count() == 0 {
		t.Fatal("pipeline produced no broadcast")
	}

	// Received counter has been incremented.
	recv := counterValue(t, metrics.ReadingsReceived.WithLabelValues(
		"mqtt", "B", "Z", "temperature",
	).(interface{ Write(*dto.Metric) error }))
	if recv != 1 {
		t.Errorf("expected ReadingsReceived=1, got %v", recv)
	}

	// Snapshot counter has been incremented.
	snap := counterValue(t, metrics.ZoneSnapshotsTotal.WithLabelValues("B", "Z").(interface {
		Write(*dto.Metric) error
	}))
	if snap < 1 {
		t.Errorf("expected ZoneSnapshotsTotal>=1, got %v", snap)
	}

	// RTPMV gauge has been set (>0 because the test registry has a
	// default valuation with non-zero StructureValue and Health > 0).
	rtpmv := counterValue(t, metrics.RTPMVCurrent.WithLabelValues("B", "Z").(interface {
		Write(*dto.Metric) error
	}))
	if rtpmv <= 0 {
		t.Errorf("expected RTPMV gauge > 0, got %v", rtpmv)
	}

	hf := counterValue(t, metrics.HealthFactorCurrent.WithLabelValues("B", "Z").(interface {
		Write(*dto.Metric) error
	}))
	if hf <= 0 {
		t.Errorf("expected HealthFactor gauge > 0, got %v", hf)
	}
}

func TestMetrics_AggregatorTracksLatestReadingTime(t *testing.T) {
	out := make(chan models.ZoneSnapshot, 4)
	a := NewAggregator(time.Second, out)
	older := time.Date(2026, 5, 8, 10, 0, 0, 0, time.UTC)
	newer := time.Date(2026, 5, 8, 10, 0, 5, 0, time.UTC)

	a.Add(models.NormalisedReading{
		SensorReading: models.SensorReading{
			Building: "B", Zone: "Z", SensorType: "temperature",
			ReceivedAt: older, Quality: 1.0,
		},
		NormalisedValue: 0.5,
	})
	a.Add(models.NormalisedReading{
		SensorReading: models.SensorReading{
			Building: "B", Zone: "Z", SensorType: "temperature",
			ReceivedAt: newer, Quality: 1.0,
		},
		NormalisedValue: 0.6,
	})

	snaps := a.FlushNow()
	if len(snaps) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(snaps))
	}
	if !snaps[0].LatestReadingReceived.Equal(newer) {
		t.Fatalf("LatestReadingReceived must be the largest ReceivedAt, got %v", snaps[0].LatestReadingReceived)
	}
}
