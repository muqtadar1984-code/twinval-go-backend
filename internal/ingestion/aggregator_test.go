package ingestion

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/twinval/internal/ingestion/models"
)

func mkNorm(building, zone, sensorType, sensorID string, value, norm, quality float64) models.NormalisedReading {
	return models.NormalisedReading{
		SensorReading: models.SensorReading{
			SensorID:   sensorID,
			Building:   building,
			Zone:       zone,
			SensorType: sensorType,
			Value:      value,
			Quality:    quality,
			ReceivedAt: time.Now(),
			Source:     models.SourceMQTT,
		},
		NormalisedValue: norm,
	}
}

func TestAggregator_FlushNow_OneZone(t *testing.T) {
	out := make(chan models.ZoneSnapshot, 4)
	a := NewAggregator(time.Second, out)

	a.Add(mkNorm("B", "Z", "temperature", "T-1", 22, 0.5, 1.0))
	a.Add(mkNorm("B", "Z", "humidity", "H-1", 50, 0.5, 0.9))

	snaps := a.FlushNow()
	if len(snaps) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(snaps))
	}
	s := snaps[0]
	if s.Building != "B" || s.Zone != "Z" {
		t.Fatalf("wrong key: %+v", s.Key())
	}
	if s.SensorValues["temperature"] != 0.5 || s.SensorValues["humidity"] != 0.5 {
		t.Fatalf("missing sensor values: %+v", s.SensorValues)
	}
	if s.SensorRawValues["temperature"] != 22 {
		t.Fatalf("expected raw temp=22, got %v", s.SensorRawValues["temperature"])
	}
	if s.ReadingCount != 2 {
		t.Fatalf("expected 2 readings, got %d", s.ReadingCount)
	}
	// MeanQuality = (1.0 + 0.9) / 2 = 0.95
	if s.MeanQuality < 0.94 || s.MeanQuality > 0.96 {
		t.Fatalf("expected mean quality ~0.95, got %v", s.MeanQuality)
	}
}

func TestAggregator_FlushNow_LatestPerType(t *testing.T) {
	out := make(chan models.ZoneSnapshot, 4)
	a := NewAggregator(time.Second, out)
	// Two temperature readings; the second (later) should win.
	a.Add(mkNorm("B", "Z", "temperature", "T-1", 18, 0.4, 1.0))
	a.Add(mkNorm("B", "Z", "temperature", "T-1", 24, 0.6, 1.0))
	snaps := a.FlushNow()
	if len(snaps) != 1 || snaps[0].SensorValues["temperature"] != 0.6 {
		t.Fatalf("expected latest=0.6, got %+v", snaps[0].SensorValues)
	}
	if snaps[0].ReadingCount != 2 {
		t.Fatalf("count should still be 2, got %d", snaps[0].ReadingCount)
	}
}

func TestAggregator_FlushNow_MultipleZones(t *testing.T) {
	out := make(chan models.ZoneSnapshot, 4)
	a := NewAggregator(time.Second, out)
	a.Add(mkNorm("B", "Z1", "temperature", "T-1", 22, 0.5, 1.0))
	a.Add(mkNorm("B", "Z2", "temperature", "T-2", 30, 0.7, 1.0))
	snaps := a.FlushNow()
	if len(snaps) != 2 {
		t.Fatalf("expected 2 snapshots, got %d", len(snaps))
	}
	// Buckets are cleared by FlushNow.
	if more := a.FlushNow(); len(more) != 0 {
		t.Fatalf("FlushNow should have cleared buckets, found %d", len(more))
	}
}

func TestAggregator_RunFlushesAfterWindow(t *testing.T) {
	out := make(chan models.ZoneSnapshot, 4)
	a := NewAggregator(200*time.Millisecond, out)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go a.Run(ctx)

	a.Add(mkNorm("B", "Z", "temperature", "T-1", 22, 0.5, 1.0))

	select {
	case s := <-out:
		if s.Building != "B" || s.Zone != "Z" {
			t.Fatalf("wrong snapshot: %+v", s)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for ZoneSnapshot from Run()")
	}
}

func TestAggregator_PartialZoneDataIsValid(t *testing.T) {
	out := make(chan models.ZoneSnapshot, 4)
	a := NewAggregator(time.Second, out)
	// Only temperature, no humidity. Should still produce a valid snapshot
	// with whatever was observed.
	a.Add(mkNorm("B", "Z", "temperature", "T-1", 22, 0.5, 1.0))
	snaps := a.FlushNow()
	if len(snaps) != 1 {
		t.Fatalf("expected 1, got %d", len(snaps))
	}
	if _, ok := snaps[0].SensorValues["temperature"]; !ok {
		t.Fatal("temperature should be present")
	}
	if _, ok := snaps[0].SensorValues["humidity"]; ok {
		t.Fatal("humidity should NOT be present (partial data)")
	}
}

func TestAggregator_ConcurrentAdd(t *testing.T) {
	out := make(chan models.ZoneSnapshot, 256)
	a := NewAggregator(time.Second, out)

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				a.Add(mkNorm("B", "Z", "temperature", "T-X", float64(i*1000+j), 0.5, 1.0))
			}
		}(i)
	}
	wg.Wait()

	snaps := a.FlushNow()
	if len(snaps) != 1 || snaps[0].ReadingCount != 1600 {
		t.Fatalf("expected one snapshot of 1600 readings, got %+v", snaps)
	}
}
