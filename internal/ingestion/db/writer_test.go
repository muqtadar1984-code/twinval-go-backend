package db

import (
	"context"
	"testing"
	"time"

	"github.com/twinval/internal/ingestion/models"
)

// These tests cover the queue / drop / drain semantics of WriterPool
// without requiring a real Postgres. The DB-touching paths (writeRaw /
// writeSnapshot) are exercised in the integration tests under
// internal/ingestion/db/integration_test.go (Phase 6).

func TestWriterPool_SubmitAfterStopIsDropped(t *testing.T) {
	wp := &WriterPool{jobs: make(chan WriteJob, 8)}
	// Don't start workers — we only test the drop path.
	wp.closed.Store(true)
	wp.Submit(WriteJob{RawReading: &models.SensorReading{SensorID: "X"}})
	if wp.Stats().Dropped != 1 {
		t.Fatalf("expected dropped=1 after submit-when-closed, got %d", wp.Stats().Dropped)
	}
}

func TestWriterPool_FullBufferDrops(t *testing.T) {
	wp := &WriterPool{jobs: make(chan WriteJob, 1)}
	// Buffer size 1, no workers consuming. First Submit fills, second drops.
	wp.Submit(WriteJob{RawReading: &models.SensorReading{SensorID: "A"}})
	wp.Submit(WriteJob{RawReading: &models.SensorReading{SensorID: "B"}})
	if wp.Stats().Dropped != 1 {
		t.Fatalf("expected dropped=1 after buffer overflow, got %d", wp.Stats().Dropped)
	}
}

func TestWriterPool_StopDrainsCleanly(t *testing.T) {
	wp := &WriterPool{jobs: make(chan WriteJob, 4)}
	// Spin up a no-op consumer goroutine so Stop() can drain.
	wp.wg.Add(1)
	go func() {
		defer wp.wg.Done()
		for range wp.jobs {
			// drop on the floor
		}
	}()
	wp.Submit(WriteJob{RawReading: &models.SensorReading{SensorID: "X"}})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := wp.Stop(ctx); err != nil {
		t.Fatalf("Stop should drain cleanly, got %v", err)
	}
}
