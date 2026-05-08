package db

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/twinval/internal/ingestion/metrics"
	"github.com/twinval/internal/ingestion/models"
)

const writeTimeout = 10 * time.Second

// WriteJob is one unit of persistence work. Exactly one of RawReading or
// ComputedSnapshot is non-nil. The writer pool dispatches to the right
// table based on which field is set.
type WriteJob struct {
	RawReading       *models.SensorReading
	ComputedSnapshot *models.ComputedZoneSnapshot
}

// WriterPool is an async DB write worker pool. The pipeline submits
// WriteJobs to Submit(); a fixed-size goroutine pool drains them to
// Postgres. The pipeline never blocks on disk I/O — slow / failing
// writes count toward DBWriteErrors but do not back-pressure the
// upstream stages.
type WriterPool struct {
	pool        *pgxpool.Pool
	jobs        chan WriteJob
	wg          sync.WaitGroup
	stopOnce    sync.Once
	closed      atomic.Bool
	dropped     atomic.Uint64
	writeErrors atomic.Uint64
}

// NewWriterPool starts `workers` goroutines and returns a pool ready to
// accept jobs. Buffer size determines how many jobs can queue before
// Submit() either blocks (block=true) or drops (block=false).
func NewWriterPool(pool *pgxpool.Pool, workers int, bufferSize int) *WriterPool {
	if workers <= 0 {
		workers = 4
	}
	if bufferSize <= 0 {
		bufferSize = 1024
	}
	wp := &WriterPool{
		pool: pool,
		jobs: make(chan WriteJob, bufferSize),
	}
	for i := 0; i < workers; i++ {
		wp.wg.Add(1)
		go wp.worker(i)
	}
	return wp
}

// Submit enqueues a write job. If the pool has been stopped or the
// internal buffer is full, the job is dropped and the dropped counter
// increments. The pipeline must not block on disk; that is the contract.
func (w *WriterPool) Submit(job WriteJob) {
	if w.closed.Load() {
		w.dropped.Add(1)
		return
	}
	select {
	case w.jobs <- job:
	default:
		w.dropped.Add(1)
	}
}

// Stop closes the job channel and waits (with the supplied context) for
// all in-flight jobs to drain. Safe to call multiple times.
func (w *WriterPool) Stop(ctx context.Context) error {
	w.stopOnce.Do(func() {
		w.closed.Store(true)
		close(w.jobs)
	})
	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return errors.New("writer pool drain timed out")
	}
}

// Stats returns a snapshot of pool counters.
type Stats struct {
	Dropped     uint64
	WriteErrors uint64
}

// Stats returns current counters.
func (w *WriterPool) Stats() Stats {
	return Stats{
		Dropped:     w.dropped.Load(),
		WriteErrors: w.writeErrors.Load(),
	}
}

func (w *WriterPool) worker(id int) {
	defer w.wg.Done()
	for job := range w.jobs {
		ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
		switch {
		case job.RawReading != nil:
			if err := w.writeRaw(ctx, job.RawReading); err != nil {
				w.writeErrors.Add(1)
				metrics.DBWriteErrors.Inc()
				slog.Warn("ingestion db: raw write failed",
					"worker", id, "error", err.Error(),
					"sensor_id", job.RawReading.SensorID)
			}
		case job.ComputedSnapshot != nil:
			if err := w.writeSnapshot(ctx, job.ComputedSnapshot); err != nil {
				w.writeErrors.Add(1)
				metrics.DBWriteErrors.Inc()
				slog.Warn("ingestion db: snapshot write failed",
					"worker", id, "error", err.Error(),
					"building", job.ComputedSnapshot.Snapshot.Building,
					"zone", job.ComputedSnapshot.Snapshot.Zone)
			}
		}
		cancel()
	}
}

func (w *WriterPool) writeRaw(ctx context.Context, r *models.SensorReading) error {
	const stmt = `
INSERT INTO sensor_readings_raw
  (sensor_id, building, zone, sensor_type, unit, raw_value, quality, source, received_at)
VALUES
  ($1, $2, $3, $4, $5, $6, $7, $8, $9)`
	_, err := w.pool.Exec(ctx, stmt,
		r.SensorID, r.Building, r.Zone, r.SensorType, r.Unit,
		r.Value, r.Quality, string(r.Source), r.ReceivedAt,
	)
	return err
}

func (w *WriterPool) writeSnapshot(ctx context.Context, s *models.ComputedZoneSnapshot) error {
	values, err := json.Marshal(s.Snapshot.SensorValues)
	if err != nil {
		return err
	}
	const stmt = `
INSERT INTO zone_snapshots
  (building, zone, snapshot_at, sensor_values,
   shf, esf, uss, pdp, ci, health_factor, rtpmv,
   land_value, structure_value, data_source)
VALUES
  ($1, $2, $3, $4::jsonb,
   $5, $6, $7, $8, $9, $10, $11,
   $12, $13, $14)`
	_, err = w.pool.Exec(ctx, stmt,
		s.Snapshot.Building, s.Snapshot.Zone, s.Snapshot.SnapshotAt, values,
		s.Indicators.SHF, s.Indicators.ESF, s.Indicators.USS, s.Indicators.PDP, s.Indicators.CI,
		s.HealthFactor, s.RTPMV,
		s.LandValue, s.StructureValue, s.DataSource,
	)
	return err
}
