package ingestion

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twinval/internal/ingestion/metrics"
	"github.com/twinval/internal/ingestion/models"
)

// Pipeline orchestrates the live ingestion stages:
//
//	adapter -> raw channel -> validator -> denoiser -> normaliser
//	        -> aggregator -> snapshot channel -> computer -> sinks
//
// Adapters call Submit() to feed the raw channel. The pipeline owns
// every goroutine downstream of that channel and manages their
// lifecycle in Start / Stop.
//
// Sinks are user-supplied — typical wiring is a HubBroadcaster + a
// DB writer pool adapter. Each sink receives every successfully
// computed snapshot.
type Pipeline struct {
	validator  *Validator
	denoiser   *Denoiser
	normaliser *Normaliser
	aggregator *Aggregator
	computer   *Computer
	sinks      []Sink

	rawCh      chan models.SensorReading
	snapshotCh chan models.ZoneSnapshot

	wg     sync.WaitGroup
	cancel context.CancelFunc
	closed atomic.Bool

	stats Stats
}

// Stats is the pipeline's externally-observable counter set.
type Stats struct {
	Submitted        atomic.Uint64
	SubmitDropped    atomic.Uint64 // dropped because the raw channel was full or pipeline closed
	ValidatedPassed  atomic.Uint64
	NormaliseDropped atomic.Uint64 // unknown sensor type after validate (defensive)
	SnapshotsEmitted atomic.Uint64
	ComputeDropped   atomic.Uint64 // registry returned not-found
	BroadcastSent    atomic.Uint64
}

// PipelineOptions bundles every constructor input. Each is required
// (no nil-tolerance) — wiring errors should fail at startup.
type PipelineOptions struct {
	Validator  *Validator
	Denoiser   *Denoiser
	Normaliser *Normaliser
	Aggregator *Aggregator
	Computer   *Computer
	Sinks      []Sink

	// Channel buffer sizes. Spec: raw=10000. SnapshotBuffer defaults to 1024.
	RawBuffer      int
	SnapshotBuffer int
}

// NewPipeline constructs a Pipeline ready to be Started. The aggregator's
// output channel is set internally — caller must NOT connect their own.
func NewPipeline(opts PipelineOptions) *Pipeline {
	if opts.RawBuffer <= 0 {
		opts.RawBuffer = 10_000
	}
	if opts.SnapshotBuffer <= 0 {
		opts.SnapshotBuffer = 1024
	}
	rawCh := make(chan models.SensorReading, opts.RawBuffer)
	snapCh := make(chan models.ZoneSnapshot, opts.SnapshotBuffer)
	// The aggregator was constructed with some output channel earlier —
	// rebind it here so the pipeline owns the wiring topology.
	opts.Aggregator.out = snapCh
	return &Pipeline{
		validator:  opts.Validator,
		denoiser:   opts.Denoiser,
		normaliser: opts.Normaliser,
		aggregator: opts.Aggregator,
		computer:   opts.Computer,
		sinks:      opts.Sinks,
		rawCh:      rawCh,
		snapshotCh: snapCh,
	}
}

// Submit hands a reading to the pipeline. Non-blocking — if the raw
// channel is full or the pipeline is closed, the reading is dropped
// and SubmitDropped increments. Adapters should never block on this.
func (p *Pipeline) Submit(r models.SensorReading) {
	if p.closed.Load() {
		p.stats.SubmitDropped.Add(1)
		return
	}
	select {
	case p.rawCh <- r:
		p.stats.Submitted.Add(1)
	default:
		p.stats.SubmitDropped.Add(1)
	}
}

// Start launches every internal goroutine. Returns immediately.
// Subsequent calls are no-ops.
func (p *Pipeline) Start() {
	if p.closed.Load() {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel

	// Stage A: validate -> denoise -> normalise -> aggregate.
	p.wg.Add(1)
	go p.runIngestStage(ctx)

	// Stage B: aggregator's own ticker.
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.aggregator.Run(ctx)
	}()

	// Stage C: snapshots -> compute -> sinks.
	p.wg.Add(1)
	go p.runComputeStage(ctx)
}

func (p *Pipeline) runIngestStage(ctx context.Context) {
	defer p.wg.Done()
	for {
		select {
		case <-ctx.Done():
			// Drain whatever's left in rawCh so adapters that
			// already submitted are not silently lost.
			for {
				select {
				case r := <-p.rawCh:
					p.processReading(r)
				default:
					return
				}
			}
		case r := <-p.rawCh:
			p.processReading(r)
		}
	}
}

func (p *Pipeline) processReading(r models.SensorReading) {
	ok, reason := p.validator.Validate(r)
	if !ok {
		slog.Debug("ingestion: dropped reading",
			"reason", reason, "sensor_id", r.SensorID,
			"building", r.Building, "zone", r.Zone)
		return
	}
	p.stats.ValidatedPassed.Add(1)
	metrics.ReadingsReceived.WithLabelValues(
		string(r.Source), r.Building, r.Zone, r.SensorType,
	).Inc()

	// Denoise mutates r.Value in place.
	p.denoiser.Apply(&r)

	// Normalise.
	nr, ok := p.normaliser.Normalise(r)
	if !ok {
		// Validator already filtered unknown types; this is a defensive
		// guard against bounds/validator skew.
		p.stats.NormaliseDropped.Add(1)
		return
	}
	p.aggregator.Add(nr)
}

func (p *Pipeline) runComputeStage(ctx context.Context) {
	defer p.wg.Done()
	for {
		select {
		case <-ctx.Done():
			// Drain remaining snapshots so the final aggregator flush
			// is not lost.
			for {
				select {
				case s := <-p.snapshotCh:
					p.processSnapshot(s)
				default:
					return
				}
			}
		case s := <-p.snapshotCh:
			p.processSnapshot(s)
		}
	}
}

func (p *Pipeline) processSnapshot(s models.ZoneSnapshot) {
	p.stats.SnapshotsEmitted.Add(1)
	metrics.ZoneSnapshotsTotal.WithLabelValues(s.Building, s.Zone).Inc()

	res, ok := p.computer.Compute(s)
	if !ok {
		p.stats.ComputeDropped.Add(1)
		return
	}

	// Surface the computed indicators on /metrics before sinks fan
	// the data out — operators want these values regardless of
	// whether the WebSocket hub or DB sink is healthy.
	metrics.RTPMVCurrent.WithLabelValues(res.Snapshot.Building, res.Snapshot.Zone).Set(res.RTPMV)
	metrics.HealthFactorCurrent.WithLabelValues(res.Snapshot.Building, res.Snapshot.Zone).Set(res.HealthFactor)

	for _, sink := range p.sinks {
		sink.Emit(res)
	}
	p.stats.BroadcastSent.Add(1)

	// Pipeline latency: from the most recent contributing reading to
	// broadcast. Skip when the snapshot has no readings (defensive).
	if !res.Snapshot.LatestReadingReceived.IsZero() {
		latency := res.ComputedAt.Sub(res.Snapshot.LatestReadingReceived).Seconds()
		if latency > 0 {
			metrics.PipelineLatency.Observe(latency)
		}
	}
}

// Stop signals every goroutine to exit, drains any remaining buffered
// data, and waits up to `deadline` for the pipeline to finish.
// Subsequent Submit() calls are dropped.
func (p *Pipeline) Stop(ctx context.Context) error {
	if !p.closed.CompareAndSwap(false, true) {
		return nil
	}
	if p.cancel != nil {
		p.cancel()
	}
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Snapshot returns a copy of the current counters.
func (p *Pipeline) Snapshot() PipelineStats {
	return PipelineStats{
		Submitted:        p.stats.Submitted.Load(),
		SubmitDropped:    p.stats.SubmitDropped.Load(),
		ValidatedPassed:  p.stats.ValidatedPassed.Load(),
		NormaliseDropped: p.stats.NormaliseDropped.Load(),
		SnapshotsEmitted: p.stats.SnapshotsEmitted.Load(),
		ComputeDropped:   p.stats.ComputeDropped.Load(),
		BroadcastSent:    p.stats.BroadcastSent.Load(),
	}
}

// PipelineStats is the snapshot type returned by Snapshot().
type PipelineStats struct {
	Submitted        uint64
	SubmitDropped    uint64
	ValidatedPassed  uint64
	NormaliseDropped uint64
	SnapshotsEmitted uint64
	ComputeDropped   uint64
	BroadcastSent    uint64
}

// minDuration is a small helper used in tests to wait for stage
// progress without explicit synchronisation primitives.
func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
