package ingest

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/twinval/internal/condition"
)

// BMSAdapter polls a configurable BMS endpoint at a fixed interval and
// fans out each successful response as a condition.RawSensorBatch.
//
// Polling errors (transport, non-200, decode failure) are silently
// dropped from this phase — the patent module treats unavailable BMS
// data as "no readings this cycle". Higher layers can attach a metrics
// sink later if observability is required.
type BMSAdapter struct {
	cfg    IngestConfig
	client *http.Client

	mu          sync.Mutex
	subscribers []chan condition.RawSensorBatch
	stopped     bool

	cancel context.CancelFunc
	done   chan struct{}
}

// NewBMSAdapter constructs an adapter. SubscriberBufferSize defaults to
// 64; BMSHTTPTimeout defaults to 10s. The HTTP client is created once
// and reused across all polls.
func NewBMSAdapter(cfg IngestConfig) *BMSAdapter {
	if cfg.SubscriberBufferSize <= 0 {
		cfg.SubscriberBufferSize = 64
	}
	if cfg.BMSHTTPTimeout <= 0 {
		cfg.BMSHTTPTimeout = 10 * time.Second
	}
	return &BMSAdapter{
		cfg:    cfg,
		client: &http.Client{Timeout: cfg.BMSHTTPTimeout},
	}
}

// Start launches the polling loop in a background goroutine. Returns
// immediately. Polling stops and subscriber channels close when ctx is
// cancelled or Stop is called.
//
// The first poll fires immediately, then the loop ticks every
// BMSPollInterval (default 30s).
func (b *BMSAdapter) Start(ctx context.Context) {
	ctx, b.cancel = context.WithCancel(ctx)
	b.done = make(chan struct{})
	go b.pollLoop(ctx)
}

// Stop triggers shutdown and blocks until the polling loop exits and
// subscriber channels are closed. Idempotent.
func (b *BMSAdapter) Stop() {
	if b.cancel != nil {
		b.cancel()
	}
	if b.done != nil {
		<-b.done
	}
}

// Subscribe returns a buffered channel that receives a RawSensorBatch
// for every successful BMS poll. Returns a pre-closed channel if the
// adapter has already shut down.
func (b *BMSAdapter) Subscribe() <-chan condition.RawSensorBatch {
	b.mu.Lock()
	defer b.mu.Unlock()
	ch := make(chan condition.RawSensorBatch, b.cfg.SubscriberBufferSize)
	if b.stopped {
		close(ch)
		return ch
	}
	b.subscribers = append(b.subscribers, ch)
	return ch
}

func (b *BMSAdapter) pollLoop(ctx context.Context) {
	defer close(b.done)
	defer b.closeSubscribers()

	interval := b.cfg.BMSPollInterval
	if interval <= 0 {
		interval = 30 * time.Second
	}

	b.pollOnce(ctx)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.pollOnce(ctx)
		}
	}
}

// pollOnce performs a single GET. Any error short-circuits silently —
// the next tick will retry.
func (b *BMSAdapter) pollOnce(ctx context.Context) {
	if b.cfg.BMSPollURL == "" {
		return
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.cfg.BMSPollURL, nil)
	if err != nil {
		return
	}
	resp, err := b.client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return
	}

	var bmsResp BMSResponse
	if err := json.NewDecoder(resp.Body).Decode(&bmsResp); err != nil {
		return
	}

	b.broadcast(bmsResponseToBatch(bmsResp, b.cfg))
}

func (b *BMSAdapter) broadcast(batch condition.RawSensorBatch) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, ch := range b.subscribers {
		select {
		case ch <- batch:
		default:
		}
	}
}

func (b *BMSAdapter) closeSubscribers() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.stopped {
		return
	}
	b.stopped = true
	for _, ch := range b.subscribers {
		close(ch)
	}
	b.subscribers = nil
}

// bmsResponseToBatch translates a BMS response into the internal batch
// shape, converting BMS millisecond timestamps to nanoseconds.
func bmsResponseToBatch(resp BMSResponse, cfg IngestConfig) condition.RawSensorBatch {
	readings := make([]condition.RawSensorReading, 0, len(resp.Readings))
	for _, r := range resp.Readings {
		readings = append(readings, condition.RawSensorReading{
			SensorID:           r.SensorTag,
			SensorType:         condition.SensorType(r.Type),
			Zone:               r.Zone,
			TimestampNs:        r.TimestampMs * 1_000_000,
			Value:              r.Reading,
			CalibrationDaysAgo: r.DaysSinceCal,
		})
	}
	return condition.RawSensorBatch{
		PropertyID:                resp.PropertyID,
		Readings:                  readings,
		ExpectedReadingsPerSensor: cfg.DefaultExpectedReadingsPerSensor,
		PropertyMeta:              cfg.DefaultPropertyMeta,
	}
}
