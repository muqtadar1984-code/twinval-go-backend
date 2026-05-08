package ingestion

import (
	"context"
	"sync"
	"time"

	"github.com/twinval/internal/ingestion/models"
)

// Aggregator buckets normalised readings by ZoneKey for an
// aggregation window. When a bucket's window expires, the latest
// NormalisedValue per sensor type is rolled into a ZoneSnapshot and
// emitted on the output channel.
//
// Run() owns the timing — it should run in its own goroutine. Add()
// is safe to call concurrently from many adapter goroutines.
type Aggregator struct {
	window time.Duration
	out    chan<- models.ZoneSnapshot
	now    func() time.Time // injectable for tests

	mu      sync.Mutex
	buckets map[models.ZoneKey]*zoneBucket
}

type zoneBucket struct {
	openedAt        time.Time
	latestByType    map[string]models.NormalisedReading
	qualitySum      float64
	count           int
	latestReceived  time.Time // largest ReceivedAt across readings
}

// NewAggregator constructs an Aggregator with the supplied window and
// output channel. The output channel must be drained by a downstream
// stage; otherwise Run() blocks on send.
func NewAggregator(window time.Duration, out chan<- models.ZoneSnapshot) *Aggregator {
	if window <= 0 {
		window = 10 * time.Second
	}
	return &Aggregator{
		window:  window,
		out:     out,
		now:     time.Now,
		buckets: make(map[models.ZoneKey]*zoneBucket),
	}
}

// Add registers a normalised reading into its zone bucket. Creates the
// bucket if this is the first reading for the zone.
func (a *Aggregator) Add(r models.NormalisedReading) {
	key := models.ZoneKey{Building: r.Building, Zone: r.Zone}
	a.mu.Lock()
	defer a.mu.Unlock()
	b, ok := a.buckets[key]
	if !ok {
		b = &zoneBucket{
			openedAt:     a.now(),
			latestByType: make(map[string]models.NormalisedReading),
		}
		a.buckets[key] = b
	}
	b.latestByType[r.SensorType] = r
	b.qualitySum += r.Quality
	b.count++
	if r.ReceivedAt.After(b.latestReceived) {
		b.latestReceived = r.ReceivedAt
	}
}

// Run loops on a half-window ticker. On each tick, any bucket older
// than `window` is flushed to the output channel and removed.
//
// On context cancel: flush every remaining bucket, then return.
func (a *Aggregator) Run(ctx context.Context) {
	tick := a.window / 2
	if tick < 100*time.Millisecond {
		tick = 100 * time.Millisecond
	}
	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			a.flushAll()
			return
		case <-ticker.C:
			a.flushExpired()
		}
	}
}

// flushExpired emits ZoneSnapshots for any bucket older than `window`.
func (a *Aggregator) flushExpired() {
	now := a.now()
	a.mu.Lock()
	expiredKeys := make([]models.ZoneKey, 0)
	for k, b := range a.buckets {
		if now.Sub(b.openedAt) >= a.window {
			expiredKeys = append(expiredKeys, k)
		}
	}
	snapshots := make([]models.ZoneSnapshot, 0, len(expiredKeys))
	for _, k := range expiredKeys {
		b := a.buckets[k]
		delete(a.buckets, k)
		snapshots = append(snapshots, a.materialise(k, b, now))
	}
	a.mu.Unlock()

	for _, s := range snapshots {
		a.out <- s
	}
}

// flushAll emits and clears every bucket regardless of age. Called
// during Run shutdown so no in-flight data is lost.
func (a *Aggregator) flushAll() {
	now := a.now()
	a.mu.Lock()
	keys := make([]models.ZoneKey, 0, len(a.buckets))
	for k := range a.buckets {
		keys = append(keys, k)
	}
	snapshots := make([]models.ZoneSnapshot, 0, len(keys))
	for _, k := range keys {
		b := a.buckets[k]
		delete(a.buckets, k)
		snapshots = append(snapshots, a.materialise(k, b, now))
	}
	a.mu.Unlock()

	for _, s := range snapshots {
		// best-effort send; in shutdown the consumer may already be gone
		select {
		case a.out <- s:
		default:
			return
		}
	}
}

// FlushNow is a test/diagnostic helper that immediately flushes every
// bucket regardless of age, returning the snapshots inline rather than
// pushing through the channel. Tests should use this rather than
// waiting on Run + the ticker.
func (a *Aggregator) FlushNow() []models.ZoneSnapshot {
	now := a.now()
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]models.ZoneSnapshot, 0, len(a.buckets))
	for k, b := range a.buckets {
		out = append(out, a.materialise(k, b, now))
	}
	a.buckets = make(map[models.ZoneKey]*zoneBucket)
	return out
}

func (a *Aggregator) materialise(k models.ZoneKey, b *zoneBucket, at time.Time) models.ZoneSnapshot {
	values := make(map[string]float64, len(b.latestByType))
	rawValues := make(map[string]float64, len(b.latestByType))
	for t, r := range b.latestByType {
		values[t] = r.NormalisedValue
		rawValues[t] = r.Value
	}
	mq := 0.0
	if b.count > 0 {
		mq = b.qualitySum / float64(b.count)
	}
	return models.ZoneSnapshot{
		Building:              k.Building,
		Zone:                  k.Zone,
		SnapshotAt:            at,
		SensorValues:          values,
		SensorRawValues:       rawValues,
		MeanQuality:           mq,
		ReadingCount:          b.count,
		LatestReadingReceived: b.latestReceived,
	}
}
