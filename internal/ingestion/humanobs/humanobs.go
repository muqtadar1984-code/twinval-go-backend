// Package humanobs computes the Confidence-Index delta contributed by
// human field observations recorded in the Intern Observation Portal.
//
// On every CI computation cycle the modifier is asked, for a given
// building+zone, "what is the human-observation correction to apply
// to the sensor-derived CI?" The answer is the sum of severity-weighted
// observations submitted within the configured window:
//
//	Normal -> +0.02   (confirms good condition)
//	Watch  -> -0.05
//	Alert  -> -0.15
//
// The positive (Normal) component is capped at DefaultMaxPositiveDelta
// so that accumulated confirmations cannot saturate CI and mask
// concurrent warnings; Watch/Alert contributions are uncapped. The
// resulting sum is clamped to [-1, +1] before being returned; the
// downstream Computer further clamps the final CI to [0, 1].
//
// Voided observations are excluded — they are the audit-correct
// equivalent of delete in the portal, and their CI signal is retracted.
//
// This package is the ONLY point where the live ingestion pipeline
// reaches into the portal's database. It is read-only — no writes ever.
package humanobs

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/twinval/internal/ingestion/metrics"
)

// Severity weights, per the spec. Stable values: rebalancing these
// changes the meaning of historical CI and should be treated as a
// formal model parameter change, not a config knob.
const (
	WeightNormal float64 = +0.02
	WeightWatch  float64 = -0.05
	WeightAlert  float64 = -0.15
)

// DefaultMaxPositiveDelta caps the total CI credit that Normal
// confirmations can accumulate within one window. The per-observation
// weights above were tuned for the 4-hour default window; with wider
// windows (the bungalow pilot runs 7 days) uncapped Normals saturate
// CI at 1.0 and mask concurrent Watch/Alert signals entirely. Only the
// positive component is capped — warnings remain uncapped so an Alert
// always shows in CI regardless of how many confirmations surround it.
// Treated as a formal model parameter, like the weights above.
const DefaultMaxPositiveDelta float64 = 0.10

// SeverityCounts is the breakdown of a window's observations by severity.
// Returned by the fetcher rather than building/zone-specific math so the
// fetcher contract stays narrow.
type SeverityCounts struct {
	Normal int
	Watch  int
	Alert  int
}

// Fetcher is the contract the modifier needs to talk to whatever data
// store holds observations. Production wires the Postgres implementation;
// tests use a fake.
type Fetcher interface {
	FetchSeverityCounts(ctx context.Context, building, zone string, since time.Time) (SeverityCounts, error)
}

// Modifier turns FetchSeverityCounts results into a CI delta. Implements
// the ingestion.CIModifier interface (Delta(building, zone) float64).
type Modifier struct {
	fetcher     Fetcher
	window      time.Duration
	timeout     time.Duration
	maxPositive float64
	now         func() time.Time

	// counters — wired to Prometheus in Phase 5.
	queries   atomic.Uint64
	fetchErrs atomic.Uint64
	applied   struct {
		normal atomic.Uint64
		watch  atomic.Uint64
		alert  atomic.Uint64
	}
}

// NewModifier returns a Modifier with the given fetcher + window.
// Per-query timeout defaults to 2s — adjust via WithTimeout if needed.
func NewModifier(fetcher Fetcher, window time.Duration) *Modifier {
	if window <= 0 {
		window = 4 * time.Hour
	}
	return &Modifier{
		fetcher:     fetcher,
		window:      window,
		timeout:     2 * time.Second,
		maxPositive: DefaultMaxPositiveDelta,
		now:         time.Now,
	}
}

// WithTimeout overrides the per-query timeout. Returns the modifier
// for chaining at construction time.
func (m *Modifier) WithTimeout(t time.Duration) *Modifier {
	if t > 0 {
		m.timeout = t
	}
	return m
}

// WithMaxPositiveDelta overrides the cap on accumulated Normal-observation
// credit. Values <= 0 are ignored (the default cap stays). Returns the
// modifier for chaining at construction time.
func (m *Modifier) WithMaxPositiveDelta(cap float64) *Modifier {
	if cap > 0 {
		m.maxPositive = cap
	}
	return m
}

// Delta implements ingestion.CIModifier. Returns the clamped sum of
// severity-weighted observation counts in the configured window.
//
// Failures are logged and return 0 — the spec says missing observations
// must NOT penalise CI, and a transient DB hiccup should not amplify
// itself into a sudden CI drop.
func (m *Modifier) Delta(building, zone string) float64 {
	m.queries.Add(1)
	since := m.now().UTC().Add(-m.window)

	ctx, cancel := context.WithTimeout(context.Background(), m.timeout)
	defer cancel()

	counts, err := m.fetcher.FetchSeverityCounts(ctx, building, zone, since)
	if err != nil {
		m.fetchErrs.Add(1)
		slog.Warn("human-obs fetch failed; returning zero delta",
			"building", building, "zone", zone, "error", err.Error())
		return 0
	}

	if counts.Normal > 0 {
		m.applied.normal.Add(uint64(counts.Normal))
		metrics.HumanObsApplied.WithLabelValues(building, zone, "Normal").Add(float64(counts.Normal))
	}
	if counts.Watch > 0 {
		m.applied.watch.Add(uint64(counts.Watch))
		metrics.HumanObsApplied.WithLabelValues(building, zone, "Watch").Add(float64(counts.Watch))
	}
	if counts.Alert > 0 {
		m.applied.alert.Add(uint64(counts.Alert))
		metrics.HumanObsApplied.WithLabelValues(building, zone, "Alert").Add(float64(counts.Alert))
	}

	positive := float64(counts.Normal) * WeightNormal
	if positive > m.maxPositive {
		positive = m.maxPositive
	}
	delta := positive +
		float64(counts.Watch)*WeightWatch +
		float64(counts.Alert)*WeightAlert
	if delta < -1 {
		delta = -1
	}
	if delta > 1 {
		delta = 1
	}
	return delta
}

// Stats is a snapshot of modifier counters.
type Stats struct {
	Queries           uint64
	FetchErrors       uint64
	NormalApplied     uint64
	WatchApplied      uint64
	AlertApplied      uint64
}

// Stats returns a counter snapshot.
func (m *Modifier) Stats() Stats {
	return Stats{
		Queries:       m.queries.Load(),
		FetchErrors:   m.fetchErrs.Load(),
		NormalApplied: m.applied.normal.Load(),
		WatchApplied:  m.applied.watch.Load(),
		AlertApplied:  m.applied.alert.Load(),
	}
}
