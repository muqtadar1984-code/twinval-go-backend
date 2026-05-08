package humanobs

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"
)

// fakeFetcher is a Fetcher that returns canned results, optionally
// recording the args passed to it.
type fakeFetcher struct {
	mu      sync.Mutex
	counts  SeverityCounts
	err     error
	calls   []fakeCall
}

type fakeCall struct {
	building, zone string
	since          time.Time
}

func (f *fakeFetcher) FetchSeverityCounts(_ context.Context, building, zone string, since time.Time) (SeverityCounts, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, fakeCall{building, zone, since})
	if f.err != nil {
		return SeverityCounts{}, f.err
	}
	return f.counts, nil
}

func TestModifier_NoObservationsReturnsZero(t *testing.T) {
	m := NewModifier(&fakeFetcher{}, time.Hour)
	if got := m.Delta("B", "Z"); got != 0 {
		t.Fatalf("expected 0 with no observations, got %v", got)
	}
}

func TestModifier_NormalRaisesCI(t *testing.T) {
	m := NewModifier(&fakeFetcher{counts: SeverityCounts{Normal: 3}}, time.Hour)
	got := m.Delta("B", "Z")
	want := 3 * WeightNormal // +0.06
	if math.Abs(got-want) > 1e-9 {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

func TestModifier_WatchLowersCI(t *testing.T) {
	m := NewModifier(&fakeFetcher{counts: SeverityCounts{Watch: 2}}, time.Hour)
	got := m.Delta("B", "Z")
	want := 2 * WeightWatch // -0.10
	if math.Abs(got-want) > 1e-9 {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

func TestModifier_AlertHeavilyLowersCI(t *testing.T) {
	m := NewModifier(&fakeFetcher{counts: SeverityCounts{Alert: 1}}, time.Hour)
	got := m.Delta("B", "Z")
	if math.Abs(got-WeightAlert) > 1e-9 {
		t.Fatalf("expected %v, got %v", WeightAlert, got)
	}
}

func TestModifier_MixedSeveritiesSum(t *testing.T) {
	m := NewModifier(&fakeFetcher{counts: SeverityCounts{
		Normal: 5, Watch: 2, Alert: 1,
	}}, time.Hour)
	got := m.Delta("B", "Z")
	// 5 * 0.02 + 2 * -0.05 + 1 * -0.15 = 0.10 - 0.10 - 0.15 = -0.15
	want := 5*WeightNormal + 2*WeightWatch + WeightAlert
	if math.Abs(got-want) > 1e-9 {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

func TestModifier_ClampsAtNegativeOne(t *testing.T) {
	// 100 alerts would yield -15.0; must clamp at -1.
	m := NewModifier(&fakeFetcher{counts: SeverityCounts{Alert: 100}}, time.Hour)
	got := m.Delta("B", "Z")
	if got != -1 {
		t.Fatalf("expected clamp at -1, got %v", got)
	}
}

func TestModifier_ClampsAtPositiveOne(t *testing.T) {
	// 100 normals would yield +2.0; must clamp at +1.
	m := NewModifier(&fakeFetcher{counts: SeverityCounts{Normal: 100}}, time.Hour)
	got := m.Delta("B", "Z")
	if got != 1 {
		t.Fatalf("expected clamp at +1, got %v", got)
	}
}

func TestModifier_FetchErrorReturnsZero(t *testing.T) {
	m := NewModifier(&fakeFetcher{err: errors.New("db down")}, time.Hour)
	if got := m.Delta("B", "Z"); got != 0 {
		t.Fatalf("error must yield 0 (no penalty for absence), got %v", got)
	}
	if m.Stats().FetchErrors != 1 {
		t.Fatalf("expected FetchErrors=1, got %d", m.Stats().FetchErrors)
	}
}

func TestModifier_FetchSinceMatchesWindow(t *testing.T) {
	f := &fakeFetcher{}
	fixed := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	m := NewModifier(f, 4*time.Hour)
	m.now = func() time.Time { return fixed }
	_ = m.Delta("Block A", "Roof")

	if len(f.calls) != 1 {
		t.Fatalf("expected 1 fetch call, got %d", len(f.calls))
	}
	want := fixed.Add(-4 * time.Hour)
	if !f.calls[0].since.Equal(want) {
		t.Fatalf("expected since=%v, got %v", want, f.calls[0].since)
	}
	if f.calls[0].building != "Block A" || f.calls[0].zone != "Roof" {
		t.Fatalf("wrong key: %+v", f.calls[0])
	}
}

func TestModifier_AppliedCountersIncrement(t *testing.T) {
	m := NewModifier(&fakeFetcher{counts: SeverityCounts{Normal: 2, Watch: 1, Alert: 0}}, time.Hour)
	m.Delta("B", "Z")
	stats := m.Stats()
	if stats.NormalApplied != 2 || stats.WatchApplied != 1 || stats.AlertApplied != 0 {
		t.Fatalf("counter mismatch: %+v", stats)
	}
}

func TestModifier_ZeroWindowFallsBackToFourHours(t *testing.T) {
	m := NewModifier(&fakeFetcher{}, 0)
	if m.window != 4*time.Hour {
		t.Fatalf("expected fallback to 4h, got %v", m.window)
	}
}
