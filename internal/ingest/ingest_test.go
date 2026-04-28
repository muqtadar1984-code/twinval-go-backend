package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/twinval/internal/condition"
)

// =============================================================================
// Helpers
// =============================================================================

func validPayload() WebhookPayload {
	return WebhookPayload{
		PropertyID:    "PROP-001",
		WindowStartNs: 1000,
		WindowEndNs:   2000,
		Readings: []WebhookReading{
			{
				SensorID:           "vib1",
				SensorType:         string(condition.SensorVibration),
				Zone:               "core",
				TimestampNs:        1500,
				Value:              0.05,
				CalibrationDaysAgo: 30,
			},
		},
	}
}

// drainUntilClosed consumes batches from sub until the channel closes
// or the deadline elapses. Returns true if the channel closed cleanly.
func drainUntilClosed(t *testing.T, sub <-chan condition.RawSensorBatch, label string, timeout time.Duration) bool {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case _, ok := <-sub:
			if !ok {
				return true
			}
		case <-deadline:
			t.Fatalf("%s: subscriber channel did not close within %s", label, timeout)
			return false
		}
	}
}

// =============================================================================
// ValidatePayload
// =============================================================================

func TestValidatePayload_AcceptsValid(t *testing.T) {
	if err := ValidatePayload(validPayload()); err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

func TestValidatePayload_RejectsEmptyPropertyID(t *testing.T) {
	p := validPayload()
	p.PropertyID = ""
	if err := ValidatePayload(p); err == nil {
		t.Errorf("expected error for empty PropertyID")
	}
}

func TestValidatePayload_RejectsZeroReadings(t *testing.T) {
	p := validPayload()
	p.Readings = nil
	if err := ValidatePayload(p); err == nil {
		t.Errorf("expected error for zero readings")
	}
}

func TestValidatePayload_RejectsUnknownSensorType(t *testing.T) {
	p := validPayload()
	p.Readings[0].SensorType = "not_a_real_sensor"
	if err := ValidatePayload(p); err == nil {
		t.Errorf("expected error for unknown sensor type")
	}
}

func TestValidatePayload_RejectsZeroTimestamp(t *testing.T) {
	p := validPayload()
	p.Readings[0].TimestampNs = 0
	if err := ValidatePayload(p); err == nil {
		t.Errorf("expected error for zero timestamp")
	}
}

func TestValidatePayload_RejectsNaN(t *testing.T) {
	p := validPayload()
	p.Readings[0].Value = math.NaN()
	if err := ValidatePayload(p); err == nil {
		t.Errorf("expected error for NaN value")
	}
}

func TestValidatePayload_RejectsInfinity(t *testing.T) {
	p := validPayload()
	p.Readings[0].Value = math.Inf(1)
	if err := ValidatePayload(p); err == nil {
		t.Errorf("expected error for +Inf value")
	}
	p.Readings[0].Value = math.Inf(-1)
	if err := ValidatePayload(p); err == nil {
		t.Errorf("expected error for -Inf value")
	}
}

// =============================================================================
// WebhookReceiver — handler-level via httptest.NewServer
// =============================================================================

func TestWebhookReceiver_AcceptsValidPostAndEmitsBatch(t *testing.T) {
	cfg := DefaultIngestConfig()
	recv := NewWebhookReceiver(cfg)
	sub := recv.Subscribe()

	ts := httptest.NewServer(recv.Handler())
	defer ts.Close()

	body, _ := json.Marshal(validPayload())
	resp, err := http.Post(ts.URL+cfg.WebhookPath, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Errorf("status: got %d, want %d", resp.StatusCode, http.StatusAccepted)
	}

	select {
	case batch := <-sub:
		if batch.WindowStartNs != 1000 {
			t.Errorf("WindowStartNs: got %d, want 1000", batch.WindowStartNs)
		}
		if batch.WindowEndNs != 2000 {
			t.Errorf("WindowEndNs: got %d, want 2000", batch.WindowEndNs)
		}
		if len(batch.Readings) != 1 {
			t.Fatalf("readings: got %d, want 1", len(batch.Readings))
		}
		if batch.Readings[0].SensorID != "vib1" {
			t.Errorf("SensorID: got %q, want %q", batch.Readings[0].SensorID, "vib1")
		}
		if batch.Readings[0].SensorType != condition.SensorVibration {
			t.Errorf("SensorType: got %q, want %q", batch.Readings[0].SensorType, condition.SensorVibration)
		}
	case <-time.After(time.Second):
		t.Fatal("did not receive batch within 1s")
	}
}

func TestWebhookReceiver_RejectsMalformedJSONWith400(t *testing.T) {
	cfg := DefaultIngestConfig()
	recv := NewWebhookReceiver(cfg)
	ts := httptest.NewServer(recv.Handler())
	defer ts.Close()

	resp, err := http.Post(ts.URL+cfg.WebhookPath, "application/json", bytes.NewReader([]byte("{not json")))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", resp.StatusCode)
	}
}

func TestWebhookReceiver_RejectsInvalidPayloadWith422(t *testing.T) {
	cfg := DefaultIngestConfig()
	recv := NewWebhookReceiver(cfg)
	ts := httptest.NewServer(recv.Handler())
	defer ts.Close()

	invalid := validPayload()
	invalid.PropertyID = ""
	body, _ := json.Marshal(invalid)
	resp, err := http.Post(ts.URL+cfg.WebhookPath, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Errorf("status: got %d, want 422", resp.StatusCode)
	}
}

func TestWebhookReceiver_RejectsNonPOSTWith405(t *testing.T) {
	cfg := DefaultIngestConfig()
	recv := NewWebhookReceiver(cfg)
	ts := httptest.NewServer(recv.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + cfg.WebhookPath)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("status: got %d, want 405", resp.StatusCode)
	}
}

// =============================================================================
// WebhookReceiver — full lifecycle via Start/Stop with port 0
// =============================================================================

func TestWebhookReceiver_StartAndContextCancelStopsCleanly(t *testing.T) {
	cfg := DefaultIngestConfig()
	cfg.WebhookListenAddr = "127.0.0.1:0"
	recv := NewWebhookReceiver(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	if err := recv.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	addr := recv.Addr()
	if addr == "" {
		t.Fatal("Addr should be populated after Start")
	}

	sub := recv.Subscribe()

	body, _ := json.Marshal(validPayload())
	resp, err := http.Post("http://"+addr+cfg.WebhookPath, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()

	select {
	case <-sub:
	case <-time.After(time.Second):
		t.Fatal("did not receive batch via running server")
	}

	cancel()
	if !drainUntilClosed(t, sub, "webhook", 2*time.Second) {
		return
	}
}

// =============================================================================
// BMSAdapter — polling against httptest mock
// =============================================================================

func TestBMSAdapter_PollsAndEmitsBatch(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := BMSResponse{
			PropertyID: "PROP-001",
			Readings: []BMSReading{
				{
					SensorTag:    "bms-vib-1",
					Type:         string(condition.SensorVibration),
					Zone:         "core",
					TimestampMs:  1500,
					Reading:      0.05,
					DaysSinceCal: 30,
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer ts.Close()

	cfg := DefaultIngestConfig()
	cfg.BMSPollURL = ts.URL
	cfg.BMSPollInterval = 100 * time.Millisecond
	cfg.BMSHTTPTimeout = time.Second

	adapter := NewBMSAdapter(cfg)
	sub := adapter.Subscribe()

	ctx, cancel := context.WithCancel(context.Background())
	adapter.Start(ctx)

	select {
	case batch := <-sub:
		if len(batch.Readings) != 1 {
			t.Fatalf("readings: got %d, want 1", len(batch.Readings))
		}
		r := batch.Readings[0]
		if r.SensorID != "bms-vib-1" {
			t.Errorf("SensorID: got %q, want %q", r.SensorID, "bms-vib-1")
		}
		if r.SensorType != condition.SensorVibration {
			t.Errorf("SensorType: got %q, want %q", r.SensorType, condition.SensorVibration)
		}
		// 1500ms → 1.5×10^9 ns
		if r.TimestampNs != 1500*1_000_000 {
			t.Errorf("TimestampNs: got %d, want %d (1500ms in ns)", r.TimestampNs, 1500*1_000_000)
		}
		if r.Value != 0.05 {
			t.Errorf("Value: got %v, want 0.05", r.Value)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive BMS batch within 2s")
	}

	cancel()
	if !drainUntilClosed(t, sub, "bms", 2*time.Second) {
		return
	}
}

func TestBMSAdapter_ContextCancelStopsPollingCleanly(t *testing.T) {
	pollCount := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pollCount++
		_ = json.NewEncoder(w).Encode(BMSResponse{PropertyID: "PROP-001"})
	}))
	defer ts.Close()

	cfg := DefaultIngestConfig()
	cfg.BMSPollURL = ts.URL
	cfg.BMSPollInterval = 50 * time.Millisecond

	adapter := NewBMSAdapter(cfg)
	sub := adapter.Subscribe()

	ctx, cancel := context.WithCancel(context.Background())
	adapter.Start(ctx)

	// Let it tick at least twice
	time.Sleep(150 * time.Millisecond)
	cancel()

	if !drainUntilClosed(t, sub, "bms", 2*time.Second) {
		return
	}

	// Adapter.Stop after cancel should be a no-op and return promptly.
	done := make(chan struct{})
	go func() {
		adapter.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Stop after cancel did not return within 1s")
	}
}
