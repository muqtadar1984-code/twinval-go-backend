package adapters

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twinval/internal/ingestion/models"
)

// recordingSubmitter captures every reading submitted for assertions.
type recordingSubmitter struct {
	mu    sync.Mutex
	items []models.SensorReading
}

func (r *recordingSubmitter) Submit(s models.SensorReading) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.items = append(r.items, s)
}

func (r *recordingSubmitter) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.items)
}

func goodWebhookBody() []byte {
	return []byte(`{"sensor_id":"X-1","building":"B","zone":"Z","sensor_type":"temperature","unit":"°C","value":22.5,"quality":1.0,"ts":"2026-05-08T10:00:00Z"}`)
}

func sign(body []byte, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

func newTestWebhook(secret string, rate int) (*WebhookAdapter, *recordingSubmitter) {
	rec := &recordingSubmitter{}
	w := NewWebhookAdapter(rec, WebhookConfig{
		HMACSecret:    secret,
		RatePerMinute: rate,
	})
	return w, rec
}

func TestWebhook_Accepts_ValidSignedPost(t *testing.T) {
	w, rec := newTestWebhook("supersecret", 0)
	body := goodWebhookBody()
	req := httptest.NewRequest(http.MethodPost, "/ingest/webhook", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(SignatureHeader, sign(body, "supersecret"))

	rr := httptest.NewRecorder()
	w.ServeHTTP(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", rr.Code, rr.Body.String())
	}
	if rec.count() != 1 {
		t.Fatalf("expected 1 submitted, got %d", rec.count())
	}
	got := rec.items[0]
	if got.SensorID != "X-1" || got.Building != "B" || got.Zone != "Z" {
		t.Errorf("fields not mapped: %+v", got)
	}
	if got.Source != models.SourceWebhook {
		t.Errorf("Source should be webhook, got %s", got.Source)
	}
}

func TestWebhook_RejectsBadSignature(t *testing.T) {
	w, rec := newTestWebhook("supersecret", 0)
	req := httptest.NewRequest(http.MethodPost, "/ingest/webhook", bytes.NewReader(goodWebhookBody()))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(SignatureHeader, "deadbeef")
	rr := httptest.NewRecorder()
	w.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr.Code)
	}
	if rec.count() != 0 {
		t.Fatalf("must not submit on bad signature")
	}
}

func TestWebhook_RejectsMissingSignature(t *testing.T) {
	w, _ := newTestWebhook("supersecret", 0)
	req := httptest.NewRequest(http.MethodPost, "/ingest/webhook", bytes.NewReader(goodWebhookBody()))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	w.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for missing signature, got %d", rr.Code)
	}
}

func TestWebhook_RejectsEmptySecret(t *testing.T) {
	// An adapter with no secret should reject every request — fail closed.
	w, _ := newTestWebhook("", 0)
	body := goodWebhookBody()
	req := httptest.NewRequest(http.MethodPost, "/ingest/webhook", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(SignatureHeader, sign(body, "anything"))
	rr := httptest.NewRecorder()
	w.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("empty secret must fail closed, got %d", rr.Code)
	}
}

func TestWebhook_RejectsInvalidJSON(t *testing.T) {
	w, _ := newTestWebhook("s", 0)
	body := []byte(`{not json}`)
	req := httptest.NewRequest(http.MethodPost, "/ingest/webhook", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(SignatureHeader, sign(body, "s"))
	rr := httptest.NewRecorder()
	w.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
}

func TestWebhook_RejectsMissingRequiredField(t *testing.T) {
	w, _ := newTestWebhook("s", 0)
	body := []byte(`{"sensor_id":"X","zone":"Z","sensor_type":"temperature","value":1,"quality":1,"ts":"2026-05-08T10:00:00Z"}`)
	req := httptest.NewRequest(http.MethodPost, "/ingest/webhook", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(SignatureHeader, sign(body, "s"))
	rr := httptest.NewRecorder()
	w.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 (missing building), got %d", rr.Code)
	}
}

func TestWebhook_RejectsWrongMethod(t *testing.T) {
	w, _ := newTestWebhook("s", 0)
	req := httptest.NewRequest(http.MethodGet, "/ingest/webhook", nil)
	rr := httptest.NewRecorder()
	w.ServeHTTP(rr, req)
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rr.Code)
	}
}

func TestWebhook_RejectsWrongContentType(t *testing.T) {
	w, _ := newTestWebhook("s", 0)
	req := httptest.NewRequest(http.MethodPost, "/ingest/webhook", bytes.NewReader([]byte(`x`)))
	req.Header.Set("Content-Type", "text/plain")
	rr := httptest.NewRecorder()
	w.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("expected 415, got %d", rr.Code)
	}
}

func TestWebhook_RateLimit(t *testing.T) {
	w, _ := newTestWebhook("s", 3)
	body := goodWebhookBody()
	sig := sign(body, "s")

	send := func() int {
		req := httptest.NewRequest(http.MethodPost, "/ingest/webhook", bytes.NewReader(body))
		req.RemoteAddr = "10.0.0.1:1234"
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(SignatureHeader, sig)
		rr := httptest.NewRecorder()
		w.ServeHTTP(rr, req)
		return rr.Code
	}

	for i := 0; i < 3; i++ {
		if send() != http.StatusAccepted {
			t.Fatalf("request %d should pass", i)
		}
	}
	// 4th hits the limit
	if c := send(); c != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d", c)
	}
}

func TestWebhook_RateLimitIsPerIP(t *testing.T) {
	w, _ := newTestWebhook("s", 1)
	body := goodWebhookBody()
	sig := sign(body, "s")
	send := func(ip string) int {
		req := httptest.NewRequest(http.MethodPost, "/ingest/webhook", bytes.NewReader(body))
		req.RemoteAddr = ip
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(SignatureHeader, sig)
		rr := httptest.NewRecorder()
		w.ServeHTTP(rr, req)
		return rr.Code
	}
	if send("10.0.0.1:9000") != http.StatusAccepted {
		t.Fatal("first IP first call should pass")
	}
	if send("10.0.0.1:9000") != http.StatusTooManyRequests {
		t.Fatal("first IP second call should be rate-limited")
	}
	if send("10.0.0.2:9000") != http.StatusAccepted {
		t.Fatal("second IP should NOT be rate-limited (per-IP isolation)")
	}
}

func TestWebhook_RespectsForwardedForHeader(t *testing.T) {
	w, _ := newTestWebhook("s", 1)
	body := goodWebhookBody()
	sig := sign(body, "s")
	send := func(xff string) int {
		req := httptest.NewRequest(http.MethodPost, "/ingest/webhook", bytes.NewReader(body))
		req.RemoteAddr = "127.0.0.1:5000"
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(SignatureHeader, sig)
		req.Header.Set("X-Forwarded-For", xff)
		rr := httptest.NewRecorder()
		w.ServeHTTP(rr, req)
		return rr.Code
	}
	// Two different XFF values should both pass under per-IP=1
	if send("203.0.113.1") != http.StatusAccepted {
		t.Fatal("XFF #1 should pass first call")
	}
	if send("203.0.113.2") != http.StatusAccepted {
		t.Fatal("XFF #2 should pass first call (different upstream IP)")
	}
}

func TestWebhook_RejectsOversizedBody(t *testing.T) {
	w, _ := newTestWebhook("s", 0)
	w.cfg.BodyByteLimit = 16
	body := []byte(`{"this":"is way too long for the limit"}`)
	req := httptest.NewRequest(http.MethodPost, "/ingest/webhook", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(SignatureHeader, sign(body, "s"))
	rr := httptest.NewRecorder()
	w.ServeHTTP(rr, req)
	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", rr.Code)
	}
}

func TestWebhook_StatsCounters(t *testing.T) {
	w, _ := newTestWebhook("s", 0)
	body := goodWebhookBody()
	good := func() {
		req := httptest.NewRequest(http.MethodPost, "/ingest/webhook", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(SignatureHeader, sign(body, "s"))
		w.ServeHTTP(httptest.NewRecorder(), req)
	}
	bad := func() {
		req := httptest.NewRequest(http.MethodPost, "/ingest/webhook", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(SignatureHeader, "wrong")
		w.ServeHTTP(httptest.NewRecorder(), req)
	}
	good()
	good()
	bad()

	s := w.Stats()
	if s.Accepted != 2 {
		t.Errorf("expected Accepted=2, got %d", s.Accepted)
	}
	if s.RejectedSignature != 1 {
		t.Errorf("expected RejectedSignature=1, got %d", s.RejectedSignature)
	}
}

func TestIPLimiter_ResetAfterWindow(t *testing.T) {
	clock := atomic.Int64{}
	clock.Store(time.Now().UnixNano())
	now := func() time.Time { return time.Unix(0, clock.Load()) }
	l := newIPLimiter(2, time.Minute, now)

	if !l.allow("ip") || !l.allow("ip") {
		t.Fatal("first 2 should pass")
	}
	if l.allow("ip") {
		t.Fatal("3rd should be blocked")
	}
	// Advance time past the window
	clock.Store(clock.Load() + int64(2*time.Minute))
	if !l.allow("ip") {
		t.Fatal("should re-allow after window reset")
	}
}
