package adapters

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twinval/internal/ingestion/models"
)

// SignatureHeader is the HTTP header carrying the HMAC-SHA256 hex
// digest of the raw request body, signed with WEBHOOK_HMAC_SECRET.
const SignatureHeader = "X-TwinVal-Signature"

// DefaultRateLimit is the spec-mandated 1000 requests / minute per
// source IP.
const DefaultRateLimit = 1000

// WebhookConfig configures the webhook adapter.
type WebhookConfig struct {
	HMACSecret      string
	RatePerMinute   int           // per-IP; <=0 means unlimited
	BodyByteLimit   int64         // hard cap on POST body size; default 64 KiB
	ClockNow        func() time.Time
}

// WebhookAdapter exposes /ingest/webhook as an http.Handler that
// validates an HMAC signature, rate-limits per IP, parses one reading,
// hands it to the pipeline, and returns 202 Accepted.
type WebhookAdapter struct {
	cfg     WebhookConfig
	submit  Submitter
	limiter *ipLimiter

	accepted atomic.Uint64
	rejected struct {
		signature   atomic.Uint64
		body        atomic.Uint64
		decode      atomic.Uint64
		rateLimited atomic.Uint64
	}
}

// NewWebhookAdapter constructs a webhook handler.
func NewWebhookAdapter(submit Submitter, cfg WebhookConfig) *WebhookAdapter {
	if cfg.RatePerMinute < 0 {
		cfg.RatePerMinute = 0
	}
	if cfg.BodyByteLimit <= 0 {
		cfg.BodyByteLimit = 64 * 1024
	}
	if cfg.ClockNow == nil {
		cfg.ClockNow = time.Now
	}
	limit := DefaultRateLimit
	if cfg.RatePerMinute > 0 {
		limit = cfg.RatePerMinute
	}
	return &WebhookAdapter{
		cfg:     cfg,
		submit:  submit,
		limiter: newIPLimiter(limit, time.Minute, cfg.ClockNow),
	}
}

// webhookPayload is the JSON shape gateways post to /ingest/webhook.
// Ts is ISO-8601; we accept either RFC3339 with timezone or no offset
// (interpreted as UTC).
type webhookPayload struct {
	SensorID   string  `json:"sensor_id"`
	Building   string  `json:"building"`
	Zone       string  `json:"zone"`
	SensorType string  `json:"sensor_type"`
	Unit       string  `json:"unit"`
	Value      float64 `json:"value"`
	Quality    float64 `json:"quality"`
	Ts         string  `json:"ts"`
}

// ServeHTTP implements http.Handler.
//
// Order of checks (matters — the spec returns specific status codes):
//   1. POST + content-type? -> 405 / 415
//   2. Body within limit?   -> 413
//   3. Rate limit OK?       -> 429
//   4. Valid HMAC?          -> 401
//   5. Decodable?           -> 400
//   6. Required fields?     -> 400
//   7. Submit + 202
func (w *WebhookAdapter) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(rw, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ct := req.Header.Get("Content-Type")
	if !strings.HasPrefix(ct, "application/json") {
		http.Error(rw, "content-type must be application/json", http.StatusUnsupportedMediaType)
		return
	}

	clientIP := remoteIP(req)
	if !w.limiter.allow(clientIP) {
		w.rejected.rateLimited.Add(1)
		rw.Header().Set("Retry-After", "60")
		http.Error(rw, "rate limit exceeded", http.StatusTooManyRequests)
		return
	}

	body, err := io.ReadAll(io.LimitReader(req.Body, w.cfg.BodyByteLimit+1))
	_ = req.Body.Close()
	if err != nil {
		w.rejected.body.Add(1)
		http.Error(rw, "read body failed", http.StatusBadRequest)
		return
	}
	if int64(len(body)) > w.cfg.BodyByteLimit {
		w.rejected.body.Add(1)
		http.Error(rw, "payload too large", http.StatusRequestEntityTooLarge)
		return
	}

	if err := w.verifyHMAC(req.Header.Get(SignatureHeader), body); err != nil {
		w.rejected.signature.Add(1)
		http.Error(rw, "invalid signature", http.StatusUnauthorized)
		return
	}

	var p webhookPayload
	if err := json.Unmarshal(body, &p); err != nil {
		w.rejected.decode.Add(1)
		http.Error(rw, "invalid json", http.StatusBadRequest)
		return
	}
	if err := p.required(); err != nil {
		w.rejected.decode.Add(1)
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}

	ts, err := parseTimestamp(p.Ts)
	if err != nil {
		w.rejected.decode.Add(1)
		http.Error(rw, "invalid ts: "+err.Error(), http.StatusBadRequest)
		return
	}

	w.submit.Submit(models.SensorReading{
		SensorID:   p.SensorID,
		Building:   p.Building,
		Zone:       p.Zone,
		SensorType: p.SensorType,
		Unit:       p.Unit,
		Value:      p.Value,
		Quality:    p.Quality,
		ReceivedAt: ts,
		Source:     models.SourceWebhook,
	})
	w.accepted.Add(1)
	rw.WriteHeader(http.StatusAccepted)
}

func (w *WebhookAdapter) verifyHMAC(provided string, body []byte) error {
	if w.cfg.HMACSecret == "" {
		// No secret configured -> reject everything. Operators must set
		// WEBHOOK_HMAC_SECRET; failing closed is correct here.
		return errors.New("hmac not configured")
	}
	if provided == "" {
		return errors.New("missing signature header")
	}
	provided = strings.TrimPrefix(provided, "sha256=")
	provided = strings.ToLower(strings.TrimSpace(provided))
	mac := hmac.New(sha256.New, []byte(w.cfg.HMACSecret))
	mac.Write(body)
	expected := hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(provided), []byte(expected)) {
		return errors.New("signature mismatch")
	}
	return nil
}

// WebhookStats is a snapshot of webhook counters for diagnostics.
type WebhookStats struct {
	Accepted          uint64
	RejectedSignature uint64
	RejectedBody      uint64
	RejectedDecode    uint64
	RateLimited       uint64
}

// Stats returns webhook counters.
func (w *WebhookAdapter) Stats() WebhookStats {
	return WebhookStats{
		Accepted:          w.accepted.Load(),
		RejectedSignature: w.rejected.signature.Load(),
		RejectedBody:      w.rejected.body.Load(),
		RejectedDecode:    w.rejected.decode.Load(),
		RateLimited:       w.rejected.rateLimited.Load(),
	}
}

func (p webhookPayload) required() error {
	switch {
	case p.SensorID == "":
		return errors.New("sensor_id required")
	case p.Building == "":
		return errors.New("building required")
	case p.Zone == "":
		return errors.New("zone required")
	case p.SensorType == "":
		return errors.New("sensor_type required")
	}
	return nil
}

// parseTimestamp accepts RFC3339 (with timezone) and falls back to a
// naive ISO format treated as UTC.
func parseTimestamp(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, errors.New("missing ts")
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t.UTC(), nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t.UTC(), nil
	}
	if t, err := time.Parse("2006-01-02T15:04:05", s); err == nil {
		return t.UTC(), nil
	}
	return time.Time{}, errors.New("unparseable ts")
}

func remoteIP(req *http.Request) string {
	if fwd := req.Header.Get("X-Forwarded-For"); fwd != "" {
		if i := strings.Index(fwd, ","); i >= 0 {
			return strings.TrimSpace(fwd[:i])
		}
		return strings.TrimSpace(fwd)
	}
	host, _, err := net.SplitHostPort(req.RemoteAddr)
	if err != nil {
		return req.RemoteAddr
	}
	return host
}

// ---------------------------------------------------------------
// Per-IP rate limiter — fixed window. Memory-bounded by mu.gc().
// ---------------------------------------------------------------

type ipLimiter struct {
	limit    int
	window   time.Duration
	now      func() time.Time
	mu       sync.Mutex
	counters map[string]*ipWindow
}

type ipWindow struct {
	start time.Time
	count int
}

func newIPLimiter(limit int, window time.Duration, now func() time.Time) *ipLimiter {
	if now == nil {
		now = time.Now
	}
	return &ipLimiter{
		limit:    limit,
		window:   window,
		now:      now,
		counters: make(map[string]*ipWindow),
	}
}

// allow returns true if this IP is under the rate limit. Atomic.
func (l *ipLimiter) allow(ip string) bool {
	if l.limit <= 0 {
		return true
	}
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()

	w, ok := l.counters[ip]
	if !ok || now.Sub(w.start) >= l.window {
		l.counters[ip] = &ipWindow{start: now, count: 1}
		// Opportunistic GC: if the table grew, drop expired windows.
		if len(l.counters) > 1024 {
			for k, v := range l.counters {
				if now.Sub(v.start) >= l.window {
					delete(l.counters, k)
				}
			}
		}
		return true
	}
	if w.count >= l.limit {
		return false
	}
	w.count++
	return true
}

// suppress "unused" warning in tests where Submitter is the only role.
var _ http.Handler = (*WebhookAdapter)(nil)

// LogReject is a tiny helper for tests / startup logging.
func (w *WebhookAdapter) LogReject(reason string) {
	slog.Warn("webhook reject", "reason", reason)
}
