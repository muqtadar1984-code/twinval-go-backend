package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/twinval/internal/condition"
)

// WebhookReceiver hosts the HTTP endpoint that sensors POST to. Each
// accepted payload becomes one condition.RawSensorBatch fanned out to
// every subscriber. Subscribers receive on buffered channels — slow
// subscribers miss batches rather than block the receiver.
type WebhookReceiver struct {
	cfg IngestConfig

	mu          sync.Mutex
	subscribers []chan condition.RawSensorBatch
	stopped     bool
	actualAddr  string

	server *http.Server
	cancel context.CancelFunc
	done   chan struct{}
}

// NewWebhookReceiver constructs a receiver. It does not bind a port —
// call Start to begin listening. SubscriberBufferSize defaults to 64.
func NewWebhookReceiver(cfg IngestConfig) *WebhookReceiver {
	if cfg.SubscriberBufferSize <= 0 {
		cfg.SubscriberBufferSize = 64
	}
	return &WebhookReceiver{cfg: cfg}
}

// Handler returns the http.Handler that processes webhook POSTs.
// Used internally by Start; exposed for tests via httptest.NewServer.
func (w *WebhookReceiver) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(w.cfg.WebhookPath, w.handlePost)
	return mux
}

// Start binds the listener and serves in a background goroutine.
// Returns immediately. The server shuts down and subscriber channels
// close when ctx is cancelled or Stop is called.
func (w *WebhookReceiver) Start(ctx context.Context) error {
	listener, err := net.Listen("tcp", w.cfg.WebhookListenAddr)
	if err != nil {
		return err
	}

	w.mu.Lock()
	w.actualAddr = listener.Addr().String()
	w.mu.Unlock()

	ctx, w.cancel = context.WithCancel(ctx)
	w.done = make(chan struct{})

	w.server = &http.Server{Handler: w.Handler()}

	go func() {
		_ = w.server.Serve(listener)
	}()

	go func() {
		defer close(w.done)
		<-ctx.Done()
		shutdownCtx, sc := context.WithTimeout(context.Background(), 5*time.Second)
		defer sc()
		_ = w.server.Shutdown(shutdownCtx)
		w.closeSubscribers()
	}()

	return nil
}

// Stop triggers shutdown and blocks until subscribers are closed and
// the HTTP server has fully drained. Idempotent — safe to call multiple
// times. Calling Stop on a never-started receiver is a no-op.
func (w *WebhookReceiver) Stop() {
	if w.cancel != nil {
		w.cancel()
	}
	if w.done != nil {
		<-w.done
	}
}

// Addr returns the actual listening address (host:port). Useful when
// the configured address used port 0 — Addr returns the OS-assigned port.
func (w *WebhookReceiver) Addr() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.actualAddr
}

// Subscribe returns a buffered channel that receives a RawSensorBatch
// for every accepted webhook POST. Returns a pre-closed channel if the
// receiver has already shut down.
func (w *WebhookReceiver) Subscribe() <-chan condition.RawSensorBatch {
	w.mu.Lock()
	defer w.mu.Unlock()
	ch := make(chan condition.RawSensorBatch, w.cfg.SubscriberBufferSize)
	if w.stopped {
		close(ch)
		return ch
	}
	w.subscribers = append(w.subscribers, ch)
	return ch
}

// handlePost is the HTTP handler. Status codes:
//
//	202 Accepted          — payload validated and emitted
//	400 Bad Request       — body did not parse as JSON
//	405 Method Not Allowed — non-POST request
//	422 Unprocessable     — payload failed semantic validation
func (w *WebhookReceiver) handlePost(rw http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(rw, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer req.Body.Close()

	var payload WebhookPayload
	if err := json.NewDecoder(req.Body).Decode(&payload); err != nil {
		http.Error(rw, "malformed JSON: "+err.Error(), http.StatusBadRequest)
		return
	}

	if err := ValidatePayload(payload); err != nil {
		var ie IngestError
		if errors.As(err, &ie) {
			http.Error(rw, ie.Message, ie.Code)
		} else {
			http.Error(rw, err.Error(), http.StatusUnprocessableEntity)
		}
		return
	}

	w.broadcast(payloadToBatch(payload, w.cfg))
	rw.WriteHeader(http.StatusAccepted)
}

// broadcast sends to every live subscriber, non-blocking per subscriber.
func (w *WebhookReceiver) broadcast(batch condition.RawSensorBatch) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, ch := range w.subscribers {
		select {
		case ch <- batch:
		default:
		}
	}
}

func (w *WebhookReceiver) closeSubscribers() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopped {
		return
	}
	w.stopped = true
	for _, ch := range w.subscribers {
		close(ch)
	}
	w.subscribers = nil
}

// payloadToBatch translates a validated webhook payload into the
// internal RawSensorBatch shape. PropertyID is forwarded so multi-
// property API layers can route the batch to the correct PropertyState.
func payloadToBatch(p WebhookPayload, cfg IngestConfig) condition.RawSensorBatch {
	readings := make([]condition.RawSensorReading, len(p.Readings))
	for i, r := range p.Readings {
		readings[i] = condition.RawSensorReading{
			SensorID:           r.SensorID,
			SensorType:         condition.SensorType(r.SensorType),
			Zone:               r.Zone,
			TimestampNs:        r.TimestampNs,
			Value:              r.Value,
			CalibrationDaysAgo: r.CalibrationDaysAgo,
		}
	}
	return condition.RawSensorBatch{
		PropertyID:                p.PropertyID,
		Readings:                  readings,
		WindowStartNs:             p.WindowStartNs,
		WindowEndNs:               p.WindowEndNs,
		ExpectedReadingsPerSensor: cfg.DefaultExpectedReadingsPerSensor,
		PropertyMeta:              cfg.DefaultPropertyMeta,
	}
}
