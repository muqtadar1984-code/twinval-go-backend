// Package metrics declares every Prometheus metric the live ingestion
// pipeline emits, plus a thin HTTP server that exposes /metrics on the
// configured port.
//
// Metrics are package-level Vars but NOT auto-registered. The live
// bootstrap calls Register(prometheus.DefaultRegisterer) once; tests
// can pass a fresh prometheus.NewRegistry() to avoid global pollution.
//
// All metric names + labels match the spec exactly. Dashboards filter
// on these strings — keep them stable.
package metrics

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// All metric Vars are package-level so call sites can `metrics.X.Inc()`
// without dependency injection. Register() wires them to whichever
// registry the operator selects.
var (
	ReadingsReceived = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "twinval_readings_received_total",
			Help: "Sensor readings accepted by the live ingestion pipeline.",
		},
		[]string{"source", "building", "zone", "sensor_type"},
	)

	ReadingsDropped = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "twinval_readings_dropped_total",
			Help: "Sensor readings rejected by the validator, by reason.",
		},
		[]string{"reason"},
	)

	ZoneSnapshotsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "twinval_zone_snapshots_total",
			Help: "ZoneSnapshots emitted by the aggregator stage.",
		},
		[]string{"building", "zone"},
	)

	RTPMVCurrent = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "twinval_rtpmv_current",
			Help: "Most recent RTPMV value computed for this zone.",
		},
		[]string{"building", "zone"},
	)

	HealthFactorCurrent = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "twinval_health_factor_current",
			Help: "Most recent Health Factor computed for this zone.",
		},
		[]string{"building", "zone"},
	)

	// PipelineLatency observes the wall-clock latency from a sensor
	// reading entering the pipeline (ReceivedAt on the latest reading
	// in the snapshot's window) to the moment the computed snapshot
	// is emitted to sinks.
	PipelineLatency = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "twinval_pipeline_latency_seconds",
			Help:    "End-to-end latency from ingestion to broadcast, seconds.",
			Buckets: prometheus.ExponentialBucketsRange(0.05, 60.0, 10),
		},
	)

	DBWriteErrors = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "twinval_db_write_errors_total",
			Help: "DB writes that failed in the async writer pool.",
		},
	)

	MQTTReconnections = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "twinval_mqtt_reconnections_total",
			Help: "MQTT broker reconnection events.",
		},
	)

	HumanObsApplied = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "twinval_human_obs_applied_total",
			Help: "Human observations contributing to the CI delta, by severity.",
		},
		[]string{"building", "zone", "severity"},
	)
)

// allCollectors lists every metric so Register() can register them all
// in one call. Order doesn't matter to Prometheus.
var allCollectors = []prometheus.Collector{
	ReadingsReceived,
	ReadingsDropped,
	ZoneSnapshotsTotal,
	RTPMVCurrent,
	HealthFactorCurrent,
	PipelineLatency,
	DBWriteErrors,
	MQTTReconnections,
	HumanObsApplied,
}

var registerOnce sync.Once
var registerErr error

// Register registers every ingestion metric on `reg`. Safe to call
// multiple times — subsequent calls are no-ops. Returns the error
// from the first registration attempt (typically a duplicate-name
// conflict if the same registerer was used twice).
func Register(reg prometheus.Registerer) error {
	registerOnce.Do(func() {
		for _, c := range allCollectors {
			if err := reg.Register(c); err != nil {
				// Duplicate registration is benign during tests where
				// init() may have already wired things up.
				var are prometheus.AlreadyRegisteredError
				if !errors.As(err, &are) {
					registerErr = fmt.Errorf("register metric: %w", err)
					return
				}
			}
		}
	})
	return registerErr
}

// Server is the metrics HTTP server. Encapsulates Start + Stop so the
// live bootstrap can register it with the lifecycle hooks.
type Server struct {
	srv *http.Server
}

// NewServer returns a server that will expose /metrics on `port`.
// It does NOT start listening until Start is called.
func NewServer(port int, gatherer prometheus.Gatherer) *Server {
	mux := http.NewServeMux()
	if gatherer == nil {
		gatherer = prometheus.DefaultGatherer
	}
	mux.Handle("/metrics", promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	}))
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("twinval ingestion metrics — see /metrics\n"))
	})
	return &Server{srv: &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}}
}

// Start begins listening. Returns nil immediately (server runs in a
// background goroutine) — fatal listen errors are logged.
func (s *Server) Start() error {
	go func() {
		if err := s.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("metrics: serve failed", "error", err.Error())
		}
	}()
	return nil
}

// Stop shuts the server down with the supplied context budget.
func (s *Server) Stop(ctx context.Context) error {
	if s == nil || s.srv == nil {
		return nil
	}
	return s.srv.Shutdown(ctx)
}

// ResetForTest zeros every counter / gauge so tests can assert
// post-state without inheriting noise from earlier tests in the
// package. Vector metrics drop their per-label-set series entirely.
//
// NOT for production use.
func ResetForTest() {
	ReadingsReceived.Reset()
	ReadingsDropped.Reset()
	ZoneSnapshotsTotal.Reset()
	RTPMVCurrent.Reset()
	HealthFactorCurrent.Reset()
	HumanObsApplied.Reset()
	// Singletons can't .Reset() — best we can do is leave them.
}
