// Package ingest implements Patent Module 310 — the input module.
//
// It exposes two ingress paths into the TwinVal pipeline:
//
//  1. WebhookReceiver — sensors POST JSON payloads to a configurable
//     HTTP endpoint. Each accepted payload becomes one
//     condition.RawSensorBatch fanned out to subscribers.
//
//  2. BMSAdapter — periodically polls a configurable Building Management
//     System URL. Each successful poll becomes one
//     condition.RawSensorBatch.
//
// Both surfaces emit condition.RawSensorBatch on buffered, non-blocking
// subscriber channels. Slow subscribers drop messages rather than back-
// pressuring the receiver.
//
// VALIDATION NOTE:
// Wire-format types (WebhookPayload, BMSResponse, BMSReading) carry the
// JSON tags that define the public contract with sensor producers. Do
// not rename JSON tags without coordinating with deployed sensors.
package ingest

import (
	"time"

	"github.com/twinval/internal/condition"
)

// IngestConfig holds every parameter for both ingress paths. A single
// IngestConfig may be shared by a WebhookReceiver and a BMSAdapter, or
// they may use separate configs.
type IngestConfig struct {
	// Webhook
	WebhookListenAddr string // "host:port"; ":0" lets the OS choose
	WebhookPath       string // e.g. "/webhook/sensors"

	// BMS polling
	BMSPollURL      string
	BMSPollInterval time.Duration
	BMSHTTPTimeout  time.Duration

	// Channel sizing for fan-out
	SubscriberBufferSize int

	// Defaults stamped onto every RawSensorBatch produced by ingest.
	// Sensor producers do not transmit property metadata — that is
	// supplied by configuration here.
	DefaultPropertyMeta              condition.PropertyMeta
	DefaultExpectedReadingsPerSensor int
}

// DefaultIngestConfig returns a baseline configuration. Callers must set
// PropertyMeta and the BMSPollURL (and likely the WebhookListenAddr)
// before passing to a constructor.
func DefaultIngestConfig() IngestConfig {
	return IngestConfig{
		WebhookListenAddr:                ":8080",
		WebhookPath:                      "/webhook/sensors",
		BMSPollInterval:                  30 * time.Second,
		BMSHTTPTimeout:                   10 * time.Second,
		SubscriberBufferSize:             64,
		DefaultExpectedReadingsPerSensor: 1,
	}
}

// WebhookPayload is the JSON shape sensors POST to the webhook endpoint.
// Field tags define the public wire format.
type WebhookPayload struct {
	PropertyID    string           `json:"property_id"`
	WindowStartNs int64            `json:"window_start_ns"`
	WindowEndNs   int64            `json:"window_end_ns"`
	Readings      []WebhookReading `json:"readings"`
}

// WebhookReading is one raw reading inside a WebhookPayload.
type WebhookReading struct {
	SensorID           string  `json:"sensor_id"`
	SensorType         string  `json:"sensor_type"` // matches condition.SensorType strings
	Zone               string  `json:"zone"`
	TimestampNs        int64   `json:"timestamp_ns"`
	Value              float64 `json:"value"`
	CalibrationDaysAgo float64 `json:"calibration_days_ago"`
}

// BMSResponse is the JSON shape returned by polling a BMS endpoint. BMS
// systems often expose timestamps in milliseconds and use different
// field names — BMSAdapter handles the translation.
type BMSResponse struct {
	PropertyID string       `json:"property_id"`
	Readings   []BMSReading `json:"readings"`
}

// BMSReading is one raw reading inside a BMSResponse.
type BMSReading struct {
	SensorTag    string  `json:"sensor_tag"`
	Type         string  `json:"type"` // matches condition.SensorType strings
	Zone         string  `json:"zone"`
	TimestampMs  int64   `json:"timestamp_ms"` // milliseconds, converted to ns by adapter
	Reading      float64 `json:"reading"`
	DaysSinceCal float64 `json:"days_since_calibration"`
}

// IngestError carries a suggested HTTP status code alongside a validation
// message. Callers extract via errors.As to decide on a response code.
type IngestError struct {
	Code    int
	Message string
}

// Error implements the error interface.
func (e IngestError) Error() string { return e.Message }

func newBadRequest(msg string) IngestError {
	return IngestError{Code: 400, Message: msg}
}

func newUnprocessable(msg string) IngestError {
	return IngestError{Code: 422, Message: msg}
}
