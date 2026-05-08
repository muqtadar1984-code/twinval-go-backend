package adapters

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/twinval/internal/ingestion/metrics"
	"github.com/twinval/internal/ingestion/models"
)

// TopicPrefix is the literal first segment of every TwinVal MQTT topic:
//
//	twinval/{building}/{zone}/{sensor_type}/{sensor_id}
const TopicPrefix = "twinval"

// SubscribeWildcard is what we subscribe to on connect. Paho's "+" matches
// exactly one topic level, so this catches every well-formed sensor topic.
const SubscribeWildcard = "twinval/+/+/+/+"

// MQTTConfig configures the MQTT adapter.
type MQTTConfig struct {
	BrokerURL     string        // e.g. mqtts://broker.iium.twinval:8883
	ClientID      string        // unique per ingestion replica
	Username      string        // optional
	Password      string        // optional
	TLS           bool          // require TLS on the connection
	QoS           byte          // 0..2; spec mandates 1
	KeepAlive     time.Duration // default 30s
	MaxReconnect  time.Duration // upper bound on backoff; spec 60s

	// TLSConfig overrides the default TLS handshake config when TLS=true.
	// Optional; nil means use sensible defaults (system CAs, no skipping).
	TLSConfig *tls.Config
}

// MQTTAdapter subscribes to twinval/+/+/+/+ and submits each well-formed
// message as a SensorReading.
type MQTTAdapter struct {
	cfg     MQTTConfig
	submit  Submitter
	client  mqtt.Client
	now     func() time.Time

	connected      atomic.Bool
	reconnects     atomic.Uint64
	messages       atomic.Uint64
	parseFailures  atomic.Uint64
	decodeFailures atomic.Uint64
}

// NewMQTTAdapter constructs the adapter but does NOT connect. Call Start
// to dial the broker.
func NewMQTTAdapter(submit Submitter, cfg MQTTConfig) *MQTTAdapter {
	if cfg.QoS == 0 {
		cfg.QoS = 1
	}
	if cfg.KeepAlive <= 0 {
		cfg.KeepAlive = 30 * time.Second
	}
	if cfg.MaxReconnect <= 0 {
		cfg.MaxReconnect = 60 * time.Second
	}
	return &MQTTAdapter{
		cfg:    cfg,
		submit: submit,
		now:    time.Now,
	}
}

// Start dials the broker and subscribes. Returns the first connection
// error if the synchronous connect fails. Reconnects after the first
// successful connect are handled by paho (auto-reconnect=true).
func (a *MQTTAdapter) Start(ctx context.Context) error {
	if a.cfg.BrokerURL == "" {
		return errors.New("MQTT_BROKER_URL is empty")
	}
	opts := mqtt.NewClientOptions().
		AddBroker(a.cfg.BrokerURL).
		SetClientID(a.cfg.ClientID).
		SetCleanSession(false).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetMaxReconnectInterval(a.cfg.MaxReconnect).
		SetKeepAlive(a.cfg.KeepAlive).
		SetOrderMatters(false)

	if a.cfg.Username != "" {
		opts.SetUsername(a.cfg.Username)
	}
	if a.cfg.Password != "" {
		opts.SetPassword(a.cfg.Password)
	}
	if a.cfg.TLS {
		if a.cfg.TLSConfig != nil {
			opts.SetTLSConfig(a.cfg.TLSConfig)
		} else {
			opts.SetTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12})
		}
	}

	opts.SetOnConnectHandler(func(c mqtt.Client) {
		a.connected.Store(true)
		slog.Info("mqtt: connected", "broker", a.cfg.BrokerURL)
		token := c.Subscribe(SubscribeWildcard, a.cfg.QoS, a.handleMessage)
		token.Wait()
		if err := token.Error(); err != nil {
			slog.Error("mqtt: subscribe failed", "error", err.Error())
		} else {
			slog.Info("mqtt: subscribed", "topic", SubscribeWildcard, "qos", a.cfg.QoS)
		}
	})
	opts.SetConnectionLostHandler(func(_ mqtt.Client, err error) {
		a.connected.Store(false)
		a.reconnects.Add(1)
		metrics.MQTTReconnections.Inc()
		slog.Warn("mqtt: connection lost", "error", err.Error())
	})

	client := mqtt.NewClient(opts)
	a.client = client

	token := client.Connect()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-tokenDone(token):
		if err := token.Error(); err != nil {
			return fmt.Errorf("mqtt connect: %w", err)
		}
	}
	return nil
}

// Stop disconnects from the broker. After Stop, no more messages are
// dispatched. Safe to call before Start (no-op).
func (a *MQTTAdapter) Stop(ctx context.Context) error {
	if a.client == nil {
		return nil
	}
	done := make(chan struct{})
	go func() {
		a.client.Disconnect(uint(disconnectQuiesceMs))
		close(done)
	}()
	select {
	case <-done:
		a.connected.Store(false)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

const disconnectQuiesceMs = 250

// MQTTStats is a snapshot of MQTT counters.
type MQTTStats struct {
	Connected      bool
	Reconnects     uint64
	Messages       uint64
	ParseFailures  uint64
	DecodeFailures uint64
}

// Stats returns adapter counters.
func (a *MQTTAdapter) Stats() MQTTStats {
	return MQTTStats{
		Connected:      a.connected.Load(),
		Reconnects:     a.reconnects.Load(),
		Messages:       a.messages.Load(),
		ParseFailures:  a.parseFailures.Load(),
		DecodeFailures: a.decodeFailures.Load(),
	}
}

// handleMessage is the paho callback. It is invoked from paho's worker
// goroutine — we do NOT do disk or network I/O here, just parse + Submit.
func (a *MQTTAdapter) handleMessage(_ mqtt.Client, msg mqtt.Message) {
	a.messages.Add(1)
	r, ok := DecodeMQTTMessage(msg.Topic(), msg.Payload(), a.now)
	if !ok {
		a.parseFailures.Add(1)
		slog.Debug("mqtt: dropping malformed message",
			"topic", msg.Topic())
		return
	}
	a.submit.Submit(r)
}

// ParseTopic extracts the four metadata segments from a well-formed
// twinval topic. Returns ok=false if the topic doesn't match the
// expected 5-segment shape.
//
// Format: twinval/{building}/{zone}/{sensor_type}/{sensor_id}
func ParseTopic(topic string) (building, zone, sensorType, sensorID string, ok bool) {
	parts := strings.Split(topic, "/")
	if len(parts) != 5 {
		return "", "", "", "", false
	}
	if parts[0] != TopicPrefix {
		return "", "", "", "", false
	}
	building, zone, sensorType, sensorID = parts[1], parts[2], parts[3], parts[4]
	if building == "" || zone == "" || sensorType == "" || sensorID == "" {
		return "", "", "", "", false
	}
	return building, zone, sensorType, sensorID, true
}

// MQTTPayload is the on-wire JSON shape for an MQTT message.
type MQTTPayload struct {
	Value   float64 `json:"value"`
	Quality float64 `json:"quality"`
	Ts      string  `json:"ts"`
	Unit    string  `json:"unit,omitempty"`
}

// DecodeMQTTMessage assembles a SensorReading from a topic + payload.
// Returns (zero, false) on any parse failure — the caller should
// increment a metric and log; the message is dropped silently on the
// pipeline side, by design (a chatty broker can otherwise spam logs).
//
// The `nowFn` argument is the fallback ReceivedAt source when the
// payload's `ts` is missing or malformed.
func DecodeMQTTMessage(topic string, payload []byte, nowFn func() time.Time) (models.SensorReading, bool) {
	building, zone, sensorType, sensorID, ok := ParseTopic(topic)
	if !ok {
		return models.SensorReading{}, false
	}
	var p MQTTPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return models.SensorReading{}, false
	}
	ts, err := parseTimestamp(p.Ts)
	if err != nil {
		if nowFn != nil {
			ts = nowFn()
		} else {
			ts = time.Now()
		}
	}
	return models.SensorReading{
		SensorID:   sensorID,
		Building:   building,
		Zone:       zone,
		SensorType: sensorType,
		Unit:       p.Unit,
		Value:      p.Value,
		Quality:    p.Quality,
		ReceivedAt: ts,
		Source:     models.SourceMQTT,
	}, true
}

// tokenDone wraps a paho Token in a channel so callers can select on it.
func tokenDone(t mqtt.Token) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		t.Wait()
		close(done)
	}()
	return done
}
