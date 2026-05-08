package adapters

import (
	"testing"
	"time"

	"github.com/twinval/internal/ingestion/models"
)

func TestParseTopic_Valid(t *testing.T) {
	b, z, st, sid, ok := ParseTopic("twinval/Block A/Zone 1/temperature/IIUM-BLK-A-TEMP-01")
	if !ok {
		t.Fatal("expected ok=true")
	}
	if b != "Block A" || z != "Zone 1" || st != "temperature" || sid != "IIUM-BLK-A-TEMP-01" {
		t.Fatalf("wrong segments: %s / %s / %s / %s", b, z, st, sid)
	}
}

func TestParseTopic_RejectsWrongShape(t *testing.T) {
	cases := []string{
		"twinval/B/Z/temperature",                // too few
		"twinval/B/Z/temperature/X-1/extra",       // too many
		"other/B/Z/temperature/X-1",               // wrong prefix
		"twinval//Z/temperature/X-1",              // empty building
		"twinval/B/Z//X-1",                        // empty sensor_type
	}
	for _, c := range cases {
		t.Run(c, func(t *testing.T) {
			if _, _, _, _, ok := ParseTopic(c); ok {
				t.Fatalf("expected reject for %q", c)
			}
		})
	}
}

func TestDecodeMQTTMessage_HappyPath(t *testing.T) {
	now := func() time.Time { return time.Date(2026, 5, 8, 10, 0, 0, 0, time.UTC) }
	r, ok := DecodeMQTTMessage(
		"twinval/Block A/Roof/temperature/T-1",
		[]byte(`{"value":22.5,"quality":1.0,"ts":"2026-05-08T10:00:00Z","unit":"°C"}`),
		now,
	)
	if !ok {
		t.Fatal("expected ok")
	}
	if r.Building != "Block A" || r.Zone != "Roof" || r.SensorType != "temperature" || r.SensorID != "T-1" {
		t.Errorf("topic mapping wrong: %+v", r)
	}
	if r.Value != 22.5 || r.Quality != 1.0 || r.Unit != "°C" {
		t.Errorf("payload mapping wrong: %+v", r)
	}
	if r.Source != models.SourceMQTT {
		t.Errorf("source should be mqtt, got %s", r.Source)
	}
}

func TestDecodeMQTTMessage_BadTopicReturnsNotOK(t *testing.T) {
	now := func() time.Time { return time.Now() }
	_, ok := DecodeMQTTMessage("garbage", []byte(`{}`), now)
	if ok {
		t.Fatal("bad topic should not decode")
	}
}

func TestDecodeMQTTMessage_BadJSONReturnsNotOK(t *testing.T) {
	now := func() time.Time { return time.Now() }
	_, ok := DecodeMQTTMessage("twinval/B/Z/temperature/T-1", []byte(`{not json}`), now)
	if ok {
		t.Fatal("bad json should not decode")
	}
}

func TestDecodeMQTTMessage_FallsBackToNowOnMissingTs(t *testing.T) {
	now := func() time.Time { return time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC) }
	r, ok := DecodeMQTTMessage(
		"twinval/B/Z/temperature/T-1",
		[]byte(`{"value":22,"quality":1.0}`),
		now,
	)
	if !ok {
		t.Fatal("missing ts should still decode (uses now)")
	}
	if !r.ReceivedAt.Equal(now()) {
		t.Errorf("expected now() fallback, got %v", r.ReceivedAt)
	}
}

func TestDecodeMQTTMessage_FallsBackToNowOnBadTs(t *testing.T) {
	now := func() time.Time { return time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC) }
	r, ok := DecodeMQTTMessage(
		"twinval/B/Z/temperature/T-1",
		[]byte(`{"value":22,"quality":1.0,"ts":"not-a-timestamp"}`),
		now,
	)
	if !ok {
		t.Fatal("bad ts should still decode")
	}
	if !r.ReceivedAt.Equal(now()) {
		t.Errorf("expected now() fallback, got %v", r.ReceivedAt)
	}
}

func TestNewMQTTAdapter_DefaultsApplied(t *testing.T) {
	a := NewMQTTAdapter(&recordingSubmitter{}, MQTTConfig{})
	if a.cfg.QoS != 1 {
		t.Errorf("expected default QoS=1, got %d", a.cfg.QoS)
	}
	if a.cfg.KeepAlive <= 0 {
		t.Errorf("expected default KeepAlive > 0, got %v", a.cfg.KeepAlive)
	}
	if a.cfg.MaxReconnect <= 0 {
		t.Errorf("expected default MaxReconnect > 0, got %v", a.cfg.MaxReconnect)
	}
}

func TestMQTTAdapter_StartFailsOnEmptyBroker(t *testing.T) {
	a := NewMQTTAdapter(&recordingSubmitter{}, MQTTConfig{})
	// No need for a context — this must fail synchronously on validation.
	err := a.Start(nil)
	if err == nil {
		t.Fatal("expected error on empty broker URL")
	}
}
