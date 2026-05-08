//go:build integration

package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/twinval/internal/ingestion"
	"github.com/twinval/internal/ingestion/adapters"
	"github.com/twinval/internal/ingestion/models"
)

type recordingSink struct {
	mu    sync.Mutex
	items []models.ComputedZoneSnapshot
}

func (r *recordingSink) Emit(s models.ComputedZoneSnapshot) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.items = append(r.items, s)
}

func (r *recordingSink) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.items)
}

// TestIntegration_MQTTToBroadcast exercises the full path:
//
//	publisher -> Mosquitto -> MQTT adapter (subscribe) -> pipeline ->
//	  compute -> recording sink (proxy for HubBroadcaster)
//
// On success, the recording sink receives at least one
// ComputedZoneSnapshot for the building/zone published to.
func TestIntegration_MQTTToBroadcast(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	brokerURL, cleanup := startMosquitto(t, ctx)
	defer cleanup()

	bounds := testBounds()
	reg := ingestion.NewStaticRegistry()
	reg.SetDefault(ingestion.PropertyValuation{
		LandValue: 500_000, StructureValue: 1_000_000, Currency: "MYR",
		ChronologicalAge: 5, MaintenanceSensitivity: 0.5,
	})

	sink := &recordingSink{}
	p := ingestion.NewPipeline(ingestion.PipelineOptions{
		Validator:  ingestion.NewValidator(bounds, 0),
		Denoiser:   ingestion.NewDenoiser(0.3),
		Normaliser: ingestion.NewNormaliser(bounds),
		Aggregator: ingestion.NewAggregator(150*time.Millisecond, nil),
		Computer:   ingestion.NewComputer(reg, nil),
		Sinks:      []ingestion.Sink{sink},
	})
	p.Start()
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = p.Stop(c)
	}()

	mqttAdapter := adapters.NewMQTTAdapter(pipelineSubmitter{p}, adapters.MQTTConfig{
		BrokerURL:    brokerURL,
		ClientID:     "twinval-ingest-test",
		QoS:          1,
		KeepAlive:    10 * time.Second,
		MaxReconnect: 5 * time.Second,
	})
	if err := mqttAdapter.Start(ctx); err != nil {
		t.Fatalf("mqtt adapter start: %v", err)
	}
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = mqttAdapter.Stop(c)
	}()

	// Give the subscription a beat to land before we publish.
	time.Sleep(500 * time.Millisecond)

	// Publish using a fresh client so we don't share state with the adapter.
	pubOpts := mqtt.NewClientOptions().
		AddBroker(brokerURL).
		SetClientID("integration-publisher").
		SetCleanSession(true)
	pub := mqtt.NewClient(pubOpts)
	if tok := pub.Connect(); tok.WaitTimeout(10*time.Second) && tok.Error() != nil {
		t.Fatalf("publisher connect: %v", tok.Error())
	}
	defer pub.Disconnect(250)

	const topic = "twinval/IIUM-Block-A/Roof/temperature/IIUM-T-INTEG-01"
	const payload = `{"value":22.5,"quality":1.0,"ts":"2026-05-08T10:00:00Z","unit":"°C"}`
	if tok := pub.Publish(topic, 1, false, []byte(payload)); tok.WaitTimeout(5*time.Second) && tok.Error() != nil {
		t.Fatalf("publish: %v", tok.Error())
	}

	// Wait for the snapshot to make it through the aggregator window
	// + compute + sink emit.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if sink.count() >= 1 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if sink.count() == 0 {
		t.Fatalf("sink never received a ComputedZoneSnapshot; pipeline=%+v mqtt=%+v",
			p.Snapshot(), mqttAdapter.Stats())
	}

	got := sink.items[0]
	if got.Snapshot.Building != "IIUM-Block-A" || got.Snapshot.Zone != "Roof" {
		t.Errorf("snapshot key wrong: %+v", got.Snapshot.Key())
	}
	if got.RTPMV <= got.LandValue {
		t.Errorf("expected RTPMV > LandValue, got %v <= %v", got.RTPMV, got.LandValue)
	}
}
