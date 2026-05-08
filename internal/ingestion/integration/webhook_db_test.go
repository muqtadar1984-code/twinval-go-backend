//go:build integration

package integration

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/twinval/internal/ingestion"
	"github.com/twinval/internal/ingestion/adapters"
	"github.com/twinval/internal/ingestion/db"
	"github.com/twinval/internal/ingestion/models"
)

// pipelineSubmitter wraps *Pipeline as an adapters.Submitter — same
// trick as cmd/api/live.go (avoids an import cycle).
type pipelineSubmitter struct{ p *ingestion.Pipeline }

func (s pipelineSubmitter) Submit(r models.SensorReading) { s.p.Submit(r) }

type writerSink struct{ pool *db.WriterPool }

func (s writerSink) Emit(snap models.ComputedZoneSnapshot) {
	cp := snap
	s.pool.Submit(db.WriteJob{ComputedSnapshot: &cp})
}

// TestIntegration_WebhookToDB exercises the full path:
//
//	signed POST -> webhook adapter -> pipeline -> compute -> writer pool -> Postgres
//
// The expected outcome is a row in zone_snapshots with the right
// building+zone+data_source within a few seconds of the POST.
func TestIntegration_WebhookToDB(t *testing.T) {
	ctx := context.Background()
	dsn, cleanup := startPostgres(t, ctx)
	defer cleanup()

	pool, err := db.NewPool(ctx, dsn)
	if err != nil {
		t.Fatalf("db pool: %v", err)
	}
	defer pool.Close()

	if err := db.MigrateUp(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writerPool := db.NewWriterPool(pool, 2, 64)
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = writerPool.Stop(c)
	}()

	bounds := testBounds()
	reg := ingestion.NewStaticRegistry()
	reg.SetDefault(ingestion.PropertyValuation{
		LandValue: 500_000, StructureValue: 1_000_000, Currency: "MYR",
		ChronologicalAge: 5, MaintenanceSensitivity: 0.5,
	})

	p := ingestion.NewPipeline(ingestion.PipelineOptions{
		Validator:  ingestion.NewValidator(bounds, 0),
		Denoiser:   ingestion.NewDenoiser(0.3),
		Normaliser: ingestion.NewNormaliser(bounds),
		Aggregator: ingestion.NewAggregator(150*time.Millisecond, nil),
		Computer:   ingestion.NewComputer(reg, nil),
		Sinks:      []ingestion.Sink{writerSink{pool: writerPool}},
	})
	p.Start()
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = p.Stop(c)
	}()

	wh := adapters.NewWebhookAdapter(pipelineSubmitter{p}, adapters.WebhookConfig{
		HMACSecret: "integration-secret",
	})

	body := []byte(`{"sensor_id":"IIUM-T-1","building":"IT-Block-A","zone":"Roof","sensor_type":"temperature","unit":"°C","value":23.4,"quality":1.0,"ts":"2026-05-08T10:00:00Z"}`)
	req := httptest.NewRequest(http.MethodPost, "/ingest/webhook", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(adapters.SignatureHeader, signHMAC(body, "integration-secret"))
	rec := httptest.NewRecorder()
	wh.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("webhook returned %d, body=%s", rec.Code, rec.Body.String())
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var n int
		err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM zone_snapshots
			WHERE building='IT-Block-A' AND zone='Roof' AND data_source='live'`).Scan(&n)
		if err == nil && n >= 1 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("zone_snapshots row never appeared within 10s; pipeline=%+v writer=%+v",
		p.Snapshot(), writerPool.Stats())
}

// TestIntegration_WebhookToDB_BadSignatureNoDBRow makes sure a rejected
// signature never reaches the writer pool.
func TestIntegration_WebhookToDB_BadSignatureNoDBRow(t *testing.T) {
	ctx := context.Background()
	dsn, cleanup := startPostgres(t, ctx)
	defer cleanup()

	pool, err := db.NewPool(ctx, dsn)
	if err != nil {
		t.Fatalf("db pool: %v", err)
	}
	defer pool.Close()
	if err := db.MigrateUp(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writerPool := db.NewWriterPool(pool, 2, 64)
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = writerPool.Stop(c)
	}()

	bounds := testBounds()
	reg := ingestion.NewStaticRegistry()
	reg.SetDefault(ingestion.PropertyValuation{
		LandValue: 500_000, StructureValue: 1_000_000, Currency: "MYR",
	})
	p := ingestion.NewPipeline(ingestion.PipelineOptions{
		Validator:  ingestion.NewValidator(bounds, 0),
		Denoiser:   ingestion.NewDenoiser(0.3),
		Normaliser: ingestion.NewNormaliser(bounds),
		Aggregator: ingestion.NewAggregator(150*time.Millisecond, nil),
		Computer:   ingestion.NewComputer(reg, nil),
		Sinks:      []ingestion.Sink{writerSink{pool: writerPool}},
	})
	p.Start()
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = p.Stop(c)
	}()

	wh := adapters.NewWebhookAdapter(pipelineSubmitter{p}, adapters.WebhookConfig{
		HMACSecret: "secret-A",
	})

	body := []byte(`{"sensor_id":"X","building":"B","zone":"Z","sensor_type":"temperature","value":22,"quality":1,"ts":"2026-05-08T10:00:00Z"}`)
	req := httptest.NewRequest(http.MethodPost, "/ingest/webhook", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(adapters.SignatureHeader, signHMAC(body, "wrong-secret"))
	rec := httptest.NewRecorder()
	wh.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}

	// Give the pipeline a chance to drain anything (it shouldn't).
	time.Sleep(500 * time.Millisecond)

	var n int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM zone_snapshots`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 0 {
		t.Fatalf("rejected webhook must NOT reach DB, found %d rows", n)
	}
}
