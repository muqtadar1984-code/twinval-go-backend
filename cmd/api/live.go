package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/twinval/api"
	"github.com/twinval/internal/ingestion"
	"github.com/twinval/internal/ingestion/adapters"
	ingconfig "github.com/twinval/internal/ingestion/config"
	"github.com/twinval/internal/ingestion/db"
	"github.com/twinval/internal/ingestion/humanobs"
	"github.com/twinval/internal/ingestion/metrics"
	"github.com/twinval/internal/ingestion/models"
)

// liveBundle wraps every component the live pipeline owns so the main
// shutdown sequence can stop them in order.
type liveBundle struct {
	pipeline   *ingestion.Pipeline
	mqtt       *adapters.MQTTAdapter
	modbus     *adapters.ModbusAdapter
	writerPool *db.WriterPool
	dbPool     DBPoolCloser
	metrics    *metrics.Server
}

// DBPoolCloser is the narrow interface needed for ordered shutdown of
// the pgx pool. *pgxpool.Pool satisfies this via its Close() method.
type DBPoolCloser interface {
	Close()
}

// startLivePipeline builds the entire live ingestion graph, mounts the
// webhook adapter on `mux`, starts adapters, and returns a function
// that stops everything in order.
//
// The simulated path is unchanged when this function is NOT called —
// callers must guard with `cfg.IsLive()`.
func startLivePipeline(
	ctx context.Context,
	cfg ingconfig.IngestionConfig,
	hub *api.Hub,
	mux *http.ServeMux,
) (*liveBundle, error) {
	if !cfg.IsLive() {
		return nil, errors.New("live pipeline started in non-live config")
	}

	log.Printf("ingestion: live mode — bootstrapping pipeline")
	log.Printf("ingestion: aggregation_window=%s ema_alpha=%.2f human_obs_window=%s",
		cfg.AggregationWindow, cfg.EMAAlpha, cfg.HumanObsWindow)

	// Register Prometheus metrics on the default registry. Subsequent
	// calls are no-ops, so this is safe even if main is invoked twice
	// in a test harness.
	if err := metrics.Register(prometheus.DefaultRegisterer); err != nil {
		return nil, fmt.Errorf("metrics register: %w", err)
	}

	// 1. Database pool + migrations.
	dbCtx, dbCancel := context.WithTimeout(ctx, 30*time.Second)
	defer dbCancel()
	pool, err := db.NewPool(dbCtx, cfg.DBURL)
	if err != nil {
		return nil, fmt.Errorf("db pool: %w", err)
	}
	if err := db.MigrateUp(dbCtx, pool); err != nil {
		pool.Close()
		return nil, fmt.Errorf("db migrate: %w", err)
	}
	log.Printf("ingestion: db migrations applied")

	writerPool := db.NewWriterPool(pool, cfg.DBWriteWorkers, 4096)
	log.Printf("ingestion: writer pool started (workers=%d)", cfg.DBWriteWorkers)

	// 2. Human-observation CI modifier (read-only on portal tables).
	fetcher := humanobs.NewPgFetcher(pool)
	modifier := humanobs.NewModifier(fetcher, cfg.HumanObsWindow).
		WithMaxPositiveDelta(cfg.HumanObsMaxPositiveDelta)

	// 3. Static property valuation registry.
	// Phase 4 wires sensible defaults; Phase 5+ can read these from a
	// portal table or a YAML override file.
	registry := ingestion.NewStaticRegistry()
	registry.SetDefault(ingestion.PropertyValuation{
		LandValue:              500_000,
		StructureValue:         1_000_000,
		Currency:               "MYR",
		ChronologicalAge:       10,
		MaintenanceSensitivity: 0.5,
	})

	// 4. Pipeline stages.
	validator := ingestion.NewValidator(cfg.SensorBounds, ingestion.QualityFloor)
	denoiser := ingestion.NewDenoiser(cfg.EMAAlpha)
	normaliser := ingestion.NewNormaliser(cfg.SensorBounds)
	aggregator := ingestion.NewAggregator(cfg.AggregationWindow, nil)
	computer := ingestion.NewComputer(registry, modifier)

	// 5. Sinks: hub broadcast + DB persist for every computed snapshot.
	hubSink := ingestion.NewHubBroadcaster(hub)
	dbSink := newDBSink(writerPool)

	pipeline := ingestion.NewPipeline(ingestion.PipelineOptions{
		Validator:  validator,
		Denoiser:   denoiser,
		Normaliser: normaliser,
		Aggregator: aggregator,
		Computer:   computer,
		Sinks:      []ingestion.Sink{hubSink, dbSink},
	})
	pipeline.Start()

	// 6. Adapters.
	webhook := adapters.NewWebhookAdapter(pipelineSubmitter{p: pipeline}, adapters.WebhookConfig{
		HMACSecret: cfg.WebhookHMACSecret,
	})
	mux.Handle("/ingest/webhook", webhook)

	bundle := &liveBundle{
		pipeline:   pipeline,
		writerPool: writerPool,
		dbPool:     pool,
	}

	if cfg.MQTTBrokerURL != "" {
		mqttAdapter := adapters.NewMQTTAdapter(pipelineSubmitter{p: pipeline}, adapters.MQTTConfig{
			BrokerURL:    cfg.MQTTBrokerURL,
			ClientID:     cfg.MQTTClientID,
			TLS:          cfg.MQTTTLS,
			Username:     cfg.MQTTUsername,
			Password:     cfg.MQTTPassword,
			QoS:          1,
			MaxReconnect: 60 * time.Second,
		})
		startCtx, startCancel := context.WithTimeout(ctx, 30*time.Second)
		if err := mqttAdapter.Start(startCtx); err != nil {
			startCancel()
			log.Printf("ingestion: MQTT adapter NOT started — %v (pipeline keeps running)", err)
		} else {
			log.Printf("ingestion: MQTT adapter connected to %s", cfg.MQTTBrokerURL)
			bundle.mqtt = mqttAdapter
		}
		startCancel()
	}

	if len(cfg.ModbusMap.Devices) > 0 {
		modbusAdapter := adapters.NewModbusAdapter(adapters.ModbusOptions{
			Map:    cfg.ModbusMap,
			Submit: pipelineSubmitter{p: pipeline},
		})
		if err := modbusAdapter.Start(ctx); err != nil {
			log.Printf("ingestion: Modbus adapter NOT started — %v", err)
		} else {
			log.Printf("ingestion: Modbus adapter polling %d device(s)", len(cfg.ModbusMap.Devices))
			bundle.modbus = modbusAdapter
		}
	}

	// Metrics HTTP server on cfg.MetricsPort. Runs on its own port so
	// the public API isn't polluted with /metrics. Failure to bind is
	// non-fatal — log and keep the pipeline running.
	if cfg.MetricsPort > 0 {
		ms := metrics.NewServer(cfg.MetricsPort, prometheus.DefaultGatherer)
		if err := ms.Start(); err != nil {
			log.Printf("ingestion: metrics server NOT started — %v", err)
		} else {
			log.Printf("ingestion: metrics on :%d/metrics", cfg.MetricsPort)
			bundle.metrics = ms
		}
	}

	log.Printf("ingestion: live pipeline online (webhook=on, mqtt=%v, modbus=%v, metrics=%v)",
		bundle.mqtt != nil, bundle.modbus != nil, bundle.metrics != nil)

	return bundle, nil
}

// shutdown stops every live component in the order mandated by the
// spec: adapters first (no new readings), pipeline drain, writer pool
// flush, db pool close. Each step honors the supplied context budget.
func (b *liveBundle) shutdown(ctx context.Context) {
	if b == nil {
		return
	}
	slog.Info("ingestion: shutdown begin")

	if b.mqtt != nil {
		if err := b.mqtt.Stop(ctx); err != nil {
			slog.Warn("ingestion: mqtt stop", "error", err.Error())
		}
	}
	if b.modbus != nil {
		if err := b.modbus.Stop(ctx); err != nil {
			slog.Warn("ingestion: modbus stop", "error", err.Error())
		}
	}
	if b.pipeline != nil {
		if err := b.pipeline.Stop(ctx); err != nil {
			slog.Warn("ingestion: pipeline stop", "error", err.Error())
		}
	}
	if b.writerPool != nil {
		if err := b.writerPool.Stop(ctx); err != nil {
			slog.Warn("ingestion: writer pool stop", "error", err.Error())
		}
	}
	if b.dbPool != nil {
		b.dbPool.Close()
	}
	if b.metrics != nil {
		if err := b.metrics.Stop(ctx); err != nil {
			slog.Warn("ingestion: metrics server stop", "error", err.Error())
		}
	}
	slog.Info("TwinVal ingestion pipeline shutdown complete")
}

// pipelineSubmitter wraps *Pipeline as an adapters.Submitter. The
// adapters package can't import internal/ingestion (would create a
// cycle), so we wire it through this trivial wrapper.
type pipelineSubmitter struct{ p *ingestion.Pipeline }

func (s pipelineSubmitter) Submit(r models.SensorReading) { s.p.Submit(r) }

// dbSink is the ingestion.Sink that persists every ComputedZoneSnapshot
// via the writer pool. It also queues a raw-reading job for every sensor
// value referenced in the snapshot (synthesised from raw values held on
// the snapshot itself), so the sensor_readings_raw table reflects the
// post-validate stream even though it is written async.
type dbSink struct {
	pool *db.WriterPool
}

func newDBSink(pool *db.WriterPool) *dbSink { return &dbSink{pool: pool} }

func (s *dbSink) Emit(snap models.ComputedZoneSnapshot) {
	if s.pool == nil {
		return
	}
	cp := snap // copy to obtain a stable address for the pointer field
	s.pool.Submit(db.WriteJob{ComputedSnapshot: &cp})
}
