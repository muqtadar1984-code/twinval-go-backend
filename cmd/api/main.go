// Command api is the TwinVal Go backend HTTP server.
//
// It wires every internal package into one pipeline:
//
//	ingest webhook → condition.Process → compute indicators / RTPMV
//	             ├→ sync.StateMachine.Update (digital twin state)
//	             ├→ cryptochain.Append      (audit chain)
//	             └→ exchange.Update         (bid/ask + trading state)
//
// Plus REST endpoints for valuation / chain / audit and a WebSocket
// stream of state updates. Configuration comes from environment
// variables (see github.com/twinval/config).
//
// Multi-property mode (TWINVAL_PROPERTY_MODE=ashrae) bootstraps the
// five Malaysian buildings catalogued in internal/ashrae and routes
// each incoming batch to its PropertyState by PropertyID. An embedded
// simulator goroutine (TWINVAL_SIM_ENABLED=true) drives the pipeline
// continuously without external traffic — useful for live demos.
package main

import (
	"context"
	"errors"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/twinval/api"
	"github.com/twinval/config"
	"github.com/twinval/internal/ashrae"
	"github.com/twinval/internal/compute"
	"github.com/twinval/internal/condition"
	"github.com/twinval/internal/cryptochain"
	"github.com/twinval/internal/exchange"
	"github.com/twinval/internal/ingest"
	ingconfig "github.com/twinval/internal/ingestion/config"
	twinsync "github.com/twinval/internal/sync"
)

func main() {
	cfg := config.LoadFromEnv()

	// Live-vs-simulated branch comes from TWINVAL_DATA_SOURCE. Loading
	// it never returns an error in simulated mode (the default), so a
	// vanilla deployment behaves byte-identically to before.
	liveCfg, err := ingconfig.Load()
	if err != nil {
		log.Fatalf("ingestion config: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// SIGINT / SIGTERM → cancel root context.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		s := <-sigCh
		log.Printf("received signal %v, shutting down", s)
		cancel()
	}()

	registry := api.NewPropertyRegistry()
	hub := api.NewHub()

	// Bootstrap one or many properties depending on TWINVAL_PROPERTY_MODE.
	bootstrapAll(ctx, cfg, registry)

	mux := http.NewServeMux()

	handlers := &api.Handlers{
		Registry:          registry,
		CORSOrigin:        cfg.CORSOrigin,
		DefaultChainLimit: cfg.ChainLimit,
		Version:           "1.0.0",
	}
	mux.HandleFunc("GET /health", handlers.Health)
	mux.HandleFunc("GET /api/v1/properties", handlers.Properties)
	mux.HandleFunc("GET /api/v1/property/{id}/valuation", handlers.Valuation)
	mux.HandleFunc("GET /api/v1/property/{id}/chain", handlers.Chain)
	mux.HandleFunc("GET /api/v1/property/{id}/audit", handlers.Audit)
	mux.Handle("/ws/property/{id}", api.WebSocketHandler(hub))

	// SIMULATED MODE — the existing ASHRAE-driven path. Untouched.
	// LIVE MODE — start the new ingestion pipeline; the legacy webhook
	// + simulator are NOT started so the two paths cannot fight over
	// the /ingest/webhook route.
	var liveBundle *liveBundle
	if liveCfg.IsLive() {
		bundle, err := startLivePipeline(ctx, liveCfg, hub, mux)
		if err != nil {
			log.Fatalf("live pipeline: %v", err)
		}
		liveBundle = bundle
	} else {
		// Set up legacy ingest webhook receiver + simulated path. We mount
		// its Handler() on the main mux instead of letting it bind its
		// own listener.
		ingestCfg := ingest.DefaultIngestConfig()
		ingestCfg.WebhookPath = "/ingest/webhook"
		receiver := ingest.NewWebhookReceiver(ingestCfg)

		batchCh := receiver.Subscribe()
		go runPipeline(ctx, cfg, registry, hub, batchCh)

		mux.Handle("/ingest/webhook", receiver.Handler())

		// Optional embedded ASHRAE simulator — feeds the pipeline without
		// requiring external POSTs. Toggled by TWINVAL_SIM_ENABLED.
		if cfg.SimEnabled {
			go runEmbeddedSimulator(ctx, cfg, registry, hub)
		}
	}

	server := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           corsMiddleware(cfg.CORSOrigin, mux),
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("twinval-api listening on :%s (mode=%s, sim=%v, properties=%d, data_source=%s)",
			cfg.Port, cfg.PropertyMode, cfg.SimEnabled, registry.Len(), liveCfg.DataSource)
		if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			log.Printf("server error: %v", err)
			cancel()
		}
	}()

	<-ctx.Done()
	log.Println("shutdown initiated")

	shutdownCtx, sc := context.WithTimeout(context.Background(), 30*time.Second)
	defer sc()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown error: %v", err)
	}
	if liveBundle != nil {
		liveBundle.shutdown(shutdownCtx)
	}
	log.Println("twinval-api stopped")
}

// bootstrapAll registers every property the API will serve. In ashrae
// mode every catalogued building gets its own PropertyState; otherwise
// the single configured PropertyID gets one.
func bootstrapAll(ctx context.Context, cfg config.AppConfig, registry *api.PropertyRegistry) {
	if cfg.PropertyMode == "ashrae" {
		for _, b := range ashrae.Buildings {
			ps := bootstrapASHRAEBuilding(ctx, b)
			registry.Set(b.Key, ps)
			log.Printf("bootstrapped %s — %s (RM %.0fM, structure RM %.0fM)",
				b.Key, b.Name, b.GovtValuation/1e6, b.StructureValue/1e6)
		}
		return
	}
	ps := bootstrapProperty(ctx, cfg)
	registry.Set(cfg.PropertyID, ps)
}

// bootstrapProperty wires the single env-configured property.
func bootstrapProperty(ctx context.Context, cfg config.AppConfig) *api.PropertyState {
	smCfg := twinsync.DefaultStateMachineConfig()
	sm := twinsync.New(smCfg)
	go sm.Run(ctx)

	chainCfg := cryptochain.DefaultChainConfig()
	chainCfg.FrozenParameters.LandValue = cfg.LandValue
	chainCfg.FrozenParameters.StructureValue = cfg.StructureValue
	chain := cryptochain.NewChain(chainCfg)

	excCfg := exchange.DefaultExchangeConfig()
	excCfg.PropertyID = cfg.PropertyID
	exch := exchange.New(excCfg)

	return &api.PropertyState{
		PropertyID:   cfg.PropertyID,
		Name:         cfg.PropertyID,
		StateMachine: sm,
		Chain:        chain,
		Exchange:     exch,
		Baseline: compute.BaselineMarketValue{
			LandValue:      cfg.LandValue,
			StructureValue: cfg.StructureValue,
			Currency:       cfg.Currency,
		},
	}
}

// bootstrapASHRAEBuilding wires one catalogued building from internal/ashrae.
func bootstrapASHRAEBuilding(ctx context.Context, b ashrae.Building) *api.PropertyState {
	smCfg := twinsync.DefaultStateMachineConfig()
	sm := twinsync.New(smCfg)
	go sm.Run(ctx)

	chainCfg := cryptochain.DefaultChainConfig()
	chainCfg.FrozenParameters.LandValue = b.LandValue
	chainCfg.FrozenParameters.StructureValue = b.StructureValue
	chain := cryptochain.NewChain(chainCfg)

	excCfg := exchange.DefaultExchangeConfig()
	excCfg.PropertyID = b.Key
	exch := exchange.New(excCfg)

	return &api.PropertyState{
		PropertyID:   b.Key,
		Name:         b.Name,
		PrimaryUse:   b.PrimaryUse,
		StateMachine: sm,
		Chain:        chain,
		Exchange:     exch,
		Baseline: compute.BaselineMarketValue{
			LandValue:      b.LandValue,
			StructureValue: b.StructureValue,
			Currency:       b.Currency,
		},
	}
}

// runPipeline consumes RawSensorBatch values and routes each batch to
// its PropertyState by batch.PropertyID.
func runPipeline(
	ctx context.Context,
	cfg config.AppConfig,
	registry *api.PropertyRegistry,
	hub *api.Hub,
	batchCh <-chan condition.RawSensorBatch,
) {
	condCfg := condition.DefaultConditioningConfig()
	indCfgs := compute.DefaultAllConfigs()

	for {
		select {
		case <-ctx.Done():
			return
		case batch, ok := <-batchCh:
			if !ok {
				return
			}
			processBatch(cfg, registry, hub, batch, condCfg, indCfgs)
		}
	}
}

// processBatch runs a single batch through the pipeline. PropertyID on
// the batch is the routing key; an empty PropertyID falls back to the
// configured single property for backward compatibility.
func processBatch(
	cfg config.AppConfig,
	registry *api.PropertyRegistry,
	hub *api.Hub,
	batch condition.RawSensorBatch,
	condCfg condition.ConditioningConfig,
	indCfgs compute.AllIndicatorConfigs,
) {
	propertyID := batch.PropertyID
	if propertyID == "" {
		propertyID = cfg.PropertyID
	}
	ps, ok := registry.Get(propertyID)
	if !ok {
		log.Printf("processBatch: unknown PropertyID %q, dropping batch", propertyID)
		return
	}

	// 1. Condition raw readings.
	cd, _ := condition.Process(batch, condCfg)

	// 2. Compute indicators, Health Factor, RTPMV.
	indicators := compute.ComputeAllIndicators(cd, indCfgs)
	hf := compute.ComputeHealthFactor(indicators)
	rtpmv := compute.ComputeRTPMV(ps.Baseline, hf)

	// 3. Drive the digital-twin state machine (async via channel).
	ps.StateMachine.Update(cd)

	// 4. Append to the audit chain.
	tokenInput := cryptochain.ValuationToken{
		PropertyID:          ps.PropertyID,
		Timestamp:           time.Now().UnixNano(),
		ConditionedDataHash: cryptochain.HashConditionedData(cd),
		IndicatorsHash:      cryptochain.HashIndicators(indicators),
		RTPMV:               rtpmv,
		Currency:            ps.Baseline.Currency,
	}
	if _, err := ps.Chain.Append(tokenInput); err != nil {
		log.Printf("chain append: %v", err)
	}

	// 5. Update the exchange (bid/ask + trading state + circuit breaker).
	params, breaker := ps.Exchange.Update(rtpmv, indicators)
	if breaker != nil {
		log.Printf("circuit breaker tripped: property=%s vol=%.4f threshold=%.4f prior=%s",
			ps.PropertyID, breaker.Volatility, breaker.Threshold,
			exchange.TradingStateString(breaker.PriorState))
	}

	// 6. Cache for synchronous REST reads.
	ps.UpdateLatest(indicators, hf, rtpmv, params)

	// 7. Push to WebSocket subscribers.
	hub.BroadcastJSON(ps.PropertyID, api.WSMessage{
		PropertyID:   ps.PropertyID,
		Timestamp:    time.Now().UnixNano(),
		Indicators:   indicators,
		HealthFactor: float64(hf),
		RTPMV:        rtpmv,
		Currency:     ps.Baseline.Currency,
		State:        ps.StateMachine.CurrentState(),
		Exchange:     params,
	})
}

// runEmbeddedSimulator drives the pipeline directly from in-process
// ASHRAE engines — one per property registered in ashrae mode. Each
// engine emits one simulated hour per real-time tick. Single-property
// mode falls back to the legacy random simulator.
func runEmbeddedSimulator(
	ctx context.Context,
	cfg config.AppConfig,
	registry *api.PropertyRegistry,
	hub *api.Hub,
) {
	condCfg := condition.DefaultConditioningConfig()
	indCfgs := compute.DefaultAllConfigs()
	interval := time.Duration(cfg.SimIntervalMs) * time.Millisecond
	if interval <= 0 {
		interval = time.Second
	}

	if cfg.PropertyMode == "ashrae" {
		// Build one engine per catalogued building. Stagger start hours
		// so the diurnal pattern is visible immediately on connect.
		engines := make([]*ashrae.SensorEngine, 0, len(ashrae.Buildings))
		metas := make([]condition.PropertyMeta, 0, len(ashrae.Buildings))
		for i, b := range ashrae.Buildings {
			engines = append(engines, ashrae.NewEngine(b, ashrae.SITE1Weather, 9+i)) // start ~working hours
			age := float64(time.Now().Year() - b.YearBuilt)
			metas = append(metas, condition.PropertyMeta{
				ChronologicalAge:       age,
				MaintenanceSensitivity: 0.6,
				ConditionQuality:       0.85,
			})
		}
		log.Printf("embedded simulator: ashrae mode, %d buildings, tick=%s", len(engines), interval)

		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				for i, e := range engines {
					reading := e.Next()
					batch := reading.ToRawSensorBatch(ashrae.Buildings[i].Key, metas[i], 30)
					processBatch(cfg, registry, hub, batch, condCfg, indCfgs)
				}
			}
		}
	}

	// Legacy single-property simulator: random uniform ranges.
	log.Printf("embedded simulator: single-property mode, tick=%s", interval)
	rng := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 1))
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			batch := simpleRandomBatch(cfg.PropertyID, rng)
			processBatch(cfg, registry, hub, batch, condCfg, indCfgs)
		}
	}
}

// simpleRandomBatch produces a batch of pseudo-realistic sensor readings
// for the legacy single-property simulator path.
func simpleRandomBatch(propertyID string, rng *rand.Rand) condition.RawSensorBatch {
	now := time.Now().UnixNano()
	r := func(lo, hi float64) float64 { return lo + rng.Float64()*(hi-lo) }
	stress := rng.Float64() < 0.05
	vibration := r(0.01, 0.08)
	strain := r(50, 300)
	if stress {
		vibration = 0.25
		strain = 700
	}
	return condition.RawSensorBatch{
		PropertyID:                propertyID,
		WindowStartNs:             now - int64(time.Second),
		WindowEndNs:               now,
		ExpectedReadingsPerSensor: 1,
		Readings: []condition.RawSensorReading{
			{SensorID: "vib1", SensorType: condition.SensorVibration, Zone: "core", TimestampNs: now, Value: vibration, CalibrationDaysAgo: 30},
			{SensorID: "str1", SensorType: condition.SensorStrain, Zone: "beam-A", TimestampNs: now, Value: strain, CalibrationDaysAgo: 30},
			{SensorID: "tmp1", SensorType: condition.SensorTemperature, Zone: "lobby", TimestampNs: now, Value: r(20, 26), CalibrationDaysAgo: 30},
			{SensorID: "hum1", SensorType: condition.SensorHumidity, Zone: "lobby", TimestampNs: now, Value: r(40, 60), CalibrationDaysAgo: 30},
			{SensorID: "pm1", SensorType: condition.SensorPM25, Zone: "lobby", TimestampNs: now, Value: r(5, 20), CalibrationDaysAgo: 30},
			{SensorID: "occ1", SensorType: condition.SensorOccupancy, Zone: "core", TimestampNs: now, Value: r(0.3, 0.9), CalibrationDaysAgo: 30},
			{SensorID: "el1", SensorType: condition.SensorElectrical, Zone: "core", TimestampNs: now, Value: r(0.4, 0.85), CalibrationDaysAgo: 30},
			{SensorID: "wat1", SensorType: condition.SensorWater, Zone: "core", TimestampNs: now, Value: r(0.2, 0.6), CalibrationDaysAgo: 30},
		},
	}
}

// corsMiddleware adds CORS headers to every response and short-circuits
// OPTIONS preflight requests with 204 No Content.
//
// originsCSV may be a single origin, "*" (wildcard), or a comma-separated
// list. When given a list, the request's Origin header is matched against
// the allowlist and the matching origin is echoed back exactly — browsers
// require an exact echo (or "*") rather than a comma-separated list in
// the Access-Control-Allow-Origin response header.
func corsMiddleware(originsCSV string, next http.Handler) http.Handler {
	allowed := parseOrigins(originsCSV)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqOrigin := r.Header.Get("Origin")
		if origin := matchOrigin(allowed, reqOrigin); origin != "" {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Vary", "Origin")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func parseOrigins(csv string) []string {
	parts := strings.Split(csv, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// matchOrigin returns "*" if any allowed entry is "*", otherwise the
// exact request origin if it matches an allowed entry, otherwise "".
func matchOrigin(allowed []string, reqOrigin string) string {
	for _, a := range allowed {
		if a == "*" {
			return "*"
		}
		if a == reqOrigin {
			return reqOrigin
		}
	}
	return ""
}
