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
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/twinval/api"
	"github.com/twinval/config"
	"github.com/twinval/internal/compute"
	"github.com/twinval/internal/condition"
	"github.com/twinval/internal/cryptochain"
	"github.com/twinval/internal/exchange"
	"github.com/twinval/internal/ingest"
	twinsync "github.com/twinval/internal/sync"
)

func main() {
	cfg := config.LoadFromEnv()

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

	// Bootstrap the single configured property.
	propState := bootstrapProperty(ctx, cfg)
	registry.Set(cfg.PropertyID, propState)

	// Set up ingest webhook receiver. We mount its Handler() on the main
	// mux instead of letting it bind its own listener.
	ingestCfg := ingest.DefaultIngestConfig()
	ingestCfg.WebhookPath = "/ingest/webhook"
	receiver := ingest.NewWebhookReceiver(ingestCfg)

	batchCh := receiver.Subscribe()
	go runPipeline(ctx, cfg, registry, hub, batchCh)

	handlers := &api.Handlers{
		Registry:          registry,
		CORSOrigin:        cfg.CORSOrigin,
		DefaultChainLimit: cfg.ChainLimit,
		Version:           "1.0.0",
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", handlers.Health)
	mux.HandleFunc("GET /api/v1/property/{id}/valuation", handlers.Valuation)
	mux.HandleFunc("GET /api/v1/property/{id}/chain", handlers.Chain)
	mux.HandleFunc("GET /api/v1/property/{id}/audit", handlers.Audit)
	mux.Handle("/ingest/webhook", receiver.Handler())
	mux.Handle("/ws/property/{id}", api.WebSocketHandler(hub))

	server := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           corsMiddleware(cfg.CORSOrigin, mux),
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("twinval-api listening on :%s (property=%s, currency=%s)",
			cfg.Port, cfg.PropertyID, cfg.Currency)
		if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			log.Printf("server error: %v", err)
			cancel()
		}
	}()

	<-ctx.Done()
	log.Println("shutdown initiated")

	shutdownCtx, sc := context.WithTimeout(context.Background(), 5*time.Second)
	defer sc()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown error: %v", err)
	}
	log.Println("twinval-api stopped")
}

// bootstrapProperty wires the per-property runtimes for one PropertyID
// and starts the StateMachine's Run goroutine bound to ctx.
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

// runPipeline consumes RawSensorBatch values and runs them through the
// full TwinVal pipeline, publishing the result to the WS hub.
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

// processBatch runs a single batch through the pipeline. Single-property
// routing for now — every batch is attributed to cfg.PropertyID.
func processBatch(
	cfg config.AppConfig,
	registry *api.PropertyRegistry,
	hub *api.Hub,
	batch condition.RawSensorBatch,
	condCfg condition.ConditioningConfig,
	indCfgs compute.AllIndicatorConfigs,
) {
	ps, ok := registry.Get(cfg.PropertyID)
	if !ok {
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
		log.Printf("circuit breaker tripped: vol=%.4f threshold=%.4f prior=%s",
			breaker.Volatility, breaker.Threshold,
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

// corsMiddleware adds permissive CORS headers to every response and
// short-circuits OPTIONS preflight requests with 204 No Content. It
// is registered as the outermost handler so per-route handlers do not
// need to manage CORS themselves.
func corsMiddleware(origin string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
