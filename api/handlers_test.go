package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/twinval/internal/compute"
	"github.com/twinval/internal/cryptochain"
	"github.com/twinval/internal/exchange"
	twinsync "github.com/twinval/internal/sync"
)

func setupRegistryWithProperty(t *testing.T, propID string, chainLen int) *PropertyRegistry {
	t.Helper()
	registry := NewPropertyRegistry()

	chain := cryptochain.NewChain(cryptochain.DefaultChainConfig())
	sm := twinsync.New(twinsync.DefaultStateMachineConfig())
	excCfg := exchange.DefaultExchangeConfig()
	excCfg.PropertyID = propID
	exch := exchange.New(excCfg)

	ps := &PropertyState{
		PropertyID:   propID,
		StateMachine: sm,
		Chain:        chain,
		Exchange:     exch,
		Baseline: compute.BaselineMarketValue{
			LandValue:      500_000.0,
			StructureValue: 1_000_000.0,
			Currency:       "MYR",
		},
	}
	ps.UpdateLatest(
		compute.TechnicalIndicators{SHF: 1.0, ESF: 1.0, USS: 0.1, PDP: 0.95, CI: 1.0},
		compute.HealthFactor(0.855),
		1_355_000.0,
		exchange.ExchangeParams{
			PropertyID:   propID,
			RTPMV:        1_355_000.0,
			BidPrice:     1_341_450.0,
			AskPrice:     1_368_550.0,
			Spread:       0.02,
			TradingState: exchange.Active,
		},
	)
	registry.Set(propID, ps)

	for i := 0; i < chainLen; i++ {
		_, err := chain.Append(cryptochain.ValuationToken{
			PropertyID:          propID,
			Timestamp:           int64(1000 + i),
			ConditionedDataHash: "deadbeef",
			IndicatorsHash:      "cafef00d",
			RTPMV:               float64(1_350_000 + i*1000),
			Currency:            "MYR",
		})
		if err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	return registry
}

func setupTestServer(t *testing.T, propID string, chainLen int, defaultLimit int) *httptest.Server {
	t.Helper()
	registry := setupRegistryWithProperty(t, propID, chainLen)
	handlers := &Handlers{
		Registry:          registry,
		CORSOrigin:        "*",
		DefaultChainLimit: defaultLimit,
		Version:           "test",
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", handlers.Health)
	mux.HandleFunc("GET /api/v1/property/{id}/valuation", handlers.Valuation)
	mux.HandleFunc("GET /api/v1/property/{id}/chain", handlers.Chain)
	mux.HandleFunc("GET /api/v1/property/{id}/audit", handlers.Audit)
	return httptest.NewServer(mux)
}

func TestHealth_Returns200WithCorrectJSON(t *testing.T) {
	ts := setupTestServer(t, "PROP-001", 0, 10)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: got %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type: got %q, want application/json", ct)
	}
	var body map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body["status"] != "ok" {
		t.Errorf("status field: got %q, want ok", body["status"])
	}
	if body["version"] == "" {
		t.Error("version field is empty")
	}
}

func TestValuation_ReturnsAllRequiredFields(t *testing.T) {
	ts := setupTestServer(t, "PROP-001", 0, 10)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/v1/property/PROP-001/valuation")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: got %d, want 200", resp.StatusCode)
	}

	var body ValuationResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if body.PropertyID != "PROP-001" {
		t.Errorf("property_id: got %q, want PROP-001", body.PropertyID)
	}
	if body.HealthFactor != 0.855 {
		t.Errorf("health_factor: got %v, want 0.855", body.HealthFactor)
	}
	if body.RTPMV != 1_355_000.0 {
		t.Errorf("rtpmv: got %v, want 1355000", body.RTPMV)
	}
	if body.Currency != "MYR" {
		t.Errorf("currency: got %q, want MYR", body.Currency)
	}
	if body.Exchange.TradingState != exchange.Active {
		t.Errorf("exchange.trading_state: got %v, want Active", body.Exchange.TradingState)
	}
}

func TestValuation_UnknownPropertyReturns404(t *testing.T) {
	ts := setupTestServer(t, "PROP-001", 0, 10)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/v1/property/UNKNOWN/valuation")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

func TestChain_DefaultLimitApplied(t *testing.T) {
	ts := setupTestServer(t, "PROP-001", 7, 3)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/v1/property/PROP-001/chain")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var body ChainResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Total != 7 {
		t.Errorf("total: got %d, want 7", body.Total)
	}
	if body.Returned != 3 {
		t.Errorf("returned: got %d, want 3", body.Returned)
	}
	if len(body.Tokens) != 3 {
		t.Errorf("tokens len: got %d, want 3", len(body.Tokens))
	}
	// Latest 3 tokens out of 7 → IDs 5, 6, 7
	if body.Tokens[0].TokenID != 5 || body.Tokens[2].TokenID != 7 {
		t.Errorf("token IDs: got %v..%v, want 5..7", body.Tokens[0].TokenID, body.Tokens[2].TokenID)
	}
}

func TestChain_LimitQueryParamOverridesDefault(t *testing.T) {
	ts := setupTestServer(t, "PROP-001", 7, 3)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/v1/property/PROP-001/chain?limit=2")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var body ChainResponse
	json.NewDecoder(resp.Body).Decode(&body)
	if body.Returned != 2 || len(body.Tokens) != 2 {
		t.Errorf("returned/tokens len: got %d/%d, want 2/2", body.Returned, len(body.Tokens))
	}
}

func TestChain_LimitGreaterThanChainReturnsAll(t *testing.T) {
	ts := setupTestServer(t, "PROP-001", 4, 3)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/v1/property/PROP-001/chain?limit=100")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var body ChainResponse
	json.NewDecoder(resp.Body).Decode(&body)
	if body.Returned != 4 {
		t.Errorf("returned: got %d, want 4", body.Returned)
	}
}

func TestAudit_ReturnsAllRecords(t *testing.T) {
	ts := setupTestServer(t, "PROP-001", 5, 10)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/v1/property/PROP-001/audit")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: got %d, want 200", resp.StatusCode)
	}

	var body AuditResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.PropertyID != "PROP-001" {
		t.Errorf("property_id: got %q, want PROP-001", body.PropertyID)
	}
	if len(body.Audit) != 5 {
		t.Errorf("audit len: got %d, want 5", len(body.Audit))
	}
	// Frozen parameters from DefaultChainConfig
	if body.Audit[0].FrozenParameters.FirstTradingThreshold != 0.7 {
		t.Errorf("FirstTradingThreshold: got %v, want 0.7",
			body.Audit[0].FrozenParameters.FirstTradingThreshold)
	}
}
