// Package api hosts the public HTTP and WebSocket surface for the
// TwinVal Go backend. It exposes the cryptochain, valuation snapshot,
// audit-mode export, and the live state stream.
//
// PropertyRegistry is the in-memory, multi-property routing table; it
// holds one PropertyState per listed property. PropertyState aggregates
// the per-property runtimes from the internal packages — StateMachine
// (sync), CryptoChain (cryptochain), and PropertyExchange (exchange)
// — plus the latest computed values cached for synchronous REST reads.
package api

import (
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"sync"

	"github.com/twinval/internal/compute"
	"github.com/twinval/internal/cryptochain"
	"github.com/twinval/internal/exchange"
	twinsync "github.com/twinval/internal/sync"
)

// PropertyState aggregates every per-property runtime.
type PropertyState struct {
	PropertyID   string
	Name         string // human-readable name (e.g. "KLCC Tower")
	PrimaryUse   string // e.g. "Grade A Office", "Healthcare"
	StateMachine *twinsync.StateMachine
	Chain        *cryptochain.CryptoChain
	Exchange     *exchange.PropertyExchange
	Baseline     compute.BaselineMarketValue

	mu                   sync.RWMutex
	latestIndicators     compute.TechnicalIndicators
	latestHF             compute.HealthFactor
	latestRTPMV          float64
	latestExchangeParams exchange.ExchangeParams
}

// UpdateLatest stores the most recent pipeline output for synchronous
// retrieval by REST handlers.
func (ps *PropertyState) UpdateLatest(
	ind compute.TechnicalIndicators,
	hf compute.HealthFactor,
	rtpmv float64,
	ep exchange.ExchangeParams,
) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.latestIndicators = ind
	ps.latestHF = hf
	ps.latestRTPMV = rtpmv
	ps.latestExchangeParams = ep
}

// Snapshot returns a copy of the latest cached values.
func (ps *PropertyState) Snapshot() (
	compute.TechnicalIndicators,
	compute.HealthFactor,
	float64,
	exchange.ExchangeParams,
) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	return ps.latestIndicators, ps.latestHF, ps.latestRTPMV, ps.latestExchangeParams
}

// PropertyRegistry is the goroutine-safe map of PropertyID → *PropertyState.
type PropertyRegistry struct {
	mu         sync.RWMutex
	properties map[string]*PropertyState
}

// NewPropertyRegistry returns an empty registry.
func NewPropertyRegistry() *PropertyRegistry {
	return &PropertyRegistry{properties: make(map[string]*PropertyState)}
}

// Get returns the PropertyState for id, or (nil, false) if absent.
func (r *PropertyRegistry) Get(id string) (*PropertyState, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ps, ok := r.properties[id]
	return ps, ok
}

// Set installs ps under id. Overwrites any prior entry.
func (r *PropertyRegistry) Set(id string, ps *PropertyState) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.properties[id] = ps
}

// Len returns the number of registered properties.
func (r *PropertyRegistry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.properties)
}

// Snapshot returns every registered PropertyState in sorted PropertyID
// order — stable across calls within a process.
func (r *PropertyRegistry) Snapshot() []*PropertyState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	keys := make([]string, 0, len(r.properties))
	for k := range r.properties {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]*PropertyState, 0, len(keys))
	for _, k := range keys {
		out = append(out, r.properties[k])
	}
	return out
}

// Handlers groups the REST handler functions with their shared state.
type Handlers struct {
	Registry          *PropertyRegistry
	CORSOrigin        string
	DefaultChainLimit int
	Version           string
}

// Health responds with a static JSON OK + version string.
// Used by Railway / load balancer health checks.
func (h *Handlers) Health(w http.ResponseWriter, r *http.Request) {
	h.setCORS(w)
	w.Header().Set("Content-Type", "application/json")
	version := h.Version
	if version == "" {
		version = "1.0.0"
	}
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status":  "ok",
		"version": version,
	})
}

// ValuationResponse is the JSON shape returned by /valuation.
type ValuationResponse struct {
	PropertyID   string                      `json:"property_id"`
	Indicators   compute.TechnicalIndicators `json:"indicators"`
	HealthFactor float64                     `json:"health_factor"`
	RTPMV        float64                     `json:"rtpmv"`
	Currency     string                      `json:"currency"`
	Exchange     exchange.ExchangeParams     `json:"exchange"`
	State        twinsync.DigitalTwinState   `json:"state"`
}

// Valuation returns the latest snapshot for a property.
func (h *Handlers) Valuation(w http.ResponseWriter, r *http.Request) {
	h.setCORS(w)
	id := r.PathValue("id")
	ps, ok := h.Registry.Get(id)
	if !ok {
		http.Error(w, "property not found", http.StatusNotFound)
		return
	}
	ind, hf, rtpmv, ep := ps.Snapshot()
	resp := ValuationResponse{
		PropertyID:   ps.PropertyID,
		Indicators:   ind,
		HealthFactor: float64(hf),
		RTPMV:        rtpmv,
		Currency:     ps.Baseline.Currency,
		Exchange:     ep,
		State:        ps.StateMachine.CurrentState(),
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// ChainResponse is the JSON shape returned by /chain.
type ChainResponse struct {
	PropertyID string                        `json:"property_id"`
	Total      int                           `json:"total"`
	Returned   int                           `json:"returned"`
	Tokens     []cryptochain.ValuationToken  `json:"tokens"`
}

// Chain returns the most recent N tokens. Default N = DefaultChainLimit;
// override via ?limit=N. Tokens are returned oldest-to-newest within the
// returned window.
func (h *Handlers) Chain(w http.ResponseWriter, r *http.Request) {
	h.setCORS(w)
	id := r.PathValue("id")
	ps, ok := h.Registry.Get(id)
	if !ok {
		http.Error(w, "property not found", http.StatusNotFound)
		return
	}

	limit := h.DefaultChainLimit
	if l := r.URL.Query().Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 {
			limit = n
		}
	}

	total := ps.Chain.Len()
	start := total - limit
	if start < 0 {
		start = 0
	}
	tokens := make([]cryptochain.ValuationToken, 0, total-start)
	for i := start; i < total; i++ {
		if t, ok := ps.Chain.GetToken(i); ok {
			tokens = append(tokens, t)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(ChainResponse{
		PropertyID: ps.PropertyID,
		Total:      total,
		Returned:   len(tokens),
		Tokens:     tokens,
	})
}

// PropertySummary is the per-property row inside PropertiesResponse.
type PropertySummary struct {
	PropertyID     string  `json:"property_id"`
	Name           string  `json:"name"`
	PrimaryUse     string  `json:"primary_use"`
	LandValue      float64 `json:"land_value"`
	StructureValue float64 `json:"structure_value"`
	GovtValuation  float64 `json:"govt_valuation"`
	Currency       string  `json:"currency"`
	ChainLength    int     `json:"chain_length"`
	LatestRTPMV    float64 `json:"latest_rtpmv"`
}

// PropertiesResponse is the JSON returned by /properties.
type PropertiesResponse struct {
	Total      int               `json:"total"`
	Properties []PropertySummary `json:"properties"`
}

// Properties lists every registered property with a one-line summary.
// Used by the demo page's property dropdown.
func (h *Handlers) Properties(w http.ResponseWriter, r *http.Request) {
	h.setCORS(w)
	all := h.Registry.Snapshot()
	out := make([]PropertySummary, 0, len(all))
	for _, ps := range all {
		_, _, rtpmv, _ := ps.Snapshot()
		out = append(out, PropertySummary{
			PropertyID:     ps.PropertyID,
			Name:           ps.Name,
			PrimaryUse:     ps.PrimaryUse,
			LandValue:      ps.Baseline.LandValue,
			StructureValue: ps.Baseline.StructureValue,
			GovtValuation:  ps.Baseline.LandValue + ps.Baseline.StructureValue,
			Currency:       ps.Baseline.Currency,
			ChainLength:    ps.Chain.Len(),
			LatestRTPMV:    rtpmv,
		})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(PropertiesResponse{Total: len(out), Properties: out})
}

// AuditResponse is the JSON shape returned by /audit.
type AuditResponse struct {
	PropertyID string                    `json:"property_id"`
	Audit      []cryptochain.AuditRecord `json:"audit"`
}

// Audit returns every chain token wrapped with its frozen parameters.
func (h *Handlers) Audit(w http.ResponseWriter, r *http.Request) {
	h.setCORS(w)
	id := r.PathValue("id")
	ps, ok := h.Registry.Get(id)
	if !ok {
		http.Error(w, "property not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(AuditResponse{
		PropertyID: ps.PropertyID,
		Audit:      ps.Chain.ToAuditMode(),
	})
}

// CORSPreflight handles OPTIONS preflight checks from browsers.
func (h *Handlers) CORSPreflight(w http.ResponseWriter, r *http.Request) {
	h.setCORS(w)
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handlers) setCORS(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", h.CORSOrigin)
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
}
