// Package exchange implements Patent Module 360 — the distribution module.
//
// It manages the multi-property exchange platform:
//   - Dynamic bid/ask pricing widened by poor health or confidence
//   - Trading state machine (ACTIVE → RESTRICTED → HALTED)
//   - Volatility circuit breaker (paragraph [0060])
//
// Each listed property gets its own *PropertyExchange instance. The state
// machine is memoryless w.r.t. classification: every Update fully
// reclassifies state from current metrics + volatility, so recovery from
// HALTED happens automatically once the underlying signals improve.
//
// VALIDATION NOTE:
// Bid/ask formulas (paragraph [0061]) and threshold transitions
// (paragraph [0065]) must produce numerically identical output to the
// Python POC for parity validation. Float fields use Go's float64 native
// arithmetic — match the Python float64 (double) type exactly.
package exchange

// TradingState indicates whether a property accepts orders and under what
// constraints.
type TradingState int

const (
	// Active — normal bid/ask trading permitted. The base case.
	Active TradingState = iota

	// Restricted — trading permitted only within a narrow band around
	// the last known good RTPMV. Triggered when min(health, confidence)
	// falls below FirstThreshold but stays at or above SecondThreshold.
	Restricted

	// Halted — all trading suspended. Triggered either by metrics falling
	// below SecondThreshold, or by the volatility circuit breaker.
	Halted
)

// ExchangeParams is the public snapshot of a property's current quote.
// Returned by Update and CurrentParams. Safe to copy.
type ExchangeParams struct {
	PropertyID   string
	Timestamp    int64        // Unix nanoseconds
	RTPMV        float64      // input RTPMV that produced these quotes
	BidPrice     float64      // post-clamp bid (after RESTRICTED band)
	AskPrice     float64      // post-clamp ask (after RESTRICTED band)
	Spread       float64      // formula-output spread before clamping
	TradingState TradingState // current state classification
}

// CircuitBreakerEvent is emitted by Update on the transition INTO Halted
// caused by the volatility circuit breaker. Subsequent updates that keep
// the breaker tripped do not re-emit; the next event fires only after a
// recovery and another trip.
type CircuitBreakerEvent struct {
	Timestamp  int64
	Volatility float64      // observed (max - min)/min over the window
	Threshold  float64      // configured VolatilityThreshold
	PriorState TradingState // state immediately before this trip
}

// ExchangeConfig holds every calibration parameter for a PropertyExchange.
// Every value must be supplied — there are no implicit defaults inside
// the pricing or state functions.
type ExchangeConfig struct {
	PropertyID string

	BaseSpread float64 // tightest spread when health & confidence are 1.0

	// Threshold band (paragraph [0065]):
	//   FirstThreshold > SecondThreshold (validated in MustValidate)
	//   min(health, confidence) >= FirstThreshold  → Active
	//   SecondThreshold ≤ min(...) < FirstThreshold → Restricted
	//   min(...) < SecondThreshold                  → Halted
	FirstThreshold  float64
	SecondThreshold float64

	// Volatility circuit breaker (paragraph [0060]):
	//   computed (max - min)/min over the last VolatilityWindowSize RTPMVs
	//   trip condition: window full AND volatility > VolatilityThreshold
	VolatilityThreshold  float64
	VolatilityWindowSize int

	// RESTRICTED band: bid/ask clamped to lastGoodRTPMV × (1 ± RestrictedBandWidth)
	RestrictedBandWidth float64
}

// MustValidate panics on configs that violate invariants. Called by both
// DefaultExchangeConfig and New so misconfiguration surfaces at
// construction time rather than at Update time.
func (cfg ExchangeConfig) MustValidate() {
	if cfg.FirstThreshold <= cfg.SecondThreshold {
		panic("exchange: FirstThreshold must be > SecondThreshold")
	}
}

// DefaultExchangeConfig returns the production defaults matching the
// Python POC. PropertyID is left empty — callers must set it before
// passing to New for non-test use.
func DefaultExchangeConfig() ExchangeConfig {
	cfg := ExchangeConfig{
		PropertyID:           "",
		BaseSpread:           0.02,
		FirstThreshold:       0.60,
		SecondThreshold:      0.35,
		VolatilityThreshold:  0.15,
		VolatilityWindowSize: 10,
		RestrictedBandWidth:  0.05,
	}
	cfg.MustValidate()
	return cfg
}
