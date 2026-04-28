package exchange

import (
	"math"
	"sync"
	"time"

	"github.com/twinval/internal/compute"
)

// PropertyExchange is the per-property runtime: it tracks the current
// trading state, the latest quote, and the rolling volatility window.
// One instance per listed property.
type PropertyExchange struct {
	cfg ExchangeConfig

	mu            sync.RWMutex
	params        ExchangeParams
	lastGoodRTPMV float64 // most recent RTPMV observed while in Active
	volatility    *VolatilityWindow
}

// New constructs a PropertyExchange seeded from the supplied config.
// Panics via cfg.MustValidate if FirstThreshold ≤ SecondThreshold.
func New(cfg ExchangeConfig) *PropertyExchange {
	cfg.MustValidate()
	if cfg.VolatilityWindowSize <= 0 {
		cfg.VolatilityWindowSize = 10
	}
	return &PropertyExchange{
		cfg: cfg,
		params: ExchangeParams{
			PropertyID:   cfg.PropertyID,
			TradingState: Active,
		},
		volatility: NewVolatilityWindow(cfg.VolatilityWindowSize),
	}
}

// Update accepts a new RTPMV and the indicators that produced it,
// returns the freshly-computed quote, and (when applicable) a non-nil
// CircuitBreakerEvent describing the transition INTO Halted via the
// volatility breaker. Subsequent calls that keep the breaker tripped
// return event = nil — only transitions emit events.
func (px *PropertyExchange) Update(rtpmv float64, ind compute.TechnicalIndicators) (ExchangeParams, *CircuitBreakerEvent) {
	px.mu.Lock()
	defer px.mu.Unlock()

	px.volatility.Push(rtpmv)

	health := float64(compute.ComputeHealthFactor(ind))
	confidence := ind.CI

	spread := ComputeSpread(health, confidence, px.cfg.BaseSpread)
	bid, ask := ComputeBidAsk(rtpmv, spread)

	natural := classifyState(health, confidence, px.cfg.FirstThreshold, px.cfg.SecondThreshold)

	vol := px.volatility.Volatility()
	breakerTripped := px.volatility.IsFull() && vol > px.cfg.VolatilityThreshold

	var state TradingState
	var event *CircuitBreakerEvent
	if breakerTripped {
		state = Halted
		if px.params.TradingState != Halted {
			event = &CircuitBreakerEvent{
				Timestamp:  time.Now().UnixNano(),
				Volatility: vol,
				Threshold:  px.cfg.VolatilityThreshold,
				PriorState: px.params.TradingState,
			}
		}
	} else {
		state = natural
	}

	if state == Restricted {
		center := px.lastGoodRTPMV
		if center == 0 {
			center = rtpmv
		}
		bid, ask = ApplyRestrictedBand(bid, ask, center, px.cfg.RestrictedBandWidth)
	}

	if state == Active {
		px.lastGoodRTPMV = rtpmv
	}

	px.params = ExchangeParams{
		PropertyID:   px.cfg.PropertyID,
		Timestamp:    time.Now().UnixNano(),
		RTPMV:        rtpmv,
		BidPrice:     bid,
		AskPrice:     ask,
		Spread:       spread,
		TradingState: state,
	}
	return px.params, event
}

// CurrentParams returns the most recent quote. Safe to call concurrently
// with Update and itself.
func (px *PropertyExchange) CurrentParams() ExchangeParams {
	px.mu.RLock()
	defer px.mu.RUnlock()
	return px.params
}

// classifyState maps (health, confidence) onto the TradingState band edges.
// Independent of the volatility circuit breaker — that is layered on top
// inside Update.
func classifyState(health, confidence, first, second float64) TradingState {
	worst := math.Min(health, confidence)
	switch {
	case worst < second:
		return Halted
	case worst < first:
		return Restricted
	default:
		return Active
	}
}

// TradingStateString returns the canonical uppercase name for a state.
// Unknown values produce "UNKNOWN".
func TradingStateString(s TradingState) string {
	switch s {
	case Active:
		return "ACTIVE"
	case Restricted:
		return "RESTRICTED"
	case Halted:
		return "HALTED"
	default:
		return "UNKNOWN"
	}
}
