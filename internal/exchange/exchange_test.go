package exchange

import (
	"math"
	stdsync "sync"
	"testing"

	"github.com/twinval/internal/compute"
)

const tolerance = 1e-9

// =============================================================================
// Indicator helpers — produce TechnicalIndicators with a target HealthFactor
// =============================================================================

func indicatorsWithHF(hf, ci float64) compute.TechnicalIndicators {
	// HF = SHF × ESF × (1 − USS) × PDP × CI
	// Pin SHF=ESF=1, USS=0, then PDP carries the load: PDP × CI = HF
	return compute.TechnicalIndicators{
		SHF: 1.0,
		ESF: 1.0,
		USS: 0.0,
		PDP: hf / ci,
		CI:  ci,
	}
}

func perfectIndicators() compute.TechnicalIndicators {
	return compute.TechnicalIndicators{SHF: 1, ESF: 1, USS: 0, PDP: 1, CI: 1}
}

func defaultCfg() ExchangeConfig {
	cfg := DefaultExchangeConfig()
	cfg.PropertyID = "TEST-001"
	return cfg
}

// =============================================================================
// Pure pricing functions
// =============================================================================

func TestComputeSpread_PerfectInputsEqualBaseSpread(t *testing.T) {
	got := ComputeSpread(1.0, 1.0, 0.02)
	if math.Abs(got-0.02) > tolerance {
		t.Errorf("got %v, want 0.02", got)
	}
}

func TestComputeSpread_WidensAsHealthDecreases(t *testing.T) {
	high := ComputeSpread(1.0, 1.0, 0.02)
	mid := ComputeSpread(0.5, 1.0, 0.02)
	low := ComputeSpread(0.1, 1.0, 0.02)
	if !(high < mid && mid < low) {
		t.Errorf("spread should monotonically widen: high=%v mid=%v low=%v", high, mid, low)
	}
}

func TestComputeBidAsk_BidBelowAskAboveRTPMV(t *testing.T) {
	bid, ask := ComputeBidAsk(1_000_000.0, 0.05)
	if bid >= 1_000_000.0 {
		t.Errorf("bid should be below RTPMV, got %v", bid)
	}
	if ask <= 1_000_000.0 {
		t.Errorf("ask should be above RTPMV, got %v", ask)
	}
}

func TestApplyRestrictedBand_ClampsToBounds(t *testing.T) {
	// center 1m, band 5% → [950k, 1050k]
	bid, ask := ApplyRestrictedBand(800_000, 1_200_000, 1_000_000, 0.05)
	if math.Abs(bid-950_000) > tolerance {
		t.Errorf("bid clamp: got %v, want 950000", bid)
	}
	if math.Abs(ask-1_050_000) > tolerance {
		t.Errorf("ask clamp: got %v, want 1050000", ask)
	}
}

// =============================================================================
// State classification
// =============================================================================

func TestUpdate_PerfectMetricsYieldActiveState(t *testing.T) {
	ex := New(defaultCfg())
	params, event := ex.Update(1_000_000, perfectIndicators())
	if params.TradingState != Active {
		t.Errorf("TradingState: got %v, want Active", TradingStateString(params.TradingState))
	}
	if event != nil {
		t.Errorf("unexpected circuit breaker event: %+v", event)
	}
	// Tightest possible spread = BaseSpread = 0.02
	if math.Abs(params.Spread-0.02) > tolerance {
		t.Errorf("Spread: got %v, want 0.02", params.Spread)
	}
}

func TestUpdate_HealthBelowFirstThresholdYieldsRestricted(t *testing.T) {
	cfg := defaultCfg() // First=0.6, Second=0.35
	ex := New(cfg)
	// HF=0.5 with CI=1.0 → min=0.5, in [0.35, 0.6) → Restricted
	params, _ := ex.Update(1_000_000, indicatorsWithHF(0.5, 1.0))
	if params.TradingState != Restricted {
		t.Errorf("TradingState: got %v, want Restricted", TradingStateString(params.TradingState))
	}
}

func TestUpdate_HealthBelowSecondThresholdYieldsHalted(t *testing.T) {
	cfg := defaultCfg() // Second=0.35
	ex := New(cfg)
	// HF=0.2 with CI=1.0 → min=0.2 < 0.35 → Halted
	params, _ := ex.Update(1_000_000, indicatorsWithHF(0.2, 1.0))
	if params.TradingState != Halted {
		t.Errorf("TradingState: got %v, want Halted", TradingStateString(params.TradingState))
	}
}

func TestUpdate_RecoveryFromHaltedReturnsToActive(t *testing.T) {
	ex := New(defaultCfg())
	// Step 1: halted
	ex.Update(1_000_000, indicatorsWithHF(0.2, 1.0))
	if ex.CurrentParams().TradingState != Halted {
		t.Fatalf("setup: expected Halted")
	}
	// Step 2: full recovery
	ex.Update(1_000_000, perfectIndicators())
	if got := ex.CurrentParams().TradingState; got != Active {
		t.Errorf("after recovery: got %v, want Active", TradingStateString(got))
	}
}

// =============================================================================
// Spread widens as health decreases (end-to-end through Update)
// =============================================================================

func TestUpdate_SpreadWidensAsHealthDecreases(t *testing.T) {
	ex := New(defaultCfg())
	high, _ := ex.Update(1_000_000, perfectIndicators())              // HF=1.0
	low, _ := ex.Update(1_000_000, indicatorsWithHF(0.4, 1.0))        // HF=0.4
	if !(low.Spread > high.Spread) {
		t.Errorf("spread should widen: high.Spread=%v, low.Spread=%v", high.Spread, low.Spread)
	}
}

// =============================================================================
// RESTRICTED band clamping (end-to-end)
// =============================================================================

func TestUpdate_RestrictedStateClampsBidAskToBand(t *testing.T) {
	cfg := defaultCfg()
	ex := New(cfg)

	// Establish lastGoodRTPMV = 1_000_000 via an Active update
	ex.Update(1_000_000, perfectIndicators())

	// Now degrade: HF=0.4 → Restricted; RTPMV drops sharply to 700k.
	// Without clamping, bid would be ~700k×(1-half), well below 950k band.
	params, _ := ex.Update(700_000, indicatorsWithHF(0.4, 1.0))

	if params.TradingState != Restricted {
		t.Fatalf("setup: expected Restricted, got %v", TradingStateString(params.TradingState))
	}

	minBand := 1_000_000 * (1.0 - cfg.RestrictedBandWidth)
	maxBand := 1_000_000 * (1.0 + cfg.RestrictedBandWidth)
	if params.BidPrice < minBand-tolerance || params.BidPrice > maxBand+tolerance {
		t.Errorf("BidPrice %v outside band [%v, %v]", params.BidPrice, minBand, maxBand)
	}
	if params.AskPrice < minBand-tolerance || params.AskPrice > maxBand+tolerance {
		t.Errorf("AskPrice %v outside band [%v, %v]", params.AskPrice, minBand, maxBand)
	}
}

// =============================================================================
// Volatility circuit breaker
// =============================================================================

func TestUpdate_VolatilitySpikeFiresCircuitBreaker(t *testing.T) {
	cfg := defaultCfg() // VolatilityWindowSize=10, Threshold=0.15
	ex := New(cfg)
	ind := perfectIndicators()

	// 9 stable values @ 100, then 10th @ 120 → vol = (120-100)/100 = 0.2 > 0.15
	values := []float64{100, 100, 100, 100, 100, 100, 100, 100, 100, 120}

	var seenEvent *CircuitBreakerEvent
	for i, v := range values {
		_, event := ex.Update(v, ind)
		if event != nil {
			if seenEvent != nil {
				t.Errorf("duplicate breaker event at index %d", i)
			}
			seenEvent = event
		}
	}

	if seenEvent == nil {
		t.Fatalf("expected circuit breaker event after volatility spike")
	}
	if seenEvent.PriorState != Active {
		t.Errorf("PriorState: got %v, want Active", TradingStateString(seenEvent.PriorState))
	}
	if seenEvent.Volatility <= cfg.VolatilityThreshold {
		t.Errorf("event Volatility %v should exceed threshold %v", seenEvent.Volatility, cfg.VolatilityThreshold)
	}

	if got := ex.CurrentParams().TradingState; got != Halted {
		t.Errorf("post-breaker state: got %v, want Halted", TradingStateString(got))
	}
}

func TestUpdate_BreakerNotFiredBeforeWindowFull(t *testing.T) {
	cfg := defaultCfg()
	ex := New(cfg)
	ind := perfectIndicators()

	// Even with a 100% swing across 9 values, breaker must NOT fire — window still warming up.
	values := []float64{100, 100, 100, 100, 100, 100, 100, 100, 200}
	for _, v := range values {
		ex.Update(v, ind)
	}
	if got := ex.CurrentParams().TradingState; got != Active {
		t.Errorf("state with non-full window: got %v, want Active", TradingStateString(got))
	}
}

// =============================================================================
// String representation
// =============================================================================

func TestTradingStateString_AllStates(t *testing.T) {
	cases := []struct {
		s    TradingState
		want string
	}{
		{Active, "ACTIVE"},
		{Restricted, "RESTRICTED"},
		{Halted, "HALTED"},
	}
	for _, c := range cases {
		if got := TradingStateString(c.s); got != c.want {
			t.Errorf("TradingStateString(%d): got %q, want %q", c.s, got, c.want)
		}
	}
}

// =============================================================================
// Volatility window unit tests
// =============================================================================

func TestVolatilityWindow_PushAndVolatility(t *testing.T) {
	w := NewVolatilityWindow(5)
	if w.IsFull() {
		t.Errorf("freshly-created window should not be full")
	}
	for _, v := range []float64{100, 110, 120, 130, 140} {
		w.Push(v)
	}
	if !w.IsFull() {
		t.Errorf("window should be full after 5 pushes into capacity-5")
	}
	// vol = (140 - 100) / 100 = 0.4
	got := w.Volatility()
	if math.Abs(got-0.4) > tolerance {
		t.Errorf("Volatility: got %v, want 0.4", got)
	}
}

func TestVolatilityWindow_RingOverwrite(t *testing.T) {
	w := NewVolatilityWindow(3)
	for _, v := range []float64{100, 200, 300, 400, 500} {
		w.Push(v)
	}
	// Window now contains {300, 400, 500}; vol = (500-300)/300 = 0.6666...
	got := w.Volatility()
	want := (500.0 - 300.0) / 300.0
	if math.Abs(got-want) > tolerance {
		t.Errorf("Volatility: got %v, want %v", got, want)
	}
}

func TestVolatilityWindow_EmptyOrSingleReturnsZero(t *testing.T) {
	w := NewVolatilityWindow(5)
	if w.Volatility() != 0.0 {
		t.Errorf("empty window: got %v, want 0", w.Volatility())
	}
	w.Push(100)
	if w.Volatility() != 0.0 {
		t.Errorf("single-value window: got %v, want 0", w.Volatility())
	}
}

// =============================================================================
// Config validation
// =============================================================================

func TestExchangeConfig_PanicWhenFirstNotGreaterThanSecond(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic on FirstThreshold == SecondThreshold")
		}
	}()
	cfg := DefaultExchangeConfig()
	cfg.FirstThreshold = 0.4
	cfg.SecondThreshold = 0.4
	cfg.MustValidate()
}

// =============================================================================
// Concurrency — must pass under -race
// =============================================================================

func TestUpdate_ConcurrentReadersAndOneWriter(t *testing.T) {
	ex := New(defaultCfg())
	ind := perfectIndicators()

	var wg stdsync.WaitGroup
	const readers = 8
	const reads = 200
	const writes = 200

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < writes; i++ {
			ex.Update(1_000_000+float64(i), ind)
		}
	}()

	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < reads; j++ {
				_ = ex.CurrentParams()
			}
		}()
	}
	wg.Wait()
}
