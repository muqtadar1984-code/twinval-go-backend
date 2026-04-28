package exchange

// =============================================================================
// Pricing — patent paragraph [0061]
//
// All three functions here are pure: same inputs always produce the same
// outputs. The PropertyExchange composes them inside Update under its mutex.
// =============================================================================

// ComputeSpread returns the formula-output spread:
//
//	spread = baseSpread × (2 − health) × (2 − confidence)
//
// Both metrics are defensively clamped to [0, 1] so the spread cannot go
// negative on out-of-range input. Result range: [baseSpread, 4×baseSpread].
func ComputeSpread(health, confidence, baseSpread float64) float64 {
	h := clamp01(health)
	c := clamp01(confidence)
	return baseSpread * (2.0 - h) * (2.0 - c)
}

// ComputeBidAsk applies a symmetric spread around RTPMV:
//
//	bid = rtpmv × (1 − spread/2)
//	ask = rtpmv × (1 + spread/2)
func ComputeBidAsk(rtpmv, spread float64) (bid, ask float64) {
	half := spread / 2.0
	return rtpmv * (1.0 - half), rtpmv * (1.0 + half)
}

// ApplyRestrictedBand clamps a bid/ask pair to the inclusive interval
// [center × (1 − bandWidth), center × (1 + bandWidth)]. This is the
// RESTRICTED-state safety rail that prevents fire-sale prices when the
// market has lost confidence in current sensor data.
//
// After clamping, bid may rise above RTPMV or ask may fall below RTPMV
// — that is by design. The band represents the protocol's view of
// fair-value bounds; transient sensor degradation must not let the
// market drift outside them.
func ApplyRestrictedBand(bid, ask, center, bandWidth float64) (newBid, newAsk float64) {
	minPrice := center * (1.0 - bandWidth)
	maxPrice := center * (1.0 + bandWidth)
	return clamp(bid, minPrice, maxPrice), clamp(ask, minPrice, maxPrice)
}

func clamp(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func clamp01(v float64) float64 {
	return clamp(v, 0.0, 1.0)
}
