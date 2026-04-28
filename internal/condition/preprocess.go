package condition

import "sort"

// =============================================================================
// Denoise — exponential moving average
// Patent paragraph [0043]
// =============================================================================

// Denoise applies an exponential moving average across a series.
//
//	EMA(0) = series[0]
//	EMA(t) = alpha*series[t] + (1-alpha)*EMA(t-1)
//
// alpha must be in (0, 1]; values outside are accepted but produce
// non-standard behaviour. Returns a new slice the same length as input.
// Empty input → empty output. Length-1 input is returned unchanged.
func Denoise(series []float64, alpha float64) []float64 {
	if len(series) == 0 {
		return []float64{}
	}
	out := make([]float64, len(series))
	out[0] = series[0]
	for i := 1; i < len(series); i++ {
		out[i] = alpha*series[i] + (1.0-alpha)*out[i-1]
	}
	return out
}

// =============================================================================
// Normalise — bounded [0, 1] scaling
// Patent paragraph [0043]
// =============================================================================

// Normalise scales value into [0.0, 1.0] given min/max bounds.
//
//	(value - min) / (max - min), clamped to [0, 1]
//
// Values <= min produce 0; values >= max produce 1. If max <= min the
// function returns 0 to avoid an undefined ratio.
func Normalise(value, min, max float64) float64 {
	if max <= min {
		return 0.0
	}
	n := (value - min) / (max - min)
	if n < 0.0 {
		return 0.0
	}
	if n > 1.0 {
		return 1.0
	}
	return n
}

// =============================================================================
// AlignTimestamps — forward-fill snapshot
// Patent paragraph [0043]
// =============================================================================

// AlignTimestamps returns the most recent reading at or before asOfNs
// per sensor ID. This is a deterministic forward-fill snapshot operator:
// sensors that have no reading at or before asOfNs are omitted from the
// result (caller decides how to handle missing sensors).
//
// Readings strictly after asOfNs are excluded — the snapshot is causal.
func AlignTimestamps(readings []RawSensorReading, asOfNs int64) map[string]float64 {
	grouped := map[string][]RawSensorReading{}
	for _, r := range readings {
		if r.TimestampNs > asOfNs {
			continue
		}
		grouped[r.SensorID] = append(grouped[r.SensorID], r)
	}

	out := make(map[string]float64, len(grouped))
	for id, series := range grouped {
		sort.Slice(series, func(i, j int) bool {
			return series[i].TimestampNs < series[j].TimestampNs
		})
		out[id] = series[len(series)-1].Value
	}
	return out
}
