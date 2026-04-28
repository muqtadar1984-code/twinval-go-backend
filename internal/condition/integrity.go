package condition

import (
	"math"
	"sort"
)

// =============================================================================
// CheckUptimeContinuity
// Patent paragraph [0045]
// =============================================================================

// CheckUptimeContinuity returns received/expected, clamped to [0, 1].
// When expected is zero the score is 1.0 (no expectation = nothing to miss).
// Over-delivery (received > expected) is clamped to 1.0; we do not reward
// it because the patent uses uptime strictly as a downside-risk signal.
func CheckUptimeContinuity(received, expected int) float64 {
	if expected <= 0 {
		return 1.0
	}
	r := float64(received) / float64(expected)
	if r < 0.0 {
		return 0.0
	}
	if r > 1.0 {
		return 1.0
	}
	return r
}

// =============================================================================
// CheckCrossConsistency
// Patent paragraph [0045]
// =============================================================================

// CheckCrossConsistency measures spatial agreement between same-type
// sensors deployed in the same zone (redundant deployment). For each
// zone with 2+ sensors of the supplied type, it computes the per-sensor
// time-mean, then takes the spread (max - min) of those means. The
// per-zone penalty is spread/tolerance; the score is 1 - mean(penalty)
// across zones, clamped to [0, 1].
//
// Zones with only one sensor are skipped (nothing to compare). If no
// zone has multiple sensors the score is 1.0 — the absence of redundant
// sensors is not a confidence problem at this stage.
//
// Caller must pre-filter readings to a single SensorType.
func CheckCrossConsistency(readings []RawSensorReading, tolerance float64) float64 {
	if tolerance <= 0.0 {
		return 1.0
	}

	byZone := map[string]map[string][]float64{}
	for _, r := range readings {
		if _, ok := byZone[r.Zone]; !ok {
			byZone[r.Zone] = map[string][]float64{}
		}
		byZone[r.Zone][r.SensorID] = append(byZone[r.Zone][r.SensorID], r.Value)
	}

	zones := make([]string, 0, len(byZone))
	for z := range byZone {
		zones = append(zones, z)
	}
	sort.Strings(zones)

	var penalties []float64
	for _, z := range zones {
		sensorMap := byZone[z]
		if len(sensorMap) < 2 {
			continue
		}

		ids := make([]string, 0, len(sensorMap))
		for id := range sensorMap {
			ids = append(ids, id)
		}
		sort.Strings(ids)

		means := make([]float64, 0, len(ids))
		for _, id := range ids {
			vals := sensorMap[id]
			sum := 0.0
			for _, v := range vals {
				sum += v
			}
			means = append(means, sum/float64(len(vals)))
		}

		min, max := means[0], means[0]
		for _, m := range means[1:] {
			if m < min {
				min = m
			}
			if m > max {
				max = m
			}
		}
		penalties = append(penalties, (max-min)/tolerance)
	}

	if len(penalties) == 0 {
		return 1.0
	}
	sum := 0.0
	for _, p := range penalties {
		sum += p
	}
	score := 1.0 - sum/float64(len(penalties))
	if score < 0.0 {
		return 0.0
	}
	if score > 1.0 {
		return 1.0
	}
	return score
}

// =============================================================================
// CheckTamper
// Patent paragraph [0045]
// =============================================================================

// CheckTamper inspects per-sensor reading sequences for impossible step
// changes. Within each sensor's stream, every consecutive pair whose
// absolute value-delta exceeds stepLimit is flagged. The score is
// 1 - flags/pairs across all sensors and pairs in the input, clamped
// to [0, 1].
//
// Sensors with only one reading contribute zero pairs (and zero flags).
// If the input contains zero pairs total the score is 1.0 — no evidence
// of tamper, no evidence against either.
//
// Caller must pre-filter readings to a single SensorType so stepLimit
// applies in consistent units.
func CheckTamper(readings []RawSensorReading, stepLimit float64) float64 {
	if stepLimit <= 0.0 {
		return 1.0
	}

	bySensor := map[string][]RawSensorReading{}
	for _, r := range readings {
		bySensor[r.SensorID] = append(bySensor[r.SensorID], r)
	}

	ids := make([]string, 0, len(bySensor))
	for id := range bySensor {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	flags := 0
	pairs := 0
	for _, id := range ids {
		series := bySensor[id]
		sort.Slice(series, func(i, j int) bool {
			return series[i].TimestampNs < series[j].TimestampNs
		})
		for i := 1; i < len(series); i++ {
			pairs++
			if math.Abs(series[i].Value-series[i-1].Value) > stepLimit {
				flags++
			}
		}
	}

	if pairs == 0 {
		return 1.0
	}
	score := 1.0 - float64(flags)/float64(pairs)
	if score < 0.0 {
		return 0.0
	}
	return score
}

// =============================================================================
// RunIntegrityChecks — aggregate all four checks
// =============================================================================

// RunIntegrityChecks executes all four integrity checks against a batch
// and assembles the result into an IntegrityReport.
//
// Aggregation rules:
//   - Uptime: total received / (ExpectedReadingsPerSensor × unique sensor IDs)
//   - Consistency: mean of per-type CheckCrossConsistency scores across types
//     that have a tolerance configured. Defaults to 1.0 if none.
//   - Tamper: mean of per-type CheckTamper scores across types that have a
//     step limit configured. Defaults to 1.0 if none.
//   - Calibration: arithmetic mean of CalibrationDaysAgo across all readings
//     in the batch. Returned in days; compute.ComputeCI handles decay.
func RunIntegrityChecks(batch RawSensorBatch, cfg ConditioningConfig) IntegrityReport {
	uniqueSensors := map[string]struct{}{}
	for _, r := range batch.Readings {
		uniqueSensors[r.SensorID] = struct{}{}
	}
	expectedTotal := batch.ExpectedReadingsPerSensor * len(uniqueSensors)
	uptime := CheckUptimeContinuity(len(batch.Readings), expectedTotal)

	byType := map[SensorType][]RawSensorReading{}
	for _, r := range batch.Readings {
		byType[r.SensorType] = append(byType[r.SensorType], r)
	}
	types := make([]SensorType, 0, len(byType))
	for t := range byType {
		types = append(types, t)
	}
	sort.Slice(types, func(i, j int) bool { return types[i] < types[j] })

	var consistencyScores []float64
	for _, t := range types {
		tol, ok := cfg.ConsistencyTolerances[t]
		if !ok {
			continue
		}
		consistencyScores = append(consistencyScores, CheckCrossConsistency(byType[t], tol))
	}
	consistency := meanOrDefault(consistencyScores, 1.0)

	var tamperScores []float64
	for _, t := range types {
		lim, ok := cfg.TamperStepLimits[t]
		if !ok {
			continue
		}
		tamperScores = append(tamperScores, CheckTamper(byType[t], lim))
	}
	tamper := meanOrDefault(tamperScores, 1.0)

	calib := 0.0
	if len(batch.Readings) > 0 {
		sum := 0.0
		for _, r := range batch.Readings {
			sum += r.CalibrationDaysAgo
		}
		calib = sum / float64(len(batch.Readings))
	}

	return IntegrityReport{
		UptimeContinuity:       uptime,
		CrossSensorConsistency: consistency,
		CalibrationRecency:     calib,
		TamperScore:            tamper,
	}
}

func meanOrDefault(vals []float64, def float64) float64 {
	if len(vals) == 0 {
		return def
	}
	sum := 0.0
	for _, v := range vals {
		sum += v
	}
	return sum / float64(len(vals))
}
