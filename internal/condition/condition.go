package condition

import (
	"sort"

	"github.com/twinval/internal/compute"
)

// =============================================================================
// Process — single entry point for the conditioning pipeline
// =============================================================================

// Process runs the full preprocess + integrity pipeline. The output is
// a compute.ConditionedData ready for compute.ComputeAllIndicators.
//
// Pipeline (patent paragraphs [0043] and [0045]):
//
//  1. Denoise each sensor stream via EMA (per SensorID, time-ordered)
//  2. Snapshot at WindowEndNs via forward-fill (AlignTimestamps)
//  3. Aggregate snapshot values by SensorType (mean across multi-zone)
//  4. Normalise occupancy / electrical / water to [0, 1] via configured bounds
//  5. Run integrity checks → CI input fields (uptime, consistency, calib, tamper)
//  6. Forward PropertyMeta directly (no preprocessing applied)
//
// Physical-unit fields (vibration, strain, temperature, humidity, pm25) are
// passed through after smoothing — compute.ComputeESF and compute.ComputeSHF
// expect physical units, not normalised values.
func Process(batch RawSensorBatch, cfg ConditioningConfig) (compute.ConditionedData, IntegrityReport) {
	smoothed := denoiseAllStreams(batch.Readings, cfg.EMAAlpha)
	snapshot := AlignTimestamps(smoothed, batch.WindowEndNs)
	typeValues := groupSnapshotByType(smoothed, snapshot)

	cd := compute.ConditionedData{
		VibrationMagnitude: meanOrZero(typeValues[SensorVibration]),
		StrainMagnitude:    meanOrZero(typeValues[SensorStrain]),
		Temperature:        meanOrZero(typeValues[SensorTemperature]),
		Humidity:           meanOrZero(typeValues[SensorHumidity]),
		AirQualityPM:       meanOrZero(typeValues[SensorPM25]),

		OccupancyRatio:   normaliseFromBounds(meanOrZero(typeValues[SensorOccupancy]), cfg, SensorOccupancy),
		ElectricalLoad:   normaliseFromBounds(meanOrZero(typeValues[SensorElectrical]), cfg, SensorElectrical),
		WaterConsumption: normaliseFromBounds(meanOrZero(typeValues[SensorWater]), cfg, SensorWater),

		ChronologicalAge:       batch.PropertyMeta.ChronologicalAge,
		MaintenanceSensitivity: batch.PropertyMeta.MaintenanceSensitivity,
		ConditionQuality:       batch.PropertyMeta.ConditionQuality,
	}

	integrity := RunIntegrityChecks(batch, cfg)
	cd.UptimeContinuity = integrity.UptimeContinuity
	cd.CrossSensorConsistency = integrity.CrossSensorConsistency
	cd.CalibrationRecency = integrity.CalibrationRecency
	cd.TamperScore = integrity.TamperScore

	return cd, integrity
}

// denoiseAllStreams applies EMA per sensor ID. Returns a flat slice of
// readings with smoothed values; SensorID, SensorType, Zone, TimestampNs,
// and CalibrationDaysAgo are preserved unchanged.
//
// Iteration is deterministic: sensors are processed in sorted ID order
// and each sensor's series is sorted by timestamp before smoothing.
func denoiseAllStreams(readings []RawSensorReading, alpha float64) []RawSensorReading {
	bySensor := map[string][]RawSensorReading{}
	for _, r := range readings {
		bySensor[r.SensorID] = append(bySensor[r.SensorID], r)
	}

	ids := make([]string, 0, len(bySensor))
	for id := range bySensor {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	out := make([]RawSensorReading, 0, len(readings))
	for _, id := range ids {
		series := bySensor[id]
		sort.Slice(series, func(i, j int) bool {
			return series[i].TimestampNs < series[j].TimestampNs
		})
		vals := make([]float64, len(series))
		for i, r := range series {
			vals[i] = r.Value
		}
		smoothed := Denoise(vals, alpha)
		for i := range series {
			series[i].Value = smoothed[i]
			out = append(out, series[i])
		}
	}
	return out
}

// groupSnapshotByType maps each snapshot value to its SensorType,
// producing a per-type slice of values that the caller can mean-aggregate
// across multi-zone deployments.
func groupSnapshotByType(smoothed []RawSensorReading, snapshot map[string]float64) map[SensorType][]float64 {
	sensorType := map[string]SensorType{}
	for _, r := range smoothed {
		sensorType[r.SensorID] = r.SensorType
	}

	ids := make([]string, 0, len(snapshot))
	for id := range snapshot {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	out := map[SensorType][]float64{}
	for _, id := range ids {
		t := sensorType[id]
		out[t] = append(out[t], snapshot[id])
	}
	return out
}

func meanOrZero(vals []float64) float64 {
	if len(vals) == 0 {
		return 0.0
	}
	sum := 0.0
	for _, v := range vals {
		sum += v
	}
	return sum / float64(len(vals))
}

// normaliseFromBounds applies Normalise using the bounds configured for
// the supplied SensorType. If no bounds are configured the value passes
// through unchanged.
func normaliseFromBounds(value float64, cfg ConditioningConfig, t SensorType) float64 {
	b, ok := cfg.Bounds[t]
	if !ok {
		return value
	}
	return Normalise(value, b.Min, b.Max)
}
