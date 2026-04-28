package sync

import (
	"math"

	"github.com/twinval/internal/compute"
)

// =============================================================================
// State evolution functions (patent paragraph [0046])
//
// Every function is pure: same inputs always produce the same output. No
// randomness, no clock reads, no shared state. This is required for parity
// validation against the Python POC.
// =============================================================================

// evolveStructuralStiffness updates the structural stiffness state.
//
// Decay path: when vibration or strain exceeds its normal ceiling (defined
// by the SHF config), the excess is multiplied by a per-unit decay rate
// and subtracted from current stiffness. Vibration excess is in m/s²
// (small numbers); strain excess is in microstrain (large numbers); the
// per-unit rates compensate for the unit difference.
//
// Recovery path: when both readings are at or below their ceilings, the
// stiffness recovers asymptotically toward 1.0 at StiffnessRecoveryRate.
// Recovery is slow by design — physical structures heal far slower than
// they degrade.
//
// Output is clamped to [0, 1].
func evolveStructuralStiffness(current float64, data compute.ConditionedData, cfg StateMachineConfig) float64 {
	vibCeiling := cfg.Indicators.SHF.VibrationNormalCeiling
	strainCeiling := cfg.Indicators.SHF.StrainNormalCeiling

	vibExcess := math.Max(0.0, data.VibrationMagnitude-vibCeiling)
	strainExcess := math.Max(0.0, data.StrainMagnitude-strainCeiling)

	penalty := vibExcess*cfg.StiffnessDecayPerUnitVibration +
		strainExcess*cfg.StiffnessDecayPerUnitStrain

	var next float64
	if penalty > 0.0 {
		next = current - penalty
	} else {
		next = current + cfg.StiffnessRecoveryRate*(1.0-current)
	}
	return clamp(next, 0.0, 1.0)
}

// evolveEnvironmentalExposure updates the rolling environmental exposure
// history via an EMA over (1 − ESF). Higher value = more accumulated
// exposure to non-ideal environmental conditions.
//
//	next = α × (1 − esf) + (1 − α) × current
//
// Output is clamped to [0, 1] — both inputs are already bounded so this
// is defensive only.
func evolveEnvironmentalExposure(current, esf float64, cfg StateMachineConfig) float64 {
	alpha := cfg.EnvironmentSmoothingFactor
	badness := 1.0 - esf
	return clamp(alpha*badness+(1.0-alpha)*current, 0.0, 1.0)
}

// evolveCumulativeUsageLoad accumulates utilisation over time.
//
//	next = current + max(0, uss × rate)
//
// Monotonically non-decreasing — this function never reduces the load.
// A separate maintenance-reset operation (not in this phase) is the only
// way to bring this counter down.
func evolveCumulativeUsageLoad(current, uss float64, cfg StateMachineConfig) float64 {
	increment := uss * cfg.UsageAccumulationRate
	if increment < 0.0 {
		return current
	}
	return current + increment
}

// evolveOverallHealth pegs OverallHealth to the freshly-computed Health
// Factor. The compute package already clamps HealthFactor to [0, 1], so
// no further processing is required.
func evolveOverallHealth(hf compute.HealthFactor) compute.HealthFactor {
	return hf
}

// clamp restricts v to [min, max].
func clamp(v, min, max float64) float64 {
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}
