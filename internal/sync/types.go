// Package sync implements Patent Module 330 — the synchronization module.
//
// It maintains the digital twin model: a computational state machine that
// mirrors the property's physical state in real time. Each ConditionedData
// arrival fires an immediate update of four mutable state variables and
// triggers recomputation of indicators and Health Factor.
//
// State variables (patent paragraph [0046]):
//
//  1. StructuralStiffness          — vibration/strain history; decays under
//                                    high-magnitude readings, recovers slowly
//                                    when readings stay below normal ceilings
//  2. EnvironmentalExposureHistory — rolling weighted average of (1 − ESF);
//                                    higher = more accumulated exposure
//  3. CumulativeUsageLoad          — integral of USS over time; monotonically
//                                    non-decreasing except via maintenance reset
//  4. OverallHealth                — current composite HealthFactor
//
// IMPORTANT: this package is named `sync` to match its directory location
// inside `internal/sync/`. External callers that also need stdlib `sync`
// must alias one of the two imports — typically:
//
//	import (
//	    "sync"
//	    twinsync "github.com/twinval/internal/sync"
//	)
//
// Inside this package, the stdlib `sync` is imported normally — the package
// declaration `package sync` does not introduce `sync` as a local identifier.
package sync

import (
	"time"

	"github.com/twinval/internal/compute"
)

// DigitalTwinState is the full snapshot of the digital twin's mutable state.
// Returned by CurrentState and embedded in every StateUpdate. Safe to copy.
type DigitalTwinState struct {
	StructuralStiffness          float64 // [0, 1]; 1.0 = perfect stiffness
	EnvironmentalExposureHistory float64 // [0, 1]; 0 = no exposure, 1 = saturated
	CumulativeUsageLoad          float64 // unbounded; integral of USS over time
	OverallHealth                compute.HealthFactor

	// Diagnostic / observability fields
	LastIndicators compute.TechnicalIndicators // indicators from most recent update
	LastUpdatedAt  time.Time                   // wall-clock of most recent update
	UpdateCount    uint64                      // total updates processed
}

// StateUpdate is broadcast on every subscriber channel after each successful
// state transition. SourceData is the raw input that triggered the update;
// Indicators is the freshly-computed plurality used in the Health Factor.
type StateUpdate struct {
	State      DigitalTwinState
	SourceData compute.ConditionedData
	Indicators compute.TechnicalIndicators
	UpdatedAt  time.Time
}

// StateMachineConfig holds every calibration knob for the state machine.
// No defaults are baked into the evolution functions — they all take cfg
// explicitly so reproducibility is preserved across runs.
type StateMachineConfig struct {
	// Indicators is forwarded to compute.ComputeAllIndicators on every update.
	Indicators compute.AllIndicatorConfigs

	// Stiffness evolution
	StiffnessDecayPerUnitVibration float64 // decay per (m/s² above ceiling)
	StiffnessDecayPerUnitStrain    float64 // decay per (microstrain above ceiling)
	StiffnessRecoveryRate          float64 // asymptotic recovery rate toward 1.0

	// Environmental exposure rolling average
	EnvironmentSmoothingFactor float64 // EMA alpha, in (0, 1]

	// Usage accumulation
	UsageAccumulationRate float64 // increment per USS unit per update

	// Channel sizing
	UpdateBufferSize     int // inbox buffer for Update; default 64
	SubscriberBufferSize int // per-subscriber buffer; default 16

	// Initial values
	InitialStructuralStiffness   float64 // typically 1.0 (perfect)
	InitialEnvironmentalExposure float64 // typically 0.0 (clean slate)
	InitialCumulativeUsageLoad   float64 // typically 0.0
}

// DefaultStateMachineConfig returns a config with sensible production defaults
// matching the Python POC. Adjust via your own config layer — never override
// these constants in calling code.
func DefaultStateMachineConfig() StateMachineConfig {
	return StateMachineConfig{
		Indicators:                     compute.DefaultAllConfigs(),
		StiffnessDecayPerUnitVibration: 0.01,
		StiffnessDecayPerUnitStrain:    0.0001,
		StiffnessRecoveryRate:          0.001,
		EnvironmentSmoothingFactor:     0.1,
		UsageAccumulationRate:          0.001,
		UpdateBufferSize:               64,
		SubscriberBufferSize:           16,
		InitialStructuralStiffness:     1.0,
		InitialEnvironmentalExposure:   0.0,
		InitialCumulativeUsageLoad:     0.0,
	}
}
