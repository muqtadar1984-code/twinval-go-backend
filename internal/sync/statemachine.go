package sync

import (
	"context"
	"sync"
	"time"

	"github.com/twinval/internal/compute"
)

// StateMachine is the concurrent runtime around the digital twin state.
// One StateMachine corresponds to one property; it should be driven by
// a single Run goroutine but may receive Update calls and CurrentState
// reads from any number of goroutines concurrently.
type StateMachine struct {
	cfg StateMachineConfig

	mu    sync.RWMutex
	state DigitalTwinState

	inbox chan compute.ConditionedData

	subMu       sync.Mutex
	subscribers []chan StateUpdate
	stopped     bool
}

// New constructs a StateMachine seeded from the supplied config. The
// returned StateMachine is ready for Subscribe and Update calls; Run
// must be started in its own goroutine to actually process updates.
func New(cfg StateMachineConfig) *StateMachine {
	if cfg.UpdateBufferSize <= 0 {
		cfg.UpdateBufferSize = 64
	}
	if cfg.SubscriberBufferSize <= 0 {
		cfg.SubscriberBufferSize = 16
	}
	return &StateMachine{
		cfg: cfg,
		state: DigitalTwinState{
			StructuralStiffness:          cfg.InitialStructuralStiffness,
			EnvironmentalExposureHistory: cfg.InitialEnvironmentalExposure,
			CumulativeUsageLoad:          cfg.InitialCumulativeUsageLoad,
			OverallHealth:                compute.HealthFactor(1.0),
		},
		inbox: make(chan compute.ConditionedData, cfg.UpdateBufferSize),
	}
}

// Run starts the update-processing loop. It exits when the supplied
// context is cancelled, at which point all subscriber channels are closed
// and further Update / Subscribe calls become no-ops (Subscribe returns
// a pre-closed channel; Update silently drops).
//
// Should be called exactly once per StateMachine instance.
func (sm *StateMachine) Run(ctx context.Context) {
	defer sm.shutdown()
	for {
		select {
		case <-ctx.Done():
			return
		case data := <-sm.inbox:
			sm.applyUpdate(data)
		}
	}
}

// Update queues a new ConditionedData for processing. Strictly non-blocking:
// if the inbox buffer is full the data is silently dropped. Sized the buffer
// (default 64) so dropping is rare under expected load; size up if your
// producer outpaces the consumer.
func (sm *StateMachine) Update(data compute.ConditionedData) {
	select {
	case sm.inbox <- data:
	default:
	}
}

// CurrentState returns a snapshot of the digital twin's current state.
// Safe to call concurrently with Update and other CurrentState calls.
func (sm *StateMachine) CurrentState() DigitalTwinState {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.state
}

// Subscribe returns a read-only channel that receives a StateUpdate after
// every successful state transition. Each subscriber gets its own buffered
// channel (default size 16). If a subscriber falls behind and its buffer
// fills, that update is dropped for that subscriber only — fast subscribers
// continue to receive normally.
//
// If Run has already exited the returned channel is pre-closed.
func (sm *StateMachine) Subscribe() <-chan StateUpdate {
	sm.subMu.Lock()
	defer sm.subMu.Unlock()

	ch := make(chan StateUpdate, sm.cfg.SubscriberBufferSize)
	if sm.stopped {
		close(ch)
		return ch
	}
	sm.subscribers = append(sm.subscribers, ch)
	return ch
}

// applyUpdate runs the indicator pipeline, evolves state, and broadcasts.
// Called only from Run's goroutine — single-writer for state mutation.
func (sm *StateMachine) applyUpdate(data compute.ConditionedData) {
	indicators := compute.ComputeAllIndicators(data, sm.cfg.Indicators)
	hf := compute.ComputeHealthFactor(indicators)

	sm.mu.Lock()
	sm.state.StructuralStiffness = evolveStructuralStiffness(sm.state.StructuralStiffness, data, sm.cfg)
	sm.state.EnvironmentalExposureHistory = evolveEnvironmentalExposure(sm.state.EnvironmentalExposureHistory, indicators.ESF, sm.cfg)
	sm.state.CumulativeUsageLoad = evolveCumulativeUsageLoad(sm.state.CumulativeUsageLoad, indicators.USS, sm.cfg)
	sm.state.OverallHealth = evolveOverallHealth(hf)
	sm.state.LastIndicators = indicators
	sm.state.LastUpdatedAt = time.Now()
	sm.state.UpdateCount++
	snapshot := sm.state
	sm.mu.Unlock()

	sm.broadcast(StateUpdate{
		State:      snapshot,
		SourceData: data,
		Indicators: indicators,
		UpdatedAt:  snapshot.LastUpdatedAt,
	})
}

// broadcast sends an update to every live subscriber, non-blocking per
// subscriber. Subscribers whose buffers are full miss this update.
func (sm *StateMachine) broadcast(upd StateUpdate) {
	sm.subMu.Lock()
	defer sm.subMu.Unlock()
	for _, ch := range sm.subscribers {
		select {
		case ch <- upd:
		default:
		}
	}
}

// shutdown closes every subscriber channel and marks the StateMachine
// stopped so future Subscribe calls return pre-closed channels.
func (sm *StateMachine) shutdown() {
	sm.subMu.Lock()
	defer sm.subMu.Unlock()
	sm.stopped = true
	for _, ch := range sm.subscribers {
		close(ch)
	}
	sm.subscribers = nil
}
