package sync

import (
	"context"
	stdsync "sync"
	"testing"
	"time"

	"github.com/twinval/internal/compute"
)

const tolerance = 1e-9

// degradedData mirrors the degraded fixture from the compute test suite —
// vibration and strain above their ceilings, environment outside optimal,
// near-full occupancy, etc.
func degradedData() compute.ConditionedData {
	return compute.ConditionedData{
		VibrationMagnitude:     0.2,
		StrainMagnitude:        600.0,
		Temperature:            35.0,
		Humidity:               80.0,
		AirQualityPM:           45.0,
		OccupancyRatio:         0.95,
		ElectricalLoad:         0.90,
		WaterConsumption:       0.85,
		UptimeContinuity:       0.70,
		CrossSensorConsistency: 0.65,
		CalibrationRecency:     200.0,
		TamperScore:            0.80,
		ChronologicalAge:       30.0,
		MaintenanceSensitivity: 0.8,
		ConditionQuality:       0.3,
	}
}

func perfectData() compute.ConditionedData {
	return compute.ConditionedData{
		VibrationMagnitude:     0.01,
		StrainMagnitude:        50.0,
		Temperature:            22.0,
		Humidity:               50.0,
		AirQualityPM:           5.0,
		OccupancyRatio:         0.0,
		ElectricalLoad:         0.0,
		WaterConsumption:       0.0,
		UptimeContinuity:       1.0,
		CrossSensorConsistency: 1.0,
		CalibrationRecency:     30.0,
		TamperScore:            1.0,
		ChronologicalAge:       0.0,
		MaintenanceSensitivity: 0.5,
		ConditionQuality:       1.0,
	}
}

// =============================================================================
// Initial state
// =============================================================================

func TestNew_InitialStateMatchesConfig(t *testing.T) {
	sm := New(DefaultStateMachineConfig())
	state := sm.CurrentState()

	if state.StructuralStiffness != 1.0 {
		t.Errorf("StructuralStiffness: got %v, want 1.0", state.StructuralStiffness)
	}
	if state.EnvironmentalExposureHistory != 0.0 {
		t.Errorf("EnvironmentalExposureHistory: got %v, want 0.0", state.EnvironmentalExposureHistory)
	}
	if state.CumulativeUsageLoad != 0.0 {
		t.Errorf("CumulativeUsageLoad: got %v, want 0.0", state.CumulativeUsageLoad)
	}
	if state.OverallHealth != compute.HealthFactor(1.0) {
		t.Errorf("OverallHealth: got %v, want 1.0", state.OverallHealth)
	}
	if state.UpdateCount != 0 {
		t.Errorf("UpdateCount: got %d, want 0", state.UpdateCount)
	}
}

// =============================================================================
// Single update
// =============================================================================

func TestUpdate_DegradedReadingMovesAllStateVars(t *testing.T) {
	sm := New(DefaultStateMachineConfig())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sub := sm.Subscribe()
	go sm.Run(ctx)

	sm.Update(degradedData())

	select {
	case upd := <-sub:
		// Stiffness must drop below the initial 1.0 because vibration and
		// strain are above their normal ceilings.
		if upd.State.StructuralStiffness >= 1.0 {
			t.Errorf("StructuralStiffness should drop below 1.0, got %v", upd.State.StructuralStiffness)
		}
		// Exposure history must rise above 0.0 because ESF will be < 1.0.
		if upd.State.EnvironmentalExposureHistory <= 0.0 {
			t.Errorf("EnvironmentalExposureHistory should rise above 0.0, got %v", upd.State.EnvironmentalExposureHistory)
		}
		// Usage load must rise above 0.0 because USS > 0.
		if upd.State.CumulativeUsageLoad <= 0.0 {
			t.Errorf("CumulativeUsageLoad should rise above 0.0, got %v", upd.State.CumulativeUsageLoad)
		}
		// OverallHealth equals the freshly-computed HealthFactor from indicators.
		expectedHF := compute.ComputeHealthFactor(upd.Indicators)
		if upd.State.OverallHealth != expectedHF {
			t.Errorf("OverallHealth: got %v, want %v (= ComputeHealthFactor(indicators))", upd.State.OverallHealth, expectedHF)
		}
		if upd.State.UpdateCount != 1 {
			t.Errorf("UpdateCount after 1 update: got %d, want 1", upd.State.UpdateCount)
		}
	case <-time.After(time.Second):
		t.Fatal("did not receive subscriber update within 1s")
	}
}

func TestUpdate_PerfectReadingKeepsStiffnessAtOne(t *testing.T) {
	sm := New(DefaultStateMachineConfig())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sub := sm.Subscribe()
	go sm.Run(ctx)

	sm.Update(perfectData())

	select {
	case upd := <-sub:
		if upd.State.StructuralStiffness != 1.0 {
			t.Errorf("perfect reading should leave Stiffness at 1.0, got %v", upd.State.StructuralStiffness)
		}
	case <-time.After(time.Second):
		t.Fatal("did not receive subscriber update within 1s")
	}
}

// =============================================================================
// Cumulative load monotonicity
// =============================================================================

func TestCumulativeUsageLoad_MonotonicAcrossManyUpdates(t *testing.T) {
	sm := New(DefaultStateMachineConfig())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sub := sm.Subscribe()
	go sm.Run(ctx)

	prev := 0.0
	const updates = 50
	for i := 0; i < updates; i++ {
		// Alternate degraded / perfect to ensure USS varies but never
		// drives load down.
		if i%2 == 0 {
			sm.Update(degradedData())
		} else {
			sm.Update(perfectData())
		}

		select {
		case upd := <-sub:
			if upd.State.CumulativeUsageLoad < prev-tolerance {
				t.Errorf("CumulativeUsageLoad decreased at step %d: %v < %v",
					i, upd.State.CumulativeUsageLoad, prev)
			}
			prev = upd.State.CumulativeUsageLoad
		case <-time.After(time.Second):
			t.Fatalf("step %d: did not receive subscriber update", i)
		}
	}

	if prev <= 0.0 {
		t.Errorf("expected positive CumulativeUsageLoad after %d updates, got %v", updates, prev)
	}
}

// =============================================================================
// Subscribe semantics
// =============================================================================

func TestSubscribe_ReceivesEveryUpdate(t *testing.T) {
	sm := New(DefaultStateMachineConfig())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sub := sm.Subscribe()
	go sm.Run(ctx)

	const n = 5
	for i := 0; i < n; i++ {
		sm.Update(perfectData())
	}

	received := 0
	deadline := time.After(2 * time.Second)
	for received < n {
		select {
		case <-sub:
			received++
		case <-deadline:
			t.Fatalf("only received %d/%d updates before deadline", received, n)
		}
	}
}

func TestSubscribe_AfterShutdownReturnsClosedChannel(t *testing.T) {
	sm := New(DefaultStateMachineConfig())
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		sm.Run(ctx)
		close(done)
	}()
	cancel()
	<-done

	sub := sm.Subscribe()
	select {
	case _, ok := <-sub:
		if ok {
			t.Errorf("expected closed channel from Subscribe after shutdown, but got a value")
		}
	case <-time.After(time.Second):
		t.Fatal("Subscribe did not return a pre-closed channel after shutdown")
	}
}

// =============================================================================
// Context cancellation
// =============================================================================

func TestRun_StopsCleanlyOnContextCancel(t *testing.T) {
	sm := New(DefaultStateMachineConfig())
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		sm.Run(ctx)
		close(done)
	}()

	cancel()

	select {
	case <-done:
		// success — no leaked goroutine
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not exit within 2s of context cancellation")
	}
}

func TestRun_SubscriberChannelClosesOnShutdown(t *testing.T) {
	sm := New(DefaultStateMachineConfig())
	ctx, cancel := context.WithCancel(context.Background())
	sub := sm.Subscribe()

	done := make(chan struct{})
	go func() {
		sm.Run(ctx)
		close(done)
	}()
	cancel()
	<-done

	// Drain any pending updates, then expect close.
	deadline := time.After(time.Second)
	for {
		select {
		case _, ok := <-sub:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("subscriber channel was not closed within 1s of shutdown")
		}
	}
}

// =============================================================================
// Concurrent safety — must pass under -race
// =============================================================================

func TestUpdate_ConcurrentUpdatesAndReadsAreSafe(t *testing.T) {
	sm := New(DefaultStateMachineConfig())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go sm.Run(ctx)

	const goroutines = 8
	const opsPerG = 200

	var wg stdsync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerG; j++ {
				sm.Update(degradedData())
				_ = sm.CurrentState()
			}
		}()
	}
	wg.Wait()

	// Run() consumes Update() submissions asynchronously — wait briefly
	// for the inbox to drain before we sample UpdateCount, otherwise
	// this test races on heavily-loaded build hosts.
	deadline := time.Now().Add(2 * time.Second)
	var final DigitalTwinState
	for time.Now().Before(deadline) {
		final = sm.CurrentState()
		if final.UpdateCount > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if final.UpdateCount == 0 {
		t.Errorf("expected non-zero UpdateCount after %d ops, got 0", goroutines*opsPerG)
	}
}

func TestUpdate_NeverBlocksCaller(t *testing.T) {
	cfg := DefaultStateMachineConfig()
	cfg.UpdateBufferSize = 4
	sm := New(cfg)
	// Deliberately do NOT start Run — inbox stays full after 4 sends.
	// Update must still return immediately for all calls.

	doneCh := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			sm.Update(perfectData())
		}
		close(doneCh)
	}()
	select {
	case <-doneCh:
		// success — Update returned for all 100 calls without blocking
	case <-time.After(time.Second):
		t.Fatal("Update blocked when inbox was full")
	}
}
