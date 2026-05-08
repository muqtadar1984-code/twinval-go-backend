package ingestion

import (
	"sync"

	"github.com/twinval/internal/ingestion/models"
)

// Denoiser applies an exponential moving average per sensor_id, in
// place. The first reading from a sensor passes through unchanged and
// initialises the EMA state.
//
// Stateful — keep one Denoiser per pipeline. Safe for concurrent use
// across goroutines (one mutex protects the state map).
type Denoiser struct {
	alpha float64

	mu    sync.Mutex
	state map[string]float64 // sensor_id -> last EMA value
}

// NewDenoiser returns a Denoiser configured with `alpha`. Higher alpha
// = more weight on recent readings (less smoothing). Spec default 0.3.
// Falls back to 0.3 on a non-positive or > 1 input.
func NewDenoiser(alpha float64) *Denoiser {
	if alpha <= 0 || alpha > 1 {
		alpha = 0.3
	}
	return &Denoiser{
		alpha: alpha,
		state: make(map[string]float64),
	}
}

// Apply mutates r.Value in place, replacing it with the EMA-smoothed
// value. The first reading per sensor passes through and seeds state.
func (d *Denoiser) Apply(r *models.SensorReading) {
	d.mu.Lock()
	defer d.mu.Unlock()
	prev, exists := d.state[r.SensorID]
	if !exists {
		d.state[r.SensorID] = r.Value
		return
	}
	smoothed := d.alpha*r.Value + (1-d.alpha)*prev
	d.state[r.SensorID] = smoothed
	r.Value = smoothed
}

// Reset drops all EMA state. Useful for tests or when reconfiguring.
func (d *Denoiser) Reset() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.state = make(map[string]float64)
}

// Snapshot returns a copy of the current EMA state for diagnostics.
func (d *Denoiser) Snapshot() map[string]float64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make(map[string]float64, len(d.state))
	for k, v := range d.state {
		out[k] = v
	}
	return out
}
