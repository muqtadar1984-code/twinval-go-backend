package exchange

// VolatilityWindow is a fixed-capacity ring buffer of recent RTPMV values.
// All storage is allocated in NewVolatilityWindow; Push reuses slots
// in-place. Not goroutine-safe — callers must externalise locking
// (PropertyExchange holds its window under a mutex).
type VolatilityWindow struct {
	capacity int
	values   []float64
	head     int // next write position (0..capacity-1)
	count    int // current number of meaningful entries (0..capacity)
}

// NewVolatilityWindow allocates a ring buffer of the given capacity.
// Capacity ≤ 0 falls back to 10. The slice is allocated once and never
// resized — Push only mutates existing slots.
func NewVolatilityWindow(capacity int) *VolatilityWindow {
	if capacity <= 0 {
		capacity = 10
	}
	return &VolatilityWindow{
		capacity: capacity,
		values:   make([]float64, capacity),
	}
}

// Push records a new value, overwriting the oldest entry once the window
// is full.
func (w *VolatilityWindow) Push(v float64) {
	w.values[w.head] = v
	w.head = (w.head + 1) % w.capacity
	if w.count < w.capacity {
		w.count++
	}
}

// Volatility returns (max − min) / min over the current window contents.
// Returns 0 when:
//   - the window holds fewer than 2 values (no swing to measure)
//   - the minimum is ≤ 0 (would divide by zero or produce negatives)
func (w *VolatilityWindow) Volatility() float64 {
	if w.count < 2 {
		return 0.0
	}
	min := w.values[0]
	max := w.values[0]
	for i := 1; i < w.count; i++ {
		v := w.values[i]
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}
	if min <= 0.0 {
		return 0.0
	}
	return (max - min) / min
}

// IsFull reports whether the ring buffer has wrapped at least once.
func (w *VolatilityWindow) IsFull() bool {
	return w.count == w.capacity
}
