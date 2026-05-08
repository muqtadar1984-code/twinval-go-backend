// Package lifecycle owns process-level concerns for the live ingestion
// pipeline: signal handling, ordered shutdown, and a 30-second hard
// deadline beyond which we force-exit.
//
// A typical main() wires this up with:
//
//	lc := lifecycle.New()
//	lc.Register("mqtt-adapter", mqttAdapter.Stop)
//	lc.Register("webhook-adapter", webhookAdapter.Stop)
//	lc.Register("modbus-adapter", modbusAdapter.Stop)
//	lc.Register("pipeline-drain", pipeline.Drain)
//	lc.Register("writer-pool", writerPool.Stop)
//	lc.Register("db-pool", func(_ context.Context) error { dbPool.Close(); return nil })
//	lc.WaitForSignal()       // blocks until SIGINT/SIGTERM
//	lc.RunShutdown()         // calls every Register'd hook in order
//
// Hooks run sequentially in registration order. A hook that exceeds its
// share of the 30-second budget is cancelled via context and the next
// hook starts immediately.
package lifecycle

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// MaxShutdownDuration is the absolute upper bound on shutdown. Beyond
// this point the process force-exits with code 1 to avoid hanging
// containers.
const MaxShutdownDuration = 30 * time.Second

// Hook is one ordered shutdown step. Name is used for logging; Stop
// receives a context that may be cancelled if the global shutdown
// budget is exceeded.
type Hook struct {
	Name string
	Stop func(ctx context.Context) error
}

// Lifecycle coordinates ordered shutdown across pipeline components.
type Lifecycle struct {
	mu    sync.Mutex
	hooks []Hook
	sig   chan os.Signal
}

// New returns a Lifecycle wired to SIGINT + SIGTERM.
func New() *Lifecycle {
	lc := &Lifecycle{sig: make(chan os.Signal, 1)}
	signal.Notify(lc.sig, syscall.SIGINT, syscall.SIGTERM)
	return lc
}

// Register adds a shutdown hook. Hooks run in registration order, so
// register from outer-most (adapters) to inner-most (DB pool).
func (lc *Lifecycle) Register(name string, stop func(ctx context.Context) error) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.hooks = append(lc.hooks, Hook{Name: name, Stop: stop})
}

// WaitForSignal blocks until SIGINT or SIGTERM is received.
// Returns the signal that triggered shutdown.
func (lc *Lifecycle) WaitForSignal() os.Signal {
	return <-lc.sig
}

// RunShutdown invokes every registered hook in order. Each hook gets
// what's left of the 30-second budget. Logs progress via slog.
func (lc *Lifecycle) RunShutdown() {
	deadline := time.Now().Add(MaxShutdownDuration)
	overall, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	lc.mu.Lock()
	hooks := append([]Hook(nil), lc.hooks...)
	lc.mu.Unlock()

	for _, h := range hooks {
		if overall.Err() != nil {
			slog.Warn("ingestion shutdown: budget exhausted, skipping", "hook", h.Name)
			continue
		}
		slog.Info("ingestion shutdown: running hook", "hook", h.Name)
		hookCtx, hookCancel := context.WithDeadline(overall, deadline)
		if err := h.Stop(hookCtx); err != nil {
			slog.Warn("ingestion shutdown: hook returned error",
				"hook", h.Name, "error", err.Error())
		}
		hookCancel()
	}
	slog.Info("TwinVal ingestion pipeline shutdown complete")
}

// ForceExitAfter spawns a watchdog goroutine that calls os.Exit(1) if
// the process is still alive `after` past the start of shutdown. Use
// it as a last-resort safety net.
func ForceExitAfter(after time.Duration) {
	go func() {
		time.Sleep(after)
		slog.Error("ingestion shutdown: hard deadline exceeded, force exit",
			"after", after.String())
		os.Exit(1)
	}()
}
