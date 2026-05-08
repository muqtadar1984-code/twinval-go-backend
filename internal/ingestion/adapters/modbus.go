package adapters

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/grid-x/modbus"

	"github.com/twinval/internal/ingestion/config"
	"github.com/twinval/internal/ingestion/models"
)

// modbusClient is the narrow interface ModbusAdapter needs from its
// transport. Defined here (not in grid-x) so tests can mock without
// real network I/O.
type modbusClient interface {
	ReadInputRegisters(address, quantity uint16) ([]byte, error)
	ReadHoldingRegisters(address, quantity uint16) ([]byte, error)
	Close() error
}

// modbusClientFactory builds a fresh client for a given host/port/unit.
// Tests inject a fake factory; production wires the grid-x TCP client.
type modbusClientFactory func(host string, port int, unitID uint8) (modbusClient, error)

// ModbusAdapter polls Modbus TCP slaves per modbus_map.yaml. One
// goroutine per device polls every PollIntervalSeconds; failures are
// logged and counted but do not stop the loop.
type ModbusAdapter struct {
	devices  []config.ModbusDevice
	interval time.Duration
	submit   Submitter
	factory  modbusClientFactory
	now      func() time.Time

	wg     sync.WaitGroup
	cancel context.CancelFunc

	closed atomic.Bool

	polls         atomic.Uint64
	readErrors    atomic.Uint64
	decodeFailures atomic.Uint64
	submissions   atomic.Uint64
}

// ModbusOptions packages everything the adapter needs at startup.
type ModbusOptions struct {
	Map     config.ModbusMap
	Submit  Submitter
	Factory modbusClientFactory // optional; nil = real TCP client
	Now     func() time.Time
}

// NewModbusAdapter builds the adapter. If Factory is nil, a real
// grid-x TCP client is used.
func NewModbusAdapter(opts ModbusOptions) *ModbusAdapter {
	if opts.Factory == nil {
		opts.Factory = realTCPClient
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	interval := time.Duration(opts.Map.PollIntervalSeconds) * time.Second
	if interval <= 0 {
		interval = 30 * time.Second
	}
	return &ModbusAdapter{
		devices:  opts.Map.Devices,
		interval: interval,
		submit:   opts.Submit,
		factory:  opts.Factory,
		now:      opts.Now,
	}
}

// Start launches one goroutine per configured device. Returns nil
// immediately even if some devices fail to connect — they will retry
// on the next tick. Returns an error only if there is nothing to do
// (no devices configured).
func (a *ModbusAdapter) Start(ctx context.Context) error {
	if a.closed.Load() {
		return errors.New("adapter already stopped")
	}
	if len(a.devices) == 0 {
		// No devices configured is not an error — the operator may
		// run with only MQTT or webhook adapters.
		return nil
	}
	pollCtx, cancel := context.WithCancel(ctx)
	a.cancel = cancel

	for i := range a.devices {
		a.wg.Add(1)
		go a.runDevice(pollCtx, a.devices[i])
	}
	return nil
}

// Stop cancels every device goroutine and waits for them to exit.
func (a *ModbusAdapter) Stop(ctx context.Context) error {
	if !a.closed.CompareAndSwap(false, true) {
		return nil
	}
	if a.cancel != nil {
		a.cancel()
	}
	done := make(chan struct{})
	go func() {
		a.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ModbusStats is a snapshot of adapter counters.
type ModbusStats struct {
	Polls          uint64
	ReadErrors     uint64
	DecodeFailures uint64
	Submissions    uint64
}

// Stats returns adapter counters.
func (a *ModbusAdapter) Stats() ModbusStats {
	return ModbusStats{
		Polls:          a.polls.Load(),
		ReadErrors:     a.readErrors.Load(),
		DecodeFailures: a.decodeFailures.Load(),
		Submissions:    a.submissions.Load(),
	}
}

func (a *ModbusAdapter) runDevice(ctx context.Context, dev config.ModbusDevice) {
	defer a.wg.Done()
	client, err := a.factory(dev.Host, dev.Port, dev.UnitID)
	if err != nil {
		slog.Warn("modbus: initial connect failed (will retry on tick)",
			"host", dev.Host, "port", dev.Port, "error", err.Error())
	}

	ticker := time.NewTicker(a.interval)
	defer ticker.Stop()

	a.pollOnce(client, dev) // immediate first poll so /metrics has data fast

	for {
		select {
		case <-ctx.Done():
			if client != nil {
				_ = client.Close()
			}
			return
		case <-ticker.C:
			if client == nil {
				// retry connect
				client, err = a.factory(dev.Host, dev.Port, dev.UnitID)
				if err != nil {
					slog.Debug("modbus: reconnect pending",
						"host", dev.Host, "error", err.Error())
					continue
				}
			}
			if !a.pollOnce(client, dev) {
				// pollOnce returns false when the device looks unhealthy;
				// drop the client so the next tick reconnects.
				_ = client.Close()
				client = nil
			}
		}
	}
}

// pollOnce reads every sensor on `dev` once. Returns false if EVERY
// sensor read failed — that's a strong signal the slave is gone and
// we should reconnect.
func (a *ModbusAdapter) pollOnce(client modbusClient, dev config.ModbusDevice) bool {
	a.polls.Add(1)
	if client == nil {
		return false
	}
	now := a.now()
	failures := 0
	for _, s := range dev.Sensors {
		raw, err := readRegister(client, s)
		if err != nil {
			a.readErrors.Add(1)
			failures++
			slog.Debug("modbus: read failed",
				"host", dev.Host, "register", s.Register, "error", err.Error())
			continue
		}
		val, ok := decodeRegister(raw, s)
		if !ok {
			a.decodeFailures.Add(1)
			continue
		}
		quality := s.Quality
		if quality <= 0 {
			quality = 1.0
		}
		a.submit.Submit(models.SensorReading{
			SensorID:   s.SensorID,
			Building:   s.Building,
			Zone:       s.Zone,
			SensorType: s.SensorType,
			Unit:       s.Unit,
			Value:      val,
			Quality:    quality,
			ReceivedAt: now,
			Source:     models.SourceModbus,
		})
		a.submissions.Add(1)
	}
	return failures < len(dev.Sensors)
}

// readRegister picks the right Modbus function based on register_type.
func readRegister(client modbusClient, s config.ModbusSensor) ([]byte, error) {
	switch s.RegisterType {
	case "", "input":
		return client.ReadInputRegisters(s.Register, 1)
	case "holding":
		return client.ReadHoldingRegisters(s.Register, 1)
	default:
		return nil, fmt.Errorf("unknown register_type %q", s.RegisterType)
	}
}

// decodeRegister turns 2 raw bytes into a physical value via the
// configured scale. We decode as int16 — that's the safe choice for
// BMS-style sensors which may report negative temperatures or strain
// values. Sensors that overflow int16 (electrical >32767 kW etc.)
// would need a wider register layout; out of scope for v1.
func decodeRegister(raw []byte, s config.ModbusSensor) (float64, bool) {
	if len(raw) < 2 {
		return 0, false
	}
	u := binary.BigEndian.Uint16(raw[:2])
	signed := int16(u)
	scale := s.Scale
	if scale == 0 {
		scale = 1.0
	}
	return float64(signed) * scale, true
}

// realTCPClient builds a grid-x Modbus TCP client wrapped in our local
// `modbusClient` interface. Each call gets its own context bounded by
// handler.Timeout — adapters do not pass long-lived contexts down.
func realTCPClient(host string, port int, unitID uint8) (modbusClient, error) {
	addr := fmt.Sprintf("%s:%d", host, port)
	handler := modbus.NewTCPClientHandler(addr)
	handler.SlaveID = unitID
	handler.Timeout = 5 * time.Second
	connectCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := handler.Connect(connectCtx); err != nil {
		return nil, fmt.Errorf("connect %s unit %d: %w", addr, unitID, err)
	}
	return &gridXClient{
		handler: handler,
		client:  modbus.NewClient(handler),
	}, nil
}

// gridXClient adapts grid-x's modbus.Client to our modbusClient interface.
type gridXClient struct {
	handler *modbus.TCPClientHandler
	client  modbus.Client
}

func (c *gridXClient) ReadInputRegisters(address, quantity uint16) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return c.client.ReadInputRegisters(ctx, address, quantity)
}

func (c *gridXClient) ReadHoldingRegisters(address, quantity uint16) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return c.client.ReadHoldingRegisters(ctx, address, quantity)
}

func (c *gridXClient) Close() error {
	if c.handler == nil {
		return nil
	}
	return c.handler.Close()
}
