package adapters

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twinval/internal/ingestion/config"
)

// fakeModbusClient lets tests control reads without a network.
type fakeModbusClient struct {
	mu          sync.Mutex
	inputData   map[uint16][]byte
	holdingData map[uint16][]byte
	readErr     error
	closed      atomic.Bool
}

func (c *fakeModbusClient) ReadInputRegisters(address, _ uint16) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.readErr != nil {
		return nil, c.readErr
	}
	v, ok := c.inputData[address]
	if !ok {
		return nil, errors.New("no input data")
	}
	return v, nil
}

func (c *fakeModbusClient) ReadHoldingRegisters(address, _ uint16) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.readErr != nil {
		return nil, c.readErr
	}
	v, ok := c.holdingData[address]
	if !ok {
		return nil, errors.New("no holding data")
	}
	return v, nil
}

func (c *fakeModbusClient) Close() error { c.closed.Store(true); return nil }

func uint16ToBytes(v uint16) []byte {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, v)
	return b
}

func TestDecodeRegister_PositiveScale(t *testing.T) {
	// Register 225, scale 0.1 -> 22.5 (e.g. 22.5°C)
	got, ok := decodeRegister(uint16ToBytes(225), config.ModbusSensor{Scale: 0.1})
	if !ok {
		t.Fatal("expected ok")
	}
	if got != 22.5 {
		t.Fatalf("expected 22.5, got %v", got)
	}
}

func TestDecodeRegister_NegativeViaInt16(t *testing.T) {
	// uint16 65486 == int16 -50; with scale 0.1 -> -5.0
	got, ok := decodeRegister(uint16ToBytes(65486), config.ModbusSensor{Scale: 0.1})
	if !ok {
		t.Fatal("expected ok")
	}
	if got != -5.0 {
		t.Fatalf("expected -5.0, got %v", got)
	}
}

func TestDecodeRegister_DefaultScaleIsOne(t *testing.T) {
	got, _ := decodeRegister(uint16ToBytes(42), config.ModbusSensor{Scale: 0})
	if got != 42 {
		t.Fatalf("expected 42 with default scale=1, got %v", got)
	}
}

func TestDecodeRegister_TooShort(t *testing.T) {
	if _, ok := decodeRegister([]byte{0x01}, config.ModbusSensor{Scale: 1}); ok {
		t.Fatal("1-byte buffer should fail decode")
	}
}

func TestModbusAdapter_PollSubmitsReadings(t *testing.T) {
	rec := &recordingSubmitter{}
	fake := &fakeModbusClient{
		inputData: map[uint16][]byte{
			100: uint16ToBytes(225), // 22.5 with scale 0.1
		},
	}

	mp := config.ModbusMap{
		PollIntervalSeconds: 1,
		Devices: []config.ModbusDevice{{
			Host: "10.0.0.1", Port: 502, UnitID: 1,
			Sensors: []config.ModbusSensor{{
				Register: 100, RegisterType: "input",
				SensorID: "T-1", SensorType: "temperature",
				Building: "B", Zone: "Z", Unit: "°C",
				Scale: 0.1, Quality: 1.0,
			}},
		}},
	}

	a := NewModbusAdapter(ModbusOptions{
		Map:    mp,
		Submit: rec,
		Factory: func(_ string, _ int, _ uint8) (modbusClient, error) {
			return fake, nil
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	if err := a.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	// Allow the immediate first poll to run.
	time.Sleep(100 * time.Millisecond)
	cancel()
	stopCtx, stopCancel := context.WithTimeout(context.Background(), time.Second)
	defer stopCancel()
	_ = a.Stop(stopCtx)

	if rec.count() == 0 {
		t.Fatalf("expected at least 1 submission, got 0; stats=%+v", a.Stats())
	}
	got := rec.items[0]
	if got.SensorID != "T-1" || got.Value != 22.5 || got.SensorType != "temperature" {
		t.Errorf("decoded reading wrong: %+v", got)
	}
	if got.Source != "modbus" {
		t.Errorf("Source should be modbus, got %s", got.Source)
	}
}

func TestModbusAdapter_HoldingRegisterPath(t *testing.T) {
	rec := &recordingSubmitter{}
	fake := &fakeModbusClient{
		holdingData: map[uint16][]byte{
			200: uint16ToBytes(15000), // 150.00 with scale 0.01
		},
	}
	mp := config.ModbusMap{
		PollIntervalSeconds: 1,
		Devices: []config.ModbusDevice{{
			Host: "10.0.0.2", Port: 502, UnitID: 1,
			Sensors: []config.ModbusSensor{{
				Register: 200, RegisterType: "holding",
				SensorID: "E-1", SensorType: "electrical_load",
				Building: "B", Zone: "Z", Unit: "kW",
				Scale: 0.01, Quality: 0.95,
			}},
		}},
	}
	a := NewModbusAdapter(ModbusOptions{
		Map: mp, Submit: rec,
		Factory: func(_ string, _ int, _ uint8) (modbusClient, error) {
			return fake, nil
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	_ = a.Start(ctx)
	time.Sleep(100 * time.Millisecond)
	cancel()
	stopCtx, stopCancel := context.WithTimeout(context.Background(), time.Second)
	defer stopCancel()
	_ = a.Stop(stopCtx)

	if rec.count() == 0 {
		t.Fatalf("expected submission for holding register, got 0; stats=%+v", a.Stats())
	}
	if rec.items[0].Value != 150.0 {
		t.Errorf("expected 150.0, got %v", rec.items[0].Value)
	}
	if rec.items[0].Quality != 0.95 {
		t.Errorf("expected quality=0.95 from map, got %v", rec.items[0].Quality)
	}
}

func TestModbusAdapter_NoDevicesIsNotError(t *testing.T) {
	a := NewModbusAdapter(ModbusOptions{Submit: &recordingSubmitter{}})
	if err := a.Start(context.Background()); err != nil {
		t.Fatalf("expected no error for empty device list, got %v", err)
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = a.Stop(stopCtx)
}

func TestModbusAdapter_ReadErrorCounted(t *testing.T) {
	rec := &recordingSubmitter{}
	fake := &fakeModbusClient{readErr: errors.New("bad slave")}
	mp := config.ModbusMap{
		PollIntervalSeconds: 1,
		Devices: []config.ModbusDevice{{
			Host: "10.0.0.1", Port: 502, UnitID: 1,
			Sensors: []config.ModbusSensor{{
				Register: 100, RegisterType: "input",
				SensorID: "T-1", SensorType: "temperature",
				Building: "B", Zone: "Z",
				Scale: 0.1, Quality: 1.0,
			}},
		}},
	}
	a := NewModbusAdapter(ModbusOptions{
		Map: mp, Submit: rec,
		Factory: func(_ string, _ int, _ uint8) (modbusClient, error) { return fake, nil },
	})
	ctx, cancel := context.WithCancel(context.Background())
	_ = a.Start(ctx)
	time.Sleep(100 * time.Millisecond)
	cancel()
	stopCtx, stopCancel := context.WithTimeout(context.Background(), time.Second)
	defer stopCancel()
	_ = a.Stop(stopCtx)

	stats := a.Stats()
	if stats.ReadErrors == 0 {
		t.Errorf("expected at least 1 read error, got %d", stats.ReadErrors)
	}
	if stats.Submissions != 0 {
		t.Errorf("read errors must not produce submissions, got %d", stats.Submissions)
	}
}

func TestModbusAdapter_ConnectFailureIsRetried(t *testing.T) {
	// Factory fails the first call, succeeds on subsequent ones.
	var calls atomic.Int32
	rec := &recordingSubmitter{}
	fake := &fakeModbusClient{
		inputData: map[uint16][]byte{100: uint16ToBytes(50)},
	}

	mp := config.ModbusMap{
		PollIntervalSeconds: 1,
		Devices: []config.ModbusDevice{{
			Host: "10.0.0.1", Port: 502, UnitID: 1,
			Sensors: []config.ModbusSensor{{
				Register: 100, RegisterType: "input",
				SensorID: "T", SensorType: "temperature",
				Building: "B", Zone: "Z", Scale: 1.0, Quality: 1.0,
			}},
		}},
	}
	a := NewModbusAdapter(ModbusOptions{
		Map: mp, Submit: rec,
		Factory: func(_ string, _ int, _ uint8) (modbusClient, error) {
			n := calls.Add(1)
			if n == 1 {
				return nil, errors.New("first connect fails")
			}
			return fake, nil
		},
	})
	// Make the poll interval really short so the test is fast.
	a.interval = 100 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	_ = a.Start(ctx)
	// Wait for the second factory call to succeed and submit.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if rec.count() > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	stopCtx, stopCancel := context.WithTimeout(context.Background(), time.Second)
	defer stopCancel()
	_ = a.Stop(stopCtx)

	if rec.count() == 0 {
		t.Fatalf("expected reconnect to succeed eventually, got 0 submissions; calls=%d stats=%+v",
			calls.Load(), a.Stats())
	}
}
