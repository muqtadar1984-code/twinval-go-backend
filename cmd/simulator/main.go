// Command simulator generates synthetic sensor data matching the
// ASHRAE GEPIII profiles used in the Python POC and POSTs it to the
// TwinVal webhook receiver at a configurable interval.
//
// Configuration:
//
//	TWINVAL_WEBHOOK_URL     (default http://localhost:8080/ingest/webhook)
//	TWINVAL_PROPERTY_ID     (default PROP-ASHRAE-001)
//	TWINVAL_SIM_INTERVAL_MS (default 1000)
//
// Stress events occur at ~5% probability per cycle, briefly pushing
// vibration and strain above their normal ceilings — useful for
// exercising the SHF Wöhler curve and the volatility circuit breaker
// in the exchange package.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/twinval/internal/condition"
	"github.com/twinval/internal/ingest"
)

func main() {
	webhookURL := getEnv("TWINVAL_WEBHOOK_URL", "http://localhost:8080/ingest/webhook")
	propertyID := getEnv("TWINVAL_PROPERTY_ID", "PROP-ASHRAE-001")
	intervalMs := getEnvInt("TWINVAL_SIM_INTERVAL_MS", 1000)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		s := <-sigCh
		log.Printf("received signal %v, stopping simulator", s)
		cancel()
	}()

	log.Printf("twinval-simulator: posting to %s every %dms (property=%s)",
		webhookURL, intervalMs, propertyID)

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	client := &http.Client{Timeout: 5 * time.Second}
	ticker := time.NewTicker(time.Duration(intervalMs) * time.Millisecond)
	defer ticker.Stop()

	cycle := 0
	for {
		select {
		case <-ctx.Done():
			log.Println("twinval-simulator stopped")
			return
		case <-ticker.C:
			cycle++
			payload := generatePayload(propertyID, rng)
			if err := postPayload(ctx, client, webhookURL, payload); err != nil {
				log.Printf("cycle %d: post error: %v", cycle, err)
			}
		}
	}
}

func postPayload(ctx context.Context, client *http.Client, url string, p ingest.WebhookPayload) error {
	body, err := json.Marshal(p)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		log.Printf("non-202 response: %d", resp.StatusCode)
	}
	return nil
}

// generatePayload returns one cycle of synthetic readings approximating
// ASHRAE GEPIII commercial-office profiles. Stress events fire at ~5%
// probability and override vibration + strain to above-ceiling values.
func generatePayload(propertyID string, rng *rand.Rand) ingest.WebhookPayload {
	now := time.Now().UnixNano()
	stress := rng.Float64() < 0.05

	vibration := 0.01 + rng.Float64()*0.07 // 0.01–0.08 m/s² normal
	strain := 50.0 + rng.Float64()*250.0   // 50–300 microstrain normal
	if stress {
		vibration = 0.25 // above the 0.05 ceiling
		strain = 700.0   // well above the 200 ceiling
	}

	temperature := 20.0 + rng.Float64()*6.0  // 20–26 °C
	humidity := 40.0 + rng.Float64()*20.0    // 40–60 %RH
	pm := 5.0 + rng.Float64()*15.0           // 5–20 µg/m³
	occupancy := 0.3 + rng.Float64()*0.6     // 0.3–0.9
	electrical := 0.4 + rng.Float64()*0.45   // 0.4–0.85
	water := 0.2 + rng.Float64()*0.4         // 0.2–0.6

	readings := []ingest.WebhookReading{
		{SensorID: "vib1", SensorType: string(condition.SensorVibration), Zone: "core", TimestampNs: now, Value: vibration, CalibrationDaysAgo: 30},
		{SensorID: "str1", SensorType: string(condition.SensorStrain), Zone: "beam-A", TimestampNs: now, Value: strain, CalibrationDaysAgo: 30},
		{SensorID: "tmp1", SensorType: string(condition.SensorTemperature), Zone: "lobby", TimestampNs: now, Value: temperature, CalibrationDaysAgo: 30},
		{SensorID: "hum1", SensorType: string(condition.SensorHumidity), Zone: "lobby", TimestampNs: now, Value: humidity, CalibrationDaysAgo: 30},
		{SensorID: "pm1", SensorType: string(condition.SensorPM25), Zone: "lobby", TimestampNs: now, Value: pm, CalibrationDaysAgo: 30},
		{SensorID: "occ1", SensorType: string(condition.SensorOccupancy), Zone: "core", TimestampNs: now, Value: occupancy, CalibrationDaysAgo: 30},
		{SensorID: "el1", SensorType: string(condition.SensorElectrical), Zone: "core", TimestampNs: now, Value: electrical, CalibrationDaysAgo: 30},
		{SensorID: "wat1", SensorType: string(condition.SensorWater), Zone: "core", TimestampNs: now, Value: water, CalibrationDaysAgo: 30},
	}

	return ingest.WebhookPayload{
		PropertyID:    propertyID,
		WindowStartNs: now - 1_000_000_000,
		WindowEndNs:   now,
		Readings:      readings,
	}
}

func getEnv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func getEnvInt(key string, def int) int {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
