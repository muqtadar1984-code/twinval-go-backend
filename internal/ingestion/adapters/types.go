// Package adapters holds the three protocol adapters that feed the
// live ingestion pipeline:
//
//   - webhook:  HTTP POST with HMAC-SHA256 + per-IP rate limit
//   - mqtt:     subscribes to twinval/{building}/{zone}/{sensor_type}/{sensor_id}
//   - modbus:   polls Modbus TCP slaves per modbus_map.yaml
//
// Each adapter is independently startable / stoppable. None know about
// each other; all converge on Submitter.Submit which is implemented by
// internal/ingestion.Pipeline.
package adapters

import "github.com/twinval/internal/ingestion/models"

// Submitter is the narrow contract every adapter needs. The Pipeline
// implements this; tests can pass a fake recorder.
type Submitter interface {
	Submit(models.SensorReading)
}
