package api

import (
	"net/http"
	"sync"

	"golang.org/x/net/websocket"

	"github.com/twinval/internal/compute"
	"github.com/twinval/internal/exchange"
	twinsync "github.com/twinval/internal/sync"
)

// Hub manages live WebSocket subscriptions per property. One Hub for the
// whole API server; each property has its own subscriber set inside.
//
// Sends are best-effort: a connection that fails Write is unregistered
// and closed; subscribers in other properties are unaffected.
type Hub struct {
	mu    sync.RWMutex
	conns map[string]map[*websocket.Conn]struct{}
}

// NewHub returns an empty hub.
func NewHub() *Hub {
	return &Hub{conns: make(map[string]map[*websocket.Conn]struct{})}
}

// Register adds conn to the subscriber set for propertyID.
func (h *Hub) Register(propertyID string, conn *websocket.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	set, ok := h.conns[propertyID]
	if !ok {
		set = make(map[*websocket.Conn]struct{})
		h.conns[propertyID] = set
	}
	set[conn] = struct{}{}
}

// Unregister removes conn from the subscriber set for propertyID.
// Idempotent — safe to call after a Hub-initiated disconnect.
func (h *Hub) Unregister(propertyID string, conn *websocket.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if set, ok := h.conns[propertyID]; ok {
		delete(set, conn)
		if len(set) == 0 {
			delete(h.conns, propertyID)
		}
	}
}

// BroadcastJSON marshals payload as JSON and sends to every subscriber
// of propertyID. Connections that error are unregistered and closed.
func (h *Hub) BroadcastJSON(propertyID string, payload interface{}) {
	h.mu.RLock()
	set := h.conns[propertyID]
	conns := make([]*websocket.Conn, 0, len(set))
	for c := range set {
		conns = append(conns, c)
	}
	h.mu.RUnlock()

	for _, c := range conns {
		if err := websocket.JSON.Send(c, payload); err != nil {
			h.Unregister(propertyID, c)
			_ = c.Close()
		}
	}
}

// SubscriberCount returns the number of live subscribers for propertyID.
// Useful for diagnostics.
func (h *Hub) SubscriberCount(propertyID string) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.conns[propertyID])
}

// WSMessage is the JSON shape pushed over each subscriber connection.
type WSMessage struct {
	PropertyID   string                      `json:"property_id"`
	Timestamp    int64                       `json:"timestamp"`
	Indicators   compute.TechnicalIndicators `json:"indicators"`
	HealthFactor float64                     `json:"health_factor"`
	RTPMV        float64                     `json:"rtpmv"`
	Currency     string                      `json:"currency"`
	State        twinsync.DigitalTwinState   `json:"state"`
	Exchange     exchange.ExchangeParams     `json:"exchange"`
}

// WebSocketHandler returns an http.Handler that upgrades GET requests
// matching /ws/property/{id} to a WebSocket. The handler holds the
// connection open by reading on it; client-side close ends the loop.
func WebSocketHandler(hub *Hub) http.Handler {
	return websocket.Handler(func(ws *websocket.Conn) {
		id := ws.Request().PathValue("id")
		if id == "" {
			return
		}

		hub.Register(id, ws)
		defer hub.Unregister(id, ws)
		defer ws.Close()

		// Park on Read until the client disconnects or the connection
		// fails. Any incoming frames are discarded — this endpoint is
		// pure server→client.
		var buf [1024]byte
		for {
			if _, err := ws.Read(buf[:]); err != nil {
				return
			}
		}
	})
}
