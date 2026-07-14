package server

import (
	ws "github.com/fasthttp/websocket"
	m "maps"
	s "slices"
	"sync"
	"time"
)

type Clients[I ID] map[I]*Client[I]

type CommonClientOptions[I ID] struct {
	WriteBuffSize int
	MaxReadBytes  int64
	PingInterval  time.Duration
	PongWait      time.Duration
}

// Regular user code should use CreateHub constructor instead that returns public interface. Hub type itself is exposed only for more advanced use cases that require custom code. It musn't be changed without mutex (Mu) when it get's used after it's creation and read/write loops run for example.
type Hub[I ID] struct {
	Mu            sync.Mutex
	Clients       Clients[I]
	ClientOptions *CommonClientOptions[I]
	Router        Router[I]
}

// Subscribe attempts to subscribe the connection in case no client with same ID exists. In case of successfull subscription read and write loop of Client get's automatically triggered (Run method) and channel that receives true after process of termination when underlaying Client get's fully unsubscribed, i.e. when both read and write loop (running in separate goroutines) terminate. Receive op. from the channel is optional - it's buffered one with size of 1. Nil is returned is case Client record altready exists for passed id.
func (h *Hub[I]) Subscribe(id I, conn *ws.Conn) <- chan bool {
	h.Mu.Lock()
	defer h.Mu.Unlock()

	if _, exists := h.Clients[id]; !exists {
		c := &Client[I]{ID: id, Hub: h, Conn: conn, WriteBuff: make(chan *MetaMessage[I], h.ClientOptions.WriteBuffSize), Terminated: make(chan bool, 1)}

		h.Clients[id] = c
		c.Run()

		return c.Terminated
	}

	return nil
}

// Returns true in case Client under passed id is subscribed.
func (h *Hub[I]) IsSubscribed(id I) bool {
	h.Mu.Lock()
	defer h.Mu.Unlock()

	if _, exists := h.Clients[id]; exists {
		return true
	}

	return false
}

// Closes client's write buffer and drains it. Connection itself get's closed afterwards in case no error arise. Not intended for external usage - client should be unsubscribed via close control message.
func (h *Hub[I]) CloseClient(c *Client[I]) {
	h.Mu.Lock()
	defer h.Mu.Unlock()

	if _, exists := h.Clients[c.ID]; exists {
		c.Conn.CloseHandler()(ws.CloseNoStatusReceived, "")
		close(c.WriteBuff)
		delete(h.Clients, c.ID)
	}
}

// Route message to appropriate clients as configured by provided Router.
func (h *Hub[I]) RouteMessage(msg *MetaMessage[I]) {
	h.Mu.Lock()
	defer h.Mu.Unlock()

	h.Router(h.Clients, msg)
}

// Safe interface for usage in user code. Other methods and Hub itself (including it's Clients) are mostly for internal usage and types themself exposed only for more complex scenarios that require custom code.
type IHub[I ID] interface {
	Subscribe(id I, conn *ws.Conn) <- chan bool // Subscribe attempts to subscribe the connection in case no client with same ID exists. In case of successfull subscription read and write loop of Client get's automatically triggered (Run method) and channel that receives true after process of termination when underlaying Client get's fully unsubscribed, i.e. when both read and write loop (running in separate goroutines) terminate. Nil is returned is case Client record altready exists for passed id.
	IsSubscribed(id I) bool                   // Returns true in case connection under passed id is subscribed.
}

// Constructor intended for a Hub that requires custom router.
func CreateHub[I ID](clientOptions CommonClientOptions[I], router Router[I]) IHub[I] {
	h := &Hub[I]{
		ClientOptions: &clientOptions,
		Clients:       make(map[I]*Client[I]),
		Router:        router,
	}

	return h
}

// Constructor for broadcasting based Hub that emits messages from any client emitter to all other receivers.
func CreateBroadHub[I ID](clientOptions CommonClientOptions[I]) IHub[I] {
	h := &Hub[I]{
		ClientOptions: &clientOptions,
		Clients:       make(map[I]*Client[I]),
		Router:        Broadcaster[I],
	}

	return h
}

// Constructor for map based Hub that emits messages from configured client emitter in ConnectionsMap to corresponding, configured receivers.
func CreateMappedHub[I ID](clientOptions CommonClientOptions[I], connections ConnectionsMap[I]) IHub[I] {
	connectionsClone := m.Clone(connections)

	for k, v := range connectionsClone {
		connectionsClone[k] = s.Clone(v)
	}

	h := &Hub[I]{
		ClientOptions: &clientOptions,
		Clients:       make(map[I]*Client[I]),
		Router:        Mapper(connectionsClone),
	}

	return h
}
