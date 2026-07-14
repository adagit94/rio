package server

import (
	"sync"
	"time"

	ws "github.com/fasthttp/websocket"
)

type Router[I ID] func(clients Clients[I], msg *MessageIntern[I])
type Clients[I ID] map[I]*Client[I]

type CommonClientOptions[I ID] struct {
	WriteBuffSize int
	MaxReadBytes  int64
	PingInterval  time.Duration
	PongWait      time.Duration
}

type Hub[I ID] struct {
	Mu            sync.Mutex
	Clients       Clients[I]
	ClientOptions *CommonClientOptions[I]
	Router        Router[I]
}

func (h *Hub[I]) Subscribe(id I, conn *ws.Conn) *Client[I] {
	h.Mu.Lock()
	defer h.Mu.Unlock()

	if _, exists := h.Clients[id]; !exists {
		c := &Client[I]{ID: id, Hub: h, Conn: conn, WriteBuff: make(chan *MessageIntern[I], h.ClientOptions.WriteBuffSize), Terminated: make(chan bool, 1)}

		h.Clients[id] = c
		c.Run()

		return c
	}

	return nil
}

func (h *Hub[I]) CloseClient(c *Client[I]) {
	h.Mu.Lock()
	defer h.Mu.Unlock()

	if _, exists := h.Clients[c.ID]; exists {
		c.Conn.CloseHandler()(ws.CloseNoStatusReceived, "")
		close(c.WriteBuff)
		delete(h.Clients, c.ID)
	}
}

func (h *Hub[I]) RouteMessage(msg *MessageIntern[I]) {
	h.Mu.Lock()
	defer h.Mu.Unlock()

	h.Router(h.Clients, msg)
}

func CreateHub[I ID](clientOptions CommonClientOptions[I], routeMessage Router[I]) *Hub[I] {
	h := &Hub[I]{
		ClientOptions: &clientOptions,
		Clients:       make(map[I]*Client[I]),
		Router:        routeMessage,
	}

	return h
}
