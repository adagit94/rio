package server

import (
	ws "github.com/fasthttp/websocket"
	"time"
)

type RouteMessage[I ID] func(clients Clients[I], msg *MessageIntern[I])
type Clients[I ID] map[I]*Client[I]

type HubOptions[I ID] struct {
	MaxReadBytes int64
	PingInterval time.Duration
	PongWait     time.Duration
}

type Hub[I ID] struct {
	Clients          Clients[I]
	SubscribeCh      chan *Client[I]
	UnsubscribeCh    chan I
	UnsubscribeAllCh chan bool
	StopCh           chan bool
	MessageToRoute   chan *MessageIntern[I]
	RouteMessage     RouteMessage[I]
	Options          *HubOptions[I]
}

func (h *Hub[I]) Subscribe(id I, conn *ws.Conn, messagesToWriteSize int) {
	h.SubscribeCh <- &Client[I]{ID: id, Hub: h, Conn: conn, MessagesToWrite: make(chan *MessageIntern[I], messagesToWriteSize)}
}

func (h *Hub[I]) Unsubscribe(id I) {
	h.UnsubscribeCh <- id
}

func (h *Hub[I]) UnsubscribeAll() {
	h.UnsubscribeAllCh <- true
}

func (h *Hub[I]) Run() {
	go h.processSignals()
}

func (h *Hub[I]) processSignals() {
	for {
		select {
		case stop := <-h.StopCh:
			if stop {
				return
			}

		case v := <-h.UnsubscribeAllCh:
			if v {
				h.closeClients()
			}

		case id := <-h.UnsubscribeCh:
			if client, exists := h.Clients[id]; exists {
				h.closeClient(client)
			}

		case client := <-h.SubscribeCh:
			if _, exists := h.Clients[client.ID]; !exists {
				h.Clients[client.ID] = client
				client.Run()
			}

		case msg := <-h.MessageToRoute:
			h.RouteMessage(h.Clients, msg)
		}
	}
}

func (h *Hub[I]) closeClient(c *Client[I]) {
	close(c.MessagesToWrite)
	delete(h.Clients, c.ID)
}

func (h *Hub[I]) closeClients() {
	for _, c := range h.Clients {
		h.closeClient(c)
	}
}

type IHub[I ID] interface {
	Subscribe(id I, conn *ws.Conn, messagesToWriteSize int)
	Unsubscribe(id I)
	UnsubscribeAll()
	Run()
}

func CreateHub[I ID](options HubOptions[I], routeMessage RouteMessage[I]) IHub[I] {
	hub := &Hub[I]{
		Options:          &options,
		Clients:          make(map[I]*Client[I]),
		SubscribeCh:      make(chan *Client[I]),
		UnsubscribeCh:    make(chan I),
		UnsubscribeAllCh: make(chan bool),
		StopCh:           make(chan bool),
		MessageToRoute:   make(chan *MessageIntern[I]),
		RouteMessage:     routeMessage,
	}

	return hub
}
