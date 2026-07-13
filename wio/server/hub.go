package server

import (
	"time"
	ws "github.com/fasthttp/websocket"
)

type RouteMessage[I ID] func(clients Clients[I], msg *MessageIntern[I])
type Clients[I ID] map[I]*Client[I]

type CommonClientOptions[I ID] struct {
	MessagesToWriteBuffSize int
	MaxReadBytes            int64
	PingInterval            time.Duration
	PongWait                time.Duration
}

type Hub[I ID] struct {
	Clients           Clients[I]
	SubscribeClientCh chan *Client[I]
	CloseClientCh     chan *Client[I]
	MessageToRoute    chan *MessageIntern[I]
	RouteMessage      RouteMessage[I]
	ClientOptions     *CommonClientOptions[I]
}

func (h *Hub[I]) SubscribeClient(id I, conn *ws.Conn) <- chan bool {
	subscribed := make(chan bool, 1)

	h.SubscribeClientCh <- &Client[I]{ID: id, Hub: h, Conn: conn, MessagesToWrite: make(chan *MessageIntern[I], h.ClientOptions.MessagesToWriteBuffSize), Subscribed: subscribed}

	return subscribed
}

func (h *Hub[I]) Run() {
	go h.processSignals()
}

func (h *Hub[I]) processSignals() {
	for {
		select {
		case c := <-h.CloseClientCh:
			if _, exists := h.Clients[c.ID]; exists {
				h.closeClient(c)
			}

		case c := <-h.SubscribeClientCh:
			if _, exists := h.Clients[c.ID]; !exists {
				h.subscribeClient(c)
				c.Subscribed <- true
			} else {
				c.Subscribed <- false
			}

		case msg := <-h.MessageToRoute:
			h.RouteMessage(h.Clients, msg)
		}
	}
}

func (h *Hub[I]) subscribeClient(c *Client[I]) {
	h.Clients[c.ID] = c
	c.Run()
}

func (h *Hub[I]) closeClient(c *Client[I]) {
	close(c.MessagesToWrite)
	delete(h.Clients, c.ID)
}

type IHub[I ID] interface {
	SubscribeClient(id I, conn *ws.Conn) <- chan bool
}

func CreateHub[I ID](clientOptions CommonClientOptions[I], routeMessage RouteMessage[I]) IHub[I] {
	h := &Hub[I]{
		ClientOptions:     &clientOptions,
		Clients:           make(map[I]*Client[I]),
		SubscribeClientCh: make(chan *Client[I]),
		CloseClientCh:     make(chan *Client[I]),
		MessageToRoute:    make(chan *MessageIntern[I]),
		RouteMessage:      routeMessage,
	}

	h.Run()

	return h
}
