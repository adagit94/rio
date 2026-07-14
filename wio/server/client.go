package server

import (
	"fmt"
	"sync"
	"time"

	ws "github.com/fasthttp/websocket"
)

type ID interface {
	~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~string
}

type MessageIntern[I ID] struct {
	Source *Client[I]
	Type   int
	Data   []byte
}

type Client[I ID] struct {
	ID         I
	Conn       *ws.Conn
	WriteBuff  chan *MessageIntern[I]
	Terminated chan bool
	Hub        *Hub[I]
}

func (c *Client[I]) Run() {
	var wg sync.WaitGroup

	wg.Go(c.ReadMessages)
	wg.Go(c.WriteMessages)

	go func() {
		wg.Wait()
		c.Terminated <- true
	}()
}

func (c *Client[I]) ReadMessages() {
	defer func() {
		c.Hub.CloseClient(c)
	}()

	c.Conn.SetReadLimit(c.Hub.ClientOptions.MaxReadBytes)

	for {
		msgType, msg, err := c.Conn.ReadMessage()

		if err != nil {
			fmt.Printf("[ERR][Client: %v] Message read error: %s.\n", c.ID, err)
			return
		}

		c.Hub.RouteMessage(&MessageIntern[I]{Source: c, Type: msgType, Data: msg})
	}
}

func (c *Client[I]) WriteMessages() {
	pingTicker := time.NewTicker(c.Hub.ClientOptions.PingInterval)
	pongTimer := time.NewTimer(c.Hub.ClientOptions.PongWait)

	pongTimer.Stop()

	var stopSigs = func() {
		pingTicker.Stop()
		pongTimer.Stop()
	}

	defer func() {
		c.Hub.CloseClient(c)
		c.CloseConn()
	}()

	c.Conn.SetPongHandler(func(appData string) error {
		pongTimer.Stop()
		return nil
	})

	c.Conn.SetCloseHandler(func(code int, text string) error {
		stopSigs()
		return nil
	})

	for {
		select {
		case msg, ok := <-c.WriteBuff:
			if !ok {
				// MessagesToSend channel closed and drained.
				c.WriteCloseMessage(ws.CloseNormalClosure, "")
				return
			}

			if !c.WriteDataMessage(msg.Type, msg.Data) {
				return
			}

		case <-pingTicker.C:
			if !c.WritePingMessage() {
				return
			}

			pongTimer.Reset(c.Hub.ClientOptions.PongWait)

		case <-pongTimer.C:
			c.WriteCloseMessage(ws.CloseProtocolError, "No pong received.")
			return
		}
	}
}

func (c *Client[I]) WriteDataMessage(msgType int, data []byte) bool {
	if err := c.Conn.WriteMessage(msgType, data); err != nil {
		fmt.Printf("[ERR][Client: %v] Failed to write a data message of %d type: %s.\n", c.ID, msgType, err)
		return false
	}

	return true
}

func (c *Client[I]) WritePingMessage() bool {
	if err := c.Conn.WriteMessage(ws.PingMessage, nil); err != nil {
		fmt.Printf("[ERR][Client: %v] Failed to write a ping message: %s.\n", c.ID, err)
		return false
	}

	return true
}

func (c *Client[I]) WriteCloseMessage(code int, text string) bool {
	if err := c.Conn.WriteMessage(ws.CloseMessage, ws.FormatCloseMessage(code, text)); err != nil {
		fmt.Printf("[ERR][Client: %v] Failed to write a close message: %s.\n", c.ID, err)
		return false
	}

	return true
}

func (c *Client[I]) CloseConn() bool {
	if err := c.Conn.Close(); err != nil {
		fmt.Printf("[ERR][Client: %v] Attempt to close the connection failed: %s.\n", c.ID, err)
		return false
	}

	return true
}
