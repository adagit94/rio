package server

import (
	"fmt"
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
	ID              I
	Hub             *Hub[I]
	Conn            *ws.Conn
	Subscribed      chan <- bool
	MessagesToWrite chan *MessageIntern[I]
}

func (c *Client[I]) Run() {
	go c.readMessages()
	go c.writeMessages()
}

func (c *Client[I]) readMessages() {
	defer func() {
		c.Hub.CloseClientCh <- c
	}()

	c.Conn.SetReadLimit(c.Hub.ClientOptions.MaxReadBytes)

	for {
		msgType, msg, err := c.Conn.ReadMessage()

		if err != nil {
			fmt.Printf("[ERR][Client: %v] Attempt to read a message failed: %s.\n", c.ID, err)
			return
		}

		c.Hub.MessageToRoute <- &MessageIntern[I]{Source: c, Type: msgType, Data: msg}
	}
}

func (c *Client[I]) writeMessages() {
	pingTicker := time.NewTicker(c.Hub.ClientOptions.PingInterval)
	pongTimer := time.NewTimer(c.Hub.ClientOptions.PongWait)

	pongTimer.Stop()

	var stopSigs = func() {
		pingTicker.Stop()
		pongTimer.Stop()
	}

	defer func() {
		stopSigs()
		c.closeConn()
		c.Hub.CloseClientCh <- c
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
		case msg, ok := <-c.MessagesToWrite:
			if !ok {
				// MessagesToSend channel closed and drained.
				c.writeCloseMessage(ws.CloseNormalClosure, "")
				return
			}

			if !c.writeDataMessage(msg.Type, msg.Data) {
				return
			}

		case <-pingTicker.C:
			if !c.writePingMessage() {
				return
			}

			pongTimer.Reset(c.Hub.ClientOptions.PongWait)

		case <-pongTimer.C:
			c.writeCloseMessage(ws.CloseProtocolError, "No pong received.")
			return
		}
	}
}

func (c *Client[I]) writeDataMessage(msgType int, data []byte) bool {
	if err := c.Conn.WriteMessage(msgType, data); err != nil {
		fmt.Printf("[ERR][Client: %v] Failed to write a data message of %d type: %s.\n", c.ID, msgType, err)
		return false
	}

	return true
}

func (c *Client[I]) writePingMessage() bool {
	if err := c.Conn.WriteMessage(ws.PingMessage, nil); err != nil {
		fmt.Printf("[ERR][Client: %v] Failed to write a ping message: %s.\n", c.ID, err)
		return false
	}

	return true
}

func (c *Client[I]) writeCloseMessage(code int, text string) bool {
	if err := c.Conn.WriteMessage(ws.CloseMessage, ws.FormatCloseMessage(code, text)); err != nil {
		fmt.Printf("[ERR][Client: %v] Failed to write a close message: %s.\n", c.ID, err)
		return false
	}

	return true
}

func (c *Client[I]) closeConn() bool {
	if err := c.Conn.Close(); err != nil {
		fmt.Printf("[ERR][Client: %v] Attempt to close the connection failed: %s.\n", c.ID, err)
		return false
	}

	return true
}
