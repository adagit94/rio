package server

import (
	"fmt"
	"sync"
	"time"

	ws "github.com/fasthttp/websocket"
)

// ID is currently constrained to common integer and string types to ease formatting of logged messages and make it predictable.
type ID interface {
	~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~string
}

// MetaMessage contains additional information useful for routing purposes. Sender musn't be changed in user code and it's channels not closed.
type MetaMessage[I ID] struct {
	Sender *Client[I]
	Type   int // Should be 1 (Text) or 2 (Binary) when passed to the router. Control messages (Ping, Pong, Close) are handled in the background.
	Data   []byte // Original message data.
}

// Provided Router should send MetaMessage to WriteBuff in case Client is relevant. True value will be send into Terminated channel after both read and write loop (running in separate goroutines) terminate. Client musn't be changed in user code and it's channels not closed after it's creation when it get's used.
type Client[I ID] struct {
	Conn       *ws.Conn
	Hub        *Hub[I]
	ID         I
	WriteBuff  chan *MetaMessage[I] // WriteBuff is intended for send channel operations as part of the Router logic.
	Terminated chan bool // When created through Hub.Subscribe method, it's buffered with size of 1 to optionally await termination event that occurs after both read and write loop (running in separate goroutines) terminate.
}

// Triggers read and write loop each running in separate goroutine. It will send true value into Terminated channel after both terminate - e.g. after client get's closed with explicit close control message and WriteBuff of Client get's drained. Usually this method doesn't have to be Run explicitly, but rather implicitly via Subscribe method of the Hub.
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

		c.Hub.RouteMessage(&MetaMessage[I]{Sender: c, Type: msgType, Data: msg})
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

// Method intended mostly for internal usage. In case it's used as part of user code, it musn't be called concurrently with running WriteMessages loop.
func (c *Client[I]) WriteDataMessage(msgType int, data []byte) bool {
	if err := c.Conn.WriteMessage(msgType, data); err != nil {
		fmt.Printf("[ERR][Client: %v] Failed to write a data message of %d type: %s.\n", c.ID, msgType, err)
		return false
	}

	return true
}

// Method intended mostly for internal usage. In case it's used as part of user code, it musn't be called concurrently with running WriteMessages loop.
func (c *Client[I]) WritePingMessage() bool {
	if err := c.Conn.WriteMessage(ws.PingMessage, nil); err != nil {
		fmt.Printf("[ERR][Client: %v] Failed to write a ping message: %s.\n", c.ID, err)
		return false
	}

	return true
}

// Method intended mostly for internal usage. In case it's used as part of user code, it musn't be called concurrently with running WriteMessages loop.
func (c *Client[I]) WriteCloseMessage(code int, text string) bool {
	if err := c.Conn.WriteMessage(ws.CloseMessage, ws.FormatCloseMessage(code, text)); err != nil {
		fmt.Printf("[ERR][Client: %v] Failed to write a close message: %s.\n", c.ID, err)
		return false
	}

	return true
}

// Method intended mostly for internal usage. Send close control message to client you wish to close to gracefully shutdown connected client.
func (c *Client[I]) CloseConn() bool {
	if err := c.Conn.Close(); err != nil {
		fmt.Printf("[ERR][Client: %v] Attempt to close the connection failed: %s.\n", c.ID, err)
		return false
	}

	return true
}