package client

import (
	"GolangRabbitMQBroker/protocol"
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"
)

type writeRequest struct {
	data []byte
	err  chan error
}

type Config struct {
	ClientName   string
	Username     string
	Password     string
	ChannelMax   int
	FrameMax     int
	HeartbeatSec int
}

type Client struct {
	conn       net.Conn
	mu         sync.Mutex
	r          *bufio.Reader
	w          *bufio.Writer
	channels   map[uint16]*ClientChannel
	nextChanID uint16
	requestID  uint16

	clientName   string
	username     string
	password     string
	channelMax   int
	framesMax    int
	heartbeatSec int
	Incoming     chan Event

	writeCh   chan writeRequest
	closeOnce sync.Once
	closed    chan struct{}
}

func Dial(address string, cfg Config) (*Client, error) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}

	c := &Client{
		conn:       conn,
		r:          bufio.NewReader(conn),
		w:          bufio.NewWriter(conn),
		channels:   make(map[uint16]*ClientChannel),
		clientName: cfg.ClientName,
		username:   cfg.Username,
		password:   cfg.Password,
		Incoming:   make(chan Event, 100),

		writeCh: make(chan writeRequest, 64),
		closed:  make(chan struct{}),
	}
	go c.writePump()

	return c, nil
}

func (c *Client) send(data []byte) error {
	req := writeRequest{data: data, err: make(chan error, 1)}
	select {
	case c.writeCh <- req:
		return <-req.err
	case <-c.closed:
		return fmt.Errorf("connection closed")
	}
}

func (c *Client) writePump() {
	for {
		select {
		case req := <-c.writeCh:
			_, err := c.w.Write(req.data)
			if err != nil {
				req.err <- err
				return
			}
			err = c.w.Flush()
			req.err <- err
			if err != nil {
				return
			}
		case <-c.closed:
			return
		}
	}
}

func (c *Client) shutdown() {
	c.closeOnce.Do(func() {
		close(c.closed)
	})
}

func (c *Client) nextRequestID() uint16 {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.requestID++
	return c.requestID
}

func (c *Client) nextChannelid() uint16 {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.nextChanID++
	return c.nextChanID
}

func (c *Client) OpenChannel(ctx context.Context) (ch *ClientChannel, err error) {
	id := c.nextChannelid()
	reqID := c.nextRequestID()
	clientCh := NewClientChannel(id, c)
	c.mu.Lock()
	c.channels[id] = clientCh
	c.mu.Unlock()

	respCh := clientCh.registerREQ(reqID)

	if err := c.WriteEnvelope(protocol.ChannelOpenType, reqID, protocol.ChannelOpen{
		ID: id,
	}); err != nil {
		delete(c.channels, id)
		return nil, err
	}

	//i am waiting for the response from the server
	//if a channel.open-ok is read the channel unblocks with no error
	//if an error is returned from the server
	select {
	case res := <-respCh:
		if res.Err != nil {
			delete(c.channels, id)
			return nil, res.Err
		}
		return clientCh, nil
	case <-ctx.Done():
		delete(c.channels, id)
		ch.unRegisterREQ(reqID)
		c.WriteEnvelope(protocol.ChannelCloseType, c.nextRequestID(), protocol.ChannelClose{
			ID: id,
		})
		return nil, ctx.Err()
	}
}

// Follow Protocol Rules to do Handshake or Return an Error
func (c *Client) Handshake() error {
	//send header
	err := c.WriteProtocolHeader()
	if err != nil {
		return err
	}
	//read connection.start
	var start protocol.ConnectionStart
	err = c.ReadMessage(&start)
	if err != nil {
		return err
	}
	//Send Connection.start_ok
	startOK := protocol.NewConnectionStartOK(c.clientName, c.username, c.password)
	err = c.WriteMessage(startOK)
	if err != nil {
		return err
	}
	//Read Connection.tune
	var connectionTune protocol.ConnectionTune
	err = c.ReadMessage(&connectionTune)
	if err != nil {
		return err
	}
	//Send Connection.tune_ok
	connectionTuneOK := protocol.NewConnectionTuneOK(c.channelMax, c.framesMax, c.heartbeatSec)
	err = c.WriteMessage(connectionTuneOK)
	if err != nil {
		return err
	}
	//Send Connection.Open
	connectionOpen := protocol.NewConnectionOpen()
	err = c.WriteMessage(connectionOpen)
	if err != nil {
		return err
	}
	//Read Connectin.Open_ok
	var connectionOpenOK protocol.ConnectionOpenOK
	err = c.ReadMessage(&connectionOpenOK)

	return nil
}

func (c *Client) WriteMessage(data any) error {
	bytes, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return c.send(append(bytes, '\n'))
}
func (c *Client) WriteChannelEnvelope(channelID uint16, envType protocol.Method, reqID uint16, msg any) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	env := protocol.Envelope{
		ChannelID: channelID,
		RequestID: reqID,
		Type:      envType,
		Payload:   payload,
	}
	bytes, err := json.Marshal(env)
	if err != nil {
		return err
	}
	return c.send(append(bytes, '\n'))
}
func (c *Client) WriteEnvelope(envType protocol.Method, reqID uint16, msg any) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	env := protocol.Envelope{
		RequestID: reqID,
		Type:      envType,
		Payload:   payload,
	}
	bytes, err := json.Marshal(env)
	if err != nil {
		return err
	}
	return c.send(append(bytes, '\n'))
}

func (c *Client) WriteProtocolHeader() error {
	return c.send([]byte(protocol.ProtocolHeader + "\n"))
}

func (c *Client) ReadMessage(pointer any) error {
	return protocol.ReadMessage(c.r, pointer)
}

func (c *Client) ReadEnvelope(env *protocol.Envelope) error {
	return protocol.ReadEnvelope(c.r, env)
}

func (c *Client) ReadLoop() {
	defer c.shutdown()
	for {
		var env protocol.Envelope
		if err := c.ReadEnvelope(&env); err != nil {
			log.Println(err)
			close(c.Incoming)
			return
		}
		switch env.Type {
		case protocol.ChannelOpenOKType:
			c.handleChannelOpenOK(env)
		case protocol.ChannelCloseOKType:
			c.handleChannelCloseOK(env)
		default:
			ch, ok := c.channels[env.ChannelID]
			if !ok {
				return
			}
			ch.route(env)
		}
	}
}

func (c *Client) handleChannelOpenOK(env protocol.Envelope) {
	var channelOpenOK protocol.ChannelOpenOK
	err := json.Unmarshal(env.Payload, &channelOpenOK)
	if err != nil {
		log.Println("unable to unmarshall server response")
	}
	ch, ok := c.channels[channelOpenOK.ID]
	if !ok {
		log.Println("this is the channelID:", env.ChannelID)
		log.Println("did not find channel")
		return
	}
	ch.resolve(env.RequestID, Response{
		Data: channelOpenOK,
	})
}

func (c *Client) handleChannelCloseOK(env protocol.Envelope) {
}
