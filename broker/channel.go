package broker

import (
	"GolangRabbitMQBroker/protocol"
	"encoding/json"
	"log"
)

type Channel struct {
	id        uint16
	conn      *Connection
	server    *Server
	consumers map[string]*Consumer
}

func (ch *Channel) route(env protocol.Envelope) {
	switch env.Type {
	case protocol.BasicPublishType:
		var event protocol.Publish
		err := json.Unmarshal(env.Payload, &event)
		if err != nil {
			log.Println(err)
		}
		ch.HandlePublish(ch.conn, &event)
	case protocol.BasicConsumeType:
		var event protocol.Consume
		err := json.Unmarshal(env.Payload, &event)
		if err != nil {
			ch.conn.WriteEnvelope(protocol.ErrorType, env.RequestID, protocol.Error{
				Message: err.Error(),
			})
		}
		ch.HandleConsume(ch.conn, &event)
	case protocol.BasicAckType:
		var event protocol.Ack
		err := json.Unmarshal(env.Payload, &event)
		if err != nil {
			ch.conn.WriteEnvelope(protocol.ErrorType, env.RequestID, protocol.Error{
				Message: err.Error(),
			})
		}
		ch.HandleAck(ch.conn, &event)
	case protocol.QueueDeclareType:
		var event protocol.QueueDeclare
		err := json.Unmarshal(env.Payload, &event)
		if err != nil {
			log.Println(err)
		}
		ch.HandleQueueDeclare(ch.conn, &event)
	case protocol.QueueBindType:
		var event protocol.QueueBind
		err := json.Unmarshal(env.Payload, &event)
		if err != nil {
			ch.conn.WriteEnvelope(protocol.ErrorType, env.RequestID, protocol.Error{
				Message: err.Error(),
			})
		}
		ch.HandleQueueBind(ch.conn, &event)
	}
}

func (ch *Channel) HandlePublish(conn *Connection, event *protocol.Publish) {
	log.Println("hello this is the message that i recieved")
	log.Println("This is the queue:", event.Queue)
	log.Println("And this is the body:", event.Body)
	conn.WriteEnvelope(protocol.BasicConsumeOKType, 0, protocol.Deliver{
		ConsumerTag: "daniel",
		Queue:       event.Queue,
		Body:        event.Body,
	})
}

func (ch *Channel) HandleConsume(conn *Connection, event *protocol.Consume) {
}

func (ch *Channel) HandleAck(conn *Connection, event *protocol.Ack) {
}

func (ch *Channel) HandleQueueDeclare(conn *Connection, event *protocol.QueueDeclare) {
	log.Println("hello")
}

func (ch *Channel) HandleQueueBind(conn *Connection, event *protocol.QueueBind) {
}
