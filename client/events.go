package client

import "GolangRabbitMQBroker/protocol"

type Event struct {
	Type protocol.Method
	Data any
}
