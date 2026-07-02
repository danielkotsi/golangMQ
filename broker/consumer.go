package broker

import "GolangRabbitMQBroker/protocol"

type Consumer struct {
	tag   string
	queue string

	ch chan protocol.Deliver

	prefetch int
	inflight int
}
