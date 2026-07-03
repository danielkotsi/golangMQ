package broker

import "GolangRabbitMQBroker/protocol"

type Consumer struct {
	tag   string
	queue *Queue

	channel *Channel
	ch      chan protocol.Deliver

	prefetch     int
	inflight     int
	inflightTags map[uint16]struct{}
}

func NewConsumer(tag string, queue *Queue, ch *Channel) *Consumer {
	return &Consumer{
		tag:          tag,
		queue:        queue,
		channel:      ch,
		ch:           make(chan protocol.Deliver, 100),
		prefetch:     10,
		inflightTags: make(map[uint16]struct{}),
	}
}
