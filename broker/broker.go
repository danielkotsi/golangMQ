package broker

import "sync"

type Broker struct {
	mu     sync.Mutex
	queues map[string]*Queue
}

func NewBroker() *Broker {
	return &Broker{
		queues: make(map[string]*Queue),
	}
}

func (b *Broker) DeclareQueue(queue string) error {
	return nil
}

func (b *Broker) Publish(queue string, msg []byte) error {
	return nil
}
