package broker

import "sync"

type Exchange struct {
	name     string
	mu       sync.Mutex
	bindings map[string]map[string]*Queue
}

func NewExchange(name string) *Exchange {
	return &Exchange{
		name:     name,
		bindings: make(map[string]map[string]*Queue),
	}
}

func (e *Exchange) bind(queue *Queue, routingkey string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.bindings[routingkey] == nil {
		e.bindings[routingkey] = make(map[string]*Queue)
	}

	e.bindings[routingkey][queue.name] = queue
}

func (e *Exchange) getQueues(routingKey string) []*Queue {
	e.mu.Lock()
	defer e.mu.Unlock()

	var queues []*Queue
	for rk, qmap := range e.bindings {
		if rk == routingKey {
			for _, q := range qmap {
				queues = append(queues, q)
			}
		}
	}
	return queues
}
