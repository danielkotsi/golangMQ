package broker

import "sync"

type Queue struct {
	name string
	mu   sync.Mutex
}
