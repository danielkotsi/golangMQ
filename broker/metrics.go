package broker

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Lock-order rule: the state collector takes Server.mu → Broker.mu → Queue.mu,
// and never holds Queue.mu while calling back into Broker. No existing code path
// acquires these in reverse, so scrape-time collection cannot deadlock.

type Metrics struct {
	Registry *prometheus.Registry

	Published             *prometheus.CounterVec // exchange
	PublishUnroutable     *prometheus.CounterVec // exchange
	PublishErrors         prometheus.Counter
	Enqueued              *prometheus.CounterVec // queue
	Delivered             *prometheus.CounterVec // queue
	DeliverErrors         *prometheus.CounterVec // queue
	Acked                 *prometheus.CounterVec // queue
	Nacked                *prometheus.CounterVec // queue, requeue
	DeadLettered          *prometheus.CounterVec // queue
	Dropped               *prometheus.CounterVec // queue
	ConnectionsOpened     prometheus.Counter
	ConnectionsClosed     prometheus.Counter
	HandshakeFailures     prometheus.Counter
	ConsumersRegistered   *prometheus.CounterVec // queue
	ConsumersUnregistered *prometheus.CounterVec // queue
}

func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()

	m := &Metrics{
		Registry: reg,

		Published:             newCounterVec("golangmq_publish_total", "Publish calls accepted for a known exchange", "exchange"),
		PublishUnroutable:     newCounterVec("golangmq_publish_unroutable_total", "Publish calls that matched 0 queues (silent drop)", "exchange"),
		PublishErrors:         newCounter("golangmq_publish_errors_total", "Publish calls rejected (exchange not found)"),
		Enqueued:              newCounterVec("golangmq_enqueue_total", "Messages appended to a queue buffer", "queue"),
		Delivered:             newCounterVec("golangmq_deliver_total", "basic.deliver written to a consumer successfully", "queue"),
		DeliverErrors:         newCounterVec("golangmq_deliver_errors_total", "Deliver writes that failed (message re-enqueued)", "queue"),
		Acked:                 newCounterVec("golangmq_ack_total", "Acks processed", "queue"),
		Nacked:                newCounterVec("golangmq_nack_total", "Nacks processed", "queue", "requeue"),
		DeadLettered:          newCounterVec("golangmq_dead_letter_total", "Nack without requeue, DLX republish executed", "queue"),
		Dropped:               newCounterVec("golangmq_drop_total", "Nack without requeue and no DLX, message discarded", "queue"),
		ConnectionsOpened:     newCounter("golangmq_connections_opened_total", "Connections accepted and registered"),
		ConnectionsClosed:     newCounter("golangmq_connections_closed_total", "Connections cleaned up"),
		HandshakeFailures:     newCounter("golangmq_handshake_failures_total", "Handshakes that failed"),
		ConsumersRegistered:   newCounterVec("golangmq_consumers_registered_total", "Consumers registered", "queue"),
		ConsumersUnregistered: newCounterVec("golangmq_consumers_unregistered_total", "Consumers removed", "queue"),
	}

	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		m.Published,
		m.PublishUnroutable,
		m.PublishErrors,
		m.Enqueued,
		m.Delivered,
		m.DeliverErrors,
		m.Acked,
		m.Nacked,
		m.DeadLettered,
		m.Dropped,
		m.ConnectionsOpened,
		m.ConnectionsClosed,
		m.HandshakeFailures,
		m.ConsumersRegistered,
		m.ConsumersUnregistered,
	)

	return m
}

func newCounterVec(name, help string, labels ...string) *prometheus.CounterVec {
	return prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: name,
		Help: help,
	}, labels)
}

func newCounter(name, help string) prometheus.Counter {
	return prometheus.NewCounter(prometheus.CounterOpts{
		Name: name,
		Help: help,
	})
}

type stateCollector struct {
	server *Server
}

var (
	queueMessagesDesc = prometheus.NewDesc(
		"golangmq_queue_messages",
		"Current depth of a queue",
		[]string{"queue"}, nil,
	)
	queueConsumersDesc = prometheus.NewDesc(
		"golangmq_queue_consumers",
		"Current number of consumers on a queue",
		[]string{"queue"}, nil,
	)
	consumerInflightDesc = prometheus.NewDesc(
		"golangmq_consumer_inflight",
		"Messages currently inflight for a consumer",
		[]string{"queue", "consumer"}, nil,
	)
	queuesDesc = prometheus.NewDesc(
		"golangmq_queues",
		"Current number of declared queues",
		nil, nil,
	)
	exchangesDesc = prometheus.NewDesc(
		"golangmq_exchanges",
		"Current number of declared exchanges",
		nil, nil,
	)
	connectionsDesc = prometheus.NewDesc(
		"golangmq_connections",
		"Current number of open connections",
		nil, nil,
	)
)

func (c *stateCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- queueMessagesDesc
	ch <- queueConsumersDesc
	ch <- consumerInflightDesc
	ch <- queuesDesc
	ch <- exchangesDesc
	ch <- connectionsDesc
}

func (c *stateCollector) Collect(ch chan<- prometheus.Metric) {
	s := c.server

	s.mu.Lock()
	ch <- prometheus.MustNewConstMetric(connectionsDesc, prometheus.GaugeValue, float64(len(s.connections)))
	s.mu.Unlock()

	b := s.Broker
	b.mu.Lock()
	ch <- prometheus.MustNewConstMetric(queuesDesc, prometheus.GaugeValue, float64(len(b.queues)))
	ch <- prometheus.MustNewConstMetric(exchangesDesc, prometheus.GaugeValue, float64(len(b.exchanges)))
	for _, q := range b.queues {
		q.mu.Lock()
		ch <- prometheus.MustNewConstMetric(queueMessagesDesc, prometheus.GaugeValue, float64(len(q.messages)), q.name)
		ch <- prometheus.MustNewConstMetric(queueConsumersDesc, prometheus.GaugeValue, float64(len(q.consumers)), q.name)
		for _, consumer := range q.consumers {
			consumer.mu.Lock()
			ch <- prometheus.MustNewConstMetric(consumerInflightDesc, prometheus.GaugeValue, float64(consumer.inflight), q.name, consumer.tag)
			consumer.mu.Unlock()
		}
		q.mu.Unlock()
	}
	b.mu.Unlock()
}
