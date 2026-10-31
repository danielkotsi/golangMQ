package broker

import (
	"log"
	"net"
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Server struct {
	addr        string
	listener    net.Listener
	mu          sync.Mutex
	connections map[*Connection]struct{}
	config      ServerConfig
	Broker      *Broker
	metrics     *Metrics
}
type ServerConfig struct {
	ChannelMax   int
	FramesMax    int
	HeartbeatSec int
}

func NewServer(addr string, serverconfig ServerConfig) *Server {
	s := &Server{
		addr:        addr,
		config:      serverconfig,
		connections: make(map[*Connection]struct{}),
	}
	m := NewMetrics()
	m.Registry.MustRegister(&stateCollector{server: s})
	s.metrics = m
	s.Broker = NewBroker(m)
	return s
}

func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	return s.Serve(ln)
}

func (s *Server) Serve(ln net.Listener) error {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}

		go s.HandleConnection(conn)
	}
}

func (s *Server) HandleConnection(c net.Conn) {
	conn := NewConnection(s, c)

	s.mu.Lock()
	s.connections[conn] = struct{}{}
	s.mu.Unlock()
	s.metrics.ConnectionsOpened.Inc()
	defer func() {
		s.metrics.ConnectionsClosed.Inc()
		conn.shutdown()
		conn.cleanup()
		s.mu.Lock()
		delete(s.connections, conn)
		s.mu.Unlock()
		c.Close()
	}()

	err := conn.RunHandshake()
	if err != nil {
		s.metrics.HandshakeFailures.Inc()
		log.Println(err)
		return
	}

	err = conn.Serve()
	if err != nil {
		log.Println(err)
		return
	}
}

func (s *Server) MetricsHandler() http.Handler {
	return promhttp.HandlerFor(s.metrics.Registry, promhttp.HandlerOpts{})
}
