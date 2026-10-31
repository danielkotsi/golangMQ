package main

import (
	"GolangRabbitMQBroker/broker"
	"flag"
	"log"
	"net/http"
)

func main() {
	addr := flag.String("addr", ":5672", "broker listen address")
	metricsAddr := flag.String("metrics-addr", ":9090", "metrics listen address (empty disables)")
	flag.Parse()

	serverconfig := &broker.ServerConfig{
		ChannelMax:   10,
		FramesMax:    10372,
		HeartbeatSec: 10,
	}
	server := broker.NewServer(*addr, *serverconfig)

	if *metricsAddr != "" {
		go func() {
			mux := http.NewServeMux()
			mux.Handle("/metrics", server.MetricsHandler())
			log.Printf("metrics listening on %s/metrics", *metricsAddr)
			log.Println(http.ListenAndServe(*metricsAddr, mux))
		}()
	}

	log.Printf("MQ server started on %s", *addr)
	if err := server.ListenAndServe(); err != nil {
		log.Println(err)
		return
	}
}
