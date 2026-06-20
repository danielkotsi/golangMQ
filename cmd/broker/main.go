package main

import (
	"log"
	"net"
)

func main() {
	ln, err := net.Listen("tcp", ":5672")
	if err != nil {
		log.Fatal(err)
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}

		go handleConnection(conn)
	}
}

func handleConnection(net.Conn) {
	log.Println("hello")

}
