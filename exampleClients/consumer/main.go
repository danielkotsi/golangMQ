package main

import (
	"GolangRabbitMQBroker/client"
	"context"
	"fmt"
	"log"
	"time"
)

func main() {
	cfg := client.Config{
		ClientName:   "publisher",
		Username:     "daniel",
		Password:     "123456789",
		ChannelMax:   10,
		FrameMax:     10372,
		HeartbeatSec: 10,
	}

	c, err := client.Dial("localhost:5672", cfg)
	if err != nil {
		log.Println(err)
		return
	}
	err = c.Handshake()
	if err != nil {
		log.Println(err)
		return
	}
	go c.ReadLoop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	channel, err := c.OpenChannel(ctx)
	fmt.Println(channel)
	q, err := channel.DeclareQueue("newqueue", ctx)
	fmt.Println(q, err)
	// incoming, err := channel.Consume(q.Name, ctx)

	// for msg := range incoming {
	// 	log.Println("hello this is the message that i recieved")
	// 	log.Println("This is the queue:", msg.Type)
	// 	log.Println("And this is the body:", msg.Data)
	// }

	log.Println("Connection was opened")
}
