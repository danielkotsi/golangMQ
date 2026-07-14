# GolangMQ

A lightweight, in-memory message broker written in Go, implementing a custom JSON protocol over TCP with channel multiplexing inspired by AMQP 0-9-1.

## Features

- **Channel multiplexing** — multiple logical channels per TCP connection, identified by uint16 channel IDs
- **Exchange/queue routing** — declare exchanges, queues, and bind them with routing keys
- **Ack/Nack support** — consumer acknowledgment with requeue or dead-letter routing
- **Dead-letter queues** — nack'd messages route to a DLX exchange and DLQ 
- **Prefetch-based flow control** — inflight message tracking per consumer

## Architecture

```
┌─────────────────────────────────────┐
│  TCP Connection (net.Conn)           │
├─────────────────────────────────────┤
│  JSON + Newline Framing              │
│  (protocol/coder-decoder.go)        │
├─────────────────────────────────────┤
│  Connection (Envelope Dispatch)      │
│  └── Route to Channel (via uint16)  │
├─────────────────────────────────────┤
│  Channel Handlers                    │
│  (Publish, Consume, Ack, Declare)   │
├─────────────────────────────────────┤
│  Broker Core                          │
│  (Exchanges → Queues → Consumers)    │
└─────────────────────────────────────┘
```

Each TCP connection supports multiple logical channels. Frames are wrapped in an `Envelope` containing a `ChannelID` and `RequestID`, allowing the server to demultiplex and correlate requests/responses over a single socket. A dedicated write-pump goroutine serializes all outgoing frames, eliminating mutex contention on writes.

## Quick Start

### 1. Start the broker

```bash
podman run -it -p 5672:5672 danielkotsi/golangmq:latest
```

You should see:

```
MQ server started on :5672
```

### 2. Publish messages

In a second terminal:

```bash
go run exampleClients/publisher/main.go
```

This declares an `emails` exchange, an `email_queue` with a dead-letter exchange (`dlx`), binds them with routing key `email.sent`, and publishes 3 messages.

### 3. Consume messages

In a third terminal:

```bash
go run exampleClients/consumer/main.go
```

Three workers start:
- **Worker A** and **Worker B** consume from `email_queue` and ack messages
- **Worker C** consumes from `dlq` (dead-letter queue)

Every 3rd message is nack'd without requeue, routing it to the DLQ where Worker C picks it up.

### 4. Publish more messages

Go back to the publisher terminal and run it again:

```bash
go run exampleClients/publisher/main.go
```

Watch the consumer terminal — messages flow through, nack'd ones land in the DLQ. Run the publisher as many times as you like to keep feeding messages.

## SDK Usage

The example clients use the [Go SDK](https://github.com/danielkotsi/golangMQSDK) - A client library to use the broker
```go
import "github.com/danielkotsi/golangMQSDK/gomqSDK"
import "github.com/danielkotsi/golangMQSDK/protocol"
```

**Connect:**

```go
cfg := gomqSDK.Config{
    ClientName:   "my-client",
    Username:     "daniel",
    Password:     "123456789",
    ChannelMax:   10,
    FrameMax:     10372,
    HeartbeatSec: 10,
}

c, err := gomqSDK.Connect("localhost:5672", cfg)
```

**Open a channel:**

```go
channel, err := c.OpenChannel(ctx)
```

**Declare exchange and queue:**

```go
channel.DeclareExchange("emails")
channel.DeclareQueue("email_queue", ctx, "dlx", "dead_emails")
channel.BindQueue("email_queue", "emails", "email.sent", ctx)
```

**Publish:**

```go
err = channel.Publish(protocol.Publish{
    Exchange:   "emails",
    RoutingKey: "email.sent",
    Body:       body,
})
```

**Consume:**

```go
incoming, err := channel.Consume("email_queue", ctx)

for msg := range incoming {
    log.Println("Received:", string(msg.Body))
    channel.Ack(msg.DeliveryTag)
}
```

## Protocol

Custom JSON-over-TCP protocol with newline-delimited framing. Handshake mirrors AMQP 0-9-1:

```
Client → Server:  GOMQ/1\n
Server → Client:  connection.start
Client → Server:  connection.start_ok
Server → Client:  connection.tune
Client → Server:  connection.tune_ok
Client → Server:  connection.open
Server → Client:  connection.open_ok
```

**Method types:**

| Category   | Methods                                                                 |
|------------|-------------------------------------------------------------------------|
| Basic      | `basic.publish`, `basic.deliver`, `basic.consume`, `basic.ack`, `basic.nack` |
| Channel    | `channel.open`, `channel.open-ok`, `channel.close`, `channel.close-ok` |
| Queue      | `queue.declare`, `queue.declare-ok`, `queue.bind`, `queue.bind-ok`    |
| Exchange   | `exchange.declare`, `exchange.declare-ok`                              |
| Error      | `error`                                                                 |

## Technical Stack

| Component       | Implementation                                  |
|-----------------|--------------------------------------------------|
| Language        | Go 1.22                                          |
| Concurrency     | goroutines, channels, `sync.Cond`, `sync.Once`  |
| Wire format     | JSON + newline-delimited framing                 |
| Transport       | TCP                                              |
| Container       | Multi-stage Docker (Alpine 3.19, ~15MB)          |
| Channel IDs     | uint16 (up to 65535 channels per connection)     |
