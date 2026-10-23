package bench

import (
	"encoding/binary"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/danielkotsi/golangMQSDK/gomqSDK"
	"github.com/danielkotsi/golangMQSDK/protocol"
)

// headerLen is the fixed prefix of every body: [8-byte BE seq][8-byte BE
// send-time unixnanos]. Payload length stays exactly -size.
const headerLen = 16

func encodeHeader(body []byte, seq uint64, sendNanos int64) {
	binary.BigEndian.PutUint64(body[0:8], seq)
	binary.BigEndian.PutUint64(body[8:16], uint64(sendNanos))
}

func decodeHeader(body []byte) (seq uint64, sendNanos int64) {
	if len(body) < headerLen {
		return 0, 0
	}
	seq = binary.BigEndian.Uint64(body[0:8])
	sendNanos = int64(binary.BigEndian.Uint64(body[8:16]))
	return seq, sendNanos
}

// publishWorker hammers the channel it is handed with Publish calls. It mints
// a globally unique seq per message (order-independent identity across N
// publishers), stamps the send time into sentTimes, and publishes a body of
// exactly cfg.Size bytes.
type publishWorker struct {
	ch         *gomqSDK.ClientChannel
	slot       *channelSlot
	st         *stats
	seqGen     *atomic.Uint64
	sentTimes  []atomic.Int64
	exchange   string
	routingKey string
	size       int
	msgs       int
	stop       <-chan struct{}
}

func (w *publishWorker) run() error {
	body := make([]byte, w.size)
	for i := 0; i < w.msgs; i++ {
		select {
		case <-w.stop:
			return nil
		default:
		}
		seq := w.seqGen.Add(1)
		if seq == 0 || int(seq)-1 >= len(w.sentTimes) {
			return fmt.Errorf("publisher seq %d exceeds message budget %d", seq, len(w.sentTimes))
		}
		sendNanos := time.Now().UnixNano()
		w.sentTimes[seq-1].Store(sendNanos)
		encodeHeader(body, seq, sendNanos)
		t0 := time.Now()
		if err := w.ch.Publish(protocol.Publish{
			Exchange:   w.exchange,
			RoutingKey: w.routingKey,
			Body:       body,
		}); err != nil {
			return err
		}
		w.st.recordPublish(w.slot, seq, float64(time.Since(t0).Nanoseconds())/1000)
	}
	return nil
}

// consumeWorker reads a per-consumer delivery stream, decodes the message
// header, records delivery latency (recv - send stamp), and acks when -ack is
// set (the broker only drains past the prefetch ceiling once inflight drops).
type consumeWorker struct {
	ch         *gomqSDK.ClientChannel
	deliveries <-chan protocol.Deliver
	slot       *channelSlot
	st         *stats
	sentTimes  []atomic.Int64
	seen       []atomic.Bool
	ack        bool
}

func (w *consumeWorker) run() error {
	for d := range w.deliveries {
		seq, _ := decodeHeader(d.Body)
		recvNanos := time.Now().UnixNano()

		var delLatUs float64
		idx := -1
		if seq > 0 && int(seq)-1 < len(w.sentTimes) {
			idx = int(seq) - 1
			if st := w.sentTimes[idx].Load(); st > 0 {
				delLatUs = float64(recvNanos-st) / 1000
			}
		}
		dup := false
		if idx >= 0 && idx < len(w.seen) {
			dup = w.seen[idx].Swap(true)
		}

		w.st.recordDeliver(w.slot, seq, delLatUs, dup)

		if w.ack {
			t0 := time.Now()
			if err := w.ch.Ack(d.DeliveryTag); err != nil {
				return err
			}
			w.st.recordAck(seq, float64(time.Since(t0).Nanoseconds())/1000)
		}
	}
	return nil
}
