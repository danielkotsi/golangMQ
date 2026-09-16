package tests

import (
	"testing"
	"time"
)

// TestFindingF001_SecondConsumeOnSameChannel guards against the broker silently
// overwriting a channel's consumer when a second Consume is issued on an
// already-consuming channel (see reports/findings/F-001.md).
//
// PASS conditions (the test is green when either holds):
//  1. the broker rejects the second consume (one-consumer-per-channel,
//     Option A) — Consume returns an error; or
//  2. the broker genuinely supports multiple consumers per channel (Option B) —
//     after the second consume, the first consumer still works end to end
//     (a nack with requeue=true on its delivery is handled and redelivered).
//
// FAIL condition (current behaviour): the second consume is silently accepted
// but overwrites the first consumer on the channel, so the first consumer's
// nack is a silent no-op and the message is never redelivered.
func TestFindingF001_SecondConsumeOnSameChannel(t *testing.T) {
	addr := startTestBroker(t)
	client := newTestClient(t, addr)
	ch := client.openChannel("finding-f001")

	ctx := testCtx(t)

	// Declare the main exchange, queue A (bound to the main exchange) and
	// queue B, all on the same channel.
	if _, err := ch.DeclareExchange("main", ctx); err != nil {
		t.Fatalf("declare main exchange: %v", err)
	}
	if _, err := ch.DeclareQueue("qa", ctx, "", ""); err != nil {
		t.Fatalf("declare queue qa: %v", err)
	}
	if err := ch.BindQueue("qa", "main", "key", ctx); err != nil {
		t.Fatalf("bind qa: %v", err)
	}
	if _, err := ch.DeclareQueue("qb", ctx, "", ""); err != nil {
		t.Fatalf("declare queue qb: %v", err)
	}

	// First consumer on the channel.
	firstDeliveries, err := ch.Consume("qa", ctx)
	if err != nil {
		t.Fatalf("first consume: %v", err)
	}

	// Publish and collect a delivery on the first consumer.
	publish(t, ch, "main", "key", body(1))
	first := collectDeliveries(t, firstDeliveries, 1)

	// Second consume on the SAME channel for a different queue.
	_, err = ch.Consume("qb", ctx)
	if err != nil {
		t.Logf("second consume rejected by broker: %v", err)
		t.Logf("F-001 resolved via Option A (one consumer per channel)")
		return
	}
	t.Logf("second consume accepted by broker")

	// The broker accepted a second consumer, so the first consumer must still
	// function: a nack(requeue=true) on its delivery must be handled and the
	// message redelivered. If the second consume silently overwrote the first
	// (F-001 present), this nack is a no-op and no redelivery ever arrives.
	if err := ch.Nack(first[0].DeliveryTag, true); err != nil {
		t.Fatalf("nack: %v", err)
	}

	redel := collectDeliveriesTimeout(t, firstDeliveries, 1, 2*time.Second)
	if len(redel) != 1 {
		t.Fatalf("expected 1 redelivery, got %d", len(redel))
	}
	if string(redel[0].Body) != string(body(1)) {
		t.Fatalf("redelivered body mismatch: got %q want %q", redel[0].Body, body(1))
	}
	t.Logf("F-001 resolved via Option B (multiple consumers per channel)")
}