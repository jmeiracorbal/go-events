package nats

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	events "github.com/jmeiracorbal/go-events"
	"github.com/nats-io/nats.go/jetstream"
)

func publishReplayEvents(t *testing.T, ctx context.Context, a *Adapter, start, end int) {
	t.Helper()
	for i := start; i <= end; i++ {
		if err := a.Publish(ctx, events.Event{
			ID: fmt.Sprintf("replay-%d", i), Topic: "work.created",
			Data: []byte(fmt.Sprintf("payload-%d", i)),
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func recvReplay(t *testing.T, ctx context.Context, received <-chan events.StoredEvent) events.StoredEvent {
	t.Helper()
	select {
	case event := <-received:
		return event
	case <-ctx.Done():
		t.Fatalf("replay event not delivered: %v", ctx.Err())
		return events.StoredEvent{}
	}
}

func waitReplayAck(t *testing.T, ctx context.Context, a *Adapter, consumer string, sequence uint64) {
	t.Helper()
	for {
		c, err := a.js.Consumer(ctx, "EVENTS", consumer)
		if err == nil {
			var info *jetstream.ConsumerInfo
			info, err = c.Info(ctx)
			if err == nil && info.AckFloor.Stream >= sequence {
				return
			}
		}
		if err := ctx.Err(); err != nil {
			t.Fatalf("durable ACK not observed for sequence %d: %v", sequence, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestReplayBacklogAndLiveUseSameConsumer(t *testing.T) {
	url := newServer(t)
	prepareStream(t, url)
	publisher := newAdapter(t, url, "")
	replayer := newAdapter(t, url, "")
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	publishReplayEvents(t, ctx, publisher, 1, 3)
	received := make(chan events.StoredEvent, 8)
	sub, err := replayer.Replay(ctx, events.Cursor{
		Stream: "EVENTS", AfterSequence: 1, UpperBound: 3,
	}, "work.>", "backlog-live", func(_ context.Context, event events.StoredEvent) error {
		received <- event
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	// Publish before the historical backlog is drained to verify that the
	// same server consumer covers the history/live boundary without a gap.
	publishReplayEvents(t, ctx, publisher, 4, 4)
	for _, sequence := range []uint64{2, 3, 4} {
		event := recvReplay(t, ctx, received)
		if event.Sequence != sequence || event.Stream != "EVENTS" ||
			event.Event.ID != fmt.Sprintf("replay-%d", sequence) ||
			string(event.Event.Data) != fmt.Sprintf("payload-%d", sequence) ||
			event.StoredAt.IsZero() || event.DeliveryAttempt != 1 {
			t.Fatalf("unexpected replay event across history/live boundary: %+v", event)
		}
	}
	publishReplayEvents(t, ctx, publisher, 5, 5)
	event := recvReplay(t, ctx, received)
	if event.Sequence != 5 || event.Event.ID != "replay-5" || event.DeliveryAttempt != 1 {
		t.Fatalf("expected ongoing live continuation: %+v", event)
	}
	waitReplayAck(t, ctx, replayer, "backlog-live", 5)
}

func TestReplayFailureRedeliveryAndResumeAfterAdapterRestart(t *testing.T) {
	url := newServer(t)
	prepareStream(t, url)
	publisher := newAdapter(t, url, "")
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	publishReplayEvents(t, ctx, publisher, 1, 1)

	firstAdapter := newAdapter(t, url, "")
	var attempts atomic.Int32
	received := make(chan events.StoredEvent, 4)
	sub, err := firstAdapter.Replay(ctx, events.Cursor{}, "work.created", "restartable", func(_ context.Context, event events.StoredEvent) error {
		if attempts.Add(1) == 1 {
			return errors.New("not committed")
		}
		received <- event
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	event := recvReplay(t, ctx, received)
	if event.Sequence != 1 || event.DeliveryAttempt < 2 || attempts.Load() < 2 {
		t.Fatalf("expected explicit retry with delivery metadata: %+v, attempts=%d", event, attempts.Load())
	}
	waitReplayAck(t, ctx, firstAdapter, "restartable", 1)
	if err := sub.Close(); err != nil {
		t.Fatal(err)
	}
	if err := firstAdapter.Close(); err != nil {
		t.Fatal(err)
	}

	publishReplayEvents(t, ctx, publisher, 2, 2)
	secondAdapter := newAdapter(t, url, "")
	resumed := make(chan events.StoredEvent, 4)
	sub2, err := secondAdapter.Replay(ctx, events.Cursor{}, "work.created", "restartable", func(_ context.Context, event events.StoredEvent) error {
		resumed <- event
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub2.Close()
	event = recvReplay(t, ctx, resumed)
	if event.Sequence != 2 || event.Event.ID != "replay-2" || event.DeliveryAttempt != 1 {
		t.Fatalf("durable consumer restarted at the wrong position: %+v", event)
	}
	waitReplayAck(t, ctx, secondAdapter, "restartable", 2)
	select {
	case duplicate := <-resumed:
		t.Fatalf("unexpected redelivery of acknowledged history: %+v", duplicate)
	case <-time.After(75 * time.Millisecond):
	}
}

func TestReplayRejectsIncompatibleExistingDurableWithoutChangingIt(t *testing.T) {
	url := newServer(t)
	prepareStream(t, url)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	a := newAdapter(t, url, "ordinary-consumer")

	ordinary, err := a.Subscribe(ctx, "work.created", func(context.Context, events.Event) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := ordinary.Close(); err != nil {
		t.Fatal(err)
	}
	noop := func(context.Context, events.StoredEvent) error { return nil }
	if _, err := a.Replay(ctx, events.Cursor{}, "work.created", "ordinary-consumer", noop); !errors.Is(err, events.ErrConsumerIncompatible) {
		t.Fatalf("expected normal subscriber's durable configuration to be preserved: %v", err)
	}
	before, err := a.js.Consumer(ctx, "EVENTS", "ordinary-consumer")
	if err != nil {
		t.Fatal(err)
	}
	info, err := before.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.Config.DeliverPolicy != jetstream.DeliverAllPolicy {
		t.Fatalf("existing durable was modified: %+v", info.Config)
	}

	sub, err := a.Replay(ctx, events.Cursor{}, "work.created", "subject-consumer", noop)
	if err != nil {
		t.Fatal(err)
	}
	if err := sub.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Replay(ctx, events.Cursor{}, "work.updated", "subject-consumer", noop); !errors.Is(err, events.ErrConsumerIncompatible) {
		t.Fatalf("expected subject mismatch to be rejected: %v", err)
	}
	cons, err := a.js.Consumer(ctx, "EVENTS", "subject-consumer")
	if err != nil {
		t.Fatal(err)
	}
	info, err = cons.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.Config.FilterSubject != "work.created" || info.Config.DeliverPolicy != jetstream.DeliverByStartSequencePolicy {
		t.Fatalf("existing replay durable was reconfigured: %+v", info.Config)
	}
}

func TestReplayRetentionAndCancellation(t *testing.T) {
	url := newServer(t)
	prepareStream(t, url)
	publisher := newAdapter(t, url, "")
	a := newAdapter(t, url, "")
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	publishReplayEvents(t, ctx, publisher, 1, 5)
	stream, err := a.js.Stream(ctx, "EVENTS")
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Purge(ctx, jetstream.WithPurgeSequence(4)); err != nil {
		t.Fatal(err)
	}
	noop := func(context.Context, events.StoredEvent) error { return nil }
	if _, err := a.Replay(ctx, events.Cursor{Stream: "EVENTS", AfterSequence: 1}, "work.created", "expired", noop); !errors.Is(err, events.ErrHistoryExpired) {
		t.Fatalf("expected expired cursor, got %v", err)
	}
	// An initial, zero-value cursor deliberately starts at the earliest
	// still-retained message, rather than treating the purge as an error.
	watchCtx, stop := context.WithCancel(ctx)
	received := make(chan events.StoredEvent, 8)
	sub, err := a.Replay(watchCtx, events.Cursor{}, "work.created", "from-retained", func(_ context.Context, event events.StoredEvent) error {
		received <- event
		return nil
	})
	if err != nil {
		stop()
		t.Fatal(err)
	}
	for _, expected := range []uint64{4, 5} {
		if event := recvReplay(t, ctx, received); event.Sequence != expected {
			t.Fatalf("unexpected first-retained event: %+v", event)
		}
	}
	waitReplayAck(t, ctx, a, "from-retained", 5)
	stop()
	for {
		a.mu.Lock()
		active := len(a.subs)
		a.mu.Unlock()
		if active == 0 {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("subscription did not stop after canceling context")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := sub.Close(); err != nil {
		t.Fatal(err)
	}
	publishReplayEvents(t, ctx, publisher, 6, 6)
	fresh := newAdapter(t, url, "")
	sub2, err := fresh.Replay(ctx, events.Cursor{}, "work.created", "from-retained", func(_ context.Context, event events.StoredEvent) error {
		received <- event
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub2.Close()
	if event := recvReplay(t, ctx, received); event.Sequence != 6 {
		t.Fatalf("durable progress not preserved after cancellation: %+v", event)
	}
}

func TestReplayValidationAndSingleActiveSubscription(t *testing.T) {
	url := newServer(t)
	prepareStream(t, url)
	a := newAdapter(t, url, "")
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	noop := func(context.Context, events.StoredEvent) error { return nil }
	for _, tc := range []struct {
		name     string
		cursor   events.Cursor
		subject  string
		consumer string
		handler  events.ReplayHandler
		target   error
	}{
		{"other stream", events.Cursor{Stream: "OTHER"}, "work.created", "m1", noop, events.ErrInvalidCursor},
		{"unscoped position", events.Cursor{AfterSequence: 1}, "work.created", "m1", noop, events.ErrInvalidCursor},
		{"invalid snapshot", events.Cursor{Stream: "EVENTS", AfterSequence: 3, UpperBound: 2}, "work.created", "m1", noop, events.ErrInvalidCursor},
		{"future position", events.Cursor{Stream: "EVENTS", AfterSequence: 1}, "work.created", "m1", noop, events.ErrInvalidCursor},
		{"bad subject", events.Cursor{}, "work.>.invalid", "m1", noop, nil},
		{"missing consumer", events.Cursor{}, "work.created", "", noop, nil},
		{"missing handler", events.Cursor{}, "work.created", "m1", nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := a.Replay(ctx, tc.cursor, tc.subject, tc.consumer, tc.handler)
			if err == nil || (tc.target != nil && !errors.Is(err, tc.target)) {
				t.Fatalf("expected validation error %v, got %v", tc.target, err)
			}
		})
	}
	canceled, stop := context.WithCancel(ctx)
	stop()
	if _, err := a.Replay(canceled, events.Cursor{}, "work.created", "cancelled", noop); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
	if _, err := a.Replay(nil, events.Cursor{}, "work.created", "nil-context", noop); err == nil {
		t.Fatal("expected nil context error")
	}
	sub, err := a.Replay(ctx, events.Cursor{}, "work.created", "active", noop)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Replay(ctx, events.Cursor{}, "work.created", "another", noop); err == nil {
		t.Fatal("expected second active subscription to be rejected")
	}
	if err := sub.Close(); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Replay(ctx, events.Cursor{}, "work.created", "after-close", noop); !errors.Is(err, events.ErrClosed) {
		t.Fatalf("expected closed adapter, got %v", err)
	}
}
