package nats

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	events "github.com/jmeiracorbal/go-events"
	"github.com/nats-io/nats.go/jetstream"
)

func ephemeralConsumerName(t *testing.T, ctx context.Context, a *Adapter) string {
	t.Helper()
	stream, err := a.js.Stream(ctx, "EVENTS")
	if err != nil {
		t.Fatal(err)
	}
	lister := stream.ListConsumers(ctx)
	var names []string
	for info := range lister.Info() {
		if info.Config.Durable == "" {
			names = append(names, info.Name)
			if info.Config.InactiveThreshold <= 0 || info.Config.AckPolicy != jetstream.AckExplicitPolicy {
				t.Fatalf("invalid ephemeral consumer configuration: %+v", info.Config)
			}
		}
	}
	if err := lister.Err(); err != nil {
		t.Fatal(err)
	}
	if len(names) != 1 {
		t.Fatalf("expected one ephemeral consumer, got %v", names)
	}
	return names[0]
}

func waitConsumerRemoved(t *testing.T, ctx context.Context, a *Adapter, name string) {
	t.Helper()
	for {
		_, err := a.js.Consumer(ctx, "EVENTS", name)
		if errors.Is(err, jetstream.ErrConsumerNotFound) {
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("consumer %q was not removed", name)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestEphemeralReplayHistoryLiveAndFreshSession(t *testing.T) {
	url := newServer(t)
	prepareStream(t, url)
	publisher := newAdapter(t, url, "")
	a := newAdapter(t, url, "")
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	publishReplayEvents(t, ctx, publisher, 1, 3)
	received := make(chan events.StoredEvent, 8)
	handler := func(_ context.Context, event events.StoredEvent) error {
		received <- event
		return nil
	}
	cursor := events.Cursor{Stream: "EVENTS", AfterSequence: 1, UpperBound: 3}
	sub, err := a.ReplayEphemeral(ctx, cursor, "work.>", time.Minute, handler)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	name := ephemeralConsumerName(t, ctx, a)
	publishReplayEvents(t, ctx, publisher, 4, 4)
	for _, seq := range []uint64{2, 3, 4} {
		got := recvReplay(t, ctx, received)
		if got.Sequence != seq || got.Stream != "EVENTS" || got.DeliveryAttempt != 1 || got.StoredAt.IsZero() {
			t.Fatalf("unexpected historical/live delivery: %+v", got)
		}
	}
	waitReplayAck(t, ctx, a, name, 4)
	if _, err := a.ReplayEphemeral(ctx, cursor, "work.>", time.Minute, handler); err == nil {
		t.Fatal("second active session accepted")
	}
	if err := sub.Close(); err != nil {
		t.Fatal(err)
	}
	if err := sub.Close(); err != nil {
		t.Fatal(err)
	}
	waitConsumerRemoved(t, ctx, publisher, name)
	// The session ACKs must not delete the retained history.
	if _, err := publisher.Get(ctx, 2); err != nil {
		t.Fatal(err)
	}
	sub, err = a.ReplayEphemeral(ctx, cursor, "work.>", time.Minute, handler)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	secondName := ephemeralConsumerName(t, ctx, a)
	if secondName == name {
		t.Fatal("fresh session reused the previous consumer")
	}
	if got := recvReplay(t, ctx, received); got.Sequence != 2 {
		t.Fatalf("fresh session inherited old ACK progress: %+v", got)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	waitConsumerRemoved(t, ctx, publisher, secondName)
}

func TestEphemeralReplayCancellationPreservesDurableConsumer(t *testing.T) {
	url := newServer(t)
	prepareStream(t, url)
	a := newAdapter(t, url, "")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	handler := func(context.Context, events.StoredEvent) error { return nil }
	durable, err := a.Replay(ctx, events.Cursor{}, "work.>", "persistent-replay", handler)
	if err != nil {
		t.Fatal(err)
	}
	if err := durable.Close(); err != nil {
		t.Fatal(err)
	}
	watchCtx, stop := context.WithCancel(ctx)
	defer stop()
	sub, err := a.ReplayEphemeral(watchCtx, events.Cursor{}, "work.>", time.Minute, handler)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	name := ephemeralConsumerName(t, ctx, a)
	stop()
	waitConsumerRemoved(t, ctx, a, name)
	if err := sub.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := a.js.Consumer(ctx, "EVENTS", "persistent-replay"); err != nil {
		t.Fatalf("ephemeral cleanup affected durable consumer: %v", err)
	}
}

func TestEphemeralReplayRejectsInvalidCursorAndDestructiveRetention(t *testing.T) {
	url := newServer(t)
	prepareStream(t, url)
	a := newAdapter(t, url, "")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	handler := func(context.Context, events.StoredEvent) error { return nil }
	if _, err := a.ReplayEphemeral(ctx, events.Cursor{}, "work.>", 0, handler); err == nil {
		t.Fatal("accepted zero inactivity timeout")
	}
	if _, err := a.ReplayEphemeral(ctx, events.Cursor{Stream: "EVENTS", AfterSequence: 1}, "work.>", time.Minute, handler); !errors.Is(err, events.ErrInvalidCursor) {
		t.Fatalf("expected invalid cursor: %v", err)
	}
	publishReplayEvents(t, ctx, a, 1, 4)
	stream, err := a.js.Stream(ctx, "EVENTS")
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Purge(ctx, jetstream.WithPurgeSequence(4)); err != nil {
		t.Fatal(err)
	}
	if _, err := a.ReplayEphemeral(ctx, events.Cursor{Stream: "EVENTS", AfterSequence: 1}, "work.>", time.Minute, handler); !errors.Is(err, events.ErrHistoryExpired) {
		t.Fatalf("expected expired history cursor: %v", err)
	}
	_, err = a.js.CreateStream(ctx, jetstream.StreamConfig{
		Name: "QUEUE", Subjects: []string{"queue.>"}, Retention: jetstream.WorkQueuePolicy,
	})
	if err != nil {
		t.Fatal(err)
	}
	queue, err := New(ctx, Config{URL: url, Stream: "QUEUE", AckWait: time.Second, RetryDelay: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	if _, err := queue.ReplayEphemeral(ctx, events.Cursor{}, "queue.>", time.Minute, handler); err == nil || !strings.Contains(err.Error(), "limits retention") {
		t.Fatalf("destructive retention was not rejected: %v", err)
	}
	sub, err := a.ReplayEphemeral(ctx, events.Cursor{}, "work.>", time.Minute, handler)
	if err != nil {
		t.Fatalf("invalid setup left the adapter reserved: %v", err)
	}
	defer sub.Close()
}

func TestEphemeralReplayRedeliveryUsesSameEvent(t *testing.T) {
	url := newServer(t)
	prepareStream(t, url)
	a := newAdapter(t, url, "")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	publishReplayEvents(t, ctx, a, 1, 1)
	deliveries := make(chan events.StoredEvent, 4)
	sub, err := a.ReplayEphemeral(ctx, events.Cursor{}, "work.>", time.Minute,
		func(_ context.Context, event events.StoredEvent) error {
			deliveries <- event
			if event.DeliveryAttempt == 1 {
				return errors.New("not committed")
			}
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	first := recvReplay(t, ctx, deliveries)
	second := recvReplay(t, ctx, deliveries)
	if first.Sequence != second.Sequence || first.Event.ID != second.Event.ID || first.DeliveryAttempt != 1 || second.DeliveryAttempt < 2 {
		t.Fatalf("unexpected redelivery: %+v, %+v", first, second)
	}
	name := ephemeralConsumerName(t, ctx, a)
	waitReplayAck(t, ctx, a, name, second.Sequence)
}
