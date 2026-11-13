package nats

import (
	"context"
	"fmt"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	events "github.com/jmeiracorbal/go-events"
	natsserver "github.com/nats-io/nats-server/v2/server"
	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func newServer(t *testing.T) string {
	t.Helper()
	server, err := natsserver.NewServer(&natsserver.Options{
		Host: "127.0.0.1", Port: -1, JetStream: true,
		StoreDir: t.TempDir(), NoLog: true, NoSigs: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	server.Start()
	if !server.ReadyForConnections(5 * time.Second) {
		server.Shutdown()
		t.Fatal("NATS server failed to start")
	}
	t.Cleanup(func() {
		server.Shutdown()
		server.WaitForShutdown()
	})
	return server.ClientURL()
}

func prepareStream(t *testing.T, url string) {
	t.Helper()
	nc, err := natsio.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name: "EVENTS", Subjects: []string{"work.>"}, Storage: jetstream.FileStorage,
	}); err != nil {
		t.Fatal(err)
	}
}

func newAdapter(t *testing.T, url, name string) *Adapter {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	a, err := New(ctx, Config{
		URL: url, Stream: "EVENTS", Consumer: name,
		AckWait: 2 * time.Second, RetryDelay: 25 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })
	return a
}

func TestReplayEventsPublishedBeforeSubscription(t *testing.T) {
	url := newServer(t)
	prepareStream(t, url)
	a := newAdapter(t, url, "replay")
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	published := events.Event{ID: "event-1", Topic: "work.created", Data: []byte("payload")}
	if err := a.Publish(ctx, published); err != nil {
		t.Fatal(err)
	}
	received := make(chan events.Event, 1)
	sub, err := a.Subscribe(ctx, "work.created", func(_ context.Context, event events.Event) error {
		received <- event
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	select {
	case event := <-received:
		if event.ID != published.ID || event.Topic != published.Topic || string(event.Data) != string(published.Data) {
			t.Fatalf("unexpected delivery: %+v", event)
		}
	case <-ctx.Done():
		t.Fatal("event was not replayed")
	}
}

func TestHandlerFailureIsRedelivered(t *testing.T) {
	url := newServer(t)
	prepareStream(t, url)
	a := newAdapter(t, url, "retry")
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	var attempts atomic.Int32
	received := make(chan events.Event, 1)
	sub, err := a.Subscribe(ctx, "work.created", func(_ context.Context, event events.Event) error {
		if attempts.Add(1) == 1 {
			return errors.New("not committed")
		}
		received <- event
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	if err := a.Publish(ctx, events.Event{ID: "event-2", Topic: "work.created"}); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-received:
		if event.ID != "event-2" || attempts.Load() < 2 {
			t.Fatalf("redelivery not observed: event=%+v attempts=%d", event, attempts.Load())
		}
	case <-ctx.Done():
		t.Fatal("failed delivery was not retried")
	}
}

func TestDuplicateEventIDIsDeduplicated(t *testing.T) {
	url := newServer(t)
	prepareStream(t, url)
	a := newAdapter(t, url, "")
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	event := events.Event{ID: "same-id", Topic: "work.created"}
	if err := a.Publish(ctx, event); err != nil {
		t.Fatal(err)
	}
	if err := a.Publish(ctx, event); err != nil {
		t.Fatal(err)
	}
	nc, err := natsio.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := js.Stream(ctx, "EVENTS")
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 1 {
		t.Fatalf("expected one stored event, got %d", info.State.Msgs)
	}
}

func TestMissingStreamFails(t *testing.T) {
	url := newServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if a, err := New(ctx, Config{URL: url, Stream: "MISSING"}); err == nil {
		_ = a.Close()
		t.Fatal("expected missing stream error")
	}
}

func TestLegacyUnheaderedEventSurvivesConsumerMigration(t *testing.T) {
	url := newServer(t)
	prepareStream(t, url)
	nc, err := natsio.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := nc.JetStream()
	if err != nil {
		nc.Close()
		t.Fatal(err)
	}
	// This is how the pre-migration Mnemo controller created its durable
	// pull subscription. Its AckWait intentionally differs from the new
	// adapter configuration to prove that the consumer is not recreated.
	oldSub, err := legacy.PullSubscribe("work.created", "mnemo-controller", natsio.BindStream("EVENTS"), natsio.AckWait(7*time.Second))
	if err != nil {
		nc.Close()
		t.Fatal(err)
	}
	payload := []byte(`{"id":"original-event-id","type":"execution.started","project":"existing"}`)
	ack, err := legacy.Publish("work.created", payload)
	if err != nil {
		nc.Close()
		t.Fatal(err)
	}
	pending, err := oldSub.Fetch(1, natsio.MaxWait(2*time.Second))
	if err != nil || len(pending) != 1 {
		nc.Close()
		t.Fatalf("fetch pre-migration event: count=%d err=%v", len(pending), err)
	}
	if len(pending[0].Header) != 0 {
		nc.Close()
		t.Fatalf("legacy event unexpectedly has headers: %v", pending[0].Header)
	}
	if err := pending[0].NakWithDelay(25 * time.Millisecond); err != nil {
		nc.Close()
		t.Fatal(err)
	}
	if err := nc.FlushTimeout(2 * time.Second); err != nil {
		nc.Close()
		t.Fatal(err)
	}
	nc.Close() // Simulate old controller stopping without deleting its durable.

	a := newAdapter(t, url, "mnemo-controller")
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	var attempts atomic.Int32
	received := make(chan events.Event, 2)
	sub, err := a.Subscribe(ctx, "work.created", func(_ context.Context, event events.Event) error {
		received <- event
		if attempts.Add(1) == 1 {
			return errors.New("transient storage failure")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	wantID := fmt.Sprintf("jetstream:EVENTS:%d", ack.Sequence)
	for i := 0; i < 2; i++ {
		select {
		case event := <-received:
			if event.ID != wantID || event.Topic != "work.created" || string(event.Data) != string(payload) {
				t.Fatalf("legacy event changed on attempt %d: %+v", i+1, event)
			}
		case <-ctx.Done():
			t.Fatal("legacy delivery was not retried with its durable identity")
		}
	}
	// Even while an old consumer is being reused, retain its original
	// acknowledgement policy, filter, AckWait and durable name.
	cons, err := a.js.Consumer(ctx, "EVENTS", "mnemo-controller")
	if err != nil {
		t.Fatal(err)
	}
	info, err := cons.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.Config.AckWait != 7*time.Second || info.Config.FilterSubject != "work.created" ||
		info.Config.AckPolicy != jetstream.AckExplicitPolicy || info.Config.Durable != "mnemo-controller" {
		t.Fatalf("existing durable consumer was modified: %+v", info.Config)
	}
}

func TestExistingConsumerWithDifferentFilterIsNotModified(t *testing.T) {
	url := newServer(t)
	prepareStream(t, url)
	nc, err := natsio.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := nc.JetStream()
	if err != nil {
		nc.Close()
		t.Fatal(err)
	}
	if _, err := legacy.PullSubscribe("work.other", "filter-mismatch", natsio.BindStream("EVENTS")); err != nil {
		nc.Close()
		t.Fatal(err)
	}
	nc.Close()

	a := newAdapter(t, url, "filter-mismatch")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if sub, err := a.Subscribe(ctx, "work.created", func(context.Context, events.Event) error { return nil }); err == nil {
		_ = sub.Close()
		t.Fatal("expected incompatible existing consumer to be rejected")
	}
	cons, err := a.js.Consumer(ctx, "EVENTS", "filter-mismatch")
	if err != nil {
		t.Fatal(err)
	}
	info, err := cons.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.Config.FilterSubject != "work.other" {
		t.Fatalf("existing consumer filter was modified: %q", info.Config.FilterSubject)
	}
}
