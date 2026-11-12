package nats

import (
	"context"
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
