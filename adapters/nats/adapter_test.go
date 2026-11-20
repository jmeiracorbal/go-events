package nats

import (
	"context"
	"errors"
	"sync/atomic"
	"strings"
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


func TestExistingDurableConsumerPreservesSettingsAndPendingDelivery(t *testing.T) {
	url := newServer(t)
	prepareStream(t, url)
	a := newAdapter(t, url, "persistent-consumer")
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	cons, err := a.js.CreateConsumer(ctx, "EVENTS", jetstream.ConsumerConfig{
		Durable: "persistent-consumer", FilterSubject: "work.created",
		AckPolicy: jetstream.AckExplicitPolicy, AckWait: 7*time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte(`{"id":"persisted-event","project":"independent"}`)
	msg := &natsio.Msg{Subject: "work.created", Data: payload, Header: natsio.Header{}}
	msg.Header.Set(messageIDHeader, "persisted-event")
	if _, err := a.js.PublishMsg(ctx, msg); err != nil {
		t.Fatal(err)
	}
	delivery, err := cons.Next(jetstream.FetchMaxWait(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if delivery.Headers().Get(messageIDHeader) != "persisted-event" {
		t.Fatal("persisted message ID changed")
	}
	if err := delivery.NakWithDelay(25*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := a.conn.FlushTimeout(2*time.Second); err != nil {
		t.Fatal(err)
	}
	var attempts atomic.Int32
	received := make(chan events.Event, 2)
	sub, err := a.Subscribe(ctx, "work.created", func(_ context.Context, event events.Event) error {
		received <- event
		if attempts.Add(1) == 1 {
			return errors.New("transient application failure")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	for i := 0; i < 2; i++ {
		select {
		case event := <-received:
			if event.ID != "persisted-event" || event.Topic != "work.created" || string(event.Data) != string(payload) {
				t.Fatalf("durable redelivery changed on attempt %d: %+v", i+1, event)
			}
		case <-ctx.Done():
			t.Fatal("durable delivery did not retry with its original identity")
		}
	}
	info, err := cons.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.Config.AckWait != 7*time.Second || info.Config.FilterSubject != "work.created" ||
		info.Config.AckPolicy != jetstream.AckExplicitPolicy || info.Config.Durable != "persistent-consumer" {
		t.Fatalf("existing durable consumer was modified: %+v", info.Config)
	}
}

func TestExistingConsumerWithDifferentFilterIsNotModified(t *testing.T) {
	url := newServer(t)
	prepareStream(t, url)
	a := newAdapter(t, url, "filter-mismatch")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cons, err := a.js.CreateConsumer(ctx, "EVENTS", jetstream.ConsumerConfig{
		Durable: "filter-mismatch", FilterSubject: "work.other",
		AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		t.Fatal(err)
	}
	if sub, err := a.Subscribe(ctx, "work.created", func(context.Context, events.Event) error { return nil }); err == nil {
		_ = sub.Close()
		t.Fatal("expected incompatible existing consumer to be rejected")
	}
	info, err := cons.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.Config.FilterSubject != "work.other" {
		t.Fatalf("existing consumer filter was modified: %q", info.Config.FilterSubject)
	}
}

func TestDeliveryWithoutMessageIDIsNeverCommitted(t *testing.T) {
	url := newServer(t)
	prepareStream(t, url)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	reported := make(chan error, 4)
	var applied atomic.Int32
	a, err := New(ctx, Config{
		URL: url, Stream: "EVENTS", Consumer: "invalid-event-consumer",
		AckWait: time.Second, RetryDelay: 100*time.Millisecond,
		OnError: func(err error) {
			select {
			case reported <- err:
			default:
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()

	// A record written outside the event contract must never be acknowledged.
	// The transport must not infer an ID from the opaque payload.
	payload := []byte(`{"id":"not-a-wire-id","type":"record.created"}`)
	pub, err := a.js.PublishMsg(ctx, &natsio.Msg{Subject: "work.created", Data: payload})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := a.Subscribe(ctx, "work.created", func(context.Context, events.Event) error {
		applied.Add(1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	select {
	case err := <-reported:
		if !strings.Contains(err.Error(), "event ID is required") {
			t.Fatalf("incorrect error for missing message ID: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("invalid event was not rejected visibly")
	}
	if err := sub.Close(); err != nil {
		t.Fatal(err)
	}
	if applied.Load() != 0 {
		t.Fatalf("invalid event reached application handler %d times", applied.Load())
	}

	stream, err := a.js.Stream(ctx, "EVENTS")
	if err != nil {
		t.Fatal(err)
	}
	stored, err := stream.GetMsg(ctx, pub.Sequence)
	if err != nil {
		t.Fatal(err)
	}
	if string(stored.Data) != string(payload) || stored.Header.Get(messageIDHeader) != "" {
		t.Fatal("invalid stored event was changed or deleted")
	}
	cons, err := a.js.Consumer(ctx, "EVENTS", "invalid-event-consumer")
	if err != nil {
		t.Fatal(err)
	}
	info, err := cons.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.AckFloor.Stream != 0 {
		t.Fatalf("invalid event was acknowledged: %+v", info)
	}
}

func TestDurableConsumerPreservesDiskStateAfterServerRestart(t *testing.T) {
	storage := t.TempDir()
	start := func() *natsserver.Server {
		t.Helper()
		server, err := natsserver.NewServer(&natsserver.Options{
			Host: "127.0.0.1", Port: -1, JetStream: true,
			StoreDir: storage, NoLog: true, NoSigs: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		server.Start()
		if !server.ReadyForConnections(5 * time.Second) {
			server.Shutdown()
			t.Fatal("persistent NATS server did not become ready")
		}
		return server
	}
	stop := func(server *natsserver.Server) {
		t.Helper()
		server.Shutdown()
		server.WaitForShutdown()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	initialServer := start()
	nc, err := natsio.Connect(initialServer.ClientURL())
	if err != nil {
		stop(initialServer)
		t.Fatal(err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		stop(initialServer)
		t.Fatal(err)
	}
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name: "RESTART_EVENTS", Subjects: []string{"audit.events.>"},
		Storage: jetstream.FileStorage, Retention: jetstream.LimitsPolicy,
	})
	if err != nil {
		nc.Close()
		stop(initialServer)
		t.Fatal(err)
	}
	consumer, err := js.CreateConsumer(ctx, "RESTART_EVENTS", jetstream.ConsumerConfig{
		Durable: "audit-consumer", FilterSubject: "audit.events.>",
		AckPolicy: jetstream.AckExplicitPolicy, AckWait: 7*time.Second,
	})
	if err != nil {
		nc.Close()
		stop(initialServer)
		t.Fatal(err)
	}
	persisted := []byte(`{"id":"event-before-restart","type":"record.created"}`)
	original := &natsio.Msg{Subject: "audit.events.source-a", Data: persisted, Header: natsio.Header{}}
	original.Header.Set(messageIDHeader, "event-before-restart")
	pub, err := js.PublishMsg(ctx, original)
	if err != nil {
		nc.Close()
		stop(initialServer)
		t.Fatal(err)
	}
	delivery, err := consumer.Next(jetstream.FetchMaxWait(3*time.Second))
	if err != nil {
		nc.Close()
		stop(initialServer)
		t.Fatal(err)
	}
	if delivery.Headers().Get(messageIDHeader) != "event-before-restart" {
		nc.Close()
		stop(initialServer)
		t.Fatal("original event ID changed")
	}
	if err := delivery.DoubleAck(ctx); err != nil {
		nc.Close()
		stop(initialServer)
		t.Fatal(err)
	}
	previous, err := consumer.Info(ctx)
	if err != nil {
		nc.Close()
		stop(initialServer)
		t.Fatal(err)
	}
	if previous.NumPending != 0 || previous.NumAckPending != 0 || previous.AckFloor.Stream != pub.Sequence {
		nc.Close()
		stop(initialServer)
		t.Fatalf("first consumer has unsettled deliveries: %+v", previous)
	}
	nc.Close()
	stop(initialServer)

	restarted := start()
	defer stop(restarted)
	a, err := New(ctx, Config{
		URL: restarted.ClientURL(), Stream: "RESTART_EVENTS",
		Consumer: "audit-consumer", AckWait: 2*time.Second,
		RetryDelay: 25*time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	received := make(chan events.Event, 2)
	sub, err := a.Subscribe(ctx, "audit.events.>", func(_ context.Context, event events.Event) error {
		received <- event
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	next := []byte(`{"id":"event-after-restart","type":"record.updated"}`)
	if err := a.Publish(ctx, events.Event{
		ID: "event-after-restart", Topic: "audit.events.source-a", Data: next,
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-received:
		if event.ID != "event-after-restart" || string(event.Data) != string(next) {
			t.Fatalf("unexpected delivery after broker restart: %+v", event)
		}
	case <-ctx.Done():
		t.Fatal("new event was not delivered after broker restart")
	}
	stream, err := a.js.Stream(ctx, "RESTART_EVENTS")
	if err != nil {
		t.Fatal(err)
	}
	stored, err := stream.GetMsg(ctx, pub.Sequence)
	if err != nil {
		t.Fatal(err)
	}
	if string(stored.Data) != string(persisted) || stored.Header.Get(messageIDHeader) != "event-before-restart" {
		t.Fatal("original stored event changed across broker restart")
	}
	until := time.Now().Add(5 * time.Second)
	for {
		existing, err := a.js.Consumer(ctx, "RESTART_EVENTS", "audit-consumer")
		if err != nil {
			t.Fatal(err)
		}
		info, err := existing.Info(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if info.NumPending == 0 && info.NumAckPending == 0 && info.AckFloor.Stream == pub.Sequence+1 {
			if info.Config.FilterSubject != "audit.events.>" ||
				info.Config.Durable != previous.Config.Durable ||
				info.Config.AckPolicy != previous.Config.AckPolicy ||
				info.Config.AckWait != previous.Config.AckWait {
				t.Fatalf("durable configuration changed across broker restart: %+v", info.Config)
			}
			break
		}
		if time.Now().After(until) {
			t.Fatalf("broker restart did not preserve durable progress: %+v", info)
		}
		time.Sleep(25 * time.Millisecond)
	}
}
