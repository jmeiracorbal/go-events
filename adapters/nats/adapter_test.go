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
	nc, err := natsio.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		t.Fatal(err)
	}
	// Create an ordinary durable subscriber with an AckWait distinct from the
	// new adapter's configuration. Reattachment must not reset its settings.
	initialSub, err := js.PullSubscribe("work.created", "persistent-consumer", natsio.BindStream("EVENTS"), natsio.AckWait(7*time.Second))
	if err != nil {
		nc.Close()
		t.Fatal(err)
	}
	payload := []byte(`{"id":"persisted-event","project":"existing"}`)
	msg := &natsio.Msg{
		Subject: "work.created",
		Data:    payload,
		Header:  natsio.Header{},
	}
	msg.Header.Set(messageIDHeader, "persisted-event")
	if _, err := js.PublishMsg(msg); err != nil {
		nc.Close()
		t.Fatal(err)
	}
	pending, err := initialSub.Fetch(1, natsio.MaxWait(2*time.Second))
	if err != nil || len(pending) != 1 {
		nc.Close()
		t.Fatalf("fetch pending event: count=%d err=%v", len(pending), err)
	}
	if pending[0].Header.Get(messageIDHeader) != "persisted-event" {
		nc.Close()
		t.Fatal("persisted event does not satisfy the message ID contract")
	}
	if err := pending[0].NakWithDelay(25 * time.Millisecond); err != nil {
		nc.Close()
		t.Fatal(err)
	}
	if err := nc.FlushTimeout(2 * time.Second); err != nil {
		nc.Close()
		t.Fatal(err)
	}
	nc.Close()

	a := newAdapter(t, url, "persistent-consumer")
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
	for i := 0; i < 2; i++ {
		select {
		case event := <-received:
			if event.ID != "persisted-event" || event.Topic != "work.created" || string(event.Data) != string(payload) {
				t.Fatalf("pending delivery changed on attempt %d: %+v", i+1, event)
			}
		case <-ctx.Done():
			t.Fatal("pending delivery was not retried with its original event identity")
		}
	}
	cons, err := a.js.Consumer(ctx, "EVENTS", "persistent-consumer")
	if err != nil {
		t.Fatal(err)
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
	nc, err := natsio.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	initialJS, err := nc.JetStream()
	if err != nil {
		nc.Close()
		t.Fatal(err)
	}
	if _, err := initialJS.PullSubscribe("work.other", "filter-mismatch", natsio.BindStream("EVENTS")); err != nil {
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

func TestJetStreamHandoverAfterAcknowledgingUnheaderedDeliveries(t *testing.T) {
	url := newServer(t)
	prepareStream(t, url)

	// Simulate the existing controller: initialJS NATS publishers store the
	// application envelope without Nats-Msg-Id, then its durable consumer
	// acknowledges each delivery before the consumer is replaced.
	nc, err := natsio.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	initialJS, err := nc.JetStream()
	if err != nil {
		nc.Close()
		t.Fatal(err)
	}
	initialConsumer, err := initialJS.PullSubscribe("work.created", "existing-controller", natsio.BindStream("EVENTS"))
	if err != nil {
		nc.Close()
		t.Fatal(err)
	}
	original := [][]byte{
		[]byte(`{"id":"before-1","type":"record.created","payload":{"name":"sample"}}`),
		[]byte(`{"id":"before-2","type":"record.updated","payload":{"name":"sample"}}`),
	}
	sequence := make([]uint64, 0, len(original))
	for _, data := range original {
		published, err := initialJS.PublishMsg(&natsio.Msg{Subject: "work.created", Data: data})
		if err != nil {
			nc.Close()
			t.Fatal(err)
		}
		sequence = append(sequence, published.Sequence)
	}
	messages, err := initialConsumer.Fetch(len(original), natsio.MaxWait(3*time.Second))
	if err != nil || len(messages) != len(original) {
		nc.Close()
		t.Fatalf("drain existing deliveries: received=%d err=%v", len(messages), err)
	}
	for i, msg := range messages {
		if msg.Header.Get(messageIDHeader) != "" || string(msg.Data) != string(original[i]) {
			nc.Close()
			t.Fatalf("pre-handover event %d was modified: %+v", i, msg)
		}
		if err := msg.AckSync(); err != nil {
			nc.Close()
			t.Fatalf("ack committed pre-handover event: %v", err)
		}
	}
	before, err := initialJS.ConsumerInfo("EVENTS", "existing-controller")
	if err != nil {
		nc.Close()
		t.Fatal(err)
	}
	if before.NumPending != 0 || before.NumAckPending != 0 || before.AckFloor.Stream != sequence[len(sequence)-1] {
		nc.Close()
		t.Fatalf("consumer not drained before handover: %+v", before)
	}
	nc.Close()

	// Reuse the exact same persisted stream and durable with the new adapter.
	a := newAdapter(t, url, "existing-controller")
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	received := make(chan events.Event, 3)
	sub, err := a.Subscribe(ctx, "work.created", func(_ context.Context, event events.Event) error {
		received <- event
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	newPayload := []byte(`{"id":"after-3","type":"record.closed","payload":{}}`)
	if err := a.Publish(ctx, events.Event{ID: "after-3", Topic: "work.created", Data: newPayload}); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-received:
		if event.ID != "after-3" || event.Topic != "work.created" || string(event.Data) != string(newPayload) {
			t.Fatalf("handover changed event identity or payload: %+v", event)
		}
	case <-ctx.Done():
		t.Fatal("post-handover event was not delivered")
	}
	select {
	case unexpected := <-received:
		t.Fatalf("pre-handover event was incorrectly replayed: %+v", unexpected)
	case <-time.After(150 * time.Millisecond):
	}

	// Confirm the stored messages remain intact and that the existing durable
	// retains its identity and delivery state.
	check, err := natsio.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer check.Close()
	checkJS, err := check.JetStream()
	if err != nil {
		t.Fatal(err)
	}
	for i, seq := range sequence {
		stored, err := checkJS.GetMsg("EVENTS", seq)
		if err != nil {
			t.Fatal(err)
		}
		if string(stored.Data) != string(original[i]) || stored.Header.Get(messageIDHeader) != "" {
			t.Fatalf("stored message at stream sequence %d was rewritten", seq)
		}
	}
	info, err := checkJS.ConsumerInfo("EVENTS", "existing-controller")
	if err != nil {
		t.Fatal(err)
	}
	if info.Config.Durable != before.Config.Durable ||
		info.Config.FilterSubject != before.Config.FilterSubject ||
		info.Config.AckPolicy != before.Config.AckPolicy ||
		info.Config.AckWait != before.Config.AckWait {
		t.Fatalf("handover modified the durable consumer: before=%+v after=%+v", before.Config, info.Config)
	}
	if info.NumPending != 0 || info.NumAckPending != 0 {
		t.Fatalf("handover left pending deliveries: %+v", info)
	}
}

func TestDeliveryWithoutMessageIDIsNeverCommitted(t *testing.T) {
	url := newServer(t)
	prepareStream(t, url)
	nc, err := natsio.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		t.Fatal(err)
	}
	headerlessPayload := []byte(`{"id":"unacknowledged-event","type":"record.created"}`)
	published, err := js.Publish("work.created", headerlessPayload)
	if err != nil {
		nc.Close()
		t.Fatal(err)
	}
	nc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	reported := make(chan error, 4)
	var applied atomic.Int32
	a, err := New(ctx, Config{
		URL: url, Stream: "EVENTS", Consumer: "unchecked-cutover",
		AckWait: time.Second, RetryDelay: 100 * time.Millisecond,
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
		t.Fatal("missing message ID did not fail visibly")
	}
	if err := sub.Close(); err != nil {
		t.Fatal(err)
	}
	if applied.Load() != 0 {
		t.Fatalf("unidentified message reached application handler %d times", applied.Load())
	}

	check, err := natsio.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer check.Close()
	checkJS, err := check.JetStream()
	if err != nil {
		t.Fatal(err)
	}
	stored, err := checkJS.GetMsg("EVENTS", published.Sequence)
	if err != nil {
		t.Fatal(err)
	}
	if string(stored.Data) != string(headerlessPayload) || stored.Header.Get(messageIDHeader) != "" {
		t.Fatal("invalid event was rewritten or deleted")
	}
	info, err := checkJS.ConsumerInfo("EVENTS", "unchecked-cutover")
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

	// Create a file-backed JetStream stream and durable consumer that must survive a server restart.
	originalServer := start()
	nc, err := natsio.Connect(originalServer.ClientURL())
	if err != nil {
		stop(originalServer)
		t.Fatal(err)
	}
	initialJS, err := nc.JetStream()
	if err != nil {
		nc.Close()
		stop(originalServer)
		t.Fatal(err)
	}
	if _, err := initialJS.AddStream(&natsio.StreamConfig{
		Name: "RESTART_EVENTS", Subjects: []string{"audit.events.>"},
		Storage: natsio.FileStorage, Retention: natsio.LimitsPolicy,
	}); err != nil {
		nc.Close()
		stop(originalServer)
		t.Fatal(err)
	}
	initialConsumer, err := initialJS.PullSubscribe("audit.events.>", "audit-consumer", natsio.BindStream("RESTART_EVENTS"))
	if err != nil {
		nc.Close()
		stop(originalServer)
		t.Fatal(err)
	}
	persisted := []byte(`{"id":"before-restart-id","type":"record.created","payload":{"name":"sample"},"occurred_at":"2026-01-01T00:00:00Z"}`)
	pub, err := initialJS.Publish("audit.events.source-a", persisted)
	if err != nil {
		nc.Close()
		stop(originalServer)
		t.Fatal(err)
	}
	pending, err := initialConsumer.Fetch(1, natsio.MaxWait(3*time.Second))
	if err != nil || len(pending) != 1 {
		nc.Close()
		stop(originalServer)
		t.Fatalf("fetch existing delivery: messages=%d err=%v", len(pending), err)
	}
	if pending[0].Header.Get(messageIDHeader) != "" || string(pending[0].Data) != string(persisted) {
		nc.Close()
		stop(originalServer)
		t.Fatal("stored message does not match its original wire format")
	}
	if err := pending[0].AckSync(); err != nil {
		nc.Close()
		stop(originalServer)
		t.Fatal(err)
	}
	previous, err := initialJS.ConsumerInfo("RESTART_EVENTS", "audit-consumer")
	if err != nil {
		nc.Close()
		stop(originalServer)
		t.Fatal(err)
	}
	if previous.NumPending != 0 || previous.NumAckPending != 0 || previous.AckFloor.Stream != pub.Sequence {
		nc.Close()
		stop(originalServer)
		t.Fatalf("initial consumer has uncommitted deliveries: %+v", previous)
	}
	nc.Close()
	stop(originalServer)

	// Restart the broker using the same file-backed storage directory.
	// Reusing the durable must not recreate the stream or replay acknowledged messages.
	restartedServer := start()
	defer stop(restartedServer)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	a, err := New(ctx, Config{
		URL: restartedServer.ClientURL(), Stream: "RESTART_EVENTS",
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
	next := []byte(`{"id":"after-restart-id","type":"record.updated","payload":{"name":"sample"},"occurred_at":"2026-01-01T00:00:01Z"}`)
	if err := a.Publish(ctx, events.Event{
		ID: "after-restart-id", Topic: "audit.events.source-a", Data: next,
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case delivered := <-received:
		if delivered.ID != "after-restart-id" || string(delivered.Data) != string(next) {
			t.Fatalf("event mutated or previous message replayed: %+v", delivered)
		}
	case <-ctx.Done():
		t.Fatal("delivery after broker restart was not processed")
	}

	inspect, err := natsio.Connect(restartedServer.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer inspect.Close()
	inspector, err := inspect.JetStream()
	if err != nil {
		t.Fatal(err)
	}
	original, err := inspector.GetMsg("RESTART_EVENTS", pub.Sequence)
	if err != nil {
		t.Fatal(err)
	}
	if string(original.Data) != string(persisted) || original.Header.Get(messageIDHeader) != "" {
		t.Fatal("previous event was modified or reencoded during broker restart")
	}
	until := time.Now().Add(5 * time.Second)
	for {
		info, err := inspector.ConsumerInfo("RESTART_EVENTS", "audit-consumer")
		if err != nil {
			t.Fatal(err)
		}
		if info.NumPending == 0 && info.NumAckPending == 0 &&
			info.AckFloor.Stream == pub.Sequence+1 {
			if info.Config.FilterSubject != "audit.events.>" ||
				info.Config.Durable != previous.Config.Durable ||
				info.Config.AckPolicy != previous.Config.AckPolicy ||
				info.Config.AckWait != previous.Config.AckWait {
				t.Fatalf("durable changed across server restart: %+v", info.Config)
			}
			break
		}
		if time.Now().After(until) {
			t.Fatalf("restarted controller has unacknowledged or replayed events: %+v", info)
		}
		time.Sleep(25 * time.Millisecond)
	}
}
