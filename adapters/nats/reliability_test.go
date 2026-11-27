package nats

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	events "github.com/jmeiracorbal/go-events"
	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestInspectStreamIsReadOnly(t *testing.T) {
	url := newServer(t)
	prepareStream(t, url)
	a := newAdapter(t, url, "")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	info, err := a.InspectStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.Name != "EVENTS" || info.Retention != RetentionLimits ||
		info.Storage != StorageFile || info.Created.IsZero() ||
		info.Replicas != 1 || len(info.Subjects) != 1 || info.Subjects[0] != "work.>" ||
		info.Consumers != 0 || info.Messages != 0 {
		t.Fatalf("unexpected stream metadata: %+v", info)
	}
	if err := a.Publish(ctx, events.Event{ID: "one", Topic: "work.created"}); err != nil {
		t.Fatal(err)
	}
	info.Subjects[0] = "other.>"
	fresh, err := a.InspectStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if fresh.Subjects[0] != "work.>" || fresh.Messages != 1 ||
		fresh.FirstSequence != 1 || fresh.LastSequence != 1 || fresh.Consumers != 0 {
		t.Fatalf("inspection did not return fresh, independent metadata: %+v", fresh)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := a.InspectStream(ctx); !errors.Is(err, events.ErrClosed) {
		t.Fatalf("expected closed error: %v", err)
	}
}

func TestCoversSubjectChecksFullPattern(t *testing.T) {
	for _, tc := range []struct {
		configured []string
		required string
		want bool
	}{
		{[]string{"work.>"}, "work.>", true},
		{[]string{"work.>"}, "work.created", true},
		{[]string{"work.>"}, "work", false},
		{[]string{"work.*"}, "work.>", false},
		{[]string{"work.*.>"}, "work.>", false},
		{[]string{"work.*.>"}, "work.*.done", true},
		{[]string{"work.created", "work.updated"}, "work.*", false},
		{[]string{"work.*"}, "work.created", true},
		{[]string{"work.created"}, "work.*", false},
		{[]string{">"}, "work.>", true},
		{nil, "work.>", false},
	} {
		got, err := (StreamInfo{Subjects: tc.configured}).CoversSubject(tc.required)
		if err != nil || got != tc.want {
			t.Errorf("%v covers %q = %v, %v; want %v", tc.configured, tc.required, got, err, tc.want)
		}
	}
	if _, err := (StreamInfo{Subjects: []string{"work.>"}}).CoversSubject("work.>.bad"); err == nil {
		t.Fatal("accepted invalid requested pattern")
	}
}

func TestPublishCannotWriteToAnotherStream(t *testing.T) {
	url := newServer(t)
	prepareStream(t, url)
	a := newAdapter(t, url, "")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	other, err := a.js.CreateStream(ctx, jetstream.StreamConfig{
		Name: "OTHER", Subjects: []string{"other.>"}, Storage: jetstream.MemoryStorage,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Publish(ctx, events.Event{ID: "wrong", Topic: "other.created"}); err == nil {
		t.Fatal("wrong stream publication succeeded")
	}
	info, err := other.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 0 {
		t.Fatalf("wrong stream was mutated: %+v", info.State)
	}
	if err := a.Publish(ctx, events.Event{ID: "invalid", Topic: "work.*"}); err == nil {
		t.Fatal("wildcard publication accepted")
	}
}

func TestConnectCancellationDuringHandshake(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			select {
			case accepted <- conn:
			case <-time.After(5 * time.Second):
				conn.Close()
			}
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	results := make(chan error, 1)
	go func() {
		a, err := New(ctx, Config{URL: "nats://" + listener.Addr().String(), Stream: "EVENTS"},
			natsio.Timeout(5*time.Second))
		if a != nil {
			a.Close()
		}
		results <- err
	}()
	select {
	case conn := <-accepted:
		defer conn.Close()
	case <-time.After(5 * time.Second):
		t.Fatal("connection did not start")
	}
	cancel()
	select {
	case err := <-results:
		if !errors.Is(err, context.Canceled) || !errors.Is(err, ErrConnect) {
			t.Fatalf("expected canceled connection, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("connection did not honor cancellation during handshake")
	}
}

func TestMissingStreamIsDistinguishable(t *testing.T) {
	url := newServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	a, err := New(ctx, Config{URL: url, Stream: "MISSING"})
	if a != nil {
		a.Close()
	}
	if !errors.Is(err, ErrStreamLookup) || !errors.Is(err, jetstream.ErrStreamNotFound) {
		t.Fatalf("expected typed stream lookup failure, got %v", err)
	}
}

func TestCapturedEmptyHistoryDoesNotExpandOrSkipRetention(t *testing.T) {
	url := newServer(t)
	prepareStream(t, url)
	a := newAdapter(t, url, "")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	page, err := a.Read(ctx, events.ReadQuery{Subject: "work.>", Limit: 1})
	if err != nil || page.Next.Subject != "work.>" || page.Next.StreamCreated.IsZero() ||
		page.Next.UpperBound != 0 || page.Next.AfterSequence != 0 {
		t.Fatalf("invalid empty cursor: %+v, %v", page, err)
	}
	for _, id := range []string{"one", "two"} {
		if err := a.Publish(ctx, events.Event{ID: id, Topic: "work.created"}); err != nil {
			t.Fatal(err)
		}
	}
	same, err := a.Read(ctx, events.ReadQuery{Subject: "work.>", Cursor: page.Next, Limit: 1})
	if err != nil || len(same.Events) != 0 || same.Next.UpperBound != 0 {
		t.Fatalf("empty captured window expanded: %+v, %v", same, err)
	}
	if _, err := a.Read(ctx, events.ReadQuery{Subject: "work.created", Cursor: page.Next, Limit: 1}); !errors.Is(err, events.ErrInvalidCursor) {
		t.Fatalf("changed cursor subject accepted: %v", err)
	}
	handler := func(context.Context, events.StoredEvent) error { return nil }
	if _, err := a.ReplayEphemeral(ctx, page.Next, "work.created", time.Minute, handler); !errors.Is(err, events.ErrInvalidCursor) {
		t.Fatalf("changed replay subject accepted: %v", err)
	}
	stream, err := a.js.Stream(ctx, "EVENTS")
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Purge(ctx, jetstream.WithPurgeSequence(2)); err != nil {
		t.Fatal(err)
	}
	if _, err := a.ReplayEphemeral(ctx, page.Next, "work.>", time.Minute, handler); !errors.Is(err, events.ErrHistoryExpired) {
		t.Fatalf("lost intervening events were not diagnosed: %v", err)
	}
	info, err := stream.Info(ctx)
	if err != nil || info.State.Consumers != 0 {
		t.Fatalf("failed replay left a consumer: %+v, %v", info, err)
	}
}

func TestCapturedHistoryRejectsRecreatedStream(t *testing.T) {
	url := newServer(t)
	prepareStream(t, url)
	a := newAdapter(t, url, "")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	page, err := a.Read(ctx, events.ReadQuery{Subject: "work.>", Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.js.DeleteStream(ctx, "EVENTS"); err != nil {
		t.Fatal(err)
	}
	if _, err := a.js.CreateStream(ctx, jetstream.StreamConfig{
		Name: "EVENTS", Subjects: []string{"work.>"}, Storage: jetstream.FileStorage,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Read(ctx, events.ReadQuery{Subject: "work.>", Cursor: page.Next, Limit: 1}); !errors.Is(err, events.ErrInvalidCursor) {
		t.Fatalf("recreated stream accepted by history: %v", err)
	}
	if _, err := a.ReplayEphemeral(ctx, page.Next, "work.>", time.Minute,
		func(context.Context, events.StoredEvent) error { return nil }); !errors.Is(err, events.ErrInvalidCursor) {
		t.Fatalf("recreated stream accepted by replay: %v", err)
	}
}

func TestConcurrentCloseReturnsSameCleanupFailure(t *testing.T) {
	url := newServer(t)
	prepareStream(t, url)
	a := newAdapter(t, url, "")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sub, err := a.ReplayEphemeral(ctx, events.Cursor{}, "work.>", time.Minute,
		func(context.Context, events.StoredEvent) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	s := sub.(*subscription)
	original := s.cleanup
	failure := errors.New("observed cleanup failure")
	s.cleanup = func() error {
		return errors.Join(original(), failure)
	}
	const callers = 8
	var wg sync.WaitGroup
	results := make(chan error, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- a.Close()
		}()
	}
	wg.Wait()
	close(results)
	for err := range results {
		if !errors.Is(err, failure) {
			t.Errorf("concurrent close hid cleanup failure: %v", err)
		}
	}
	if err := sub.Close(); !errors.Is(err, failure) {
		t.Fatalf("subscription close hid cleanup failure: %v", err)
	}
}
