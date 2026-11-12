package local

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"

	events "github.com/jmeiracorbal/go-events"
)

func TestFanoutAndIndependentPayloads(t *testing.T) {
	b := New()
	defer b.Close()
	ctx := context.Background()
	var calls []string
	_, err := b.Subscribe(ctx, "work.created", func(_ context.Context, ev events.Event) error {
		ev.Data[0] = 'X'
		calls = append(calls, "first")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = b.Subscribe(ctx, "work.created", func(_ context.Context, ev events.Event) error {
		if string(ev.Data) != "ready" {
			t.Fatalf("other subscriber mutated the payload: %s", ev.Data)
		}
		calls = append(calls, "second")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	ev := events.Event{ID: "a", Topic: "work.created", Data: []byte("ready")}
	if err := b.Publish(ctx, ev); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(calls, []string{"first", "second"}) {
		t.Fatalf("unexpected order: %v", calls)
	}
	if string(ev.Data) != "ready" {
		t.Fatal("published data was mutated")
	}
}

func TestHandlerErrorsAreReturned(t *testing.T) {
	b := New()
	defer b.Close()
	want := errors.New("commit failed")
	s, err := b.Subscribe(context.Background(), "work.created", func(context.Context, events.Event) error {
		return want
	})
	if err != nil {
		t.Fatal(err)
	}
	event := events.Event{ID: "a", Topic: "work.created"}
	if err := b.Publish(context.Background(), event); !errors.Is(err, want) {
		t.Fatalf("expected handler error, got %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.Publish(context.Background(), event); err != nil {
		t.Fatal(err)
	}
}

func TestContextCancellationAndClose(t *testing.T) {
	b := New()
	ctx, cancel := context.WithCancel(context.Background())
	s, err := b.Subscribe(ctx, "work.created", func(context.Context, events.Event) error {
		t.Fatal("subscriber was called after context cancellation")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.Publish(context.Background(), events.Event{ID: "a", Topic: "work.created"}); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.Publish(context.Background(), events.Event{ID: "b", Topic: "work.created"}); !errors.Is(err, events.ErrClosed) {
		t.Fatalf("expected closed error, got %v", err)
	}
	if _, err := b.Subscribe(context.Background(), "work.created", func(context.Context, events.Event) error { return nil }); !errors.Is(err, events.ErrClosed) {
		t.Fatalf("expected closed error, got %v", err)
	}
}

func TestConcurrentPublishAndSubscribe(t *testing.T) {
	b := New()
	defer b.Close()
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 25; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := b.Subscribe(ctx, "work.created", func(context.Context, events.Event) error { return nil })
			if err != nil {
				t.Error(err)
				return
			}
			if err := b.Publish(ctx, events.Event{ID: "a", Topic: "work.created"}); err != nil {
				t.Error(err)
			}
			_ = s.Close()
		}()
	}
	wg.Wait()
}
