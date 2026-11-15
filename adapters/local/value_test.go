package local

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"

	events "github.com/jmeiracorbal/go-events"
)

type taskNotification struct {
	ID string
	Revision uint64
}

func TestTypedLocalEventsShareExistingBusWithoutSerialization(t *testing.T) {
	b := New()
	defer b.Close()
	ctx := context.Background()
	var typed []taskNotification
	var serialized int
	_, err := b.SubscribeValue(ctx, "swarm.task", func(_ context.Context, value any) error {
		item, ok := value.(taskNotification)
		if !ok {
			t.Fatalf("unexpected value type: %T", value)
		}
		typed = append(typed, item)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = b.Subscribe(ctx, "swarm.task", func(_ context.Context, e events.Event) error {
		serialized++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range []taskNotification{{"a", 1}, {"b", 2}} {
		if err := b.PublishValue(ctx, "swarm.task", item); err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(typed, []taskNotification{{"a", 1}, {"b", 2}}) || serialized != 0 {
		t.Fatalf("typed subscribers must receive unmodified Go values only: %v serialized=%d", typed, serialized)
	}
	if err := b.Publish(ctx, events.Event{ID: "c", Topic: "swarm.task"}); err != nil {
		t.Fatal(err)
	}
	if len(typed) != 2 || serialized != 1 {
		t.Fatalf("independent payload contracts must not cross: %v serialized=%d", typed, serialized)
	}
}

func TestTypedLocalEventSubscriberFailuresAndCancellation(t *testing.T) {
	b := New()
	defer b.Close()
	ctx := context.Background()
	want := errors.New("watch queue reached limit")
	s, err := b.SubscribeValue(ctx, "swarm.node", func(context.Context, any) error {
		return want
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.PublishValue(ctx, "swarm.node", "ready"); !errors.Is(err, want) {
		t.Fatalf("subscriber failures must reach publisher: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.PublishValue(ctx, "swarm.node", "ready"); err != nil {
		t.Fatal(err)
	}
	if err := b.PublishValue(ctx, "swarm.node", nil); err == nil {
		t.Fatal("nil local value was accepted")
	}
	if _, err := b.SubscribeValue(ctx, "swarm.node", nil); err == nil {
		t.Fatal("nil typed subscriber was accepted")
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.PublishValue(ctx, "swarm.node", "ready"); !errors.Is(err, events.ErrClosed) {
		t.Fatalf("closed bus must reject values: %v", err)
	}
}

func TestTypedLocalEventConcurrentSubscribers(t *testing.T) {
	b := New()
	defer b.Close()
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sub, err := b.SubscribeValue(ctx, "swarm.task", func(context.Context, any) error { return nil })
			if err != nil {
				t.Error(err)
				return
			}
			if err := b.PublishValue(ctx, "swarm.task", taskNotification{ID: "x"}); err != nil {
				t.Error(err)
			}
			_ = sub.Close()
		}()
	}
	wg.Wait()
}
