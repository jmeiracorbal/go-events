package local

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

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

func TestTypedWatchFiltersAndRetainsEventOrder(t *testing.T) {
	b := New()
	defer b.Close()
	ctx := context.Background()
	initial := []any{taskNotification{ID: "initial", Revision: 1}}
	w, err := b.WatchValues(ctx, "swarm.state", func(value any) bool {
		return value.(taskNotification).Revision > 0
	}, initial, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for _, value := range []taskNotification{{ID: "ignored"}, {ID: "live", Revision: 2}} {
		if err := b.PublishValue(ctx, "swarm.state", value); err != nil {
			t.Fatal(err)
		}
	}
	for _, expected := range []taskNotification{{ID: "initial", Revision: 1}, {ID: "live", Revision: 2}} {
		select {
		case got := <-w.Events():
			if got != expected {
				t.Fatalf("unexpected typed value: got=%v expected=%v", got, expected)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("missing typed value %v", expected)
		}
	}
}

func TestTypedWatchOverflowIsReportedToPublisher(t *testing.T) {
	b := New()
	defer b.Close()
	w, err := b.WatchValues(context.Background(), "swarm.task", nil, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	var overflow error
	for i := range 20 {
		if err := b.PublishValue(context.Background(), "swarm.task", i); err != nil {
			overflow = err
			break
		}
	}
	if !errors.Is(overflow, ErrWatchFull) {
		t.Fatalf("typed watch overflow must be explicit: %v", overflow)
	}
	select {
	case <-w.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("overflowed typed watcher was not closed")
	}
}
