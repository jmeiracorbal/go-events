package local

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	events "github.com/jmeiracorbal/go-events"
)

func receiveWatched(t *testing.T, watch *Watcher) events.Event {
	t.Helper()
	select {
	case event, ok := <-watch.Events():
		if !ok {
			t.Fatal("watch closed before receiving expected event")
		}
		return event
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for watched event")
		return events.Event{}
	}
}

func TestWatchInitialOrderingAndIndependentData(t *testing.T) {
	b := New()
	defer b.Close()
	ctx := context.Background()
	initial := []events.Event{
		{ID: "snapshot-1", Topic: "swarm.state", Data: []byte("first")},
		{ID: "snapshot-2", Topic: "swarm.state", Data: []byte("second")},
	}
	w, err := b.Watch(ctx, "swarm.state", initial, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	initial[0].Data[0] = 'X'
	if err := b.Publish(ctx, events.Event{ID: "live-3", Topic: "swarm.state", Data: []byte("third")}); err != nil {
		t.Fatal(err)
	}
	var got []string
	for range 3 {
		got = append(got, string(receiveWatched(t, w).Data))
	}
	if !reflect.DeepEqual(got, []string{"first", "second", "third"}) {
		t.Fatalf("initial and live events must remain ordered: %v", got)
	}
}

func TestWatchDoesNotBlockPublisherAndReportsOverflow(t *testing.T) {
	b := New()
	defer b.Close()
	w, err := b.Watch(context.Background(), "swarm.task", nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	var overflow error
	for i := range 20 {
		err := b.Publish(context.Background(), events.Event{ID: string(rune('a' + i)), Topic: "swarm.task"})
		if err != nil {
			overflow = err
			break
		}
	}
	if !errors.Is(overflow, ErrWatchFull) {
		t.Fatalf("a saturated watcher must fail publishing explicitly: %v", overflow)
	}
	select {
	case <-w.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("saturated watcher was not closed")
	}
	select {
	case _, ok := <-w.Events():
		if ok {
			t.Fatal("overflowed watcher must not dispatch pending events")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("overflowed watch did not terminate")
	}
}

func TestWatchCancellationAndClose(t *testing.T) {
	b := New()
	defer b.Close()
	ctx, cancel := context.WithCancel(context.Background())
	w, err := b.Watch(ctx, "swarm.node", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case <-w.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("watch context cancellation was not observed")
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case _, ok := <-w.Events():
		if ok {
			t.Fatal("cancelled watch delivered an event")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancelled watch remained open")
	}
	if err := b.Publish(context.Background(), events.Event{ID: "other", Topic: "swarm.node"}); err != nil {
		t.Fatalf("cancelled watcher must be removed from the existing bus: %v", err)
	}
}

func TestWatchRejectsInvalidInitialEvents(t *testing.T) {
	b := New()
	defer b.Close()
	for _, tc := range []struct {
		name string
		topic string
		initial []events.Event
		limit int
	}{
		{"invalid topic", "bad topic", nil, 0},
		{"negative limit", "task", nil, -1},
		{"wrong event topic", "task", []events.Event{{ID: "1", Topic: "node"}}, 0},
		{"missing ID", "task", []events.Event{{Topic: "task"}}, 0},
		{"initial overflow", "task", []events.Event{{ID: "1", Topic: "task"}, {ID: "2", Topic: "task"}}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := b.Watch(context.Background(), tc.topic, tc.initial, tc.limit); err == nil {
				t.Fatal("invalid watcher configuration was accepted")
			}
		})
	}
}
