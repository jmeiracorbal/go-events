package local

import (
	"context"
	"errors"
	"fmt"
	"sync"

	events "github.com/jmeiracorbal/go-events"
)

// ErrWatchFull reports that a watcher could not retain an incoming event.
// Overflow terminates that watcher; it is never silently dropped.
var ErrWatchFull = errors.New("local event watch queue is full")

// Watcher exposes an ordered, asynchronous view of one topic on an existing
// local Bus. It does not create another transport or use durable storage.
//
// Events are copied before entering the queue. Close cancels pending
// deliveries; a closed Events channel is the definitive end of the watch.
type Watcher struct {
	mu     sync.Mutex
	ready  *sync.Cond
	sub    events.Subscription
	queue  []events.Event
	head   int
	limit  int
	closed bool
	done   chan struct{}
	output chan events.Event
}

// Watch registers one subscriber of topic and seeds its private queue with
// initial events in the supplied order. Callers that require an atomic
// snapshot-and-watch transition must hold their own state read lock while
// collecting initial and registering the watcher.
//
// limit is the maximum number of queued, not-yet-dispatched events; 0 means
// unlimited. A full queue fails the publisher explicitly and terminates the
// watcher. No event is silently discarded.
func (b *Bus) Watch(ctx context.Context, topic string, initial []events.Event, limit int) (*Watcher, error) {
	if ctx == nil {
		return nil, errors.New("watch context is required")
	}
	if limit < 0 {
		return nil, errors.New("watch queue limit cannot be negative")
	}
	if err := events.ValidateTopic(topic); err != nil {
		return nil, err
	}
	if limit > 0 && len(initial) > limit {
		return nil, fmt.Errorf("%w: %d initial events exceed limit %d", ErrWatchFull, len(initial), limit)
	}
	queue := make([]events.Event, len(initial))
	for i, event := range initial {
		if err := event.Validate(); err != nil {
			return nil, fmt.Errorf("initial event %d: %w", i, err)
		}
		if event.Topic != topic {
			return nil, fmt.Errorf("initial event %d has topic %q, expected %q", i, event.Topic, topic)
		}
		queue[i] = event.Clone()
	}
	w := &Watcher{
		queue: queue, limit: limit, done: make(chan struct{}),
		output: make(chan events.Event),
	}
	w.ready = sync.NewCond(&w.mu)
	sub, err := b.Subscribe(ctx, topic, func(_ context.Context, event events.Event) error {
		w.mu.Lock()
		if w.closed {
			w.mu.Unlock()
			return events.ErrClosed
		}
		if w.limit > 0 && len(w.queue)-w.head >= w.limit {
			w.mu.Unlock()
			_ = w.Close()
			return ErrWatchFull
		}
		w.queue = append(w.queue, event.Clone())
		w.ready.Signal()
		w.mu.Unlock()
		return nil
	})
	if err != nil {
		return nil, err
	}
	w.sub = sub
	go w.deliver()
	go func() {
		select {
		case <-ctx.Done():
			_ = w.Close()
		case <-w.done:
		}
	}()
	return w, nil
}

// Events returns the ordered stream. The receiver must not modify the payload
// when forwarding it to another consumer without cloning it.
func (w *Watcher) Events() <-chan events.Event { return w.output }

// Done closes when the watch is cancelled, closed, or overflows.
func (w *Watcher) Done() <-chan struct{} { return w.done }

// Close is idempotent and stops the stream without waiting on a consumer
// that may no longer be reading the Events channel.
func (w *Watcher) Close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	close(w.done)
	w.ready.Broadcast()
	w.mu.Unlock()
	if w.sub != nil {
		return w.sub.Close()
	}
	return nil
}

func (w *Watcher) deliver() {
	defer close(w.output)
	for {
		w.mu.Lock()
		for len(w.queue)-w.head == 0 && !w.closed {
			w.ready.Wait()
		}
		if w.closed {
			w.mu.Unlock()
			return
		}
		event := w.queue[w.head]
		w.queue[w.head] = events.Event{}
		w.head++
		if w.head == len(w.queue) {
			w.queue = nil
			w.head = 0
		} else if w.head >= 64 && w.head >= len(w.queue)/2 {
			w.queue = append([]events.Event(nil), w.queue[w.head:]...)
			w.head = 0
		}
		w.mu.Unlock()
		select {
		case w.output <- event:
		case <-w.done:
			return
		}
	}
}
