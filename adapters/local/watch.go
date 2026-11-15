package local

import (
	"context"
	"errors"
	"fmt"
	"sync"

	events "github.com/jmeiracorbal/go-events"
)

// ErrWatchFull reports that a watcher cannot retain an incoming event.
// Overflow closes that watcher and is always reported to the publisher.
var ErrWatchFull = errors.New("local event watch queue is full")

// watchQueue implements ordered asynchronous delivery for both serializable
// envelopes and native Go values. Both subscribe to the SAME local Bus.
type watchQueue[T any] struct {
	mu     sync.Mutex
	ready  *sync.Cond
	sub    events.Subscription
	queue  []T
	head   int
	limit  int
	closed bool
	done   chan struct{}
	output chan T
}

type Watcher struct{ *watchQueue[events.Event] }

// ValueWatcher observes native Go values. They are never serialized or
// forwarded to a durable transport, and callers must treat them as immutable.
type ValueWatcher struct{ *watchQueue[any] }

func newWatchQueue[T any](initial []T, limit int) *watchQueue[T] {
	w := &watchQueue[T]{
		queue: append([]T(nil), initial...), limit: limit,
		done: make(chan struct{}), output: make(chan T),
	}
	w.ready = sync.NewCond(&w.mu)
	return w
}

func (w *watchQueue[T]) attach(ctx context.Context, sub events.Subscription) {
	w.sub = sub
	go w.deliver()
	go func() {
		select {
		case <-ctx.Done():
			_ = w.Close()
		case <-w.done:
		}
	}()
}

func (w *watchQueue[T]) enqueue(value T) error {
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
	w.queue = append(w.queue, value)
	w.ready.Signal()
	w.mu.Unlock()
	return nil
}

// Events returns the channel of queued values in their published order.
func (w *watchQueue[T]) Events() <-chan T { return w.output }

// Done closes when a watcher is cancelled, closed or overflows.
func (w *watchQueue[T]) Done() <-chan struct{} { return w.done }

// Close is idempotent and does not wait for a consumer that is not reading.
func (w *watchQueue[T]) Close() error {
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

func (w *watchQueue[T]) deliver() {
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
		value := w.queue[w.head]
		var zero T
		w.queue[w.head] = zero
		w.head++
		if w.head == len(w.queue) {
			w.queue = nil
			w.head = 0
		} else if w.head >= 64 && w.head >= len(w.queue)/2 {
			w.queue = append([]T(nil), w.queue[w.head:]...)
			w.head = 0
		}
		w.mu.Unlock()
		select {
		case w.output <- value:
		case <-w.done:
			return
		}
	}
}

func validateWatch(ctx context.Context, topic string, initialCount, limit int) error {
	if ctx == nil {
		return errors.New("watch context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if limit < 0 {
		return errors.New("watch queue limit cannot be negative")
	}
	if err := events.ValidateTopic(topic); err != nil {
		return err
	}
	if limit > 0 && initialCount > limit {
		return fmt.Errorf("%w: %d initial events exceed limit %d", ErrWatchFull, initialCount, limit)
	}
	return nil
}

// Watch registers a subscriber of serialized event envelopes and seeds its
// private queue with initial envelopes in their supplied order.
func (b *Bus) Watch(ctx context.Context, topic string, initial []events.Event, limit int) (*Watcher, error) {
	if err := validateWatch(ctx, topic, len(initial), limit); err != nil {
		return nil, err
	}
	seed := make([]events.Event, len(initial))
	for i, event := range initial {
		if err := event.Validate(); err != nil {
			return nil, fmt.Errorf("initial event %d: %w", i, err)
		}
		if event.Topic != topic {
			return nil, fmt.Errorf("initial event %d has topic %q, expected %q", i, event.Topic, topic)
		}
		seed[i] = event.Clone()
	}
	w := &Watcher{newWatchQueue(seed, limit)}
	sub, err := b.Subscribe(ctx, topic, func(_ context.Context, event events.Event) error {
		return w.enqueue(event.Clone())
	})
	if err != nil {
		return nil, err
	}
	w.attach(ctx, sub)
	return w, nil
}

// WatchValues registers a native-value watcher on the existing local Bus.
// filter is applied before queuing; nil accepts all values. Initial values
// precede live ones. The caller must hold its own state lock while acquiring
// a state snapshot and subscribing, if an atomic transition is required.
//
// A full queue closes this watcher, returning ErrWatchFull to PublishValue.
// limit=0 selects an unbounded queue; no event is silently discarded.
func (b *Bus) WatchValues(ctx context.Context, topic string, filter func(any) bool, initial []any, limit int) (*ValueWatcher, error) {
	if err := validateWatch(ctx, topic, len(initial), limit); err != nil {
		return nil, err
	}
	for i, value := range initial {
		if value == nil {
			return nil, fmt.Errorf("initial local value %d is nil", i)
		}
		if filter != nil && !filter(value) {
			return nil, fmt.Errorf("initial local value %d does not match filter", i)
		}
	}
	w := &ValueWatcher{newWatchQueue(initial, limit)}
	sub, err := b.SubscribeValue(ctx, topic, func(_ context.Context, value any) error {
		if filter != nil && !filter(value) {
			return nil
		}
		return w.enqueue(value)
	})
	if err != nil {
		return nil, err
	}
	w.attach(ctx, sub)
	return w, nil
}
