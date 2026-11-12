// Package local implements synchronous, in-process fan-out. It does not
// persist events or retry handlers; Publish returns their errors directly.
package local

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	events "github.com/jmeiracorbal/go-events"
)

type Bus struct {
	mu     sync.RWMutex
	next   uint64
	subs   map[uint64]*subscription
	closed bool
}

type subscription struct {
	bus     *Bus
	id      uint64
	topic   string
	handler events.Handler
	done    chan struct{}
	closed  atomic.Bool
}

var (
	_ events.Publisher    = (*Bus)(nil)
	_ events.Subscriber   = (*Bus)(nil)
	_ events.Subscription = (*subscription)(nil)
)

func New() *Bus {
	return &Bus{subs: make(map[uint64]*subscription)}
}

// Publish invokes each matching handler in registration order. Handlers may
// publish recursively or unsubscribe without holding the bus lock.
func (b *Bus) Publish(ctx context.Context, event events.Event) error {
	if ctx == nil {
		return errors.New("publish context is required")
	}
	if err := event.Validate(); err != nil {
		return err
	}
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return events.ErrClosed
	}
	subs := make([]*subscription, 0, len(b.subs))
	for _, s := range b.subs {
		if s.topic == event.Topic {
			subs = append(subs, s)
		}
	}
	b.mu.RUnlock()
	sort.Slice(subs, func(i, j int) bool { return subs[i].id < subs[j].id })

	var errs []error
	for _, s := range subs {
		if err := ctx.Err(); err != nil {
			return errors.Join(append(errs, err)...)
		}
		if s.closed.Load() {
			continue
		}
		if err := s.handler(ctx, event.Clone()); err != nil {
			errs = append(errs, fmt.Errorf("subscriber %d: %w", s.id, err))
		}
	}
	return errors.Join(errs...)
}

func (b *Bus) Subscribe(ctx context.Context, topic string, handler events.Handler) (events.Subscription, error) {
	if ctx == nil {
		return nil, errors.New("subscription context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := events.ValidateTopic(topic); err != nil {
		return nil, err
	}
	if handler == nil {
		return nil, errors.New("subscriber handler is required")
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, events.ErrClosed
	}
	b.next++
	s := &subscription{bus: b, id: b.next, topic: topic, handler: handler, done: make(chan struct{})}
	b.subs[s.id] = s
	b.mu.Unlock()

	go func() {
		select {
		case <-ctx.Done():
			_ = s.Close()
		case <-s.done:
		}
	}()
	return s, nil
}

func (s *subscription) Close() error {
	b := s.bus
	b.mu.Lock()
	defer b.mu.Unlock()
	if !s.closed.Swap(true) {
		delete(b.subs, s.id)
		close(s.done)
	}
	return nil
}

// Close prevents further publishing and subscribing. Calls already running
// may finish; no callbacks are started from subsequently published events.
func (b *Bus) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	b.closed = true
	for _, s := range b.subs {
		if !s.closed.Swap(true) {
			close(s.done)
		}
	}
	clear(b.subs)
	return nil
}
