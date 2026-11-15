// Package local implements synchronous, in-process fan-out. It does not
// persist events or retry handlers; publishing returns handler errors directly.
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
	handler func(context.Context, any) error
	kind    eventKind
	done    chan struct{}
	closed  atomic.Bool
}

type eventKind uint8

const (
	bytesEvent eventKind = iota
	localValue
)

var (
	_ events.Publisher    = (*Bus)(nil)
	_ events.Subscriber   = (*Bus)(nil)
	_ events.Subscription = (*subscription)(nil)
)

func New() *Bus {
	return &Bus{subs: make(map[uint64]*subscription)}
}

// Publish dispatches serializable event envelopes in registration order.
// Each matching handler receives an independent copy of Event.Data.
func (b *Bus) Publish(ctx context.Context, event events.Event) error {
	if err := event.Validate(); err != nil {
		return err
	}
	return b.publish(ctx, event.Topic, bytesEvent, event)
}

// PublishValue sends an arbitrary local-only Go value through the same bus.
// The value must remain immutable while subscribers process it. It is never
// serialized or forwarded to a durable transport.
func (b *Bus) PublishValue(ctx context.Context, topic string, value any) error {
	if err := events.ValidateTopic(topic); err != nil {
		return err
	}
	if value == nil {
		return errors.New("local event value is required")
	}
	return b.publish(ctx, topic, localValue, value)
}

func (b *Bus) publish(ctx context.Context, topic string, kind eventKind, value any) error {
	if ctx == nil {
		return errors.New("publish context is required")
	}
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return events.ErrClosed
	}
	subs := make([]*subscription, 0, len(b.subs))
	for _, s := range b.subs {
		if s.topic == topic && s.kind == kind {
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
		delivered := value
		if kind == bytesEvent {
			delivered = value.(events.Event).Clone()
		}
		if err := s.handler(ctx, delivered); err != nil {
			errs = append(errs, fmt.Errorf("subscriber %d: %w", s.id, err))
		}
	}
	return errors.Join(errs...)
}

func (b *Bus) Subscribe(ctx context.Context, topic string, handler events.Handler) (events.Subscription, error) {
	if handler == nil {
		return nil, errors.New("subscriber handler is required")
	}
	return b.subscribe(ctx, topic, bytesEvent, func(ctx context.Context, value any) error {
		return handler(ctx, value.(events.Event))
	})
}

// SubscribeValue registers a handler for in-process Go values, without
// requiring a serialization format or assigning event IDs. Only PublishValue
// dispatches to this handler; ordinary Publish subscribers remain independent.
func (b *Bus) SubscribeValue(ctx context.Context, topic string, handler func(context.Context, any) error) (events.Subscription, error) {
	return b.subscribe(ctx, topic, localValue, handler)
}

func (b *Bus) subscribe(ctx context.Context, topic string, kind eventKind, handler func(context.Context, any) error) (events.Subscription, error) {
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
	s := &subscription{bus: b, id: b.next, topic: topic, handler: handler, kind: kind, done: make(chan struct{})}
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
// may finish; no callbacks start from subsequently published events.
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
