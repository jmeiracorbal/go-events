// Package nats implements a durable go-events transport using NATS JetStream.
// Streams must be provisioned explicitly before creating an Adapter.
package nats

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	events "github.com/jmeiracorbal/go-events"
	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const messageIDHeader = "Nats-Msg-Id"

// Config identifies an existing stream. Consumer is required for subscriptions,
// and must be distinct for each independently acknowledged subscriber.
// AckWait and RetryDelay must be set when subscribing.
type Config struct {
	URL        string
	Stream     string
	Consumer   string
	AckWait    time.Duration
	RetryDelay time.Duration
	OnError    func(error)
}

type Adapter struct {
	cfg    Config
	conn   *natsio.Conn
	js     jetstream.JetStream
	mu       sync.Mutex
	subs     map[*subscription]struct{}
	starting chan struct{}
	closed   bool
	closeOnce sync.Once
	closeErr error
}

type subscription struct {
	adapter *Adapter
	consume jetstream.ConsumeContext
	done    chan struct{}
	once    sync.Once
	cleanup func() error
	closeErr error
}

var (
	_ events.Publisher    = (*Adapter)(nil)
	_ events.Subscriber   = (*Adapter)(nil)
	_ events.Subscription = (*subscription)(nil)
)

func New(ctx context.Context, cfg Config, options ...natsio.Option) (*Adapter, error) {
	if ctx == nil {
		return nil, errors.New("NATS context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if cfg.URL == "" || cfg.Stream == "" {
		return nil, errors.New("NATS URL and stream are required")
	}
	nc, err := connectContext(ctx, cfg.URL, options...)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrConnect, err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("%w: %w", ErrJetStream, err)
	}
	if _, err := js.Stream(ctx, cfg.Stream); err != nil {
		nc.Close()
		return nil, fmt.Errorf("%w %q: %w", ErrStreamLookup, cfg.Stream, err)
	}
	if err := ctx.Err(); err != nil {
		nc.Close()
		return nil, err
	}
	return &Adapter{cfg: cfg, conn: nc, js: js, subs: make(map[*subscription]struct{})}, nil
}

// Publish returns only after JetStream confirms persistence in the selected
// stream. JetStream's message ID header enables broker-side deduplication
// within the stream's configured duplicate window.
func (a *Adapter) Publish(ctx context.Context, event events.Event) error {
	if ctx == nil {
		return errors.New("publish context is required")
	}
	if err := event.Validate(); err != nil {
		return err
	}
	a.mu.Lock()
	closed := a.closed
	a.mu.Unlock()
	if closed {
		return events.ErrClosed
	}
	msg := &natsio.Msg{
		Subject: event.Topic,
		Data:    append([]byte(nil), event.Data...),
		Header:  natsio.Header{},
	}
	msg.Header.Set(messageIDHeader, event.ID)
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validatePublishSubject(event.Topic); err != nil {
		return err
	}
	ack, err := a.js.PublishMsg(ctx, msg, jetstream.WithExpectStream(a.cfg.Stream))
	if err != nil {
		return fmt.Errorf("publish event %q: %w", event.ID, err)
	}
	if ack.Stream != a.cfg.Stream {
		return fmt.Errorf("event %q was published to stream %q, expected %q", event.ID, ack.Stream, a.cfg.Stream)
	}
	return nil
}

// Subscribe creates or attaches to a durable pull consumer. A given adapter
// instance supports one active subscription, keyed by Config.Consumer.
func (a *Adapter) Subscribe(ctx context.Context, topic string, handler events.Handler) (events.Subscription, error) {
	if ctx == nil {
		return nil, errors.New("subscription context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateHistorySubject(topic); err != nil {
		return nil, err
	}
	if handler == nil {
		return nil, errors.New("subscriber handler is required")
	}
	if a.cfg.Consumer == "" || a.cfg.AckWait <= 0 || a.cfg.RetryDelay <= 0 {
		return nil, errors.New("NATS consumer, ack wait, and retry delay must be configured")
	}
	if err := a.reserveSubscription(); err != nil {
		return nil, err
	}
	reserved := true
	defer func() {
		if reserved {
			a.releaseSubscriptionReservation()
		}
	}()

	// Reuse an existing durable consumer without rewriting its delivery
	// configuration or progress. Creating a missing consumer is the normal
	// subscription lifecycle, not a compatibility path.
	cons, err := a.js.Consumer(ctx, a.cfg.Stream, a.cfg.Consumer)
	switch {
	case errors.Is(err, jetstream.ErrConsumerNotFound):
		cons, err = a.js.CreateConsumer(ctx, a.cfg.Stream, jetstream.ConsumerConfig{
			Durable:       a.cfg.Consumer,
			FilterSubject: topic,
			AckPolicy:     jetstream.AckExplicitPolicy,
			AckWait:       a.cfg.AckWait,
		})
		if err != nil {
			return nil, fmt.Errorf("create durable consumer %q: %w", a.cfg.Consumer, err)
		}
	case err != nil:
		return nil, fmt.Errorf("open durable consumer %q: %w", a.cfg.Consumer, err)
	default:
		info, err := cons.Info(ctx)
		if err != nil {
			return nil, fmt.Errorf("inspect durable consumer %q: %w", a.cfg.Consumer, err)
		}
		if info.Config.Durable != a.cfg.Consumer || info.Config.FilterSubject != topic || info.Config.AckPolicy != jetstream.AckExplicitPolicy || info.Config.DeliverSubject != "" {
			return nil, fmt.Errorf("%w: durable consumer %q", events.ErrConsumerIncompatible, a.cfg.Consumer)
		}
	}
	consume, err := cons.Consume(func(msg jetstream.Msg) {
		event := eventFromMessage(msg)
		a.processDelivery(ctx, msg, event, fmt.Sprintf("event %q", event.ID), func() error {
			return handler(ctx, event)
		})
	}, a.consumeErrorHandler())
	if err != nil {
		return nil, fmt.Errorf("consume JetStream stream %q: %w", a.cfg.Stream, err)
	}
	reserved = false
	return a.registerSubscription(ctx, consume, nil)
}

// eventFromMessage preserves the transport's opaque event envelope.
func eventFromMessage(msg jetstream.Msg) events.Event {
	return events.Event{
		ID:    msg.Headers().Get(messageIDHeader),
		Topic: msg.Subject(),
		Data:  append([]byte(nil), msg.Data()...),
	}
}

// processDelivery centralizes validation, cancellation, error reporting,
// redelivery and confirmed ACK for both normal and replay subscriptions.
func (a *Adapter) processDelivery(ctx context.Context, msg jetstream.Msg, event events.Event, description string, handle func() error) {
	if err := event.Validate(); err != nil {
		a.rejectDelivery(msg, fmt.Errorf("invalid %s: %w", description, err))
		return
	}
	if err := ctx.Err(); err != nil {
		a.rejectDelivery(msg, fmt.Errorf("%s canceled: %w", description, err))
		return
	}
	if err := handle(); err != nil {
		a.rejectDelivery(msg, fmt.Errorf("handle %s: %w", description, err))
		return
	}
	if err := msg.DoubleAck(ctx); err != nil {
		a.report(fmt.Errorf("ack %s: %w", description, err))
	}
}

func (a *Adapter) rejectDelivery(msg jetstream.Msg, cause error) {
	a.report(cause)
	if err := msg.NakWithDelay(a.cfg.RetryDelay); err != nil {
		a.report(fmt.Errorf("request redelivery: %w", err))
	}
}

// reserveSubscription is held across JetStream consumer lookup, creation and
// Consume setup. Competing subscriptions must not create consumers that
// they will not be permitted to use.
func (a *Adapter) reserveSubscription() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return events.ErrClosed
	}
	if a.starting != nil || len(a.subs) != 0 {
		return errors.New("NATS adapter already has an active subscriber")
	}
	a.starting = make(chan struct{})
	return nil
}

// releaseSubscriptionReservation runs only when setup returns early.
// The successful path transfers the reservation to registerSubscription.
func (a *Adapter) releaseSubscriptionReservation() {
	a.mu.Lock()
	close(a.starting)
	a.starting = nil
	a.mu.Unlock()
}

// registerSubscription transfers the reservation to an active consume loop.
// Only session-scoped subscriptions own a broker cleanup operation.
func (a *Adapter) registerSubscription(ctx context.Context, consume jetstream.ConsumeContext, cleanup func() error) (events.Subscription, error) {
	s := &subscription{adapter: a, consume: consume, done: make(chan struct{}), cleanup: cleanup}
	a.mu.Lock()
	if a.closed || len(a.subs) != 0 {
		closed := a.closed
		a.mu.Unlock()
		err := s.Close()
		a.releaseSubscriptionReservation()
		if closed {
			return nil, errors.Join(events.ErrClosed, err)
		}
		return nil, errors.Join(errors.New("NATS adapter already has an active subscriber"), err)
	}
	a.subs[s] = struct{}{}
	close(a.starting)
	a.starting = nil
	a.mu.Unlock()
	go func() {
		select {
		case <-ctx.Done():
			if err := s.Close(); err != nil {
				a.report(err)
			}
		case <-s.done:
		}
	}()
	return s, nil
}

// Both durable subscription modes surface errors reported by the SDK.
func (a *Adapter) consumeErrorHandler() jetstream.PullConsumeOpt {
	return jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
		a.report(fmt.Errorf("consume JetStream stream %q: %w", a.cfg.Stream, err))
	})
}

func (a *Adapter) report(err error) {
	if a.cfg.OnError != nil {
		a.cfg.OnError(err)
	}
}

func (s *subscription) Close() error {
	s.once.Do(func() {
		s.consume.Stop()
		if s.cleanup != nil {
			s.closeErr = s.cleanup()
		}
		s.adapter.mu.Lock()
		delete(s.adapter.subs, s)
		s.adapter.mu.Unlock()
		close(s.done)
	})
	return s.closeErr
}

// Close waits for concurrent closure and returns the same cleanup result to
// every caller. The broker retains durable consumers.
func (a *Adapter) Close() error {
	a.closeOnce.Do(func() {
		a.mu.Lock()
		a.closed = true
		starting := a.starting
		a.mu.Unlock()
		// Let in-flight setup release its consumer before closing the connection.
		if starting != nil {
			<-starting
		}
		a.mu.Lock()
		subs := make([]*subscription, 0, len(a.subs))
		for sub := range a.subs {
			subs = append(subs, sub)
		}
		a.mu.Unlock()
		for _, sub := range subs {
			a.closeErr = errors.Join(a.closeErr, sub.Close())
		}
		a.conn.Close()
	})
	return a.closeErr
}
