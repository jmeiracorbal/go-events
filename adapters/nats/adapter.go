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
	mu     sync.Mutex
	subs   map[*subscription]struct{}
	closed bool
}

type subscription struct {
	adapter *Adapter
	consume jetstream.ConsumeContext
	done    chan struct{}
	once    sync.Once
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
	nc, err := natsio.Connect(cfg.URL, options...)
	if err != nil {
		return nil, fmt.Errorf("connect NATS: %w", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("initialize JetStream: %w", err)
	}
	if _, err := js.Stream(ctx, cfg.Stream); err != nil {
		nc.Close()
		return nil, fmt.Errorf("find JetStream stream %q: %w", cfg.Stream, err)
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
	ack, err := a.js.PublishMsg(ctx, msg)
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
	if err := events.ValidateTopic(topic); err != nil {
		return nil, err
	}
	if handler == nil {
		return nil, errors.New("subscriber handler is required")
	}
	if a.cfg.Consumer == "" || a.cfg.AckWait <= 0 || a.cfg.RetryDelay <= 0 {
		return nil, errors.New("NATS consumer, ack wait, and retry delay must be configured")
	}
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return nil, events.ErrClosed
	}
	if len(a.subs) != 0 {
		a.mu.Unlock()
		return nil, errors.New("NATS adapter already has an active subscriber")
	}
	a.mu.Unlock()

	cons, err := a.js.CreateConsumer(ctx, a.cfg.Stream, jetstream.ConsumerConfig{
		Durable:       a.cfg.Consumer,
		FilterSubject: topic,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       a.cfg.AckWait,
	})
	if err != nil {
		return nil, fmt.Errorf("create durable consumer %q: %w", a.cfg.Consumer, err)
	}
	consume, err := cons.Consume(func(msg jetstream.Msg) {
		event := events.Event{
			ID:    msg.Headers().Get(messageIDHeader),
			Topic: msg.Subject(),
			Data:  append([]byte(nil), msg.Data()...),
		}
		if err := event.Validate(); err != nil {
			a.report(fmt.Errorf("invalid event delivery: %w", err))
			if err := msg.NakWithDelay(a.cfg.RetryDelay); err != nil {
				a.report(fmt.Errorf("reject invalid delivery: %w", err))
			}
			return
		}
		if err := ctx.Err(); err != nil {
			if nakErr := msg.NakWithDelay(a.cfg.RetryDelay); nakErr != nil {
				a.report(fmt.Errorf("reject canceled delivery: %w", nakErr))
			}
			return
		}
		if err := handler(ctx, event); err != nil {
			a.report(fmt.Errorf("handle event %q: %w", event.ID, err))
			if nakErr := msg.NakWithDelay(a.cfg.RetryDelay); nakErr != nil {
				a.report(fmt.Errorf("request redelivery of event %q: %w", event.ID, nakErr))
			}
			return
		}
		if err := msg.DoubleAck(ctx); err != nil {
			a.report(fmt.Errorf("ack event %q: %w", event.ID, err))
		}
	})
	if err != nil {
		return nil, fmt.Errorf("consume JetStream stream %q: %w", a.cfg.Stream, err)
	}
	s := &subscription{adapter: a, consume: consume, done: make(chan struct{})}
	a.mu.Lock()
	if a.closed || len(a.subs) != 0 {
		a.mu.Unlock()
		consume.Stop()
		return nil, events.ErrClosed
	}
	a.subs[s] = struct{}{}
	a.mu.Unlock()
	go func() {
		select {
		case <-ctx.Done():
			_ = s.Close()
		case <-s.done:
		}
	}()
	return s, nil
}

func (a *Adapter) report(err error) {
	if a.cfg.OnError != nil {
		a.cfg.OnError(err)
	}
}

func (s *subscription) Close() error {
	s.once.Do(func() {
		close(s.done)
		s.consume.Stop()
		s.adapter.mu.Lock()
		delete(s.adapter.subs, s)
		s.adapter.mu.Unlock()
	})
	return nil
}

func (a *Adapter) Close() error {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return nil
	}
	a.closed = true
	subs := make([]*subscription, 0, len(a.subs))
	for sub := range a.subs {
		subs = append(subs, sub)
	}
	a.mu.Unlock()
	for _, sub := range subs {
		_ = sub.Close()
	}
	a.conn.Close()
	return nil
}
