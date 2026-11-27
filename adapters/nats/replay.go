package nats

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	events "github.com/jmeiracorbal/go-events"
	"github.com/nats-io/nats.go/jetstream"
)

// Bound how many messages a replay subscription may buffer in the client.
// JetStream owns the durable cursor and the pending ACK state.
const replayMaxPendingMessages = 64

var (
	_ events.ReplaySubscriber = (*Adapter)(nil)
	_ events.EphemeralReplaySubscriber = (*Adapter)(nil)
)

// Replay consumes retained events strictly after the supplied stream sequence
// and continues with newly published events using a single durable JetStream
// pull consumer. An existing durable consumer resumes at its server-managed
// acknowledged progress and is never recreated, reset or reconfigured.
//
// A zero-value cursor starts at the first event still retained. A cursor from
// Read may include UpperBound; that snapshot bound does not limit Replay.
func (a *Adapter) Replay(ctx context.Context, from events.Cursor, subject, consumer string, handler events.ReplayHandler) (events.Subscription, error) {
	if consumer == "" || strings.TrimSpace(consumer) != consumer || strings.ContainsAny(consumer, " \t\r\n") {
		return nil, errors.New("replay durable consumer name is required without whitespace")
	}
	return a.replay(ctx, from, subject, consumer, 0, handler)
}

// ReplayEphemeral uses a new temporary consumer for each session. The broker
// removes abandoned consumers after inactivityTimeout; normal closure deletes
// the consumer explicitly. Limits retention is required so ACK and deletion
// cannot consume the underlying historical data.
func (a *Adapter) ReplayEphemeral(ctx context.Context, from events.Cursor, subject string, inactivityTimeout time.Duration, handler events.ReplayHandler) (events.Subscription, error) {
	if inactivityTimeout <= 0 {
		return nil, errors.New("replay inactivity timeout must be positive")
	}
	return a.replay(ctx, from, subject, "", inactivityTimeout, handler)
}

func (a *Adapter) replay(ctx context.Context, from events.Cursor, subject, durable string, inactivityTimeout time.Duration, handler events.ReplayHandler) (events.Subscription, error) {
	if ctx == nil {
		return nil, errors.New("replay context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateHistorySubject(subject); err != nil {
		return nil, err
	}
	if handler == nil {
		return nil, errors.New("replay handler is required")
	}
	if a.cfg.AckWait <= 0 || a.cfg.RetryDelay <= 0 {
		return nil, errors.New("NATS ack wait and retry delay must be configured for replay")
	}
	if err := validateCursor(from, a.cfg.Stream, subject); err != nil {
		return nil, err
	}
	if (from.UpperBound != 0 || from.Subject != "") && from.AfterSequence > from.UpperBound {
		return nil, fmt.Errorf("%w: position %d exceeds snapshot upper bound %d", events.ErrInvalidCursor, from.AfterSequence, from.UpperBound)
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

	var cons jetstream.Consumer
	var err error
	var cleanup func() error
	if durable == "" {
		cons, err = a.createReplayConsumer(ctx, from, subject, "", inactivityTimeout)
		if err != nil {
			return nil, err
		}
		name := cons.CachedInfo().Name
		cleanup = func() error { return a.deleteReplayConsumer(name) }
	} else {
		cons, err = a.durableReplayConsumer(ctx, from, subject, durable)
		if err != nil {
			return nil, err
		}
	}

	consume, err := cons.Consume(func(msg jetstream.Msg) {
		meta, err := msg.Metadata()
		if err != nil {
			a.rejectDelivery(msg, fmt.Errorf("decode replay metadata: %w", err))
			return
		}
		if meta.Stream != a.cfg.Stream {
			a.rejectDelivery(msg, fmt.Errorf("unexpected replay stream %q, expected %q", meta.Stream, a.cfg.Stream))
			return
		}
		stored := events.StoredEvent{
			Event:           eventFromMessage(msg),
			Stream:          meta.Stream,
			Sequence:        meta.Sequence.Stream,
			StoredAt:        meta.Timestamp,
			DeliveryAttempt: meta.NumDelivered,
		}
		a.processDelivery(ctx, msg, stored.Event,
			fmt.Sprintf("replay event %q at sequence %d", stored.Event.ID, stored.Sequence),
			func() error { return handler(ctx, stored) })
	}, jetstream.PullMaxMessages(replayMaxPendingMessages), a.consumeErrorHandler())
	if err != nil {
		if cleanup != nil {
			err = errors.Join(err, cleanup())
		}
		return nil, historyError(fmt.Sprintf("consume replay stream %q", a.cfg.Stream), err)
	}
	reserved = false
	return a.registerSubscription(ctx, consume, cleanup)
}

func (a *Adapter) durableReplayConsumer(ctx context.Context, from events.Cursor, subject, name string) (jetstream.Consumer, error) {
	cons, err := a.js.Consumer(ctx, a.cfg.Stream, name)
	switch {
	case errors.Is(err, jetstream.ErrConsumerNotFound):
		return a.createReplayConsumer(ctx, from, subject, name, 0)
	case err != nil:
		return nil, historyError(fmt.Sprintf("open replay consumer %q", name), err)
	default:
		info, err := cons.Info(ctx)
		if err != nil {
			return nil, historyError(fmt.Sprintf("inspect replay consumer %q", name), err)
		}
		if info.Config.FilterSubject != subject ||
			info.Config.AckPolicy != jetstream.AckExplicitPolicy ||
			info.Config.DeliverPolicy != jetstream.DeliverByStartSequencePolicy ||
			info.Config.DeliverSubject != "" || info.Config.Durable != name {
			return nil, fmt.Errorf("%w: durable consumer %q in stream %q", events.ErrConsumerIncompatible, name, a.cfg.Stream)
		}
		return cons, nil
	}
}

// Only new consumers use the supplied cursor. Both lifecycles share the
// retained sequence validation and delivery configuration.
func (a *Adapter) createReplayConsumer(ctx context.Context, from events.Cursor, subject, durable string, inactivityTimeout time.Duration) (jetstream.Consumer, error) {
	stream, err := a.historyStream(ctx)
	if err != nil {
		return nil, err
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return nil, historyError("inspect stream before replay", err)
	}
	if err := validateStreamIdentity(from, info); err != nil {
		return nil, err
	}
	if durable == "" && info.Config.Retention != jetstream.LimitsPolicy {
		return nil, errors.New("ephemeral replay requires limits retention")
	}
	if from.AfterSequence > info.State.LastSeq || from.UpperBound > info.State.LastSeq {
		return nil, fmt.Errorf("%w: cursor is beyond stream last sequence %d", events.ErrInvalidCursor, info.State.LastSeq)
	}
	if err := validateRetention(from, info); err != nil {
		return nil, err
	}
	start := uint64(1)
	if from.AfterSequence != 0 || from.Subject != "" {
		start = from.AfterSequence + 1
	} else if info.State.FirstSeq != 0 {
		start = info.State.FirstSeq
	}
	cons, err := a.js.CreateConsumer(ctx, a.cfg.Stream, jetstream.ConsumerConfig{
		Durable: durable,
		FilterSubject: subject,
		AckPolicy: jetstream.AckExplicitPolicy,
		AckWait: a.cfg.AckWait,
		DeliverPolicy: jetstream.DeliverByStartSequencePolicy,
		OptStartSeq: start,
		MaxAckPending: replayMaxPendingMessages,
		InactiveThreshold: inactivityTimeout,
	})
	if err != nil {
		return nil, historyError("create replay consumer", err)
	}
	return cons, nil
}

func (a *Adapter) deleteReplayConsumer(name string) error {
	// Closing may be triggered by cancellation of the session context.
	// Cleanup therefore has an independent, bounded deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := a.js.DeleteConsumer(ctx, a.cfg.Stream, name)
	if err != nil && !errors.Is(err, jetstream.ErrConsumerNotFound) {
		return historyError(fmt.Sprintf("delete ephemeral replay consumer %q", name), err)
	}
	return nil
}
