package nats

import (
	"context"
	"errors"
	"fmt"
	"strings"

	events "github.com/jmeiracorbal/go-events"
	"github.com/nats-io/nats.go/jetstream"
)

// Bound how many messages a replay subscription may buffer in the client.
// JetStream owns the durable cursor and the pending ACK state.
const replayMaxPendingMessages = 64

var _ events.ReplaySubscriber = (*Adapter)(nil)

// Replay consumes retained events strictly after the supplied stream sequence
// and continues with newly published events using a single durable JetStream
// pull consumer. An existing durable consumer resumes at its server-managed
// acknowledged progress and is never recreated, reset or reconfigured.
//
// A zero-value cursor starts at the first event still retained. A cursor from
// Read may include UpperBound; that snapshot bound does not limit Replay.
func (a *Adapter) Replay(ctx context.Context, from events.Cursor, subject, consumer string, handler events.ReplayHandler) (events.Subscription, error) {
	if ctx == nil {
		return nil, errors.New("replay context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateHistorySubject(subject); err != nil {
		return nil, err
	}
	if consumer == "" || strings.TrimSpace(consumer) != consumer || strings.ContainsAny(consumer, " \t\r\n") {
		return nil, errors.New("replay durable consumer name is required without whitespace")
	}
	if handler == nil {
		return nil, errors.New("replay handler is required")
	}
	if a.cfg.AckWait <= 0 || a.cfg.RetryDelay <= 0 {
		return nil, errors.New("NATS ack wait and retry delay must be configured for replay")
	}
	if from.Stream != "" && from.Stream != a.cfg.Stream {
		return nil, fmt.Errorf("%w: cursor stream %q does not match %q", events.ErrInvalidCursor, from.Stream, a.cfg.Stream)
	}
	if from.Stream == "" && (from.AfterSequence != 0 || from.UpperBound != 0) {
		return nil, fmt.Errorf("%w: positioned cursor must identify its stream", events.ErrInvalidCursor)
	}
	if from.UpperBound != 0 && from.AfterSequence > from.UpperBound {
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

	cons, err := a.js.Consumer(ctx, a.cfg.Stream, consumer)
	switch {
	case errors.Is(err, jetstream.ErrConsumerNotFound):
		// Only a new durable consumer uses the caller's starting cursor.
		// Existing consumers own their delivery progress on the server.
		stream, streamErr := a.historyStream(ctx)
		if streamErr != nil {
			return nil, streamErr
		}
		info, infoErr := stream.Info(ctx)
		if infoErr != nil {
			return nil, historyError("inspect stream before replay", infoErr)
		}
		if from.AfterSequence > info.State.LastSeq || from.UpperBound > info.State.LastSeq {
			return nil, fmt.Errorf("%w: cursor is beyond stream last sequence %d", events.ErrInvalidCursor, info.State.LastSeq)
		}
		if from.AfterSequence > 0 && info.State.FirstSeq > 0 && from.AfterSequence < info.State.FirstSeq-1 {
			return nil, fmt.Errorf("%w: position %d predates first retained sequence %d", events.ErrHistoryExpired, from.AfterSequence, info.State.FirstSeq)
		}
		start := uint64(1)
		if from.AfterSequence != 0 {
			start = from.AfterSequence + 1
		} else if info.State.FirstSeq != 0 {
			start = info.State.FirstSeq
		}
		cons, err = a.js.CreateConsumer(ctx, a.cfg.Stream, jetstream.ConsumerConfig{
			Durable:       consumer,
			FilterSubject: subject,
			AckPolicy:     jetstream.AckExplicitPolicy,
			AckWait:       a.cfg.AckWait,
			DeliverPolicy: jetstream.DeliverByStartSequencePolicy,
			OptStartSeq:   start,
			MaxAckPending: replayMaxPendingMessages,
		})
		if err != nil {
			return nil, historyError(fmt.Sprintf("create replay consumer %q", consumer), err)
		}
	case err != nil:
		return nil, historyError(fmt.Sprintf("open replay consumer %q", consumer), err)
	default:
		info, infoErr := cons.Info(ctx)
		if infoErr != nil {
			return nil, historyError(fmt.Sprintf("inspect replay consumer %q", consumer), infoErr)
		}
		if info.Config.FilterSubject != subject ||
			info.Config.AckPolicy != jetstream.AckExplicitPolicy ||
			info.Config.DeliverPolicy != jetstream.DeliverByStartSequencePolicy ||
			info.Config.DeliverSubject != "" {
			return nil, fmt.Errorf("%w: durable consumer %q in stream %q", events.ErrConsumerIncompatible, consumer, a.cfg.Stream)
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
	}, jetstream.PullMaxMessages(replayMaxPendingMessages), jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
		a.report(fmt.Errorf("consume replay stream %q: %w", a.cfg.Stream, err))
	}))
	if err != nil {
		return nil, historyError(fmt.Sprintf("consume replay stream %q", a.cfg.Stream), err)
	}
	reserved = false
	return a.registerSubscription(ctx, consume)
}

