package nats

import (
	"context"
	"errors"
	"fmt"
	"strings"

	events "github.com/jmeiracorbal/go-events"
	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var _ events.HistoryReader = (*Adapter)(nil)

// Get returns a retained event at an exact stream sequence. Logical event IDs
// are not a JetStream index and are intentionally not used for lookup.
func (a *Adapter) Get(ctx context.Context, sequence uint64) (events.StoredEvent, error) {
	if sequence == 0 {
		return events.StoredEvent{}, fmt.Errorf("%w: sequence must be positive", events.ErrInvalidCursor)
	}
	stream, err := a.historyStream(ctx)
	if err != nil {
		return events.StoredEvent{}, err
	}
	msg, err := stream.GetMsg(ctx, sequence)
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return events.StoredEvent{}, fmt.Errorf("stream %q sequence %d: %w", a.cfg.Stream, sequence, events.ErrEventNotFound)
	}
	if err != nil {
		return events.StoredEvent{}, historyError("read stored event", err)
	}
	return a.storedEvent(msg), nil
}

// Read returns one bounded page of retained events without creating or
// modifying a JetStream consumer. A server-side next-by-subject lookup handles
// deleted sequence gaps and wildcard subjects without an unbounded scan.
func (a *Adapter) Read(ctx context.Context, query events.ReadQuery) (events.EventPage, error) {
	if query.Limit <= 0 || query.Limit > events.MaxHistoryPageSize {
		return events.EventPage{}, fmt.Errorf("history page limit must be between 1 and %d", events.MaxHistoryPageSize)
	}
	if query.Subject != "" {
		if err := validateHistorySubject(query.Subject); err != nil {
			return events.EventPage{}, err
		}
	}
	if query.Cursor.Stream == "" && (query.Cursor.AfterSequence != 0 || query.Cursor.UpperBound != 0) {
		return events.EventPage{}, fmt.Errorf("%w: positioned cursor must identify its stream", events.ErrInvalidCursor)
	}
	if query.Cursor.Stream != "" && query.Cursor.Stream != a.cfg.Stream {
		return events.EventPage{}, fmt.Errorf("%w: cursor stream %q does not match %q", events.ErrInvalidCursor, query.Cursor.Stream, a.cfg.Stream)
	}
	stream, err := a.historyStream(ctx)
	if err != nil {
		return events.EventPage{}, err
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return events.EventPage{}, historyError("inspect stream history", err)
	}

	bound := query.Cursor.UpperBound
	if bound == 0 {
		bound = info.State.LastSeq
	} else if bound > info.State.LastSeq {
		return events.EventPage{}, fmt.Errorf("%w: upper bound %d exceeds current stream sequence %d", events.ErrInvalidCursor, bound, info.State.LastSeq)
	}
	if query.Cursor.AfterSequence > bound {
		return events.EventPage{}, fmt.Errorf("%w: position %d exceeds upper bound %d", events.ErrInvalidCursor, query.Cursor.AfterSequence, bound)
	}
	if query.Cursor.AfterSequence != 0 && info.State.FirstSeq > 0 && query.Cursor.AfterSequence < info.State.FirstSeq-1 {
		return events.EventPage{}, fmt.Errorf("%w: position %d predates first retained sequence %d", events.ErrHistoryExpired, query.Cursor.AfterSequence, info.State.FirstSeq)
	}

	cursor := events.Cursor{Stream: a.cfg.Stream, AfterSequence: bound, UpperBound: bound}
	page := events.EventPage{Events: make([]events.StoredEvent, 0, query.Limit), Next: cursor}
	if info.State.Msgs == 0 || query.Cursor.AfterSequence == bound {
		return page, nil
	}

	next := query.Cursor.AfterSequence + 1
	if query.Cursor.AfterSequence == 0 && next < info.State.FirstSeq {
		next = info.State.FirstSeq
	}
	subject := query.Subject
	if subject == "" {
		subject = ">"
	}
	for next <= bound {
		if err := ctx.Err(); err != nil {
			return events.EventPage{}, err
		}
		msg, err := stream.GetMsg(ctx, next, jetstream.WithGetMsgSubject(subject))
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			current, infoErr := stream.Info(ctx)
			if infoErr != nil {
				return events.EventPage{}, historyError("check history retention", infoErr)
			}
			if current.State.FirstSeq > next && next <= bound {
				return events.EventPage{}, fmt.Errorf("%w: sequence %d predates first retained sequence %d", events.ErrHistoryExpired, next, current.State.FirstSeq)
			}
			break
		}
		if err != nil {
			return events.EventPage{}, historyError("scan stream history", err)
		}
		if msg.Sequence > bound {
			break
		}
		if msg.Sequence < next {
			return events.EventPage{}, fmt.Errorf("invalid JetStream history sequence %d (expected at least %d)", msg.Sequence, next)
		}
		if len(page.Events) == query.Limit {
			page.HasMore = true
			page.Next.AfterSequence = next - 1
			return page, nil
		}
		page.Events = append(page.Events, a.storedEvent(msg))
		next = msg.Sequence + 1
	}
	return page, nil
}

func (a *Adapter) historyStream(ctx context.Context) (jetstream.Stream, error) {
	if ctx == nil {
		return nil, errors.New("history context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	a.mu.Lock()
	closed := a.closed
	a.mu.Unlock()
	if closed {
		return nil, events.ErrClosed
	}
	stream, err := a.js.Stream(ctx, a.cfg.Stream)
	if err != nil {
		return nil, historyError("open stream history", err)
	}
	return stream, nil
}

func (a *Adapter) storedEvent(msg *jetstream.RawStreamMsg) events.StoredEvent {
	return events.StoredEvent{
		Event: events.Event{
			ID:    msg.Header.Get(messageIDHeader),
			Topic: msg.Subject,
			Data:  append([]byte(nil), msg.Data...),
		},
		Stream:   a.cfg.Stream,
		Sequence: msg.Sequence,
		StoredAt: msg.Time,
	}
}

// NATS subjects consist of dot-separated tokens; wildcards must occupy whole
// tokens and '>' can only be the final token.
func validateHistorySubject(subject string) error {
	if err := events.ValidateTopic(subject); err != nil {
		return err
	}
	tokens := strings.Split(subject, ".")
	for i, token := range tokens {
		if token == "" || (strings.ContainsAny(token, "*>") && token != "*" && token != ">") || (token == ">" && i != len(tokens)-1) {
			return fmt.Errorf("invalid history subject %q", subject)
		}
	}
	return nil
}

func historyError(op string, err error) error {
	if errors.Is(err, natsio.ErrConnectionClosed) || errors.Is(err, natsio.ErrNoServers) {
		return fmt.Errorf("%s: %w: %w", op, events.ErrHistoryUnavailable, err)
	}
	return fmt.Errorf("%s: %w", op, err)
}
