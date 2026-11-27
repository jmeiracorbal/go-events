package events

import (
	"context"
	"errors"
	"time"
)

const MaxHistoryPageSize = 256

var (
	ErrEventNotFound      = errors.New("stored event not found")
	ErrInvalidCursor      = errors.New("invalid history cursor")
	ErrHistoryExpired     = errors.New("history cursor is older than retained events")
	ErrHistoryUnavailable = errors.New("event history is unavailable")
	ErrConsumerIncompatible = errors.New("incompatible durable consumer configuration")
)

// StoredEvent preserves the original opaque event and its server-side
// position. Stream sequence is independent of the logical event ID.
type StoredEvent struct {
	Event    Event
	Stream   string
	Sequence uint64
	StoredAt time.Time

	// DeliveryAttempt is set only for Replay deliveries; Get and Read return
	// zero because historical reads do not have a delivery attempt.
	DeliveryAttempt uint64
}

// Cursor identifies a stable, stream-scoped historical window. AfterSequence
// is exclusive. UpperBound is inclusive and fixed on the first Read request.
// A zero-valued cursor starts at the earliest currently retained message.
// Read returns a cursor bound to its normalized subject (including ">" for
// all subjects) and stream creation time. A nonempty Subject identifies a
// captured snapshot even when UpperBound is zero. Pass the returned cursor
// unchanged to subsequent reads or replay of the same subject.
type Cursor struct {
	Stream        string
	AfterSequence uint64
	UpperBound    uint64
	Subject       string
	StreamCreated time.Time
}

// ReadQuery limits the history returned by one call. Subject can be a NATS
// subject pattern. An empty subject reads all subjects in the selected stream.
type ReadQuery struct {
	Subject string
	Cursor  Cursor
	Limit   int
}

// EventPage returns events in ascending stream-sequence order. Next can be
// used only with the same stream and subject. HasMore applies to the captured historical
// window and does not predict future publishes.
type EventPage struct {
	Events  []StoredEvent
	Next    Cursor
	HasMore bool
}

// HistoryReader is an optional capability; transports without historical
// storage need not implement it.
type HistoryReader interface {
	Get(context.Context, uint64) (StoredEvent, error)
	Read(context.Context, ReadQuery) (EventPage, error)
}

// ReplayHandler handles one retained or newly published event. Returning nil
// indicates its effects have been committed and allows the transport to ACK.
// On error the durable transport requests redelivery; handlers must be
// idempotent because delivery is at least once.
type ReplayHandler func(context.Context, StoredEvent) error

// ReplaySubscriber is an optional capability that resumes from a stream cursor
// and continues with live events using the same durable broker consumer.
// A previously created durable consumer resumes from broker-acknowledged
// progress, rather than resetting its position from the supplied cursor.
// UpperBound, used by Read for snapshot pagination, does not stop Replay.
type ReplaySubscriber interface {
	Replay(context.Context, Cursor, string, string, ReplayHandler) (Subscription, error)
}

// EphemeralReplaySubscriber is an optional capability for session-scoped
// replay. Every invocation starts from the supplied cursor using a new
// temporary broker consumer. It never resumes another session's ACK progress.
// The subscription is removed when closed; inactivityTimeout bounds abandoned
// consumer lifetime when a session cannot close cleanly. As with Replay,
// delivery is at least once and UpperBound does not stop live delivery.
type EphemeralReplaySubscriber interface {
	ReplayEphemeral(context.Context, Cursor, string, time.Duration, ReplayHandler) (Subscription, error)
}
