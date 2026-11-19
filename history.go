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
)

// StoredEvent preserves the original opaque event and its server-side
// position. Stream sequence is independent of the logical event ID.
type StoredEvent struct {
	Event    Event
	Stream   string
	Sequence uint64
	StoredAt time.Time
}

// Cursor identifies a stable, stream-scoped historical window. AfterSequence
// is exclusive. UpperBound is inclusive and fixed on the first Read request.
// A zero-valued cursor starts at the earliest currently retained message.
type Cursor struct {
	Stream        string
	AfterSequence uint64
	UpperBound    uint64
}

// ReadQuery limits the history returned by one call. Subject can be a NATS
// subject pattern. An empty subject reads all subjects in the selected stream.
type ReadQuery struct {
	Subject string
	Cursor  Cursor
	Limit   int
}

// EventPage returns events in ascending stream-sequence order. Next can be
// used only with the same stream. HasMore applies to the captured historical
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
