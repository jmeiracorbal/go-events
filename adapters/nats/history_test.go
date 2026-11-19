package nats

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	events "github.com/jmeiracorbal/go-events"
	"github.com/nats-io/nats.go/jetstream"
)

func TestHistoryGetBySequenceAndRestart(t *testing.T) {
	url := newServer(t)
	prepareStream(t, url)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	first := newAdapter(t, url, "")
	for i := 1; i <= 3; i++ {
		if err := first.Publish(ctx, events.Event{
			ID: fmt.Sprintf("event-%d", i), Topic: "work.created",
			Data: []byte(fmt.Sprintf("payload-%d", i)),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	reader := newAdapter(t, url, "")
	msg, err := reader.Get(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	if msg.Sequence != 2 || msg.Stream != "EVENTS" || msg.Event.ID != "event-2" ||
		msg.Event.Topic != "work.created" || string(msg.Event.Data) != "payload-2" || msg.StoredAt.IsZero() {
		t.Fatalf("unexpected stored event: %+v", msg)
	}
	if _, err := reader.Get(ctx, 999); !errors.Is(err, events.ErrEventNotFound) {
		t.Fatalf("expected event not found, got %v", err)
	}
	if _, err := reader.Get(ctx, 0); !errors.Is(err, events.ErrInvalidCursor) {
		t.Fatalf("expected invalid sequence, got %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.Get(ctx, 1); !errors.Is(err, events.ErrClosed) {
		t.Fatalf("expected closed adapter, got %v", err)
	}
}

func TestHistoryReadPagesFilterAndSnapshot(t *testing.T) {
	url := newServer(t)
	prepareStream(t, url)
	a := newAdapter(t, url, "")
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	for i := 1; i <= 6; i++ {
		topic := "work.updated"
		if i%2 != 0 {
			topic = "work.created"
		}
		if err := a.Publish(ctx, events.Event{ID: fmt.Sprintf("event-%d", i), Topic: topic}); err != nil {
			t.Fatal(err)
		}
	}
	page1, err := a.Read(ctx, events.ReadQuery{Subject: "work.created", Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if !page1.HasMore || page1.Next.Stream != "EVENTS" || page1.Next.UpperBound != 6 ||
		page1.Next.AfterSequence != 3 || len(page1.Events) != 2 ||
		page1.Events[0].Sequence != 1 || page1.Events[1].Sequence != 3 {
		t.Fatalf("unexpected first page: %+v", page1)
	}
	if err := a.Publish(ctx, events.Event{ID: "event-7", Topic: "work.created"}); err != nil {
		t.Fatal(err)
	}
	page2, err := a.Read(ctx, events.ReadQuery{Subject: "work.created", Limit: 2, Cursor: page1.Next})
	if err != nil {
		t.Fatal(err)
	}
	if page2.HasMore || len(page2.Events) != 1 || page2.Events[0].Sequence != 5 ||
		page2.Next.AfterSequence != 6 || page2.Next.UpperBound != 6 {
		t.Fatalf("unexpected second page: %+v", page2)
	}
	fresh, err := a.Read(ctx, events.ReadQuery{Subject: "work.created", Limit: 4})
	if err != nil {
		t.Fatal(err)
	}
	if fresh.HasMore || len(fresh.Events) != 4 || fresh.Events[3].Sequence != 7 {
		t.Fatalf("fresh history did not include newly published event: %+v", fresh)
	}
	missing, err := a.Read(ctx, events.ReadQuery{Subject: "work.unknown", Limit: 3})
	if err != nil || missing.HasMore || len(missing.Events) != 0 {
		t.Fatalf("expected empty filtered page, got %+v, %v", missing, err)
	}
}

func TestHistoryReadWildcardAndDeletedGaps(t *testing.T) {
	url := newServer(t)
	prepareStream(t, url)
	a := newAdapter(t, url, "")
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	for i := 1; i <= 4; i++ {
		if err := a.Publish(ctx, events.Event{ID: fmt.Sprintf("e-%d", i), Topic: "work.created"}); err != nil {
			t.Fatal(err)
		}
	}
	stream, err := a.js.Stream(ctx, "EVENTS")
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.DeleteMsg(ctx, 2); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name    string
		subject string
	}{
		{"all subjects", ""},
		{"wildcard", "work.>"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			page, err := a.Read(ctx, events.ReadQuery{Subject: tc.subject, Limit: 10})
			if err != nil {
				t.Fatal(err)
			}
			if page.HasMore || len(page.Events) != 3 ||
				page.Events[0].Sequence != 1 || page.Events[1].Sequence != 3 || page.Events[2].Sequence != 4 ||
				page.Next.AfterSequence != 4 {
				t.Fatalf("unexpected gap handling: %+v", page)
			}
		})
	}
	if _, err := a.Get(ctx, 2); !errors.Is(err, events.ErrEventNotFound) {
		t.Fatalf("expected deleted event to be absent, got %v", err)
	}
}

func TestHistoryReadRetentionExpiresSavedCursor(t *testing.T) {
	url := newServer(t)
	prepareStream(t, url)
	a := newAdapter(t, url, "")
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	for i := 1; i <= 5; i++ {
		if err := a.Publish(ctx, events.Event{ID: fmt.Sprintf("event-%d", i), Topic: "work.created"}); err != nil {
			t.Fatal(err)
		}
	}
	first, err := a.Read(ctx, events.ReadQuery{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !first.HasMore || first.Next.AfterSequence != 1 {
		t.Fatalf("unexpected initial cursor: %+v", first)
	}
	stream, err := a.js.Stream(ctx, "EVENTS")
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Purge(ctx, jetstream.WithPurgeSequence(4)); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Read(ctx, events.ReadQuery{Limit: 2, Cursor: first.Next}); !errors.Is(err, events.ErrHistoryExpired) {
		t.Fatalf("expected expired saved cursor, got %v", err)
	}
	fresh, err := a.Read(ctx, events.ReadQuery{Limit: 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(fresh.Events) != 2 || fresh.Events[0].Sequence != 4 || fresh.Events[1].Sequence != 5 {
		t.Fatalf("fresh cursor should start at earliest retained event: %+v", fresh)
	}
}

func TestHistoryReadValidationAndEmptyStream(t *testing.T) {
	url := newServer(t)
	prepareStream(t, url)
	a := newAdapter(t, url, "")
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	empty, err := a.Read(ctx, events.ReadQuery{Limit: 1})
	if err != nil || empty.HasMore || len(empty.Events) != 0 || empty.Next.Stream != "EVENTS" {
		t.Fatalf("expected empty history: %+v, %v", empty, err)
	}
	if err := a.Publish(ctx, events.Event{ID: "e-1", Topic: "work.created"}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name  string
		query events.ReadQuery
		want  error
	}{
		{"unscoped position", events.ReadQuery{Limit: 1, Cursor: events.Cursor{AfterSequence: 1}}, events.ErrInvalidCursor},
		{"wrong stream", events.ReadQuery{Limit: 1, Cursor: events.Cursor{Stream: "OTHER"}}, events.ErrInvalidCursor},
		{"future bound", events.ReadQuery{Limit: 1, Cursor: events.Cursor{Stream: "EVENTS", UpperBound: 2}}, events.ErrInvalidCursor},
		{"future position", events.ReadQuery{Limit: 1, Cursor: events.Cursor{Stream: "EVENTS", AfterSequence: 2}}, events.ErrInvalidCursor},
		{"invalid subject", events.ReadQuery{Limit: 1, Subject: "work.>.created"}, nil},
		{"empty token", events.ReadQuery{Limit: 1, Subject: "work..created"}, nil},
		{"zero limit", events.ReadQuery{Limit: 0}, nil},
		{"unbounded limit", events.ReadQuery{Limit: events.MaxHistoryPageSize + 1}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := a.Read(ctx, tc.query)
			if err == nil || (tc.want != nil && !errors.Is(err, tc.want)) {
				t.Fatalf("expected error %v, got %v", tc.want, err)
			}
		})
	}
	canceled, stop := context.WithCancel(ctx)
	stop()
	if _, err := a.Read(canceled, events.ReadQuery{Limit: 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected canceled context, got %v", err)
	}
	if _, err := a.Get(canceled, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected canceled context for Get, got %v", err)
	}
	if _, err := a.Read(nil, events.ReadQuery{Limit: 1}); err == nil {
		t.Fatal("expected nil-context error")
	}
}
