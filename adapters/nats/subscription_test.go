package nats

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	events "github.com/jmeiracorbal/go-events"
	"github.com/nats-io/nats.go/jetstream"
)

func TestSubscriptionReservationPreventsConcurrentConsumerCreation(t *testing.T) {
	url := newServer(t)
	prepareStream(t, url)
	a := newAdapter(t, url, "reserved-subscribe")
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	// An in-flight setup owns the reservation before it contacts JetStream.
	// Competing calls must fail without creating a durable consumer.
	if err := a.reserveSubscription(); err != nil {
		t.Fatal(err)
	}
	defer a.releaseSubscriptionReservation()

	const attempts = 16
	start := make(chan struct{})
	errs := make(chan error, attempts)
	var wg sync.WaitGroup
	var names []string
	for i := 0; i < attempts; i++ {
		name := fmt.Sprintf("reserved-replay-%d", i)
		if i%2 == 0 {
			names = append(names, name)
		}
		wg.Add(1)
		go func(i int, name string) {
			defer wg.Done()
			<-start
			var sub events.Subscription
			var err error
			if i%2 == 0 {
				sub, err = a.Replay(ctx, events.Cursor{}, "work.created", name,
					func(context.Context, events.StoredEvent) error { return nil })
			} else {
				sub, err = a.Subscribe(ctx, "work.created",
					func(context.Context, events.Event) error { return nil })
			}
			if sub != nil {
				_ = sub.Close()
				errs <- errors.New("competing subscription unexpectedly succeeded")
				return
			}
			errs <- err
		}(i, name)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err == nil || !strings.Contains(err.Error(), "already has an active subscriber") {
			t.Fatalf("expected reserved subscription rejection, got %v", err)
		}
	}

	names = append(names, "reserved-subscribe")
	for _, name := range names {
		_, err := a.js.Consumer(ctx, "EVENTS", name)
		if !errors.Is(err, jetstream.ErrConsumerNotFound) {
			t.Fatalf("reservation race created an unused consumer %q: %v", name, err)
		}
	}
}

func TestConcurrentSubscribeAndReplayCreateOnlyOneConsumer(t *testing.T) {
	url := newServer(t)
	prepareStream(t, url)
	a := newAdapter(t, url, "parallel-subscribe")
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	const attempts = 16
	type result struct {
		name string
		sub  events.Subscription
		err  error
	}
	start := make(chan struct{})
	results := make(chan result, attempts)
	var wg sync.WaitGroup
	var names []string
	for i := 0; i < attempts; i++ {
		name := fmt.Sprintf("parallel-replay-%d", i)
		if i%2 == 0 {
			names = append(names, name)
		}
		wg.Add(1)
		go func(i int, name string) {
			defer wg.Done()
			<-start
			if i%2 == 0 {
				sub, err := a.Replay(ctx, events.Cursor{}, "work.created", name,
					func(context.Context, events.StoredEvent) error { return nil })
				results <- result{name: name, sub: sub, err: err}
				return
			}
			sub, err := a.Subscribe(ctx, "work.created",
				func(context.Context, events.Event) error { return nil })
			results <- result{name: "parallel-subscribe", sub: sub, err: err}
		}(i, name)
	}
	close(start)
	wg.Wait()
	close(results)

	winner := ""
	var active events.Subscription
	for outcome := range results {
		if outcome.err == nil {
			if active != nil {
				_ = outcome.sub.Close()
				t.Fatalf("more than one concurrent subscription succeeded: %q and %q", winner, outcome.name)
			}
			winner, active = outcome.name, outcome.sub
		} else if !strings.Contains(outcome.err.Error(), "already has an active subscriber") {
			t.Fatalf("unexpected setup error for %q: %v", outcome.name, outcome.err)
		}
	}
	if active == nil {
		t.Fatal("no concurrent subscription succeeded")
	}
	defer active.Close()

	names = append(names, "parallel-subscribe")
	found := 0
	for _, name := range names {
		_, err := a.js.Consumer(ctx, "EVENTS", name)
		if err == nil {
			found++
			if name != winner {
				t.Fatalf("unused durable consumer %q was created, winner was %q", name, winner)
			}
		} else if !errors.Is(err, jetstream.ErrConsumerNotFound) {
			t.Fatalf("inspect consumer %q: %v", name, err)
		}
	}
	if found != 1 {
		t.Fatalf("expected exactly one JetStream durable consumer, found %d", found)
	}
}

func TestFailedReplaySetupReleasesSubscriptionReservation(t *testing.T) {
	url := newServer(t)
	prepareStream(t, url)
	a := newAdapter(t, url, "")
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	_, err := a.js.CreateConsumer(ctx, "EVENTS", jetstream.ConsumerConfig{
		Durable:       "unrelated-config",
		FilterSubject: "work.created",
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	if err != nil {
		t.Fatal(err)
	}
	handler := func(context.Context, events.StoredEvent) error { return nil }
	if _, err := a.Replay(ctx, events.Cursor{}, "work.created", "unrelated-config", handler); !errors.Is(err, events.ErrConsumerIncompatible) {
		t.Fatalf("expected incompatible durable error, got %v", err)
	}
	sub, err := a.Replay(ctx, events.Cursor{}, "work.created", "retry-after-failure", handler)
	if err != nil {
		t.Fatalf("failed setup left the adapter reserved: %v", err)
	}
	if err := sub.Close(); err != nil {
		t.Fatal(err)
	}
	sub, err = a.Replay(ctx, events.Cursor{}, "work.created", "retry-after-failure", handler)
	if err != nil {
		t.Fatalf("closing a subscription did not release the adapter: %v", err)
	}
	defer sub.Close()
}
