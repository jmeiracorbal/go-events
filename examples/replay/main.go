// Command replay reads retained events and follows new events through one
// existing JetStream stream. Configure NATS_URL, NATS_STREAM, NATS_SUBJECT
// and NATS_CONSUMER explicitly; no broker or stream is provisioned by it.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	events "github.com/jmeiracorbal/go-events"
	natstransport "github.com/jmeiracorbal/go-events/adapters/nats"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	url := os.Getenv("NATS_URL")
	stream := os.Getenv("NATS_STREAM")
	subject := os.Getenv("NATS_SUBJECT")
	consumer := os.Getenv("NATS_CONSUMER")
	if url == "" || stream == "" || subject == "" || consumer == "" {
		return errors.New("NATS_URL, NATS_STREAM, NATS_SUBJECT and NATS_CONSUMER are required")
	}

	adapter, err := natstransport.New(ctx, natstransport.Config{
		URL:        url,
		Stream:     stream,
		AckWait:    30 * time.Second,
		RetryDelay: time.Second,
	})
	if err != nil {
		return err
	}
	defer adapter.Close()

	sub, err := adapter.Replay(ctx, events.Cursor{Stream: stream}, subject, consumer,
		func(_ context.Context, stored events.StoredEvent) error {
			// Printing illustrates delivery only. A real handler must commit
			// idempotent effects before returning nil to acknowledge the event.
			fmt.Printf("%s/%d (attempt %d) %s: %s\n",
				stored.Stream, stored.Sequence, stored.DeliveryAttempt,
				stored.Event.ID, stored.Event.Data)
			return nil
		})
	if err != nil {
		return err
	}
	defer sub.Close()

	<-ctx.Done()
	return nil
}
