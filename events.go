// Package events defines broker-independent contracts for publishing and
// consuming events. Event payloads are opaque to this package.
package events

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

var ErrClosed = errors.New("event transport is closed")

// Event is the envelope delivered to a subscriber. ID identifies the logical
// event across redeliveries, and Topic is used for subscription routing.
type Event struct {
	ID    string
	Topic string
	Data  []byte
}

// Validate checks the minimum wire contract. Payload encoding is owned by
// the application, not by the event transport.
func (e Event) Validate() error {
	if strings.TrimSpace(e.ID) == "" {
		return errors.New("event ID is required")
	}
	return ValidateTopic(e.Topic)
}

// ValidateTopic requires a nonempty topic without whitespace. Individual
// adapters may impose additional subject syntax constraints.
func ValidateTopic(topic string) error {
	if strings.TrimSpace(topic) == "" || strings.ContainsAny(topic, " \t\r\n") {
		return fmt.Errorf("invalid event topic %q", topic)
	}
	return nil
}

// Clone copies the payload so independent subscribers cannot mutate each
// other's input.
func (e Event) Clone() Event {
	e.Data = append([]byte(nil), e.Data...)
	return e
}

// Handler returns nil only after the event's effect is committed. Durable
// transports acknowledge successful handlers and redeliver failed ones.
type Handler func(context.Context, Event) error

type Publisher interface {
	Publish(context.Context, Event) error
}

type Subscriber interface {
	Subscribe(context.Context, string, Handler) (Subscription, error)
}

// Subscription controls the lifetime of an active subscription. Closing it
// does not delete any durable consumer retained by a broker.
type Subscription interface {
	Close() error
}
