package nats

import (
	"context"
	"errors"
	"time"

	natsio "github.com/nats-io/nats.go"
)

var (
	ErrConnect = errors.New("connect NATS")
	ErrJetStream = errors.New("initialize JetStream")
	ErrStreamLookup = errors.New("find JetStream stream")
)

// connectContext bounds the caller's wait even during DNS, dial or handshake.
// The SDK has no context-aware Connect; clamp its per-attempt timeout and
// close any connection that completes after cancellation. Custom dialers
// and authentication callbacks must return themselves.
func connectContext(ctx context.Context, url string, options ...natsio.Option) (*natsio.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	type result struct {
		conn *natsio.Conn
		err error
	}
	results := make(chan result)
	opts := append([]natsio.Option(nil), options...)
	opts = append(opts, func(o *natsio.Options) error {
		if deadline, ok := ctx.Deadline(); ok {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				return context.DeadlineExceeded
			}
			if o.Timeout == 0 || o.Timeout > remaining {
				o.Timeout = remaining
			}
		}
		return nil
	})
	go func() {
		conn, err := natsio.Connect(url, opts...)
		select {
		case results <- result{conn: conn, err: err}:
		case <-ctx.Done():
			if conn != nil {
				conn.Close()
			}
		}
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r := <-results:
		if err := ctx.Err(); err != nil {
			if r.conn != nil {
				r.conn.Close()
			}
			return nil, err
		}
		return r.conn, r.err
	}
}
