# go-events

Small event contracts for Go applications, with separate local and durable transports.

## Transports

- `adapters/local` dispatches to in-process subscribers synchronously. It does not persist or retry events.
- `adapters/nats` publishes to an existing NATS JetStream stream and uses named durable pull consumers. Successful handlers are acknowledged; failed handlers are negatively acknowledged with a configured retry delay.

Applications choose the transport explicitly. The core package does not import NATS or contain application-specific types.

## Usage

```go
bus := local.New()
defer bus.Close()

sub, err := bus.Subscribe(ctx, "task.created", func(ctx context.Context, event events.Event) error {
    return handleTask(ctx, event.Data)
})
if err != nil {
    return err
}
defer sub.Close()

return bus.Publish(ctx, events.Event{ID: "task-42", Topic: "task.created", Data: []byte(`{"task":42}`)})
```

For JetStream, provision a stream covering the published subjects before connecting:

```go
adapter, err := nats.New(ctx, nats.Config{
    URL: "nats://127.0.0.1:4222", Stream: "EVENTS", Consumer: "worker-1",
    AckWait: 30 * time.Second, RetryDelay: time.Second,
})
if err != nil {
    return err
}
defer adapter.Close()
```

Use a different durable consumer name for each independent subscriber. Multiple replicas sharing a consumer name share its delivery workload. The NATS adapter does not embed or provision a broker.

## Delivery semantics

JetStream publishes are confirmed by the server. Delivery is at least once, not exactly once: consumers must commit changes idempotently before returning nil. Broker-side duplicate suppression using event IDs is limited to the stream's configured duplicate window. Event ordering and retry behavior depend on the selected transport.

The local adapter delivers synchronously in subscription order and returns handler errors to the publisher. It does not offer broker durability or replay.

## Tests

`go test ./...` runs local tests and JetStream integration tests against an embedded NATS server.

## Releases

Every pull request and push to `main` runs the same CI. Checks include tests on Linux and macOS, the Linux race detector, `go vet`, compilation, module tidiness and `govulncheck` with Go 1.26.11. Go 1.25.14 is tested separately for compatibility with existing consumers; it is no longer a supported Go security release.

A release is published only when a `vMAJOR.MINOR.PATCH` tag is pushed. The release workflow reruns CI, checks that the tagged commit is part of `main`, and creates a GitHub Release only after all checks pass. Merging a pull request does not automatically publish a version.

This repository is a Go library. Releases are versioned source modules, not executable artifacts.
