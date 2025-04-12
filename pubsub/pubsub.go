package pubsub

import (
	"context"
)

// Message represents a single message to be published.
// We might add headers or other metadata later.
type Message struct {
	Payload any
}

// PubSub defines the interface for a publish/subscribe system.
type PubSub interface {
	// Publish sends messages to the given topic.
	// It blocks until all messages are sent to all subscribers
	// or the context is canceled.
	Publish(ctx context.Context, topic string, messages ...*Message) error

	// TryPublish attempts to send messages to the given topic.
	// It does not block and ignores subscribers that are not ready
	// to receive immediately (e.g., full channel buffer).
	TryPublish(ctx context.Context, topic string, messages ...*Message) error

	// Subscribe creates a subscription to the given topic.
	// The handler can be a channel (chan<- T or chan<- *Message)
	// or a function (func(T) or func(*Message) or func(msg1 T1, msg2 T2, ...)).
	// Options can configure the subscription behavior (e.g., concurrency).
	// Returns a unique subscription ID and an error if subscription fails.
	Subscribe(ctx context.Context, topic string, handler any, opts ...Option) (string, error)

	// Unsubscribe removes the subscription with the given ID.
	Unsubscribe(ctx context.Context, id string) error

	// Close shuts down the pub/sub system, cleaning up resources.
	Close() error
}
