package pubsub

import "time"

// --- Subscription Options ---

type subscriptionConfig struct {
	concurrency     int           // Max concurrent workers for function subs (1 for channel/sequential func)
	bufferSize      int           // Size of the subscription's internal message buffer
	dispatchTimeout time.Duration // How long the topic dispatcher waits for subscription buffer
}

// SubscriptionOption configures a Subscription.
type SubscriptionOption func(*subscriptionConfig)

var defaultSubscriptionConfig = subscriptionConfig{
	concurrency:     1,
	bufferSize:      128,             // Sensible default buffer per subscription
	dispatchTimeout: 1 * time.Second, // Avoid blocking dispatcher for too long
}

// WithConcurrency sets the number of concurrent workers for function subscriptions.
// Must be 1 for channel subscriptions. Use value > 1 for concurrent function execution.
// Default is 1.
func WithConcurrency(n int) SubscriptionOption {
	return func(cfg *subscriptionConfig) {
		if n > 0 {
			cfg.concurrency = n
		} else {
			// Log perhaps? Reset to 1?
			cfg.concurrency = 1
		}
	}
}

// WithBufferSize sets the buffer size for the subscription's internal channel.
// Default is 128.
func WithBufferSize(size int) SubscriptionOption {
	return func(cfg *subscriptionConfig) {
		if size > 0 {
			cfg.bufferSize = size
		}
	}
}

// WithDispatchTimeout sets how long the topic dispatcher will wait (per message)
// for a subscription's internal buffer to accept the message before potentially
// logging a warning and moving on. Default is 1 second.
func WithDispatchTimeout(timeout time.Duration) SubscriptionOption {
	return func(cfg *subscriptionConfig) {
		if timeout > 0 {
			cfg.dispatchTimeout = timeout
		}
	}
}

// --- Broker Options ---

type brokerConfig struct {
	topicBufferSize int // Default buffer size for new topic channels
}

// BrokerOption configures a Broker.
type BrokerOption func(*brokerConfig)

var defaultBrokerConfig = brokerConfig{
	topicBufferSize: 256, // Default buffer for the main channel of each topic
}

// WithTopicBufferSize sets the buffer size for newly created topic channels.
// Default is 256.
func WithTopicBufferSize(size int) BrokerOption {
	return func(cfg *brokerConfig) {
		if size > 0 {
			cfg.topicBufferSize = size
		}
	}
}
