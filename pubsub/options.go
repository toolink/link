package pubsub

// SubscriptionOptions holds configuration for a subscription.
type SubscriptionOptions struct {
	// Concurrency specifies the maximum number of concurrent handler executions.
	// Only applicable when the handler is a function.
	// Defaults to 1.
	Concurrency int
	// MaxQueueSize specifies the maximum number of messages to queue for Redis list subscriptions
	// before Publish starts failing. Only applicable for Redis implementation.
	// Defaults to 0 (no limit).
	MaxQueueSize int64
	// Redis specific options might go here in the future,
	// e.g., block timeout for BRPOP.
	// RedisBlockTimeout time.Duration
}

// Option is a function type used to configure subscriptions.
type Option func(*SubscriptionOptions)

// DefaultSubscriptionOptions returns the default options.
func DefaultSubscriptionOptions() *SubscriptionOptions {
	return &SubscriptionOptions{
		Concurrency:  1,
		MaxQueueSize: 0, // 0 means no limit by default
	}
}

// WithConcurrency sets the concurrency level for function handlers.
func WithConcurrency(n int) Option {
	return func(o *SubscriptionOptions) {
		if n > 0 {
			o.Concurrency = n
		}
	}
}

// WithMaxQueueSize sets the maximum queue size for Redis list subscriptions.
func WithMaxQueueSize(size int64) Option {
	return func(o *SubscriptionOptions) {
		if size >= 0 { // Allow 0 for no limit
			o.MaxQueueSize = size
		}
	}
}

// Apply applies the options to the SubscriptionOptions struct.
func (o *SubscriptionOptions) Apply(opts ...Option) {
	for _, opt := range opts {
		opt(o)
	}
}
