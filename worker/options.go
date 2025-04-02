package worker

import (
	"time"
)

// Define consumption mode (applies to how handlers *within* this Go process run)
type consumptionMode int

const (
	Sequential consumptionMode = iota
	Concurrent
)

// Define consumer method (channel or function)
type consumerMethod int

const (
	Channel consumerMethod = iota
	Function
)

// --- Subscription Options ---

type subscriptionOptions struct {
	// Redis List specific
	blockTime time.Duration // How long BRPOP should block waiting for messages

	// Go-side processing specific
	mode        consumptionMode
	concurrency int // Number of concurrent Go handlers (if mode == Concurrent)
	bufferSize  int // Buffer size for the internal channel feeding Go handlers
}

func defaultSubscriptionOptions() subscriptionOptions {
	return subscriptionOptions{
		blockTime:   5 * time.Second, // Longer block time is okay for BRPOP
		mode:        Sequential,
		concurrency: 1,
		bufferSize:  128,
	}
}

// SubscriptionOption configures a subscription.
type SubscriptionOption func(*subscriptionOptions)

// WithBlockTime sets the maximum time BRPOP should block waiting for a message.
// Defaults to 5 seconds.
func WithBlockTime(d time.Duration) SubscriptionOption {
	return func(o *subscriptionOptions) {
		if d > 0 {
			o.blockTime = d
		}
	}
}

// WithConcurrency sets the number of concurrent Go handler goroutines
// processing messages popped from Redis for this subscription.
// Setting n > 1 enables Concurrent mode. Defaults to 1 (Sequential).
func WithConcurrency(n int) SubscriptionOption {
	return func(o *subscriptionOptions) {
		if n > 0 {
			o.concurrency = n
			if n > 1 {
				o.mode = Concurrent
			} else {
				o.mode = Sequential
			}
		}
	}
}

// WithBufferSize sets the internal buffer size between the Redis poller goroutine
// and the Go handler goroutine(s). Defaults to 128.
func WithBufferSize(size int) SubscriptionOption {
	return func(o *subscriptionOptions) {
		if size > 0 {
			o.bufferSize = size
		}
	}
}

// --- Publisher Options ---

type publisherOptions struct {
	defaultPubTimeout time.Duration // Timeout for standard Pub/PubCtx LPUSH
	broadcastTimeout  time.Duration // Shorter timeout for Broadcast LPUSH
	listMaxLen        int64         // Approx max list length (uses LTRIM). 0=disabled
}

func defaultPublisherOptions() publisherOptions {
	return publisherOptions{
		defaultPubTimeout: 5 * time.Second,
		broadcastTimeout:  500 * time.Millisecond, // Much shorter for "drop if busy"
		listMaxLen:        0,                      // Don't trim by default
	}
}

// PublisherOption configures the Publisher.
type PublisherOption func(*publisherOptions)

// WithDefaultPubTimeout sets the context timeout for Pub/PubCtx LPUSH calls.
func WithDefaultPubTimeout(d time.Duration) PublisherOption {
	return func(o *publisherOptions) {
		if d > 0 {
			o.defaultPubTimeout = d
		}
	}
}

// WithBroadcastTimeout sets the context timeout for Broadcast LPUSH calls.
// If this timeout is hit, the message is effectively dropped.
func WithBroadcastTimeout(d time.Duration) PublisherOption {
	return func(o *publisherOptions) {
		if d > 0 {
			o.broadcastTimeout = d
		}
	}
}

// WithListMaxLen sets the approximate maximum length for the list using LTRIM.
// After a successful LPUSH, LTRIM 0 (maxLen-1) is called to keep the newest maxLen elements.
// 0 disables trimming.
func WithListMaxLen(maxLen int64) PublisherOption {
	return func(o *publisherOptions) {
		if maxLen >= 0 {
			o.listMaxLen = maxLen
		}
	}
}
