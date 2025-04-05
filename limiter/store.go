package limiter

import (
	"context"
	"time"
)

// Store defines the interface for storing and checking rate limit states.
type Store interface {
	// Allow checks if a request identified by key is allowed based on the rate and period duration (token bucket).
	// It must update the internal state atomically.
	// rate: max tokens (burst)
	// period: time in seconds to regenerate 'rate' tokens (determines refill rate)
	// Returns true if allowed, false otherwise.
	Allow(ctx context.Context, key string, rate float64, period float64) (bool, error)
}

// limiterState holds the state for a specific key in the memory store.
type limiterState struct {
	Allowance float64   // current number of tokens
	LastCheck time.Time // timestamp of the last check
}
