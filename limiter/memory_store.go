package limiter

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// memoryStore implements the Store interface using an in-memory map.
type memoryStore struct {
	mu    sync.Mutex
	state map[string]limiterState
}

// NewMemoryStore creates a new in-memory rate limit store.
func NewMemoryStore() Store {
	return &memoryStore{
		state: make(map[string]limiterState),
	}
}

// Allow implements the Store interface for memory storage.
func (s *memoryStore) Allow(ctx context.Context, key string, rate float64, period float64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	currentState, exists := s.state[key]

	if !exists {
		// first request for this key
		currentState = limiterState{
			Allowance: rate - 1.0, // consume one token now
			LastCheck: now,
		}
		s.state[key] = currentState
		log.Debug().Str("key", key).Float64("rate", rate).Float64("period", period).Msg("first request, allowed")
		return true, nil // allowed
	}

	// calculate time passed and tokens to add
	timePassed := now.Sub(currentState.LastCheck).Seconds()
	tokensToAdd := timePassed * (rate / period) // rate / period is tokens per second
	currentState.LastCheck = now

	// update allowance
	currentState.Allowance += tokensToAdd
	if currentState.Allowance > rate {
		currentState.Allowance = rate // clamp to max allowance (burst)
	}

	// check if allowed and consume token if yes
	allowed := currentState.Allowance >= 1.0
	if allowed {
		currentState.Allowance -= 1.0
		log.Debug().Str("key", key).Float64("allowance", currentState.Allowance+1.0).Float64("consumed", 1.0).Bool("allowed", true).Msg("request checked")
	} else {
		log.Warn().Str("key", key).Float64("allowance", currentState.Allowance).Bool("allowed", false).Msg("rate limit exceeded")
	}

	// store updated state
	s.state[key] = currentState
	return allowed, nil
}
