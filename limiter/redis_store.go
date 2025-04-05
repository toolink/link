package limiter

import (
	"context"
	_ "embed" // needed for go:embed
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

//go:embed limiter.lua
var redisLimiterScript string // embed the lua script content

var redisScript = redis.NewScript(redisLimiterScript)

// redisStore implements the Store interface using Redis.
type redisStore struct {
	client redis.Cmdable // Use Cmdable for compatibility with ClusterClient, SentinelClient, etc.
}

// NewRedisStore creates a new Redis rate limit store.
// It expects a pre-configured redis.Cmdable (e.g., redis.Client or redis.ClusterClient).
func NewRedisStore(client redis.Cmdable) Store {
	// We might want to load the script into Redis here using SCRIPT LOAD
	// for better performance, but running it directly also works.
	// Loading returns a SHA, which can be used with EVALSHA.
	// For simplicity, we'll use EVAL directly here.
	return &redisStore{
		client: client,
	}
}

// Allow implements the Store interface for Redis storage using a Lua script for atomicity.
func (s *redisStore) Allow(ctx context.Context, key string, rate float64, period float64) (bool, error) {
	now := time.Now()
	// Convert time.Time to Unix timestamp with fractional seconds for Lua script
	nowFloat := float64(now.UnixNano()) / 1e9

	// Keys for the Lua script: {rate_limit_key}
	keys := []string{fmt.Sprintf("ratelimit:%s", key)} // add a prefix for namespacing

	// Args for the Lua script: {max_tokens (rate), tokens_per_second, current_timestamp, tokens_to_consume}
	args := []any{
		rate,          // ARG[1]: max_tokens (burst capacity)
		rate / period, // ARG[2]: tokens_per_second (refill rate)
		nowFloat,      // ARG[3]: current timestamp (float seconds)
		1.0,           // ARG[4]: tokens to consume for this request
	}

	// Run the Lua script
	result, err := redisScript.Run(ctx, s.client, keys, args...).Result()
	if err != nil {
		// Handle potential Redis errors (e.g., connection issues, script errors)
		// Check for specific errors like redis.Nil if the script might return nil on error
		log.Error().Err(err).Str("key", key).Msg("redis lua script execution failed")
		// Decide on behavior: fail-open (allow) or fail-closed (deny)?
		// Fail-closed is generally safer for rate limiting.
		return false, fmt.Errorf("redis command failed for key %s: %w", key, err)
	}

	// The Lua script returns 1 if allowed, 0 if denied.
	allowedInt, ok := result.(int64)
	if !ok {
		log.Error().Str("key", key).Interface("result", result).Msg("redis lua script returned unexpected type")
		return false, fmt.Errorf("unexpected result type from redis script for key %s: %T", key, result)
	}

	allowed := allowedInt == 1

	if allowed {
		log.Debug().Str("key", key).Bool("allowed", true).Msg("redis request allowed")
	} else {
		log.Warn().Str("key", key).Bool("allowed", false).Msg("redis rate limit exceeded")
	}
	return allowed, nil
}
