package redlb

import (
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

// Options holds configuration for the registry.
type Options struct {
	// Redis client instance (required).
	Client redis.Cmdable
	// Prefix for all keys stored in Redis (default: "redlb:svc").
	KeyPrefix string
	// Time-to-live for registered service instances (default: 30s).
	TTL time.Duration
	// Interval for heartbeats to renew the TTL (recommended: TTL / 3).
	HeartbeatInterval time.Duration
	// Interval for the gRPC resolver to check for updates (default: 15s).
	WatchInterval time.Duration
}

// Option defines a function type for setting options.
type Option func(*Options)

// Default values
const (
	DefaultKeyPrefix        = "redlb:svc" // Changed default prefix
	DefaultTTL              = 30 * time.Second
	DefaultWatchInterval    = 15 * time.Second
	DefaultHeartbeatDivisor = 3
)

// newOptions creates default options and applies user overrides.
func newOptions(opts ...Option) *Options {
	options := &Options{
		KeyPrefix:     DefaultKeyPrefix,
		TTL:           DefaultTTL,
		WatchInterval: DefaultWatchInterval,
	}
	// Calculate default heartbeat based on TTL
	options.HeartbeatInterval = options.TTL / DefaultHeartbeatDivisor
	if options.HeartbeatInterval <= 0 {
		options.HeartbeatInterval = 1 * time.Second // Ensure positive minimum
	}

	for _, o := range opts {
		o(options)
	}

	// Ensure Heartbeat is realistic relative to TTL after overrides
	if options.HeartbeatInterval >= options.TTL {
		originalHB := options.HeartbeatInterval
		options.HeartbeatInterval = options.TTL / DefaultHeartbeatDivisor
		if options.HeartbeatInterval <= 0 {
			options.HeartbeatInterval = 1 * time.Second
		}
		log.Warn().
			Dur("configured_heartbeat", originalHB).
			Dur("ttl", options.TTL).
			Dur("adjusted_heartbeat", options.HeartbeatInterval).
			Msg("heartbeat interval was >= ttl, adjusted")
	}

	if options.Client == nil {
		// This will be checked in NewRedisRegistry, returning an error.
		// Logging here might be premature if the user intends to set it later.
	}

	return options
}

// WithRedisClient sets the Redis client.
func WithRedisClient(client redis.Cmdable) Option {
	return func(o *Options) {
		o.Client = client
	}
}

// WithKeyPrefix sets the key prefix.
func WithKeyPrefix(prefix string) Option {
	return func(o *Options) {
		o.KeyPrefix = prefix
	}
}

// WithTTL sets the service instance TTL.
func WithTTL(ttl time.Duration) Option {
	return func(o *Options) {
		if ttl > 0 {
			o.TTL = ttl
		} else {
			log.Warn().Dur("invalid_ttl", ttl).Msg("ignoring non-positive ttl option")
		}
	}
}

// WithHeartbeatInterval sets the heartbeat interval.
func WithHeartbeatInterval(interval time.Duration) Option {
	return func(o *Options) {
		if interval > 0 {
			o.HeartbeatInterval = interval
		} else {
			log.Warn().Dur("invalid_heartbeat", interval).Msg("ignoring non-positive heartbeat interval option")
		}
	}
}

// WithWatchInterval sets the gRPC resolver watch interval.
func WithWatchInterval(interval time.Duration) Option {
	return func(o *Options) {
		if interval > 0 {
			o.WatchInterval = interval
		} else {
			log.Warn().Dur("invalid_watch_interval", interval).Msg("ignoring non-positive watch interval option")
		}
	}
}
