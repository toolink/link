package redlb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

type redisRegistry struct {
	opts    *Options
	client  redis.Cmdable
	mu      sync.Mutex               // Protects stopChs map
	stopChs map[string]chan struct{} // Map instance key -> stop channel for keepalive
	// wg for managing watcher goroutines started by this registry instance (if any global ones exist)
	// wg sync.WaitGroup - Currently watchers are per-call, not global
}

// NewRedisRegistry creates a new Redis-backed registry.
func NewRedisRegistry(opts ...Option) (Registry, error) {
	options := newOptions(opts...)
	if options.Client == nil {
		return nil, errors.New("redis client is required")
	}

	// Ping Redis to ensure connectivity during setup
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second) // Short timeout for ping
	defer cancel()
	if err := options.Client.Ping(ctx).Err(); err != nil {
		log.Error().Err(err).Msg("failed to connect to redis")
		return nil, fmt.Errorf("failed to connect to redis: %w", err)
	}

	log.Info().Str("prefix", options.KeyPrefix).Dur("ttl", options.TTL).Dur("heartbeat", options.HeartbeatInterval).Dur("watch_interval", options.WatchInterval).Msg("redis registry initialized")

	return &redisRegistry{
		opts:    options,
		client:  options.Client,
		stopChs: make(map[string]chan struct{}),
	}, nil
}

// instanceKey generates the Redis key for a specific service instance.
func (r *redisRegistry) instanceKey(instance *ServiceInstance) string {
	// Format: prefix:serviceName:instanceID
	return fmt.Sprintf("%s:%s:%s", r.opts.KeyPrefix, instance.Name, instance.ID)
}

// servicePrefix generates the prefix pattern for scanning keys of a service.
func (r *redisRegistry) servicePrefix(serviceName string) string {
	// Format: prefix:serviceName:
	return fmt.Sprintf("%s:%s:", r.opts.KeyPrefix, serviceName)
}

// Register adds/updates a service instance and starts its heartbeat.
func (r *redisRegistry) Register(ctx context.Context, instance *ServiceInstance) (func(context.Context) error, error) {
	if instance.Name == "" || instance.Address == "" {
		return nil, errors.New("instance name and address are required")
	}
	if instance.ID == "" {
		instance.ID = uuid.NewString()
		log.Debug().Str("service", instance.Name).Str("generated_id", instance.ID).Msg("generated instance id")
	}
	if instance.Metadata == nil {
		instance.Metadata = make(map[string]string) // Ensure not nil for JSON
	}

	key := r.instanceKey(instance)
	valueBytes, err := json.Marshal(instance)
	if err != nil {
		log.Error().Err(err).Stringer("instance", instance).Msg("failed to marshal instance data")
		return nil, fmt.Errorf("failed to marshal instance data: %w", err)
	}

	// Set/update the key with TTL
	if err := r.client.Set(ctx, key, valueBytes, r.opts.TTL).Err(); err != nil {
		log.Error().Err(err).Str("key", key).Msg("failed to set instance key in redis")
		return nil, fmt.Errorf("failed to register instance with redis: %w", err)
	}

	log.Info().Stringer("instance", instance).Dur("ttl", r.opts.TTL).Msg("instance registered")

	// --- Heartbeat Management ---
	r.mu.Lock()
	// If a heartbeat goroutine already exists for this key, stop the old one first.
	if oldCh, exists := r.stopChs[key]; exists {
		log.Warn().Str("key", key).Msg("stopping existing heartbeat for re-registration")
		close(oldCh)
	}
	// Create and store the new stop channel
	stopCh := make(chan struct{})
	r.stopChs[key] = stopCh
	r.mu.Unlock()

	// Start the new heartbeat goroutine
	go r.keepAlive(instance, stopCh)

	deregisterFunc := func(deregisterCtx context.Context) error {
		return r.Deregister(deregisterCtx, instance)
	}

	return deregisterFunc, nil
}

// keepAlive runs in a goroutine, periodically renewing the instance's TTL in Redis.
func (r *redisRegistry) keepAlive(instance *ServiceInstance, stopCh <-chan struct{}) {
	key := r.instanceKey(instance)
	ticker := time.NewTicker(r.opts.HeartbeatInterval)
	defer ticker.Stop()

	log.Debug().Stringer("instance", instance).Dur("interval", r.opts.HeartbeatInterval).Msg("starting heartbeat")

	ctx, cancel := context.WithCancel(context.Background()) // Background context for ongoing task
	defer cancel()

	for {
		select {
		case <-stopCh:
			log.Info().Stringer("instance", instance).Msg("heartbeat stopped")
			return
		case <-ticker.C:
			// Renew the TTL using EXPIRE. Returns 1 if key exists and TTL was set, 0 if key doesn't exist.
			res, err := r.client.Expire(ctx, key, r.opts.TTL).Result()
			if err != nil {
				// Log Redis errors, but continue trying unless explicitly stopped
				log.Error().Err(err).Stringer("instance", instance).Msg("heartbeat failed to renew ttl")
				continue // Try again on next tick
			}
			if res { // Key likely expired or was deleted
				log.Warn().Stringer("instance", instance).Msg("instance key not found during heartbeat, attempting re-register")
				// Attempt to re-register the instance immediately
				valueBytes, marshalErr := json.Marshal(instance)
				if marshalErr != nil {
					log.Error().Err(marshalErr).Stringer("instance", instance).Msg("failed to marshal instance for re-registration")
					continue // Cannot re-register without valid data
				}
				// Use SET with EX again for re-registration
				setErr := r.client.Set(ctx, key, valueBytes, r.opts.TTL).Err()
				if setErr != nil {
					log.Error().Err(setErr).Stringer("instance", instance).Msg("failed to re-register expired instance")
				} else {
					log.Info().Stringer("instance", instance).Msg("instance re-registered after expiration")
				}
			} else {
				log.Debug().Stringer("instance", instance).Dur("ttl", r.opts.TTL).Msg("heartbeat ttl renewed")
			}
		}
	}
}

// Deregister removes the instance from Redis and stops its heartbeat.
func (r *redisRegistry) Deregister(ctx context.Context, instance *ServiceInstance) error {
	key := r.instanceKey(instance)

	// --- Stop Heartbeat ---
	r.mu.Lock()
	if stopCh, exists := r.stopChs[key]; exists {
		close(stopCh)          // Signal keepAlive goroutine to stop
		delete(r.stopChs, key) // Remove from map
	} else {
		log.Warn().Stringer("instance", instance).Msg("deregister called but no active heartbeat found")
	}
	r.mu.Unlock()

	// --- Remove Key from Redis ---
	deletedCount, err := r.client.Del(ctx, key).Result()
	// Ignore redis.Nil error if Del is called multiple times or key expired
	if err != nil && !errors.Is(err, redis.Nil) {
		log.Error().Err(err).Stringer("instance", instance).Msg("failed to delete instance key from redis")
		return fmt.Errorf("failed to deregister instance from redis: %w", err)
	}

	if deletedCount > 0 {
		log.Info().Stringer("instance", instance).Msg("instance deregistered")
	} else {
		log.Info().Stringer("instance", instance).Msg("instance already gone or never registered")
	}
	return nil
}

// Discover finds active instances of a service using SCAN and MGET.
func (r *redisRegistry) Discover(ctx context.Context, serviceName string) ([]*ServiceInstance, error) {
	prefix := r.servicePrefix(serviceName)
	pattern := prefix + "*" // Pattern for SCAN

	keys, err := r.scanKeys(ctx, pattern)
	if err != nil {
		log.Error().Err(err).Str("service", serviceName).Str("pattern", pattern).Msg("failed to scan keys for service")
		return nil, fmt.Errorf("failed to scan keys for service %s: %w", serviceName, err)
	}

	if len(keys) == 0 {
		log.Debug().Str("service", serviceName).Msg("discovery found no instances")
		return []*ServiceInstance{}, nil // Return empty slice, not nil
	}

	// MGET fetches values for multiple keys efficiently
	// Note: MGET returns []any in go-redis v8
	valuesAny, err := r.client.MGet(ctx, keys...).Result()
	if err != nil {
		// MGET generally shouldn't fail unless Redis connection issue
		log.Error().Err(err).Str("service", serviceName).Int("key_count", len(keys)).Msg("failed to mget instance data")
		return nil, fmt.Errorf("failed to MGET instance data for service %s: %w", serviceName, err)
	}

	instances := make([]*ServiceInstance, 0, len(valuesAny))
	for i, valAny := range valuesAny {
		// MGET returns nil interface for keys that don't exist (e.g., expired between SCAN and MGET)
		if valAny == nil {
			log.Warn().Str("key", keys[i]).Msg("mget returned nil for key, likely expired concurrently")
			continue
		}

		valueStr, ok := valAny.(string)
		if !ok {
			log.Warn().Str("key", keys[i]).Interface("type", fmt.Sprintf("%T", valAny)).Msg("unexpected type from mget, expected string")
			continue
		}

		var instance ServiceInstance
		if err := json.Unmarshal([]byte(valueStr), &instance); err != nil {
			log.Warn().Err(err).Str("key", keys[i]).Str("value", valueStr).Msg("failed to unmarshal instance data, skipping")
			continue // Skip malformed entries
		}
		instances = append(instances, &instance)
	}

	log.Debug().Str("service", serviceName).Int("count", len(instances)).Msg("discovery successful")
	return instances, nil
}

// scanKeys uses SCAN to find keys matching a pattern without blocking Redis.
func (r *redisRegistry) scanKeys(ctx context.Context, pattern string) ([]string, error) {
	var keys []string
	var cursor uint64
	var err error
	scanCmd := r.client.Scan(ctx, cursor, pattern, 100) // Fetch 100 keys per iteration

	for {
		var batch []string
		batch, cursor, err = scanCmd.Result()
		if err != nil {
			return nil, err
		}
		keys = append(keys, batch...)
		if cursor == 0 { // Iteration complete
			break
		}
		// Prepare command for the next iteration
		scanCmd = r.client.Scan(ctx, cursor, pattern, 100)
	}
	return keys, nil
}

// Watch provides a channel for observing changes in service instances via polling.
func (r *redisRegistry) Watch(ctx context.Context, serviceName string) (<-chan []*ServiceInstance, error) {
	ch := make(chan []*ServiceInstance, 1) // Buffered channel (size 1 is usually sufficient)

	go func() {
		defer close(ch) // Close channel when goroutine exits
		ticker := time.NewTicker(r.opts.WatchInterval)
		defer ticker.Stop()

		var lastHash string

		// Perform initial discovery and send immediately
		initialInstances, err := r.Discover(ctx, serviceName)
		if err != nil {
			// Log error but continue watching; send empty list initially on error
			log.Error().Err(err).Str("service", serviceName).Msg("watcher failed initial discovery")
			initialInstances = []*ServiceInstance{} // Send empty list
		}
		lastHash = hashInstances(initialInstances) // Calculate hash even if empty/error
		// Send initial state (non-blocking send might drop if context is already done)
		select {
		case ch <- initialInstances:
			log.Debug().Str("service", serviceName).Int("count", len(initialInstances)).Msg("watcher sent initial state")
		case <-ctx.Done():
			log.Warn().Str("service", serviceName).Msg("watcher context canceled before sending initial state")
			return
		}

		// Start polling loop
		for {
			select {
			case <-ctx.Done():
				log.Info().Str("service", serviceName).Msg("watcher stopping due to context cancellation")
				return
			case <-ticker.C:
				currentInstances, err := r.Discover(ctx, serviceName)
				if err != nil {
					// Log error, maybe report to gRPC ClientConn? Or just retry next tick?
					// For gRPC, sending an empty list on persistent errors might be safer
					// than keeping stale data. For now, just log and retry.
					log.Warn().Err(err).Str("service", serviceName).Msg("watcher failed discovery during poll")
					// Optionally, could send empty list here if error persists:
					// if hashInstances(nil) != lastHash {
					//   select { case ch <- []*ServiceInstance{}: lastHash = hashInstances(nil) default: }
					// }
					continue // Skip update on error
				}

				newHash := hashInstances(currentInstances)
				if newHash != lastHash {
					log.Debug().Str("service", serviceName).Int("count", len(currentInstances)).Str("old_hash", lastHash).Str("new_hash", newHash).Msg("watcher detected change")
					// Non-blocking send: if receiver isn't ready, drop the older update.
					select {
					case ch <- currentInstances:
						lastHash = newHash
					default:
						// This indicates the consumer (e.g., gRPC resolver) is slow or stuck.
						log.Warn().Str("service", serviceName).Msg("watcher channel full, dropping update")
						// Update hash anyway so we don't repeatedly try to send the same dropped data.
						lastHash = newHash
					}
				} else {
					log.Trace().Str("service", serviceName).Msg("watcher poll found no changes") // Use Trace for frequent "no change" logs
				}
			}
		}
	}()

	log.Info().Str("service", serviceName).Dur("interval", r.opts.WatchInterval).Msg("watcher started")
	return ch, nil // Return the channel immediately
}

// Close stops all active heartbeats managed by this registry instance.
func (r *redisRegistry) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	count := 0
	for key, ch := range r.stopChs {
		close(ch) // Signal goroutine to stop
		delete(r.stopChs, key)
		count++
	}
	if count > 0 {
		log.Info().Int("count", count).Msg("stopped active heartbeats")
	} else {
		log.Debug().Msg("close called, no active heartbeats to stop")
	}
	// Note: Does not close the Redis client passed in options.
	return nil
}
