package pubsub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

var (
	errRedisPubSubClosed = errors.New("pubsub: redis pubsub is closed")
	errQueueFull         = errors.New("pubsub: redis queue is full")
)

const (
	// Default block timeout for BRPOP
	redisBlockTimeout = 5 * time.Second
	// Key prefix for Redis lists used as queues
	redisQueueKeyPrefix = "pubsub:queue:"
)

// redisSubscription holds information specific to a Redis subscription
type redisSubscription struct {
	*Subscription                // Embed common Subscription fields
	redisClient   redis.Cmdable  // Redis client
	queueKey      string         // Redis list key for the topic
	stopChan      chan struct{}  // Channel to signal the listener goroutine to stop
	listenerWg    sync.WaitGroup // Waits for the listener goroutine to finish
}

// RedisPubSub implements the PubSub interface using Redis Lists.
type RedisPubSub struct {
	redisClient redis.Cmdable
	mu          sync.RWMutex
	closed      bool
	subs        map[string]*redisSubscription // subID -> redisSubscription
	stopWg      sync.WaitGroup                // Waits for all listener goroutines during close
}

// NewRedisPubSub creates a new Redis-based PubSub instance.
// It requires a redis.Cmdable interface (e.g., *redis.Client or *redis.ClusterClient).
func NewRedisPubSub(client redis.Cmdable) PubSub {
	if client == nil {
		panic("pubsub: redis client cannot be nil")
	}
	return &RedisPubSub{
		redisClient: client,
		subs:        make(map[string]*redisSubscription),
	}
}

// getQueueKey generates the Redis key for a topic's list.
func getQueueKey(topic string) string {
	return redisQueueKeyPrefix + topic
}

// Publish sends messages to the Redis list for the topic.
// It checks MaxQueueSize if configured for any subscriber.
func (r *RedisPubSub) Publish(ctx context.Context, topic string, messages ...*Message) error {
	return r.publishInternal(ctx, topic, messages, false) // false = not TryPublish
}

// TryPublish attempts to send messages to the Redis list, potentially dropping if queue limits are hit.
func (r *RedisPubSub) TryPublish(ctx context.Context, topic string, messages ...*Message) error {
	return r.publishInternal(ctx, topic, messages, true) // true = TryPublish
}

// publishInternal handles the core logic for publishing messages to Redis.
func (r *RedisPubSub) publishInternal(ctx context.Context, topic string, messages []*Message, try bool) error {
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return errRedisPubSubClosed
	}
	// We don't need to track subscribers here directly for publishing,
	// but we might need options like MaxQueueSize from them.
	// For simplicity now, let's assume MaxQueueSize check happens here if needed,
	// though ideally it's associated with the topic/queue itself, not individual subs.
	// Let's find the *minimum* non-zero MaxQueueSize among subscribers for this topic.
	minQueueSize := int64(0) // 0 means no limit initially
	// This check is inefficient, ideally options are stored per-topic.
	// Revisit this if performance becomes an issue.
	for _, sub := range r.subs {
		if sub.Topic == topic && sub.options.MaxQueueSize > 0 {
			if minQueueSize == 0 || sub.options.MaxQueueSize < minQueueSize {
				minQueueSize = sub.options.MaxQueueSize
			}
		}
	}
	r.mu.RUnlock() // Unlock before Redis command

	queueKey := getQueueKey(topic)

	// Check queue size limit if applicable
	if minQueueSize > 0 {
		currentLen, err := r.redisClient.LLen(ctx, queueKey).Result()
		if err != nil && !errors.Is(err, redis.Nil) { // Ignore Nil error (key doesn't exist)
			log.Error().Err(err).Str("topic", topic).Str("queue_key", queueKey).Msg("failed to get queue length")
			return fmt.Errorf("failed to check queue length: %w", err)
		}
		if currentLen >= minQueueSize {
			if try {
				log.Warn().Str("topic", topic).Str("queue_key", queueKey).Int64("current_len", currentLen).Int64("max_size", minQueueSize).Msg("redis queue full, dropping message (tryPublish)")
				return nil // Drop message for TryPublish
			}
			log.Error().Str("topic", topic).Str("queue_key", queueKey).Int64("current_len", currentLen).Int64("max_size", minQueueSize).Msg("redis queue full")
			return errQueueFull
		}
	}

	// Serialize messages and push to Redis list
	// We push messages individually to allow BRPOP to get one at a time.
	// Using RPUSH/BRPOP for FIFO behavior.
	for _, msg := range messages {
		payloadBytes, err := json.Marshal(msg) // Use JSON serialization
		if err != nil {
			log.Error().Err(err).Str("topic", topic).Msg("failed to marshal message payload")
			// Decide: skip this message or fail the whole publish? Let's skip.
			continue
		}

		// Use RPUSH to add to the end of the list
		err = r.redisClient.RPush(ctx, queueKey, payloadBytes).Err()
		if err != nil {
			log.Error().Err(err).Str("topic", topic).Str("queue_key", queueKey).Msg("failed to RPUSH message to redis")
			// If one push fails, should we stop? For now, return the error.
			return fmt.Errorf("failed to push message to redis: %w", err)
		}
	}

	return nil
}

// Subscribe creates a Redis subscription.
func (r *RedisPubSub) Subscribe(ctx context.Context, topic string, handler any, opts ...Option) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return "", errRedisPubSubClosed
	}

	// Create the base subscription object
	baseSub, err := newSubscription(topic, handler, opts...)
	if err != nil {
		return "", err
	}

	queueKey := getQueueKey(topic)
	redisSub := &redisSubscription{
		Subscription: baseSub,
		redisClient:  r.redisClient,
		queueKey:     queueKey,
		stopChan:     make(chan struct{}),
	}

	r.subs[redisSub.ID] = redisSub
	r.stopWg.Add(1) // Increment wait group for the new listener

	// Start the listener goroutine
	go redisSub.listenLoop()

	log.Debug().Str("subscription_id", redisSub.ID).Str("topic", topic).Str("queue_key", queueKey).Msg("new redis subscription created")
	return redisSub.ID, nil
}

// Unsubscribe removes a Redis subscription and stops its listener.
func (r *RedisPubSub) Unsubscribe(ctx context.Context, id string) error {
	r.mu.Lock()
	sub, ok := r.subs[id]
	if !ok {
		r.mu.Unlock()
		return nil // Already unsubscribed
	}
	delete(r.subs, id)
	r.mu.Unlock() // Unlock before stopping/closing

	// Signal the listener to stop and wait for it
	close(sub.stopChan)
	sub.listenerWg.Wait() // Wait for listener to finish processing current item if any
	r.stopWg.Done()       // Decrement wait group after listener fully stopped

	// Close the base subscription part
	err := sub.Subscription.Close() // Close function/chan handlers, workers etc.
	if err != nil {
		log.Error().Err(err).Str("subscription_id", id).Msg("error closing base subscription during unsubscribe")
		// Continue even if closing fails
	}

	log.Debug().Str("subscription_id", id).Str("topic", sub.Topic).Msg("redis subscription removed")
	return nil
}

// Close shuts down the RedisPubSub instance, stopping all listeners.
func (r *RedisPubSub) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil // Already closed
	}
	r.closed = true

	// Capture subscriptions to close outside the lock
	subsToClose := make([]*redisSubscription, 0, len(r.subs))
	for _, sub := range r.subs {
		subsToClose = append(subsToClose, sub)
	}

	// Clear internal map immediately under lock
	r.subs = make(map[string]*redisSubscription) // Or set to nil

	r.mu.Unlock() // Unlock before stopping listeners

	log.Info().Msg("redis pubsub closing...")

	// Signal all listeners to stop
	for _, sub := range subsToClose {
		close(sub.stopChan)
	}

	// Wait for all listeners to finish
	r.stopWg.Wait()
	log.Debug().Msg("all redis listeners stopped")

	// Close base subscriptions (redundant if Unsubscribe was called, but safe)
	var closeWg sync.WaitGroup
	closeWg.Add(len(subsToClose))
	for _, sub := range subsToClose {
		go func(s *redisSubscription) {
			defer closeWg.Done()
			if err := s.Subscription.Close(); err != nil {
				// Log error, already logged in Unsubscribe if called
			}
		}(sub)
	}
	closeWg.Wait()

	log.Info().Msg("redis pubsub closed")
	return nil
}

// listenLoop is the goroutine that continuously fetches messages from Redis.
func (rs *redisSubscription) listenLoop() {
	defer rs.listenerWg.Done() // Ensure WaitGroup is decremented on exit
	rs.listenerWg.Add(1)       // Increment WaitGroup when starting

	log.Debug().Str("subscription_id", rs.ID).Str("queue_key", rs.queueKey).Msg("starting redis listener loop")

	for {
		select {
		case <-rs.stopChan:
			log.Debug().Str("subscription_id", rs.ID).Str("queue_key", rs.queueKey).Msg("stopping redis listener loop")
			return
		default:
			// Use BRPOP to wait for a message
			// Use a context for potential cancellation within BRPOP, though stopChan is primary mechanism
			ctx, cancel := context.WithTimeout(context.Background(), redisBlockTimeout+1*time.Second) // Timeout slightly longer than BRPOP

			result, err := rs.redisClient.BRPop(ctx, redisBlockTimeout, rs.queueKey).Result()
			cancel() // Release context resources

			if err != nil {
				if errors.Is(err, redis.Nil) {
					// Timeout, no message received, continue loop
					continue
				}
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					// Context issue, likely during shutdown or long block, continue check stopChan
					continue
				}
				// Check if the error indicates a closed connection during shutdown
				select {
				case <-rs.stopChan: // Check again if stop was requested during the error
					log.Debug().Str("subscription_id", rs.ID).Str("queue_key", rs.queueKey).Msg("stopping redis listener loop after redis error")
					return
				default:
				}

				log.Error().Err(err).Str("subscription_id", rs.ID).Str("queue_key", rs.queueKey).Msg("redis BRPOP error")
				// Optional: implement backoff strategy before retrying
				time.Sleep(1 * time.Second) // Simple backoff
				continue
			}

			// result should be []string{queueKey, messageData}
			if len(result) != 2 {
				log.Error().Str("subscription_id", rs.ID).Str("queue_key", rs.queueKey).Int("result_len", len(result)).Msg("invalid result format from BRPOP")
				continue
			}
			messageData := result[1]

			// Deserialize message
			var msg Message
			if err := json.Unmarshal([]byte(messageData), &msg); err != nil {
				log.Error().Err(err).Str("subscription_id", rs.ID).Str("queue_key", rs.queueKey).Msg("failed to unmarshal message from redis")
				continue // Skip malformed message
			}

			// Deliver the message using the embedded Subscription's deliver method
			// Use a background context for delivery, as cancellation is handled by stopChan
			// Pass only the single received message.
			// Use 'false' for 'try' as BRPOP implies we should process it.
			deliveryErr := rs.Subscription.deliver(context.Background(), []*Message{&msg}, false)
			if deliveryErr != nil && !errors.Is(deliveryErr, errSubscriptionClosed) {
				log.Error().Err(deliveryErr).Str("subscription_id", rs.ID).Str("topic", rs.Topic).Msg("failed to deliver message from redis")
				// What to do on delivery failure? Message is already popped. Log and continue.
			}
		}
	}
}

// Ensure RedisPubSub implements PubSub interface
var _ PubSub = (*RedisPubSub)(nil)
