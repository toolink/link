package pubsub

import (
	"context"
	"errors"
	"sync"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

// Broker acts as a wrapper around a PubSub implementation.
// It allows easy switching between different PubSub backends (memory, redis).
type Broker struct {
	impl PubSub
	mu   sync.RWMutex // Protects the impl field if we allow changing it later
}

// BrokerOption defines an option for configuring the Broker.
type BrokerOption func(*brokerOptions)

type brokerOptions struct {
	redisClient redis.Cmdable // Optional Redis client for Redis backend
	// Add other options like initial capacity for memory backend, etc. if needed
}

// WithRedisClient provides a Redis client for the Redis PubSub backend.
func WithRedisClient(client redis.Cmdable) BrokerOption {
	return func(o *brokerOptions) {
		o.redisClient = client
	}
}

// New creates a new Broker instance.
// By default, it uses the MemoryPubSub.
// Use options like WithRedisClient to select the Redis backend.
func New(opts ...BrokerOption) (*Broker, error) {
	options := &brokerOptions{}
	for _, opt := range opts {
		opt(options)
	}

	var ps PubSub
	if options.redisClient != nil {
		log.Info().Msg("initializing broker with redis pubsub backend")
		ps = NewRedisPubSub(options.redisClient)
	} else {
		log.Info().Msg("initializing broker with memory pubsub backend")
		ps = NewMemoryPubSub()
	}

	if ps == nil {
		// This should ideally not happen with the current logic
		return nil, errors.New("failed to initialize pubsub implementation")
	}

	return &Broker{
		impl: ps,
	}, nil
}

// Publish delegates the call to the underlying PubSub implementation.
func (b *Broker) Publish(ctx context.Context, topic string, messages ...*Message) error {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.impl == nil {
		return errors.New("broker not initialized")
	}
	return b.impl.Publish(ctx, topic, messages...)
}

// TryPublish delegates the call to the underlying PubSub implementation.
func (b *Broker) TryPublish(ctx context.Context, topic string, messages ...*Message) error {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.impl == nil {
		return errors.New("broker not initialized")
	}
	return b.impl.TryPublish(ctx, topic, messages...)
}

// Subscribe delegates the call to the underlying PubSub implementation.
func (b *Broker) Subscribe(ctx context.Context, topic string, handler any, opts ...Option) (string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.impl == nil {
		return "", errors.New("broker not initialized")
	}
	return b.impl.Subscribe(ctx, topic, handler, opts...)
}

// Unsubscribe delegates the call to the underlying PubSub implementation.
func (b *Broker) Unsubscribe(ctx context.Context, id string) error {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.impl == nil {
		return errors.New("broker not initialized")
	}
	return b.impl.Unsubscribe(ctx, id)
}

// Close closes the underlying PubSub implementation.
func (b *Broker) Close() error {
	b.mu.Lock() // Use Lock for closing
	defer b.mu.Unlock()
	if b.impl == nil {
		return nil // Already closed or never initialized
	}
	err := b.impl.Close()
	b.impl = nil // Set impl to nil after closing
	return err
}

// GetImpl returns the underlying PubSub implementation (for testing or specific needs).
// Be cautious when using the returned implementation directly.
func (b *Broker) GetImpl() PubSub {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.impl
}
