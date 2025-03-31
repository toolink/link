package pubsub

import (
	"context"
	"fmt"
	"sync"

	"github.com/rs/zerolog/log"
)

// Broker manages topics and facilitates publishing and subscribing.
type Broker struct {
	config        brokerConfig
	topics        map[string]*topic
	mu            sync.RWMutex // Protects the topics map
	stopOnce      sync.Once
	stopChan      chan struct{}     // Signals global shutdown (mainly for broker state checks)
	wg            sync.WaitGroup    // Tracks ALL running goroutines (dispatchers, workers)
	subTopicMap   map[string]string // Optional: Map subscription ID -> topic name for faster Unsubscribe
	subTopicMapMu sync.RWMutex      // Mutex for the optional map
}

// NewBroker creates a new message broker.
func New(opts ...BrokerOption) *Broker {
	cfg := defaultBrokerConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	return &Broker{
		config:      cfg,
		topics:      make(map[string]*topic),
		stopChan:    make(chan struct{}),
		subTopicMap: make(map[string]string), // Initialize the map
	}
}

// getOrCreateTopic retrieves an existing topic or creates a new one.
// Assumes broker's write lock is held externally or uses internal locking.
func (b *Broker) getOrCreateTopic(topicName string) *topic {
	// Check read lock first
	b.mu.RLock()
	t, exists := b.topics[topicName]
	b.mu.RUnlock()
	if exists {
		return t
	}

	// Acquire write lock to create
	b.mu.Lock()
	defer b.mu.Unlock() // Ensure unlock

	// Double-check after acquiring write lock
	t, exists = b.topics[topicName]
	if exists {
		return t
	}

	log.Info().Str("topic", topicName).Msg("creating new topic")
	t = newTopic(topicName, b.config, &b.wg) // Pass broker's WaitGroup
	b.topics[topicName] = t
	return t
}

// Pub publishes messages to a topic. Fire and forget. Errors are logged.
func (b *Broker) Pub(topicName string, msgs ...any) {
	// Use a background context for fire-and-forget
	err := b.PubCtx(context.Background(), topicName, msgs...)
	if err != nil {
		// Log the error, but don't return it as Pub is fire-and-forget
		log.Error().Err(err).Str("topic", topicName).Int("num_msgs", len(msgs)).Msg("error during Pub")
	}
}

// PubCtx publishes messages to a topic with context awareness.
func (b *Broker) PubCtx(ctx context.Context, topicName string, msgs ...any) error {
	// Basic validation
	if topicName == "" {
		return fmt.Errorf("topic name cannot be empty")
	}
	if len(msgs) == 0 {
		// Allow publishing empty message? Decide based on use case. Let's allow it for now.
		// return fmt.Errorf("must publish at least one message")
	}

	// Check if broker is shutting down early
	select {
	case <-b.stopChan:
		return fmt.Errorf("broker is shutting down")
	default:
	}

	// Create payload slice
	payload := make(messagePayload, len(msgs))
	copy(payload, msgs) // Copy msgs into the slice

	// Get or create topic
	t := b.getOrCreateTopic(topicName) // This handles locking internally

	// Publish via the topic's method
	err := t.publish(ctx, payload)
	if err != nil {
		return fmt.Errorf("failed to publish to topic %q: %w", topicName, err)
	}
	log.Debug().Str("topic", topicName).Int("num_msgs", len(msgs)).Msg("message published")
	return nil
}

// SubscribeChannel subscribes to a topic using a channel.
// Returns the channel to receive messages on and the subscription ID.
func (b *Broker) SubscribeChannel(topicName string, opts ...SubscriptionOption) (chan messagePayload, string, error) {
	if topicName == "" {
		return nil, "", fmt.Errorf("topic name cannot be empty")
	}
	// Check if broker is shutting down early
	select {
	case <-b.stopChan:
		return nil, "", fmt.Errorf("broker is shutting down")
	default:
	}

	t := b.getOrCreateTopic(topicName)

	// Create subscription configuration
	sub := newSubscription(topicName, t.stopChan, &b.wg, opts...) // Pass topic stop chan & broker WG
	sub.subType = channelSubscription
	// Force concurrency 1 for channel subscriptions in the internal worker
	sub.config.concurrency = 1

	// Create the user-facing channel (unbuffered recommended, user controls consumption)
	userChan := make(chan messagePayload) // Let user decide buffering if needed outside
	sub.consumeChan = userChan

	// Add the subscription to the topic (which also starts its workers)
	t.addSubscription(sub)

	// Add to the subID -> topicName map for faster unsubscribe
	b.addSubMapping(sub.ID(), topicName)

	log.Info().Str("topic", topicName).Str("subscription_id", sub.id).Str("type", "channel").Msg("channel subscription created")
	return userChan, sub.ID(), nil
}

// SubscribeFunc subscribes to a topic using a handler function.
// Returns the subscription ID.
func (b *Broker) SubscribeFunc(topicName string, handlerFunc any, opts ...SubscriptionOption) (string, error) {
	if topicName == "" {
		return "", fmt.Errorf("topic name cannot be empty")
	}
	// Check if broker is shutting down early
	select {
	case <-b.stopChan:
		return "", fmt.Errorf("broker is shutting down")
	default:
	}

	// Validate the handler function signature BEFORE creating topic/sub
	handlerVal, handlerType, err := validateHandlerFunc(handlerFunc)
	if err != nil {
		return "", fmt.Errorf("invalid handler function for topic %q: %w", topicName, err)
	}

	t := b.getOrCreateTopic(topicName)

	// Create subscription configuration
	sub := newSubscription(topicName, t.stopChan, &b.wg, opts...) // Pass topic stop chan & broker WG
	sub.subType = functionSubscription
	sub.handlerFunc = handlerVal
	sub.handlerType = handlerType
	// Concurrency is set by options, default is 1

	// Add the subscription to the topic (which also starts its workers)
	t.addSubscription(sub)

	// Add to the subID -> topicName map
	b.addSubMapping(sub.ID(), topicName)

	log.Info().Str("topic", topicName).Str("subscription_id", sub.id).Str("type", "function").Int("concurrency", sub.config.concurrency).Msg("function subscription created")
	return sub.ID(), nil
}

// Unsubscribe removes a subscription by its ID.
// Returns true if the subscription was found and removal was initiated.
func (b *Broker) Unsubscribe(subscriptionID string) bool {
	// Check if broker is shutting down early
	select {
	case <-b.stopChan:
		log.Warn().Str("subscription_id", subscriptionID).Msg("cannot unsubscribe: broker is shutting down")
		return false
	default:
	}

	b.subTopicMapMu.RLock()
	topicName, found := b.subTopicMap[subscriptionID]
	b.subTopicMapMu.RUnlock()

	if !found {
		log.Warn().Str("subscription_id", subscriptionID).Msg("unsubscribe failed: subscription ID not found in mapping")
		return false
	}

	// Now find the topic (it should exist if the mapping exists)
	b.mu.RLock()
	t, topicExists := b.topics[topicName]
	b.mu.RUnlock()

	if !topicExists {
		// This indicates an inconsistency, potentially a race condition or bug
		log.Error().Str("subscription_id", subscriptionID).Str("mapped_topic", topicName).Msg("unsubscribe inconsistency: subscription mapped to non-existent topic")
		b.removeSubMapping(subscriptionID) // Clean up inconsistent mapping
		return false
	}

	// Ask the topic to remove the subscription
	removed := t.removeSubscription(subscriptionID)

	if removed {
		// Clean up the mapping only if successfully removed from topic
		b.removeSubMapping(subscriptionID)
		log.Info().Str("subscription_id", subscriptionID).Str("topic", topicName).Msg("unsubscribe successful")
	} else {
		// removeSubscription already logs if it couldn't find it within the topic
		log.Warn().Str("subscription_id", subscriptionID).Str("topic", topicName).Msg("unsubscribe failed: topic could not remove subscription (potentially already removed?)")
	}

	return removed
}

// Helper to add subscription mapping safely
func (b *Broker) addSubMapping(subID, topicName string) {
	b.subTopicMapMu.Lock()
	defer b.subTopicMapMu.Unlock()
	b.subTopicMap[subID] = topicName
}

// Helper to remove subscription mapping safely
func (b *Broker) removeSubMapping(subID string) {
	b.subTopicMapMu.Lock()
	defer b.subTopicMapMu.Unlock()
	delete(b.subTopicMap, subID)
}

// Shutdown gracefully stops the broker, all topics, and subscriptions.
// It waits for all processing goroutines to finish or the context to expire.
func (b *Broker) Shutdown(ctx context.Context) error {
	var err error
	b.stopOnce.Do(func() {
		log.Info().Msg("broker shutdown initiated")
		close(b.stopChan) // 1. Signal global shutdown intention

		// 2. Get a snapshot of topics to stop under lock
		b.mu.RLock()
		topicsToStop := make([]*topic, 0, len(b.topics))
		for _, t := range b.topics {
			topicsToStop = append(topicsToStop, t)
		}
		// Clear the topics map now? Or let stop() handle cleanup?
		// Let's clear it here to prevent new subs/pubs to stopped topics via getOrCreateTopic races.
		b.topics = make(map[string]*topic)
		b.mu.RUnlock() // Release lock before stopping topics

		// Clear the subscription mapping too
		b.subTopicMapMu.Lock()
		b.subTopicMap = make(map[string]string)
		b.subTopicMapMu.Unlock()

		log.Debug().Int("topic_count", len(topicsToStop)).Msg("stopping all topics...")

		// 3. Stop all topics concurrently (each topic stop is synchronous internally)
		var stopWg sync.WaitGroup
		stopWg.Add(len(topicsToStop))
		for _, t := range topicsToStop {
			go func(topicToStop *topic) {
				defer stopWg.Done()
				topicToStop.stop() // This waits for the topic's dispatcher and subscriptions
			}(t)
		}
		stopWg.Wait() // Wait for all topic stop() calls to complete

		log.Info().Msg("all topics stopped, waiting for worker goroutines to exit...")
		// At this point, all stop signals have been sent, and we just need
		// to wait for the main broker WaitGroup which tracks all workers/dispatchers.
	})

	// 4. Wait for all goroutines tracked by b.wg to finish, or context timeout
	waitChan := make(chan struct{})
	go func() {
		b.wg.Wait() // Wait for ALL dispatchers and workers registered with the broker's WG
		close(waitChan)
	}()

	select {
	case <-waitChan:
		log.Info().Msg("broker shutdown complete")
		err = nil
	case <-ctx.Done():
		log.Error().Err(ctx.Err()).Msg("broker shutdown timed out waiting for workers")
		err = fmt.Errorf("broker shutdown timed out: %w", ctx.Err())
	}
	return err // Return error state
}
