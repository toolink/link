package pubsub

import (
	"context"
	"errors"
	"sync"

	"github.com/rs/zerolog/log"
)

var (
	errMemoryPubSubClosed = errors.New("pubsub: memory pubsub is closed")
)

// MemoryPubSub implements the PubSub interface using in-memory data structures.
type MemoryPubSub struct {
	mu      sync.RWMutex
	closed  bool
	topics  map[string]map[string]*Subscription // topic -> subID -> Subscription
	subs    map[string]*Subscription            // subID -> Subscription (for fast unsubscribe)
	closeWg sync.WaitGroup                      // Waits for publish operations during close
}

// NewMemoryPubSub creates a new in-memory PubSub instance.
func NewMemoryPubSub() PubSub {
	return &MemoryPubSub{
		topics: make(map[string]map[string]*Subscription),
		subs:   make(map[string]*Subscription),
	}
}

// Publish sends messages to the given topic, waiting for all subscribers.
func (m *MemoryPubSub) Publish(ctx context.Context, topic string, messages ...*Message) error {
	m.mu.RLock()
	if m.closed {
		m.mu.RUnlock()
		return errMemoryPubSubClosed
	}

	subsToDeliver := m.getSubscriptionsForTopicLocked(topic)
	m.mu.RUnlock() // Release read lock before potentially long delivery

	if len(subsToDeliver) == 0 {
		return nil // No subscribers for this topic
	}

	// Add to wait group only if we are potentially closing
	m.mu.RLock()
	isClosing := m.closed // Check again in case Close was called
	if !isClosing {
		m.closeWg.Add(1)
		defer m.closeWg.Done()
	}
	m.mu.RUnlock()

	if isClosing {
		return errMemoryPubSubClosed
	}

	var wg sync.WaitGroup
	wg.Add(len(subsToDeliver))
	errChan := make(chan error, len(subsToDeliver)) // Buffered channel to collect errors non-blockingly

	for _, sub := range subsToDeliver {
		go func(s *Subscription) {
			defer wg.Done()
			// Create a new context for delivery to avoid canceling sibling deliveries
			// if one subscriber's context gets canceled somehow (though unlikely here).
			// The main context cancellation is checked below.
			deliveryCtx := ctx // Use the original context for cancellation signal

			err := s.deliver(deliveryCtx, messages, false) // false for Publish (blocking)
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, errSubscriptionClosed) {
				// Log error but don't necessarily stop other publishes
				log.Error().Err(err).Str("subscription_id", s.ID).Str("topic", topic).Msg("failed to deliver message")
				// Collect significant errors (optional, maybe just log?)
				select {
				case errChan <- err:
				default: // Avoid blocking if channel is full (shouldn't happen with correct size)
				}
			}
		}(sub)
	}

	// Wait for all deliveries or context cancellation
	waitDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitDone)
	}()

	select {
	case <-waitDone:
		// All deliveries completed
		close(errChan) // Close errChan after wg is done
		// Check collected errors (return the first one?)
		if err := <-errChan; err != nil {
			return err // Return the first encountered significant error
		}
		return nil
	case <-ctx.Done():
		// Context canceled before all deliveries finished
		log.Warn().Str("topic", topic).Msg("publish context canceled before all subscribers finished")
		return ctx.Err()
	}
}

// TryPublish attempts to send messages, ignoring subscribers that are not ready.
func (m *MemoryPubSub) TryPublish(ctx context.Context, topic string, messages ...*Message) error {
	m.mu.RLock()
	if m.closed {
		m.mu.RUnlock()
		return errMemoryPubSubClosed
	}

	subsToDeliver := m.getSubscriptionsForTopicLocked(topic)
	m.mu.RUnlock()

	if len(subsToDeliver) == 0 {
		return nil
	}

	// No need for closeWg here as TryPublish is non-blocking

	// Use a WaitGroup just to launch goroutines, not to wait for completion
	var wg sync.WaitGroup
	wg.Add(len(subsToDeliver))

	for _, sub := range subsToDeliver {
		go func(s *Subscription) {
			defer wg.Done()
			// Use background context for delivery attempt, main context check is implicit in TryPublish nature
			err := s.deliver(context.Background(), messages, true) // true for TryPublish (non-blocking)
			if err != nil && !errors.Is(err, errSubscriptionClosed) {
				// Log errors from TryPublish attempts, but don't return them
				log.Warn().Err(err).Str("subscription_id", s.ID).Str("topic", topic).Msg("failed to try-deliver message")
			}
		}(sub)
	}

	// We don't wait for wg.Wait() here for TryPublish

	// Check context cancellation immediately (optional, as delivery is non-blocking)
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

// Subscribe creates a new subscription.
func (m *MemoryPubSub) Subscribe(ctx context.Context, topic string, handler any, opts ...Option) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return "", errMemoryPubSubClosed
	}

	sub, err := newSubscription(topic, handler, opts...)
	if err != nil {
		return "", err
	}

	if _, ok := m.topics[topic]; !ok {
		m.topics[topic] = make(map[string]*Subscription)
	}
	m.topics[topic][sub.ID] = sub
	m.subs[sub.ID] = sub

	log.Debug().Str("subscription_id", sub.ID).Str("topic", topic).Msg("new subscription created")
	return sub.ID, nil
}

// Unsubscribe removes a subscription.
func (m *MemoryPubSub) Unsubscribe(ctx context.Context, id string) error {
	m.mu.Lock()
	// No need to check m.closed here, allow unsubscribe even if closing/closed

	sub, ok := m.subs[id]
	if !ok {
		m.mu.Unlock()
		return nil // Subscription already gone
	}

	// Remove from global map
	delete(m.subs, id)

	// Remove from topic map
	if topicSubs, ok := m.topics[sub.Topic]; ok {
		delete(topicSubs, id)
		if len(topicSubs) == 0 {
			delete(m.topics, sub.Topic) // Clean up empty topic map
		}
	}
	m.mu.Unlock() // Unlock before closing subscription

	// Close the subscription outside the lock
	err := sub.Close()
	if err != nil {
		log.Error().Err(err).Str("subscription_id", id).Msg("error closing subscription during unsubscribe")
		// Continue even if closing fails
	}

	log.Debug().Str("subscription_id", id).Str("topic", sub.Topic).Msg("subscription removed")
	return nil
}

// Close shuts down the MemoryPubSub instance.
func (m *MemoryPubSub) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil // Already closed
	}
	m.closed = true

	// Capture subscriptions to close outside the lock
	subsToClose := make([]*Subscription, 0, len(m.subs))
	for _, sub := range m.subs {
		subsToClose = append(subsToClose, sub)
	}

	// Clear internal maps immediately under lock
	m.topics = make(map[string]map[string]*Subscription) // Or set to nil
	m.subs = make(map[string]*Subscription)              // Or set to nil

	m.mu.Unlock() // Unlock before waiting and closing subs

	log.Info().Msg("memory pubsub closing...")

	// Wait for ongoing Publish operations to complete
	done := make(chan struct{})
	go func() {
		m.closeWg.Wait()
		close(done)
	}()

	// Set a timeout for waiting? For now, wait indefinitely.
	<-done
	log.Debug().Msg("ongoing publish operations completed")

	// Close all subscriptions
	var closeWg sync.WaitGroup
	closeWg.Add(len(subsToClose))
	for _, sub := range subsToClose {
		go func(s *Subscription) {
			defer closeWg.Done()
			if err := s.Close(); err != nil {
				log.Error().Err(err).Str("subscription_id", s.ID).Msg("error closing subscription during pubsub close")
			}
		}(sub)
	}
	closeWg.Wait()

	log.Info().Msg("memory pubsub closed")
	return nil
}

// getSubscriptionsForTopicLocked returns a slice of subscriptions for a topic.
// Requires RLock to be held.
func (m *MemoryPubSub) getSubscriptionsForTopicLocked(topic string) []*Subscription {
	topicSubs, ok := m.topics[topic]
	if !ok {
		return nil
	}
	// Create a copy to avoid holding lock during delivery
	subs := make([]*Subscription, 0, len(topicSubs))
	for _, sub := range topicSubs {
		subs = append(subs, sub)
	}
	return subs
}

// Ensure MemoryPubSub implements PubSub interface
var _ PubSub = (*MemoryPubSub)(nil)
