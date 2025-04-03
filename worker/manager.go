package worker

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

// ConsumerManager manages multiple subscribers within the application.
type ConsumerManager struct {
	rdb         redis.Cmdable
	mu          sync.Mutex
	subscribers map[string]*subscriber // Key: topic
	wg          sync.WaitGroup
	shutdown    chan struct{}
	running     bool
}

// NewConsumerManager creates a new ConsumerManager.
func NewConsumerManager(rdb redis.Cmdable) *ConsumerManager {
	return &ConsumerManager{
		rdb:         rdb,
		subscribers: make(map[string]*subscriber),
		shutdown:    make(chan struct{}),
		running:     true,
	}
}

// Subscribe registers a handler for a topic (Redis List).
// 'handler' must be a function `func(T1, T2, ...)` or a channel `chan<- []any` or `chan any`.
// Only one subscriber per topic is generally recommended with BRPOP unless coordinated externally.
func (cm *ConsumerManager) Subscribe(topic string, handler any, opts ...SubscriptionOption) (*subscriber, error) {
	if topic == "" {
		return nil, errors.New("topic cannot be empty")
	}
	if handler == nil {
		return nil, errors.New("handler cannot be nil")
	}

	cfg := defaultSubscriptionOptions()
	for _, opt := range opts {
		opt(&cfg)
	}

	sub := &subscriber{
		rdb:         cm.rdb,
		topic:       topic,
		handler:     handler,
		opts:        cfg,
		processChan: make(chan []byte, cfg.bufferSize), // Receives raw bytes from Redis poller
		stopChan:    make(chan struct{}),
		managerWg:   &cm.wg,           // Share manager's WaitGroup
		internalWg:  sync.WaitGroup{}, // WG for this subscriber's internal goroutines
	}

	// Determine handler type
	handlerVal := reflect.ValueOf(handler)
	handlerType := handlerVal.Type()

	if handlerType.Kind() == reflect.Chan && handlerType.ChanDir()&reflect.SendDir != 0 {
		sub.method = Channel
		elemType := handlerType.Elem()
		// Expecting chan<- []any or chan []any or chan any
		isValidChan := false
		if elemType.Kind() == reflect.Slice {
			sliceElem := elemType.Elem()
			// Check for []any (which is slice of interface{})
			if sliceElem.Kind() == reflect.Interface && sliceElem.Name() == "" {
				isValidChan = true
			}
		} else if elemType.Kind() == reflect.Interface && elemType.Name() == "" { // Check for 'any'
			isValidChan = true
		}

		if !isValidChan {
			return nil, fmt.Errorf("channel handler must be of type chan<- []any, chan []any or chan any (receiving []any), got %s", handlerType)
		}
		sub.targetChan = handlerVal
		sub.chanElemType = elemType // Store chan []any or chan any
	} else if handlerType.Kind() == reflect.Func {
		sub.method = Function
		sub.handlerFunc = handlerVal
		sub.handlerType = handlerType
		if handlerVal.IsNil() {
			return nil, errors.New("handler function cannot be nil")
		}
	} else {
		return nil, fmt.Errorf("handler must be a function or a sendable channel, got %T", handler)
	}

	// Register subscriber internally
	cm.mu.Lock()
	if !cm.running {
		cm.mu.Unlock()
		return nil, errors.New("consumer manager is not running")
	}
	// Using simple BRPOP, having multiple subscribers on the *same topic* within the same manager
	// will lead to messages being distributed randomly between them. This might be desired or not.
	// If only one logical consumer process is intended per topic, ensure only one Subscribe call per topic.
	if _, exists := cm.subscribers[topic]; exists {
		// Allow multiple subscribers per topic if needed, they will compete for messages via BRPOP
		log.Warn().Str("topic", topic).Msg("subscribing to a topic with existing subscriber(s), they will compete for messages")
	}
	cm.subscribers[topic] = sub // Storing by topic; if multiple allowed, need a different key or list
	cm.mu.Unlock()

	// Start the subscriber's main loop in a goroutine
	cm.wg.Add(1) // Add to the main manager WG for the subscriber's primary poller loop
	go sub.run()

	log.Info().Str("topic", topic).Str("mode", fmt.Sprintf("%v", cfg.mode)).Int("concurrency", cfg.concurrency).Dur("block_time", cfg.blockTime).Msg("subscriber started polling list")

	return sub, nil
}

// Unsubscribe stops and removes a specific subscriber.
// Note: If multiple subscribers exist for the topic, this only removes one.
func (cm *ConsumerManager) Unsubscribe(subToUnsubscribe *subscriber) error {
	if subToUnsubscribe == nil {
		return errors.New("cannot unsubscribe nil subscriber")
	}

	cm.mu.Lock()
	// Allow unsubscribe even during shutdown
	sub, ok := cm.subscribers[subToUnsubscribe.topic] // Simple lookup by topic
	if !ok || sub != subToUnsubscribe {
		// If multiple subs per topic were allowed, this check needs refinement
		// (e.g., store a list per topic or use a more unique key)
		cm.mu.Unlock()
		log.Warn().Str("topic", subToUnsubscribe.topic).Msg("unsubscribe called for subscriber not found or mismatched")
		return nil // Or return error
	}

	delete(cm.subscribers, subToUnsubscribe.topic)
	cm.mu.Unlock() // Release lock before potentially blocking stop

	sub.stop() // Signal the subscriber to stop and wait for its goroutines

	log.Info().Str("topic", sub.topic).Msg("subscriber stopped")
	return nil
}

// Shutdown signals all subscribers to stop and waits for them to finish.
func (cm *ConsumerManager) Shutdown(ctx context.Context) error {
	cm.mu.Lock()
	if !cm.running {
		cm.mu.Unlock()
		return errors.New("consumer manager already shut down")
	}
	cm.running = false
	close(cm.shutdown)

	subsToStop := make([]*subscriber, 0, len(cm.subscribers))
	for _, sub := range cm.subscribers {
		subsToStop = append(subsToStop, sub)
	}
	cm.subscribers = make(map[string]*subscriber) // Clear map
	cm.mu.Unlock()

	log.Info().Int("subscriber_count", len(subsToStop)).Msg("shutting down subscribers...")

	// Stop subscribers concurrently
	var stopWg sync.WaitGroup
	for _, sub := range subsToStop {
		stopWg.Add(1)
		go func(s *subscriber) {
			defer stopWg.Done()
			s.stop() // Signal and wait for the individual subscriber
		}(sub)
	}
	stopWg.Wait() // Wait for all stop() calls to complete signaling AND internal waits

	// Now wait for all subscriber goroutines (main loop + handlers) to exit via the manager's WG
	waitChan := make(chan struct{})
	go func() {
		cm.wg.Wait() // Wait for Add(1)/Done() calls associated with the manager WG
		close(waitChan)
	}()

	log.Info().Msg("waiting for all consumer poller goroutines to exit...")

	select {
	case <-waitChan:
		log.Info().Msg("consumer manager shutdown complete")
		return nil
	case <-ctx.Done():
		log.Error().Err(ctx.Err()).Msg("consumer manager shutdown timed out waiting for pollers")
		return fmt.Errorf("shutdown timed out: %w", ctx.Err())
	}
}
