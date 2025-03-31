package pubsub

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// topic manages subscriptions for a specific topic name.
type topic struct {
	name          string
	brokerConfig  brokerConfig
	messageChan   chan messagePayload // Incoming messages for this topic
	subscriptions map[string]*subscription
	subMu         sync.RWMutex // Protects the subscriptions map
	stopOnce      sync.Once
	stopChan      chan struct{}   // Signals topic dispatcher (& subs) to stop
	dispatcherWg  sync.WaitGroup  // Tracks the dispatcher goroutine
	brokerWg      *sync.WaitGroup // Broker's WaitGroup
}

// newTopic creates and starts a topic manager.
func newTopic(name string, brokerCfg brokerConfig, brokerWg *sync.WaitGroup) *topic {
	t := &topic{
		name:          name,
		brokerConfig:  brokerCfg,
		messageChan:   make(chan messagePayload, brokerCfg.topicBufferSize),
		subscriptions: make(map[string]*subscription),
		stopChan:      make(chan struct{}),
		brokerWg:      brokerWg,
	}
	t.startDispatcher() // Start the single dispatcher goroutine
	return t
}

// startDispatcher runs the SINGLE goroutine responsible for fanning out messages
// SEQUENTIALLY to subscription buffers.
func (t *topic) startDispatcher() {
	t.dispatcherWg.Add(1)
	t.brokerWg.Add(1) // Add to broker's WG

	go func() {
		defer t.brokerWg.Done() // Decrement broker's WG when dispatcher exits

		l := log.With().Str("topic", t.name).Logger()
		l.Debug().Msg("topic dispatcher started")

		defer func() {
			// Cleanup: Drain messageChan after stopping to allow blocked publishers to potentially exit.
			// This happens *after* the loop exits.
			t.dispatcherWg.Done()
			l.Debug().Msg("draining remaining messages in topic channel post-shutdown")
			for range t.messageChan {
				// Discard messages if any arrived after stop signal but before channel close
			}
			l.Debug().Msg("topic dispatcher finished")
		}()

		for {
			select {
			case <-t.stopChan:
				l.Debug().Msg("topic dispatcher stopping")
				return // Exit the loop
			case msg, ok := <-t.messageChan:
				if !ok {
					l.Debug().Msg("topic dispatcher stopping because messageChan closed")
					return // Should ideally happen only via Shutdown
				}
				t.dispatchMessageSequentially(msg, l)
			}
		}
	}()
}

// dispatchMessageSequentially sends a message to all active subscriptions' internal buffers.
// It iterates sequentially to avoid creating goroutines per message per subscriber.
func (t *topic) dispatchMessageSequentially(msg messagePayload, l zerolog.Logger) {
	t.subMu.RLock() // Read lock to get the list of subscriptions

	// Optimization: If no subscribers, don't even copy
	if len(t.subscriptions) == 0 {
		t.subMu.RUnlock()
		l.Debug().Msg("no active subscriptions for message, dropping.")
		return
	}

	// Create a snapshot of subscriptions to process under read lock
	subsToNotify := make([]*subscription, 0, len(t.subscriptions))
	for _, sub := range t.subscriptions {
		subsToNotify = append(subsToNotify, sub)
	}
	t.subMu.RUnlock() // Release lock before potentially blocking sends

	l.Debug().Int("sub_count", len(subsToNotify)).Msg("dispatching message sequentially to subscription buffers")

	// Iterate through the snapshot sequentially
	for _, sub := range subsToNotify {
		subL := l.With().Str("subscription_id", sub.id).Logger()

		// Use a timer for the send attempt to the subscription's internal buffer
		// This prevents one slow/full subscriber from blocking the dispatcher indefinitely.
		timer := time.NewTimer(sub.config.dispatchTimeout)

		select {
		case sub.internalChan <- msg:
			// Successfully sent to buffer
			subL.Debug().Msg("message queued to subscription buffer")
		case <-sub.stopChan: // Check if the specific subscription is stopping
			subL.Warn().Msg("failed to queue message: subscription is stopping")
		case <-t.stopChan: // Check if the entire topic is stopping
			subL.Warn().Msg("failed to queue message: topic is stopping")
			// If topic stops, we might as well stop trying to dispatch this message
			if !timer.Stop() {
				<-timer.C
			} // Drain timer
			return
		case <-timer.C:
			// Timeout occurred - subscription buffer likely full or workers blocked
			subL.Error().
				Dur("timeout", sub.config.dispatchTimeout).
				Int("buffer_cap", cap(sub.internalChan)).
				Int("buffer_len", len(sub.internalChan)).
				Msg("failed to queue message for subscription: buffer full or worker blocked. Skipping subscription for this message.")
			// Action: Logged error, message is dropped for THIS subscriber. Continue to next subscriber.
		}
		// Ensure timer is stopped and drained if it didn't fire or wasn't stopped already
		if !timer.Stop() {
			// If timer.Stop() returns false, the timer either already fired (handled in case <-timer.C)
			// or was already stopped. If it fired, we need to drain its channel.
			select {
			case <-timer.C: // Drain channel if it fired
			default: // Already drained or wasn't fired
			}
		}
	}
}

// addSubscription registers a new subscription and starts it.
func (t *topic) addSubscription(sub *subscription) {
	t.subMu.Lock()
	t.subscriptions[sub.id] = sub
	t.subMu.Unlock()
	sub.start() // Start the subscription workers *after* adding to map
	log.Info().Str("topic", t.name).Str("subscription_id", sub.id).Msg("subscription added and started")
}

// removeSubscription stops and removes a subscription. Returns true if found.
func (t *topic) removeSubscription(subID string) bool {
	t.subMu.Lock()
	sub, exists := t.subscriptions[subID]
	if exists {
		delete(t.subscriptions, subID) // Remove from map first
	}
	t.subMu.Unlock() // Unlock before potentially long stop() call

	if exists {
		log.Info().Str("topic", t.name).Str("subscription_id", subID).Msg("removing subscription")
		sub.stop() // Stop the subscription workers and wait for them
		log.Info().Str("topic", t.name).Str("subscription_id", subID).Msg("subscription removed and stopped")
		return true
	}
	log.Warn().Str("topic", t.name).Str("subscription_id", subID).Msg("attempted to remove non-existent subscription")
	return false
}

// stop signals the topic dispatcher and all its subscriptions to terminate.
func (t *topic) stop() {
	t.stopOnce.Do(func() {
		l := log.With().Str("topic", t.name).Logger()
		l.Info().Msg("stopping topic")

		// 1. Signal dispatcher and subscriptions (via topicStopChan passed to them)
		close(t.stopChan)

		// 2. Wait for the dispatcher goroutine to finish processing any message it already read
		t.dispatcherWg.Wait()
		l.Debug().Msg("topic dispatcher finished")

		// 3. Stop all remaining subscriptions concurrently (they might have finished already if unsubscribed)
		t.subMu.RLock()
		subsToStop := make([]*subscription, 0, len(t.subscriptions))
		for _, sub := range t.subscriptions {
			subsToStop = append(subsToStop, sub)
		}
		// Clear map while holding lock? Maybe not necessary if only stop() modifies it after init.
		// Let's clear it to be safe regarding memory.
		t.subscriptions = make(map[string]*subscription)
		t.subMu.RUnlock() // Unlock before calling sub.stop()

		var wg sync.WaitGroup
		wg.Add(len(subsToStop))
		for _, sub := range subsToStop {
			go func(s *subscription) {
				defer wg.Done()
				s.stop() // This waits for the subscription's workers
			}(sub)
		}
		wg.Wait()
		l.Debug().Msg("all associated subscriptions stopped")

		// 4. Close the main message channel *after* dispatcher is stopped.
		// The dispatcher drain logic handles any remaining messages.
		close(t.messageChan)

		l.Info().Msg("topic stopped successfully")
	})
}

// publish sends a message to the topic's main channel, respecting context.
func (t *topic) publish(ctx context.Context, msg messagePayload) error {
	// Check if topic is stopped first (non-blocking)
	select {
	case <-t.stopChan:
		return fmt.Errorf("topic %q is stopped", t.name)
	default:
	}

	// Attempt to send with context awareness
	select {
	case t.messageChan <- msg:
		return nil // Success
	case <-t.stopChan: // Check again in case it stopped while trying to send
		return fmt.Errorf("topic %q stopped during publish", t.name)
	case <-ctx.Done():
		log.Warn().Str("topic", t.name).Err(ctx.Err()).Msg("context cancelled or timed out during publish")
		return fmt.Errorf("publishing to topic %q failed: %w", t.name, ctx.Err())
	}
}
