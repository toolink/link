package pubsub

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type subscriptionType int

const (
	channelSubscription subscriptionType = iota
	functionSubscription
)

// messagePayload represents the data passed through the system.
type messagePayload []any

// subscription represents a consumer attached to a topic.
type subscription struct {
	id            string
	topicName     string
	subType       subscriptionType
	config        subscriptionConfig
	handlerFunc   reflect.Value         // For function subscriptions
	handlerType   reflect.Type          // For validating function args
	consumeChan   chan<- messagePayload // For channel subscriptions (owned by user)
	internalChan  chan messagePayload   // Internal buffer queue fed by topic dispatcher
	stopOnce      sync.Once
	stopChan      chan struct{}   // Signals workers to stop
	workerWg      sync.WaitGroup  // Tracks this subscription's workers
	brokerWg      *sync.WaitGroup // Broker's WaitGroup for shutdown coordination
	topicStopChan <-chan struct{} // To know if the parent topic is stopping
}

// newSubscription creates a common subscription base.
func newSubscription(topicName string, topicStopChan <-chan struct{}, brokerWg *sync.WaitGroup, opts ...SubscriptionOption) *subscription {
	cfg := defaultSubscriptionConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	// Ensure channel subscriptions always have concurrency 1 internally
	// The user controls concurrency by how they read from the channel.
	// if subType == channelSubscription {
	//  cfg.concurrency = 1 // This logic needs to move to where type is known
	// }

	return &subscription{
		id:            uuid.NewString(),
		topicName:     topicName,
		config:        cfg,                                       // Apply options first
		internalChan:  make(chan messagePayload, cfg.bufferSize), // Use configured buffer size
		stopChan:      make(chan struct{}),
		brokerWg:      brokerWg,
		topicStopChan: topicStopChan,
	}
}

// start launches the worker goroutine(s) for the subscription.
func (s *subscription) start() {
	// Channel subscriptions effectively have 1 worker pushing to the user channel.
	// Function subscriptions use configured concurrency.
	numWorkers := 1
	if s.subType == functionSubscription {
		numWorkers = s.config.concurrency
	}

	s.workerWg.Add(numWorkers)
	s.brokerWg.Add(numWorkers) // Add to broker's WG for overall shutdown

	l := log.With().
		Str("subscription_id", s.id).
		Str("topic", s.topicName).
		Str("type", s.subType.String()).
		Int("workers", numWorkers).
		Int("buffer", s.config.bufferSize).
		Logger()

	l.Debug().Msg("starting subscription workers")

	for i := 0; i < numWorkers; i++ {
		go s.runWorker(i, l)
	}
}

// runWorker is the main loop for a single worker goroutine.
func (s *subscription) runWorker(workerID int, l zerolog.Logger) {
	defer func() {
		s.workerWg.Done()
		s.brokerWg.Done() // Decrement broker's WG when worker exits
	}()

	workerLogger := l.With().Int("worker_id", workerID).Logger()
	workerLogger.Debug().Msg("subscription worker started")

	for {
		select {
		case <-s.stopChan:
			workerLogger.Debug().Msg("subscription worker stopping via stopChan")
			return
		case <-s.topicStopChan:
			workerLogger.Debug().Msg("subscription worker stopping via topicStopChan")
			return
		case msg, ok := <-s.internalChan:
			if !ok {
				workerLogger.Debug().Msg("subscription worker stopping because internalChan closed")
				return // Channel closed
			}
			s.processMessage(msg, workerLogger)
		}
	}
}

// processMessage handles a single message according to the subscription type.
func (s *subscription) processMessage(msg messagePayload, l zerolog.Logger) {
	l.Debug().Int("msg_args", len(msg)).Msg("worker processing message")
	switch s.subType {
	case channelSubscription:
		// Send to the user-provided channel.
		// If the user channel blocks, this worker blocks.
		// Use a select with stopChan/topicStopChan to allow cancellation.
		select {
		case s.consumeChan <- msg:
			// Message sent successfully to user channel
		case <-s.stopChan:
			l.Warn().Msg("dropping message: subscription stopping during send to user channel")
		case <-s.topicStopChan:
			l.Warn().Msg("dropping message: topic stopping during send to user channel")
		}

	case functionSubscription:
		s.callHandlerFunc(msg, l)
	}
}

// callHandlerFunc executes the registered function using reflection.
func (s *subscription) callHandlerFunc(payload messagePayload, l zerolog.Logger) {
	numArgs := s.handlerType.NumIn()
	if len(payload) != numArgs {
		l.Error().
			Int("expected_args", numArgs).
			Int("received_args", len(payload)).
			Interface("payload_preview", payload). // Log part of payload for debug
			Msg("argument count mismatch for handler function. Skipping.")
		return
	}

	callArgs := make([]reflect.Value, numArgs)
	for i := 0; i < numArgs; i++ {
		argType := s.handlerType.In(i)
		providedVal := reflect.ValueOf(payload[i]) // payload[i] is 'any'

		// Check for nil and target type compatibility
		if payload[i] == nil {
			// Can we assign nil to the target type? (Interface, Ptr, Slice, Map, Chan, Func)
			kind := argType.Kind()
			if kind == reflect.Interface || kind == reflect.Ptr || kind == reflect.Slice ||
				kind == reflect.Map || kind == reflect.Chan || kind == reflect.Func {
				callArgs[i] = reflect.Zero(argType) // Use Zero value for nil-able types
			} else {
				l.Error().
					Int("arg_index", i).
					Str("expected_type", argType.String()).
					Msg("argument type mismatch: cannot assign nil to non-nilable type. Skipping.")
				return
			}
		} else if !providedVal.Type().AssignableTo(argType) {
			// Check if types are assignable (covers basic types, interfaces, etc.)
			l.Error().
				Int("arg_index", i).
				Str("expected_type", argType.String()).
				Str("received_type", providedVal.Type().String()).
				Msg("argument type mismatch for handler function. Skipping.")
			return
		} else {
			callArgs[i] = providedVal
		}
	}

	// Call the function with panic recovery
	func() {
		defer func() {
			if r := recover(); r != nil {
				l.Error().Interface("panic_info", r).Msg("panic recovered during handler function execution")
				// Maybe add stack trace here in real-world scenario
			}
		}()
		l.Debug().Msg("calling handler function")
		_ = s.handlerFunc.Call(callArgs) // Ignore return values
		l.Debug().Msg("handler function call complete")
	}()
}

// validateHandlerFunc checks if the provided function is valid.
func validateHandlerFunc(handler any) (reflect.Value, reflect.Type, error) {
	if handler == nil {
		return reflect.Value{}, nil, fmt.Errorf("handler function cannot be nil")
	}
	handlerVal := reflect.ValueOf(handler)
	handlerType := handlerVal.Type()

	if handlerType.Kind() != reflect.Func {
		return reflect.Value{}, nil, fmt.Errorf("handler must be a function, got %s", handlerType.Kind())
	}

	if handlerType.NumOut() > 0 {
		log.Warn().Str("func_type", handlerType.String()).Msg("handler function has return values, which will be ignored")
	}

	return handlerVal, handlerType, nil
}

// stop signals the subscription workers to terminate and waits for them.
func (s *subscription) stop() {
	s.stopOnce.Do(func() {
		l := log.With().Str("subscription_id", s.id).Str("topic", s.topicName).Logger()
		l.Debug().Msg("stopping subscription")
		close(s.stopChan)     // 1. Signal workers to stop reading internalChan
		s.workerWg.Wait()     // 2. Wait for all workers to finish processing in-flight messages
		close(s.internalChan) // 3. Close internal channel AFTER workers are done
		l.Debug().Msg("subscription stopped")
	})
}

// String representation for logging subscription type
func (st subscriptionType) String() string {
	switch st {
	case channelSubscription:
		return "channel"
	case functionSubscription:
		return "function"
	default:
		return "unknown"
	}
}

// ID returns the unique identifier of the subscription.
func (s *subscription) ID() string {
	return s.id
}
