package pubsub

import (
	"context"
	"errors"
	"reflect"
	"sync"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

var (
	errInvalidHandler     = errors.New("pubsub: handler must be a function or a channel")
	errHandlerArgs        = errors.New("pubsub: handler function signature mismatch")
	errChanTypeMismatch   = errors.New("pubsub: channel type mismatch")
	errSendToClosedChan   = errors.New("pubsub: attempt to send on closed channel")
	errSubscriptionClosed = errors.New("pubsub: subscription is closed")
)

// handlerType identifies the type of the subscription handler.
type handlerType int

const (
	handlerTypeInvalid handlerType = iota
	handlerTypeFunc
	handlerTypeChan
)

// Subscription represents a single subscription to a topic.
type Subscription struct {
	ID      string
	Topic   string
	options *SubscriptionOptions
	mu      sync.RWMutex
	closed  bool

	// Handler specific fields
	handlerType handlerType
	handlerFunc reflect.Value  // For function handlers
	handlerArgs []reflect.Type // Expected argument types for function handlers
	handlerChan reflect.Value  // For channel handlers
	chanType    reflect.Type   // Type of the channel element

	// For function handlers with concurrency > 1
	deliveryQueue chan []*Message    // Queue for messages to be processed by handler funcs
	wg            sync.WaitGroup     // Waits for handler goroutines to finish
	cancel        context.CancelFunc // To stop handler goroutines
}

// newSubscription creates a new subscription instance.
// It validates the handler and prepares it for message delivery.
func newSubscription(topic string, handler any, opts ...Option) (*Subscription, error) {
	options := DefaultSubscriptionOptions()
	options.Apply(opts...)

	s := &Subscription{
		ID:      uuid.NewString(),
		Topic:   topic,
		options: options,
	}

	handlerVal := reflect.ValueOf(handler)
	handlerTyp := handlerVal.Type()

	switch handlerTyp.Kind() {
	case reflect.Func:
		s.handlerType = handlerTypeFunc
		s.handlerFunc = handlerVal
		// Store argument types for validation during publish
		s.handlerArgs = make([]reflect.Type, handlerTyp.NumIn())
		for i := 0; i < handlerTyp.NumIn(); i++ {
			s.handlerArgs[i] = handlerTyp.In(i)
		}
		// Validate return types (optional, could enforce none)

		// If concurrency is needed, set up the delivery queue and workers
		if options.Concurrency > 1 {
			ctx, cancel := context.WithCancel(context.Background()) // Use background context for workers
			s.cancel = cancel
			s.deliveryQueue = make(chan []*Message, options.Concurrency*2) // Buffer size heuristic
			s.wg.Add(options.Concurrency)
			for i := 0; i < options.Concurrency; i++ {
				go s.runWorker(ctx)
			}
		}

	case reflect.Chan:
		if handlerTyp.ChanDir()&reflect.SendDir == 0 {
			return nil, errors.New("pubsub: channel must be sendable (chan<- T or chan T)")
		}
		s.handlerType = handlerTypeChan
		s.handlerChan = handlerVal
		s.chanType = handlerTyp.Elem() // Store element type T for chan T

	default:
		return nil, errInvalidHandler
	}

	return s, nil
}

// runWorker processes messages from the delivery queue for concurrent function handlers.
func (s *Subscription) runWorker(ctx context.Context) {
	defer s.wg.Done()
	for {
		select {
		case <-ctx.Done():
			log.Debug().Str("subscription_id", s.ID).Str("topic", s.Topic).Msg("worker shutting down")
			// Drain remaining messages? Or rely on Close to handle this?
			// For now, just exit. Messages published after Close starts might be lost.
			return
		case msgs := <-s.deliveryQueue:
			if msgs == nil { // Channel closed signal
				continue
			}
			s.invokeFuncHandler(msgs)
		}
	}
}

// deliver sends messages to the subscription's handler.
// It handles both channel and function handlers, including concurrency.
func (s *Subscription) deliver(ctx context.Context, messages []*Message, try bool) error {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return errSubscriptionClosed
	}
	htype := s.handlerType
	s.mu.RUnlock() // Unlock before potentially blocking operations

	switch htype {
	case handlerTypeFunc:
		if s.options.Concurrency > 1 {
			// Use delivery queue for concurrent execution
			select {
			case s.deliveryQueue <- messages:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			default:
				if try {
					log.Warn().Str("subscription_id", s.ID).Str("topic", s.Topic).Msg("delivery queue full, dropping message (tryPublish)")
					return nil // Ignore error for TryPublish
				}
				// Block until space is available or context is canceled
				select {
				case s.deliveryQueue <- messages:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		} else {
			// Direct invocation for non-concurrent function
			return s.invokeFuncHandler(messages)
		}
	case handlerTypeChan:
		return s.sendToChanHandler(ctx, messages, try)
	default:
		// Should not happen
		return errInvalidHandler
	}
}

// invokeFuncHandler calls the function handler with the given messages.
// It performs reflection-based argument matching.
func (s *Subscription) invokeFuncHandler(messages []*Message) error {
	s.mu.RLock() // Lock needed to access handlerFunc and handlerArgs safely
	fn := s.handlerFunc
	expectedArgs := s.handlerArgs
	s.mu.RUnlock()

	if fn.IsZero() {
		return errSubscriptionClosed // Handler cleared during close
	}

	// Check if the number of arguments matches
	if len(expectedArgs) != len(messages) {
		log.Error().Str("subscription_id", s.ID).Str("topic", s.Topic).Int("expected", len(expectedArgs)).Int("got", len(messages)).Msg("handler argument count mismatch")
		return errHandlerArgs
	}

	// Prepare arguments for the function call
	callArgs := make([]reflect.Value, len(messages))
	for i, msg := range messages {
		if msg == nil || msg.Payload == nil {
			// Handle nil message/payload - pass zero value of expected type
			callArgs[i] = reflect.Zero(expectedArgs[i])
			continue
		}

		payloadVal := reflect.ValueOf(msg.Payload)
		if !payloadVal.Type().AssignableTo(expectedArgs[i]) {
			log.Error().Str("subscription_id", s.ID).Str("topic", s.Topic).Int("arg_index", i).Str("expected", expectedArgs[i].String()).Str("got", payloadVal.Type().String()).Msg("handler argument type mismatch")
			return errHandlerArgs
		}
		callArgs[i] = payloadVal
	}

	// Call the function
	// We might want to run this in a separate goroutine with recovery
	// but for simplicity now, call directly. Concurrency handled by runWorker.
	fn.Call(callArgs)
	return nil
}

// sendToChanHandler sends messages to the channel handler.
// It handles different channel element types (any or specific type).
func (s *Subscription) sendToChanHandler(ctx context.Context, messages []*Message, try bool) error {
	s.mu.RLock() // Lock needed to access handlerChan safely
	ch := s.handlerChan
	chType := s.chanType
	s.mu.RUnlock()

	if ch.IsZero() {
		return errSubscriptionClosed // Channel cleared during close
	}

	// Determine the value to send based on channel type
	var valueToSend reflect.Value
	if chType.Kind() == reflect.Interface && chType.NumMethod() == 0 { // Check if it's 'any' (interface{})
		// If channel is chan any or chan *Message, send the first message
		// Note: Sending multiple messages to a single chan element doesn't make sense.
		// We'll send the first message only. Consider logging a warning if len(messages) > 1?
		if len(messages) > 0 {
			valueToSend = reflect.ValueOf(messages[0]) // Send *Message
		} else {
			return nil // No messages to send
		}
	} else {
		// Channel expects a specific type T (chan T)
		if len(messages) != 1 {
			log.Warn().Str("subscription_id", s.ID).Str("topic", s.Topic).Int("num_messages", len(messages)).Msg("sending multiple messages to a typed channel (chan T) is ambiguous; sending only the first message's payload")
		}
		if len(messages) == 0 {
			return nil // No messages to send
		}
		payloadVal := reflect.ValueOf(messages[0].Payload)
		if !payloadVal.Type().AssignableTo(chType) {
			log.Error().Str("subscription_id", s.ID).Str("topic", s.Topic).Str("expected", chType.String()).Str("got", payloadVal.Type().String()).Msg("channel type mismatch")
			return errChanTypeMismatch
		}
		valueToSend = payloadVal
	}

	if try {
		// Non-blocking send for TryPublish
		if ch.TrySend(valueToSend) {
			return nil
		} else {
			// TrySend failed (channel full or closed)
			// Check if closed
			// Note: Detecting closed channel reliably without select is hard.
			// TrySend returning false could mean full or closed. Assume full for TryPublish.
			log.Warn().Str("subscription_id", s.ID).Str("topic", s.Topic).Msg("channel full or closed, dropping message (tryPublish)")
			return nil // Ignore error for TryPublish
		}
	} else {
		// Blocking send for Publish using reflect.Select
		ctxDoneCase := reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(ctx.Done()),
		}
		sendCase := reflect.SelectCase{
			Dir:  reflect.SelectSend,
			Chan: ch,
			Send: valueToSend,
		}

		chosen, _, recvOK := reflect.Select([]reflect.SelectCase{ctxDoneCase, sendCase})
		_ = recvOK // Explicitly use recvOK to avoid unused variable error
		switch chosen {
		case 0: // ctx.Done()
			// The context was canceled. The value received is zero, but recvOK is true if the channel wasn't closed.
			// We only care that the context is done.
			return ctx.Err()
		case 1: // sendCase
			// Message successfully sent
			return nil
		default:
			// Should not happen with only two cases unless channel is closed concurrently?
			// If the channel 'ch' is closed, SelectSend will be chosen, but the send will panic.
			// We might need a recover mechanism around this Select or ensure Close handles this.
			// For now, assume successful send if chosen == 1.
			// If TrySend failed earlier because the channel was closed, we might reach here too.
			// Let's return an error indicating potential closure.
			log.Warn().Str("subscription_id", s.ID).Str("topic", s.Topic).Msg("select send case failed unexpectedly, channel might be closed")
			return errSendToClosedChan // Or a more specific error
		}
	}
}

// Close cleans up the subscription resources.
func (s *Subscription) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil // Already closed
	}
	s.closed = true

	// Stop concurrent workers if any
	if s.cancel != nil {
		s.cancel()
	}
	if s.deliveryQueue != nil {
		close(s.deliveryQueue) // Signal workers to stop processing new messages
	}

	// Clear handler references to allow GC
	s.handlerFunc = reflect.Value{}
	s.handlerChan = reflect.Value{}

	s.mu.Unlock() // Unlock before waiting

	// Wait for workers to finish processing queued messages
	if s.options.Concurrency > 1 {
		s.wg.Wait()
	}

	log.Debug().Str("subscription_id", s.ID).Str("topic", s.Topic).Msg("subscription closed")
	return nil
}
