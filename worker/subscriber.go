package worker

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

// --- Subscriber Logic (subscriber.go) ---

type subscriber struct {
	rdb         redis.Cmdable
	topic       string
	handler     any // Original handler (func or chan)
	opts        subscriptionOptions
	processChan chan []byte     // Channel between Redis poller and Go handler(s) [raw bytes]
	stopChan    chan struct{}   // Signals subscriber to stop
	managerWg   *sync.WaitGroup // Pointer to the manager's WG (for main poller loop)
	internalWg  sync.WaitGroup  // WaitGroup for this subscriber's internal processor goroutines
	stopOnce    sync.Once

	// Resolved handler specifics
	method       consumerMethod
	handlerFunc  reflect.Value
	handlerType  reflect.Type
	targetChan   reflect.Value
	chanElemType reflect.Type // Needed to differentiate chan []any vs chan any
}

// run is the main goroutine for a subscriber. It polls Redis using BRPOP.
func (s *subscriber) run() {
	// Signal manager's WG *only* when this main poller loop exits
	defer s.managerWg.Done()

	// Start internal processor goroutine(s) based on concurrency setting
	s.internalWg.Add(s.opts.concurrency)
	for i := 0; i < s.opts.concurrency; i++ {
		go s.runProcessor(i)
	}

	log.Debug().Str("topic", s.topic).Msg("redis list poller loop started (brpop)")

	// Ensure processChan is closed when poller loop exits
	defer close(s.processChan)

	// Main polling loop
	for {
		select {
		case <-s.stopChan:
			log.Debug().Str("topic", s.topic).Msg("redis list poller loop stopping")
			return // Exit loop if stop signal received
		default:
			// Proceed with BRPOP
		}

		// Poll Redis using BRPOP
		// Block for opts.blockTime seconds waiting for an element on the list `s.topic`
		// Use a background context for BRPOP, relying on blockTime and stopChan for termination control.
		// Adding a timeout to the BRPOP context itself could cause it to return early unnecessarily.
		cmd := s.rdb.BRPop(context.Background(), s.opts.blockTime, s.topic)
		result, err := cmd.Result()

		// BRPOP returns []string{listName, value} on success, or redis.Nil on timeout

		if err != nil {
			if errors.Is(err, redis.Nil) {
				// Timeout occurred, no message received. Continue loop to check stopChan or block again.
				log.Trace().Str("topic", s.topic).Msg("brpop timeout")
				continue
			}
			// Check if the error is due to context cancellation tied to stopChan (less likely with Background ctx)
			if errors.Is(err, context.Canceled) {
				log.Warn().Err(err).Str("topic", s.topic).Msg("brpop context canceled")
				continue // Will check stopChan on next iteration
			}
			// Log other Redis errors and potentially back off
			log.Error().Err(err).Str("topic", s.topic).Msg("error during brpop")
			// Simple backoff
			select {
			case <-time.After(1 * time.Second):
			case <-s.stopChan:
				return // Exit if stopped during backoff
			}
			continue
		}

		// Message received - result should be ["topic", "message_payload"]
		if len(result) != 2 || result[0] != s.topic {
			log.Error().Str("topic", s.topic).Strs("brpop_result", result).Msg("invalid result format from brpop")
			continue // Skip this malformed result
		}
		messagePayload := []byte(result[1]) // Extract payload as bytes

		log.Debug().Str("topic", s.topic).Int("payload_size", len(messagePayload)).Msg("received message from list")

		// Send raw payload bytes to internal processing channel, respecting stop signal
		select {
		case s.processChan <- messagePayload:
			// Successfully queued for processing by handler goroutine(s)
		case <-s.stopChan:
			log.Warn().Str("topic", s.topic).Msg("subscriber stopping, discarding fetched message from list")
			// *** Reliability Note: Message is lost here as it was already popped ***
			return // Exit run loop
		}
	}
}

// runProcessor is the goroutine that takes raw message bytes from processChan,
// deserializes, and executes the handler.
func (s *subscriber) runProcessor(processorID int) {
	defer s.internalWg.Done() // Signal subscriber's internal WG when processor exits
	log.Debug().Str("topic", s.topic).Int("processor_id", processorID).Msg("handler processor started")

	for rawPayload := range s.processChan { // Loop exits when processChan is closed by run()
		// 1. Deserialize
		args, err := deserializeArgs(rawPayload)
		if err != nil {
			log.Error().Err(err).Str("topic", s.topic).Int("processor_id", processorID).Msg("failed to deserialize message payload, skipping")
			// *** Reliability Note: Message is lost here ***
			continue // Skip this message
		}

		// 2. Execute Handler
		s.executeHandler(args, processorID)
		// *** Reliability Note: If executeHandler panics, message is lost ***
	}
	log.Debug().Str("topic", s.topic).Int("processor_id", processorID).Msg("handler processor finished")
}

// executeHandler runs the user-provided function or sends to the channel.
func (s *subscriber) executeHandler(args []any, processorID int) {
	// Panic Recovery for user handler code
	defer func() {
		if r := recover(); r != nil {
			log.Error().Str("topic", s.topic).Int("processor_id", processorID).Interface("panic_value", r).Msg("panic recovered during handler execution")
			// *** Reliability Note: Message is lost on panic ***
		}
	}()

	switch s.method {
	case Channel:
		var valueToSend reflect.Value
		// Check if target channel expects `any` (interface{}) or `[]any` (slice of interface{})
		if s.chanElemType.Kind() == reflect.Interface && s.chanElemType.Name() == "" {
			// Target is chan any, send the []any slice wrapped in `any`
			valueToSend = reflect.ValueOf(args)
		} else {
			// Target is chan []any, send the slice directly
			valueToSend = reflect.ValueOf(args)
		}

		// Use Select to attempt send while respecting stopChan (though less likely to hit here vs poller)
		// A direct send might be okay, but select is safer if channel buffer fills unexpectedly.
		selectCases := []reflect.SelectCase{
			{Dir: reflect.SelectSend, Chan: s.targetChan, Send: valueToSend},
			{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(s.stopChan)}, // Check if subscriber is stopping
		}
		chosen, _, sndOk := reflect.Select(selectCases)

		if chosen == 1 { // stopChan was selected
			log.Warn().Str("topic", s.topic).Int("processor_id", processorID).Msg("handler channel send aborted, subscriber stopping")
		} else if !sndOk { // chosen == 0 but send failed (channel closed)
			// This case implies the user closed their channel, which is unusual.
			log.Error().Str("topic", s.topic).Int("processor_id", processorID).Msg("handler channel send failed (channel closed by receiver?)")
		}
		// If chosen == 0 and sndOk is true, send succeeded.

	case Function:
		numArgs := len(args)
		numExpected := s.handlerType.NumIn()

		if numArgs != numExpected {
			log.Error().Str("topic", s.topic).Int("processor_id", processorID).Int("expected_args", numExpected).Int("received_args", numArgs).Msg("argument count mismatch for handler function")
			return // Skip execution
		}

		callArgs := make([]reflect.Value, numArgs)
		for i, arg := range args {
			expectedType := s.handlerType.In(i)
			argVal, err := prepareArgForCall(arg, expectedType) // Use helper
			if err != nil {
				log.Error().Err(err).Str("topic", s.topic).Int("processor_id", processorID).Int("arg_index", i).Str("expected_type", expectedType.String()).Msg("argument type mismatch/conversion failed for handler function")
				return // Skip execution
			}
			callArgs[i] = argVal
		}

		// Call the function (panic is recovered by defer)
		_ = s.handlerFunc.Call(callArgs)
		// Success assumed if no panic

	default:
		log.Error().Str("topic", s.topic).Int("processor_id", processorID).Msg("unknown handler method during execution")
	}
}

// stop signals the subscriber goroutines to stop and waits for them.
func (s *subscriber) stop() {
	s.stopOnce.Do(func() {
		log.Debug().Str("topic", s.topic).Msg("stopping subscriber...")
		// Signal all internal goroutines (poller, processors) to stop
		close(s.stopChan)

		// The poller (run()) will close processChan upon exiting.

		// Wait for the processor goroutines (runProcessor) to finish processing
		// any messages already read from processChan.
		s.internalWg.Wait()
		log.Debug().Str("topic", s.topic).Msg("subscriber processor goroutines finished")

		// The manager's Shutdown will wait on managerWg, which ensures the
		// main poller loop (run) has also exited. No explicit wait needed here for that.
	})
}
