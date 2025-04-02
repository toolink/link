package worker

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

// Publisher sends messages to Redis Lists.
type Publisher struct {
	rdb  redis.Cmdable
	opts publisherOptions
}

// NewPublisher creates a new Publisher instance.
func NewPublisher(rdb redis.Cmdable, opts ...PublisherOption) *Publisher {
	cfg := defaultPublisherOptions()
	for _, opt := range opts {
		opt(&cfg)
	}
	return &Publisher{
		rdb:  rdb,
		opts: cfg,
	}
}

// Pub publishes messages reliably to a topic (Redis List).
// Uses context.Background() with configured default timeout.
func (p *Publisher) Pub(topic string, args ...any) error {
	ctx, cancel := context.WithTimeout(context.Background(), p.opts.defaultPubTimeout)
	defer cancel()
	return p.publishInternal(ctx, topic, false, args...)
}

// PubCtx publishes messages reliably to a topic (Redis List) with context.
func (p *Publisher) PubCtx(ctx context.Context, topic string, args ...any) error {
	// Respect caller's context, but ensure a timeout exists
	if _, deadlineSet := ctx.Deadline(); !deadlineSet {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.opts.defaultPubTimeout)
		defer cancel() // Ensure cancel is called if we added a timeout
	}
	return p.publishInternal(ctx, topic, false, args...)
}

// Broadcast publishes messages to a topic (Redis List) with best-effort delivery.
// If the LPUSH command times out (using configured broadcastTimeout), the error is ignored.
func (p *Publisher) Broadcast(ctx context.Context, topic string, args ...any) error {
	// Apply broadcast timeout
	bctx, cancel := context.WithTimeout(ctx, p.opts.broadcastTimeout)
	defer cancel()

	err := p.publishInternal(bctx, topic, true, args...)

	// For Broadcast, specifically ignore context deadline exceeded/canceled errors from LPUSH
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		log.Warn().Str("topic", topic).Err(err).Msg("broadcast message dropped due to timeout during lpush")
		return nil // Indicate success (message dropped as intended on timeout)
	}
	// Propagate other errors (e.g., connection errors, serialization errors)
	return err
}

// publishInternal handles serialization, LPUSH, and optional LTRIM.
func (p *Publisher) publishInternal(ctx context.Context, topic string, isBroadcast bool, args ...any) error {
	if topic == "" {
		return errors.New("topic cannot be empty")
	}
	if len(args) == 0 {
		// Allow publishing empty messages? Or require args? Let's require for now.
		return errors.New("no message arguments provided")
	}

	payload, err := serializeArgs(args...)
	if err != nil {
		log.Error().Err(err).Str("topic", topic).Msg("failed to serialize message arguments")
		return fmt.Errorf("serialization failed: %w", err)
	}

	// Execute LPUSH
	cmd := p.rdb.LPush(ctx, topic, payload)
	_, err = cmd.Result() // Returns the new length of the list

	if err != nil {
		// Log differently based on type for clarity
		logFields := log.Error().Err(err).Str("topic", topic)
		if isBroadcast {
			// Context errors are expected for broadcast timeouts, handled in Broadcast wrapper
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				return err // Return the specific context error for Broadcast() to check
			}
			logFields = log.Warn().Err(err).Str("topic", topic) // Log other errors as warnings for broadcast
			logFields.Msg("broadcast failed during lpush")
		} else {
			logFields.Msg("failed to publish message (lpush)")
		}
		return err // Return Redis error
	}

	// Optional: Trim the list if maxLen is set
	// Do this *after* the push succeeds.
	if p.opts.listMaxLen > 0 {
		// LTRIM topic 0 maxLen-1 keeps the *first* maxLen elements (newest ones due to LPUSH)
		trimCmd := p.rdb.LTrim(ctx, topic, 0, p.opts.listMaxLen-1)
		if trimErr := trimCmd.Err(); trimErr != nil {
			// Log LTRIM errors but don't fail the publish operation itself
			log.Warn().Err(trimErr).Str("topic", topic).Int64("max_len", p.opts.listMaxLen).Msg("failed to trim list after lpush")
		}
	}

	log.Debug().Str("topic", topic).Int("arg_count", len(args)).Bool("is_broadcast", isBroadcast).Msg("message published to list")
	return nil
}
