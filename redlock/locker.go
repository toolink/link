package redlock

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

const (
	// defaultTTL is the default lock expiry time if not set via WithTTL.
	defaultTTL = 1 * time.Second
	// defaultRetryDelay is the default time to wait between retries in Lock.
	defaultRetryDelay = 100 * time.Millisecond
	// defaultMaxRetries is the default maximum number of retries in Lock.
	// Set relatively high, relying more on context timeout in typical usage.
	// Set to 0 via WithMaxRetries for infinite retries (context permitting).
	defaultMaxRetries = 30
)

var (
	// ErrLockNotAcquired is returned when TryLock fails to acquire the lock immediately.
	ErrLockNotAcquired = errors.New("redlock: lock not acquired")
	// ErrUnlockFailed is returned when unlocking fails (e.g., lock expired or held by someone else).
	ErrUnlockFailed = errors.New("redlock: failed to unlock")
	// ErrLockWaitTimeout is returned when Lock fails to acquire the lock within the context deadline.
	ErrLockWaitTimeout = errors.New("redlock: waiting for lock timed out or context cancelled")
	// ErrLockMaxRetriesExceeded is returned when Lock fails after exceeding the maximum retry attempts.
	ErrLockMaxRetriesExceeded = errors.New("redlock: maximum lock retries exceeded")
)

// unlockScript ensures atomic deletion only if the value matches.
// KEYS[1]: The lock key
// ARGV[1]: The unique value held by the locker instance
// Returns 1 if deletion occurred, 0 otherwise.
const unlockScript = `
if redis.call("get", KEYS[1]) == ARGV[1] then
	return redis.call("del", KEYS[1])
else
	return 0
end
`

// Locker represents a distributed lock for a specific resource key.
type Locker struct {
	client     redis.Cmdable
	key        string        // The resource key to lock in Redis.
	value      string        // A unique value for this lock instance (set on successful lock).
	ttl        time.Duration // Time-to-live for the lock.
	retryDelay time.Duration // Delay between retries for the Lock method.
	maxRetries int           // Max number of retries for Lock (0 means infinite, subject to context).
}

// Option defines a function type for configuring a Locker.
type Option func(*Locker) error

// WithTTL sets the time-to-live for the lock.
// Default is 1 second.
func WithTTL(ttl time.Duration) Option {
	return func(l *Locker) error {
		if ttl <= 0 {
			ttl = defaultTTL
		}
		l.ttl = ttl
		return nil
	}
}

// WithRetryDelay sets the delay between lock acquisition attempts for the Lock method.
// Default is 100ms.
func WithRetryDelay(delay time.Duration) Option {
	return func(l *Locker) error {
		if delay <= 0 {
			delay = defaultRetryDelay
		}
		l.retryDelay = delay
		return nil
	}
}

// WithMaxRetries sets the maximum number of retries for the Lock method.
// Set to 0 for infinite retries (limited only by context deadline/cancellation).
// Default is 50 retries.
func WithMaxRetries(retries int) Option {
	return func(l *Locker) error {
		if retries < 0 {
			retries = defaultMaxRetries
		}
		l.maxRetries = retries
		return nil
	}
}

// NewLocker creates a new Locker instance.
// client: An initialized go-redis client interface.
// key: The identifier for the resource to be locked.
// options: Optional configuration functions (e.g., WithTTL, WithRetryDelay, WithMaxRetries).
func NewLocker(client redis.Cmdable, key string, options ...Option) (*Locker, error) {
	l := &Locker{
		client:     client,
		key:        key,
		ttl:        defaultTTL,
		retryDelay: defaultRetryDelay,
		maxRetries: defaultMaxRetries,
		// value is generated during Lock() or TryLock()
	}

	for _, opt := range options {
		opt(l)
	}

	log.Debug().Str("key", key).Dur("ttl", l.ttl).Dur("retry_delay", l.retryDelay).Int("max_retries", l.maxRetries).Msg("new locker created")
	return l, nil
}

// tryLockInternal attempts the core SETNX operation.
// Returns the unique lock value on success, or an error.
func (l *Locker) tryLockInternal(ctx context.Context) (string, error) {
	lockValue := uuid.NewString()
	logCtx := log.With().Str("key", l.key).Str("attempt_value", lockValue).Dur("ttl", l.ttl).Logger()

	// SET key value NX EX ttl_seconds
	ok, err := l.client.SetNX(ctx, l.key, lockValue, l.ttl).Result()
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			logCtx.Warn().Err(err).Msg("context cancelled or deadline exceeded during setnx")
			return "", ErrLockWaitTimeout // Map context errors during redis call
		}
		// Log underlying redis errors
		logCtx.Error().Err(err).Msg("failed to execute setnx command")
		return "", err // Propagate other underlying errors
	}

	if !ok {
		// Key already exists, lock is held by someone else.
		logCtx.Trace().Msg("trylock attempt failed: lock already held") // Trace as it can be frequent
		return "", ErrLockNotAcquired
	}

	// Lock acquired successfully
	logCtx.Debug().Str("held_value", lockValue).Msg("trylock attempt succeeded")
	return lockValue, nil
}

// TryLock attempts to acquire the lock immediately without waiting.
// Returns nil if successful, ErrLockNotAcquired if locked, or other errors.
func (l *Locker) TryLock(ctx context.Context) error {
	logCtx := log.With().Str("key", l.key).Logger()
	logCtx.Debug().Msg("attempting trylock")

	lockValue, err := l.tryLockInternal(ctx)
	if err != nil {
		// ErrLockNotAcquired or other errors
		// Log the failure reason if it wasn't just "not acquired"
		if !errors.Is(err, ErrLockNotAcquired) {
			logCtx.Warn().Err(err).Msg("trylock failed")
		} else {
			logCtx.Debug().Msg("trylock failed, lock held by another instance")
		}
		return err
	}

	// Success
	l.value = lockValue // Store the value associated with the *held* lock
	logCtx.Info().Str("held_value", l.value).Msg("trylock acquired successfully")
	return nil
}

// Lock attempts to acquire the lock, waiting and retrying according to configuration.
// It respects the deadline/cancellation of the context AND the maxRetries limit.
// Returns nil on success.
// Returns ErrLockWaitTimeout if context expires.
// Returns ErrLockMaxRetriesExceeded if max retries are hit before context expires.
// Returns other errors for Redis issues.
func (l *Locker) Lock(ctx context.Context) error {
	logCtx := log.With().Str("key", l.key).Dur("ttl", l.ttl).Dur("retry_delay", l.retryDelay).Int("max_retries", l.maxRetries).Logger()
	logCtx.Debug().Msg("attempting lock with waiting/retries")

	// Attempt immediately first.
	lockValue, err := l.tryLockInternal(ctx)
	if err == nil {
		l.value = lockValue
		logCtx.Info().Str("held_value", l.value).Msg("lock acquired immediately")
		return nil
	}
	if !errors.Is(err, ErrLockNotAcquired) {
		// An unexpected error occurred during the first try
		logCtx.Error().Err(err).Msg("initial lock attempt failed with unexpected error")
		return err
	}
	// If err is ErrLockNotAcquired, proceed to waiting loop.

	// Start the retry loop
	retryCount := 0
	ticker := time.NewTicker(l.retryDelay)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logCtx.Warn().Err(ctx.Err()).Int("retries_attempted", retryCount).Msg("context cancelled or deadline exceeded while waiting for lock")
			return ErrLockWaitTimeout

		case <-ticker.C:
			retryCount++
			logCtx.Trace().Int("retry_count", retryCount).Msg("retrying lock acquisition...")

			lockValue, err := l.tryLockInternal(ctx)
			if err == nil {
				// Lock acquired successfully during retry.
				l.value = lockValue
				logCtx.Info().Str("held_value", l.value).Int("retries_needed", retryCount).Msg("lock acquired after waiting")
				return nil
			}

			if !errors.Is(err, ErrLockNotAcquired) {
				// An unexpected error occurred during a retry attempt
				logCtx.Error().Err(err).Int("retry_count", retryCount).Msg("lock retry failed with unexpected error")
				return err // Propagate error
			}

			// Lock still not acquired, check retry limit
			// Note: maxRetries == 0 means infinite retries (only limited by context)
			if l.maxRetries > 0 && retryCount >= l.maxRetries {
				logCtx.Warn().Int("retries_attempted", retryCount).Msg("maximum lock retries exceeded")
				return ErrLockMaxRetriesExceeded
			}

			// Continue loop if lock not acquired and retries/context not exhausted
			logCtx.Trace().Int("retry_count", retryCount).Msg("retry failed, lock still held")
		}
	}
	// Should be unreachable due to select logic
}

// Unlock releases the distributed lock using the Lua script for safety.
func (l *Locker) Unlock(ctx context.Context) error {
	if l.value == "" {
		log.Warn().Str("key", l.key).Msg("unlock attempted without acquiring lock first or value missing")
		// Indicate misuse or that lock wasn't held by this instance
		return ErrUnlockFailed
	}

	heldValue := l.value
	l.value = "" // Clear value immediately

	logCtx := log.With().Str("key", l.key).Str("held_value", heldValue).Logger()

	// Execute the Lua script atomically. Pass heldValue.
	res, err := l.client.Eval(ctx, unlockScript, []string{l.key}, heldValue).Result() // res is `any`

	if err != nil {
		if errors.Is(err, redis.Nil) {
			// Key is gone - likely expired or unlocked by another process/ttl. Considered success.
			logCtx.Warn().Msg("key not found during unlock (likely expired or unlocked elsewhere)")
			return nil
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			logCtx.Warn().Err(err).Msg("context cancelled or deadline exceeded during unlock eval")
			// Return context error? Or ErrUnlockFailed? Context error is more accurate.
			return ctx.Err()
		}
		// Log other underlying redis errors
		logCtx.Error().Err(err).Msg("failed to execute unlock script")
		return err // Propagate underlying error
	}

	// Check the script's return value
	if val, ok := res.(int64); ok && val == 1 {
		logCtx.Info().Msg("lock unlocked successfully")
		return nil
	}

	// Script returned 0 (value didn't match) or unexpected result type.
	logCtx.Warn().Interface("script_result", res).Msg("unlock failed: value did not match or unexpected script result (lock may be held by another instance or expired/re-acquired)")
	return ErrUnlockFailed
}

// Key returns the resource key associated with this locker.
func (l *Locker) Key() string {
	return l.key
}

// Value returns the unique value generated for the current lock instance (if locked).
func (l *Locker) Value() string {
	return l.value
}
