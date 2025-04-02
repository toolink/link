// Package meta provides utilities for managing request-scoped metadata within a context.Context.
// It facilitates setting and retrieving typed values, commonly used in gRPC or HTTP handlers.
package meta

import (
	"context"
	"fmt"
	"sync"

	"github.com/rs/zerolog/log"
)

// metadataKey is the private key type used for context.WithValue.
// Using a private type prevents collisions with other context keys.
type metadataKey struct{}

// Metadata holds the key-value pairs.
type Metadata struct {
	mu   sync.RWMutex
	data map[string]any
}

// New creates a new, empty Metadata store.
func New() *Metadata {
	return &Metadata{
		data: make(map[string]any),
	}
}

// Set adds or updates a key-value pair in the metadata store.
// Keys are typically strings following conventions like "X-Meta-*".
func (m *Metadata) Set(key string, value any) {
	if m == nil {
		log.Error().Str("key", key).Msg("attempted to set metadata on nil *metadata instance")
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.data == nil {
		m.data = make(map[string]any)
		log.Debug().Msg("metadata map initialized lazily on set")
	}

	m.data[key] = value
	log.Debug().Str("key", key).Any("value", value).Msg("metadata set")
}

// Get retrieves a value by key from the metadata store.
// It returns the value (as any) and a boolean indicating if the key was found.
func (m *Metadata) Get(key string) (any, bool) {
	if m == nil {
		log.Warn().Str("key", key).Msg("attempted to get metadata from nil *metadata instance")
		return nil, false
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.data == nil {
		return nil, false
	}

	value, ok := m.data[key]
	return value, ok
}

// WithContext returns a new context derived from ctx that carries the metadata 'm'.
// If 'm' is nil, the original context is returned with a warning.
// If 'ctx' is nil, a background context carrying the metadata is returned with an error log.
func (m *Metadata) WithContext(ctx context.Context) context.Context {
	if m == nil {
		log.Warn().Msg("attempted to call withcontext on nil *metadata, returning original context")
		// If ctx is also nil here, we return nil, which might be desired
		// or could lead to panics later. Returning background() might be safer.
		// Let's stick to returning ctx as is if m is nil.
		if ctx == nil {
			log.Error().Msg("attempted to call withcontext with nil metadata and nil context")
			return nil // Or context.Background()? Returning nil matches input.
		}
		return ctx
	}
	if ctx == nil {
		log.Error().Msg("attempted to attach metadata to a nil context, using background context")
		ctx = context.Background()
	}
	return context.WithValue(ctx, metadataKey{}, m)
}

// FromContext extracts the *Metadata store from the context 'ctx'.
// If the context is nil or does not contain the metadata, it returns a new,
// empty *Metadata instance to prevent nil pointer exceptions on subsequent calls.
// It logs a debug message if no metadata is found or an error if the type is wrong.
func FromContext(ctx context.Context) *Metadata {
	if ctx == nil {
		log.Debug().Msg("attempted to get metadata from nil context, returning empty metadata")
		return New() // Return a new empty one to be safe
	}

	value := ctx.Value(metadataKey{})
	if value == nil {
		log.Debug().Msg("no metadata found in context, returning empty metadata")
		return New() // Return a new empty one
	}

	if md, ok := value.(*Metadata); ok {
		return md
	}

	log.Error().Interface("value_type", fmt.Sprintf("%T", value)).Msg("metadata key found in context but value has wrong type, returning empty metadata")
	return New() // Return a new empty one to be safe
}

// --- Generic Get Function ---

// Get retrieves a value associated with the key from the metadata stored in the context.
// It performs a type assertion to the requested type T.
//
// Returns:
//   - t (T): The value if found and type assertion is successful. Zero value otherwise.
//   - err (error): An error if the key is not found or the type assertion fails.
func Get[T any](ctx context.Context, key string) (t T, err error) {
	md := FromContext(ctx) // Safely gets *Metadata, potentially a new empty one

	rawValue, ok := md.Get(key)
	if !ok {
		// Use fmt.Errorf for simple error creation
		err = fmt.Errorf("meta: key '%s' not found in context metadata", key)
		// t is already the zero value of T
		return
	}

	typedValue, ok := rawValue.(T)
	if !ok {
		// Provide more informative error about type mismatch
		err = fmt.Errorf("meta: value for key '%s' has type %T, but type %T was requested", key, rawValue, *new(T))
		// t is already the zero value of T
		return
	}

	t = typedValue
	// err is already nil
	return
}

// MustGet retrieves a value associated with the key from the metadata stored in the context.
// It performs a type assertion to the requested type T.
//
// Returns:
//   - t (T): The value if found and type assertion is successful.
//
// Panics if the key is not found or the type assertion fails.
func MustGet[T any](ctx context.Context, key string) T {
	t, err := Get[T](ctx, key)
	if err != nil {
		panic(err)
	}
	return t
}
