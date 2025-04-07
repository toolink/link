// Package extension defines the interface for extensions and the manager
// responsible for their lifecycle.
package extension

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// ExtensionManager manages the registration and lifecycle (loading, shutdown) of extensions.
// It uses the global zerolog/log instance for logging.
type ExtensionManager struct {
	mu         sync.RWMutex         // protects concurrent access to internal state.
	extensions map[string]Extension // stores registered extensions, keyed by name.
	loadOrder  []string             // defines the order for loading extensions (shutdown is reverse).
	loaded     map[string]bool      // tracks successfully loaded extensions for precise shutdown/rollback.
}

// New creates and initializes a new ExtensionManager.
func New() *ExtensionManager {
	// Uses the globally configured zerolog/log logger. Ensure it's configured before use.
	return &ExtensionManager{
		extensions: make(map[string]Extension),
		loadOrder:  make([]string, 0),
		loaded:     make(map[string]bool),
	}
}

// Register adds a new extension to the manager.
// By default, the new extension is appended to the end of the load order.
// Returns ErrExtensionAlreadyRegistered if an extension with the same name exists.
func (m *ExtensionManager) Register(ext Extension) error {
	m.mu.Lock()         // acquire write lock
	defer m.mu.Unlock() // ensure lock is released

	name := ext.Name()
	if _, exists := m.extensions[name]; exists {
		log.Error().Str("extension", name).Msg("attempted to register duplicate extension")
		return fmt.Errorf("%w: %s", ErrExtensionAlreadyRegistered, name)
	}

	m.extensions[name] = ext
	// append to the end of the default load order
	m.loadOrder = append(m.loadOrder, name)
	log.Info().Str("extension", name).Msg("extension registered")
	return nil
}

// Unregister removes an extension from the manager by its name.
// Returns ErrExtensionNotFound if the extension is not found.
// It also removes the extension from the load order and loaded status.
func (m *ExtensionManager) Unregister(name string) error {
	m.mu.Lock()         // acquire write lock
	defer m.mu.Unlock() // ensure lock is released

	if _, exists := m.extensions[name]; !exists {
		log.Warn().Str("extension", name).Msg("attempted to unregister non-existent extension")
		return fmt.Errorf("%w: %s", ErrExtensionNotFound, name)
	}

	// remove from extensions map
	delete(m.extensions, name)
	// remove from loaded map if present
	delete(m.loaded, name)

	// remove from load order slice
	newLoadOrder := make([]string, 0, len(m.loadOrder)-1)
	for _, extName := range m.loadOrder {
		if extName != name {
			newLoadOrder = append(newLoadOrder, extName)
		}
	}
	m.loadOrder = newLoadOrder

	log.Info().Str("extension", name).Msg("extension unregistered")
	return nil
}

// SetLoadOrder explicitly sets the order in which extensions should be loaded.
// The provided `names` list must contain exactly the names of all registered extensions,
// without duplicates or missing/extra names.
// This order determines LoadAll execution sequence and the reverse order for ShutdownAll.
func (m *ExtensionManager) SetLoadOrder(names []string) error {
	m.mu.Lock()         // acquire write lock
	defer m.mu.Unlock() // ensure lock is released

	// 1. check if counts match
	if len(names) != len(m.extensions) {
		log.Error().
			Int("provided_count", len(names)).
			Int("registered_count", len(m.extensions)).
			Msg("failed to set load order: count mismatch")
		return fmt.Errorf("%w (provided: %d, registered: %d)", ErrLoadOrderMismatch, len(names), len(m.extensions))
	}

	// 2. check for existence and duplicates
	tempSet := make(map[string]struct{}, len(names))
	for _, name := range names {
		if _, exists := m.extensions[name]; !exists {
			log.Error().Str("extension", name).Msg("failed to set load order: extension not registered")
			return fmt.Errorf("%w: %s", ErrLoadOrderMissing, name)
		}
		if _, duplicate := tempSet[name]; duplicate {
			log.Error().Str("extension", name).Msg("failed to set load order: duplicate extension name")
			return fmt.Errorf("%w: %s", ErrLoadOrderDuplicate, name)
		}
		tempSet[name] = struct{}{}
	}

	// 3. validation passed, update the load order (create a copy)
	m.loadOrder = append([]string(nil), names...)
	log.Info().Strs("load_order", m.loadOrder).Msg("extension load order set")
	return nil
}

// LoadAll loads all registered extensions according to the configured `loadOrder`.
// If any extension fails to load:
//  1. an error is logged.
//  2. it attempts to shut down (rollback) the extensions that were successfully loaded
//     *before* the failure occurred, in reverse order of their loading.
//  3. the original error from the failed extension's Load() method is returned.
func (m *ExtensionManager) LoadAll() error {
	m.mu.RLock() // acquire read lock to safely read loadOrder
	// copy loadOrder to avoid holding the lock during potentially long Load() calls
	order := append([]string(nil), m.loadOrder...)
	m.mu.RUnlock() // release read lock

	successfullyLoaded := make([]string, 0, len(order)) // track successfully loaded extensions in this call

	for _, name := range order {
		m.mu.RLock() // acquire read lock briefly to get the extension instance
		ext, exists := m.extensions[name]
		m.mu.RUnlock() // release read lock

		if !exists {
			// this should theoretically not happen if SetLoadOrder is used correctly,
			// but handle defensively (e.g., if unregistered concurrently).
			log.Warn().Str("extension", name).Msg("extension found in load order but not registered during loadAll (possibly unregistered concurrently?)")
			continue // or return an error depending on strictness
		}

		log.Debug().Str("extension", name).Msg("loading extension...")
		startTime := time.Now()
		if err := ext.Load(); err != nil {
			duration := time.Since(startTime)
			log.Error().Str("extension", name).Dur("duration", duration).Err(err).Msg("failed to load extension")

			// load failed, initiate rollback of successfully loaded extensions
			m.shutdownSpecific(successfullyLoaded)                          // call helper for rollback
			return fmt.Errorf("failed to load extension %s: %w", name, err) // return the original error
		}
		duration := time.Since(startTime)

		// mark as successfully loaded (requires write lock)
		m.mu.Lock()
		m.loaded[name] = true
		m.mu.Unlock()
		successfullyLoaded = append(successfullyLoaded, name) // also track locally for potential rollback

		log.Info().Str("extension", name).Dur("duration", duration).Msg("extension loaded successfully")
	}

	return nil
}

// ShutdownAll shuts down all extensions that were *successfully loaded*.
// It performs the shutdown in the *reverse* order of `loadOrder`.
// It attempts to shut down all applicable extensions even if errors occur during the process.
// If any shutdown errors occur, they are collected and returned together using errors.Join (Go 1.20+).
func (m *ExtensionManager) ShutdownAll() error {
	m.mu.RLock() // acquire read lock to safely read loadOrder
	// copy loadOrder to avoid holding the lock during potentially long Shutdown() calls
	order := append([]string(nil), m.loadOrder...)
	m.mu.RUnlock() // release read lock

	var allErrors []error // slice to collect errors during shutdown

	// iterate in reverse load order
	for i := len(order) - 1; i >= 0; i-- {
		name := order[i]

		m.mu.RLock() // acquire read lock to check status
		ext, extExists := m.extensions[name]
		isLoaded := m.loaded[name] // check if it was marked as successfully loaded
		m.mu.RUnlock()             // release read lock

		if !extExists {
			log.Warn().Str("extension", name).Msg("extension found in load order but not registered during shutdownAll (possibly unregistered concurrently?)")
			continue
		}

		// only shut down extensions that were successfully loaded
		if isLoaded {
			log.Debug().Str("extension", name).Msg("shutting down extension...")
			startTime := time.Now()
			if err := ext.Shutdown(); err != nil {
				duration := time.Since(startTime)
				log.Error().Str("extension", name).Dur("duration", duration).Err(err).Msg("failed to shut down extension")
				// collect error but continue shutting down others
				allErrors = append(allErrors, fmt.Errorf("failed to shutdown extension %s: %w", name, err))
			} else {
				duration := time.Since(startTime)
				log.Info().Str("extension", name).Dur("duration", duration).Msg("extension shut down successfully")
			}

			// mark as unloaded (requires write lock), regardless of shutdown success/failure,
			// as an attempt was made.
			m.mu.Lock()
			delete(m.loaded, name)
			m.mu.Unlock()
		} else {
			// optionally log skipped extensions
			// log.Debug().Str("extension", name).Msg("skipping shutdown (was not loaded)")
		}
	}

	if len(allErrors) > 0 {
		// Use Warn level for completion with errors
		log.Warn().Int("error_count", len(allErrors)).Msg("--- shutdown completed with errors ---")
		// Use errors.Join (Go 1.20+) to combine errors.
		// For older Go versions, return the first error or a custom multi-error type.
		return errors.Join(allErrors...)
	}

	return nil
}

// shutdownSpecific is an internal helper function used for rollback during a LoadAll failure.
// It shuts down the extensions listed in `names` (which are the ones successfully loaded before the failure)
// in the reverse order they appear in the list.
// This function assumes the caller does not hold the manager's lock.
func (m *ExtensionManager) shutdownSpecific(names []string) {
	var allErrors []error
	// shutdown in reverse order of the provided list (which is the load order)
	for i := len(names) - 1; i >= 0; i-- {
		name := names[i]

		m.mu.RLock() // acquire read lock to check status
		ext, exists := m.extensions[name]
		isLoaded := m.loaded[name] // double check status
		m.mu.RUnlock()

		if exists && isLoaded { // ensure extension still exists and is marked loaded
			log.Warn().Str("extension", name).Msg("executing rollback shutdown...")
			startTime := time.Now()
			if err := ext.Shutdown(); err != nil {
				duration := time.Since(startTime)
				log.Error().
					Str("extension", name).
					Dur("duration", duration).
					Err(err).
					Msg("rollback shutdown failed")
				allErrors = append(allErrors, fmt.Errorf("rollback shutdown failed for %s: %w", name, err))
				// continue trying to shut down others
			} else {
				duration := time.Since(startTime)
				// use Warn level to indicate this is part of a failure recovery
				log.Warn().Str("extension", name).Dur("duration", duration).Msg("rollback shutdown successful")
			}
			// mark as unloaded (requires write lock)
			m.mu.Lock()
			delete(m.loaded, name)
			m.mu.Unlock()
		}
	}
	if len(allErrors) > 0 {
		// log collected errors during rollback
		log.Error().Errs("rollback_errors", allErrors).Msg("errors occurred during load failure rollback")
	}
}

// GetExtension retrieves a registered extension by its name.
// Returns the extension instance and true if found, otherwise nil and false.
// Useful for direct interaction with a specific extension after it has been loaded.
func (m *ExtensionManager) GetExtension(name string) (Extension, bool) {
	m.mu.RLock()         // acquire read lock
	defer m.mu.RUnlock() // ensure lock is released
	ext, exists := m.extensions[name]
	return ext, exists
}
