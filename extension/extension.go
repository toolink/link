// Package extension defines the interface for extensions and the manager
// responsible for their lifecycle.
package extension

import "fmt"

// Extension defines the interface that all extension modules must implement.
type Extension interface {
	// Name returns the unique name of the extension.
	// This name is used for registration, lookup, and ordering.
	Name() string

	// Load performs the initialization logic for the extension.
	// This might involve setting up resources, connecting to services,
	// starting background goroutines, etc.
	// It should return an error if loading fails.
	Load() error

	// Shutdown performs the cleanup and termination logic for the extension.
	// This should release resources, stop background tasks, disconnect services, etc.
	// While this method can return an error, the manager will typically attempt
	// to shut down other extensions even if one fails.
	Shutdown() error
}

// Predefined errors for common scenarios in extension management.
var (
	ErrExtensionAlreadyRegistered = fmt.Errorf("extension name is already registered")
	ErrExtensionNotFound          = fmt.Errorf("extension not found")
	ErrLoadOrderMismatch          = fmt.Errorf("load order list count does not match registered extensions count")
	ErrLoadOrderMissing           = fmt.Errorf("extension specified in load order but not registered")
	ErrLoadOrderDuplicate         = fmt.Errorf("duplicate extension name found in load order")
)
