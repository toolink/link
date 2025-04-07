package global

import (
	"sync/atomic"

	"github.com/toolink/link/registry"
)

// defaultDependencyRegistry is a global instance of DependencyRegistry.
func defaultDependencyRegistry() *atomic.Value {
	v := &atomic.Value{}
	v.Store(registry.New())
	return v
}

var globalDependencyRegistry = defaultDependencyRegistry()

// SetDependencyRegistry sets the global registry instance.
func SetDependencyRegistry(r *registry.DependencyRegistry) {
	globalDependencyRegistry.Store(r)
}

// GetDependencyRegistry returns the global registry instance.
func GetDependencyRegistry() *registry.DependencyRegistry {
	return globalDependencyRegistry.Load().(*registry.DependencyRegistry)
}
