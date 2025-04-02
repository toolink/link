package redlb

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

// ServiceInstance represents a single instance of a service.
type ServiceInstance struct {
	ID       string            `json:"id"`       // Unique identifier (e.g., UUID)
	Name     string            `json:"name"`     // Service name (e.g., "user-service")
	Address  string            `json:"address"`  // Network address (e.g., "10.0.1.5:8080")
	Metadata map[string]string `json:"metadata"` // Optional key-value metadata
}

// String provides a human-readable representation.
func (si *ServiceInstance) String() string {
	return fmt.Sprintf("%s/%s@%s", si.Name, si.ID, si.Address)
}

// Registry defines the interface for service registration and discovery.
type Registry interface {
	// Register adds a service instance to the registry and starts heartbeating.
	// Returns a function to gracefully deregister the instance.
	// The context is used for the initial registration operation.
	Register(ctx context.Context, instance *ServiceInstance) (deregister func(context.Context) error, err error)

	// Deregister removes a service instance from the registry and stops its heartbeat.
	// The context is used for the deregistration operation in Redis.
	Deregister(ctx context.Context, instance *ServiceInstance) error

	// Discover retrieves a list of currently active instances for a given service name.
	// The context can be used for cancellation or deadlines.
	Discover(ctx context.Context, serviceName string) ([]*ServiceInstance, error)

	// Watch returns a channel that emits the full list of active instances
	// whenever changes are detected for a given service name.
	// The watcher runs until the provided context is canceled.
	// The returned error indicates failure during initial setup.
	Watch(ctx context.Context, serviceName string) (<-chan []*ServiceInstance, error)

	// Close cleans up registry resources, like stopping active watchers or heartbeats
	// managed globally by the registry (if any). It does NOT typically close the
	// underlying Redis client, which should be managed externally.
	Close() error
}

// --- Helper Functions ---

// Generate a simple hash/fingerprint for a list of instances.
// Sorts by ID to ensure consistency.
func hashInstances(instances []*ServiceInstance) string {
	if len(instances) == 0 {
		return "empty" // Explicit hash for empty list
	}
	// Sort by ID for consistent hashing
	sort.SliceStable(instances, func(i, j int) bool {
		return instances[i].ID < instances[j].ID
	})

	var sb strings.Builder
	for i, inst := range instances {
		if i > 0 {
			sb.WriteString(";")
		}
		sb.WriteString(inst.ID)
		sb.WriteString("@")
		sb.WriteString(inst.Address)
		// Optionally include metadata hash if relevant for change detection
	}
	// Consider using a proper hash function (like fnv or sha256) if collisions are a concern
	// For typical service discovery, simple string comparison is often enough.
	return sb.String()
}
