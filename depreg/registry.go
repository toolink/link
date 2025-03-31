// Package depreg provides a simple type-based dependency registry
// to decouple modules relying on shared global state.
package depreg

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// Custom errors
var (
	ErrDependencyNotFound      = errors.New("dependency not found")
	ErrInvalidTargetType       = errors.New("target argument must be a non-nil pointer")
	ErrAmbiguousInterface      = errors.New("multiple registered types implement the requested interface")
	ErrWaitTimeout             = errors.New("timed out waiting for dependencies")
	ErrUnsupportedMapValueType = errors.New("cannot register map value as dependency directly, register the map itself")
)

// registry holds the dependencies and synchronization primitives.
type DependencyRegistry struct {
	mu    sync.RWMutex
	store map[reflect.Type]dependencyHolder
	cond  *sync.Cond // Used for GetWait notifications
}

// dependencyHolder wraps the actual value and stores its original type info.
// This is important for interface implementation checks.
type dependencyHolder struct {
	value       any
	reflectType reflect.Type
	reflectVal  reflect.Value
}

// New creates and initializes a new registry instance.
func New() *DependencyRegistry {
	r := &DependencyRegistry{
		store: make(map[reflect.Type]dependencyHolder),
	}
	r.cond = sync.NewCond(&r.mu) // Use the write lock part for condition signaling
	return r
}

// Set registers one or more dependencies in the global registry.
// Each dependency is stored using its concrete type as the key.
// If a dependency with the same type already exists, it will be overwritten,
// and a warning will be logged.
// Registering nil values is ignored.
// Registering map values directly is disallowed; register the map variable itself.
func (r *DependencyRegistry) Set(vars ...any) {
	if len(vars) == 0 {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	registeredSomething := false
	for _, v := range vars {
		if v == nil {
			log.Warn().Msg("Ignoring nil value passed to Set")
			continue
		}

		rv := reflect.ValueOf(v)
		rt := rv.Type()

		// Prevent registering map values directly, as type inference can be tricky
		// and map lookups usually want the whole map, not a specific entry's type.
		// Allow map *types* though (registering the map variable itself).
		if rv.Kind() == reflect.Map && rt.Key() != nil { // Check if it's an actual map value, not just map type
			// Check if the value itself is a map - rv.Kind() == reflect.Map is sufficient here.
			log.Error().Str("type", rt.String()).Msgf("%s", ErrUnsupportedMapValueType)
			// Potentially panic or return an error if Set had error return
			continue // Skip registering map values directly
		}

		// Use the concrete type as the primary key
		keyType := rt

		if _, exists := r.store[keyType]; exists {
			log.Warn().Str("type", keyType.String()).Msg("Overwriting existing dependency registration")
		}

		holder := dependencyHolder{
			value:       v,
			reflectType: rt,
			reflectVal:  rv,
		}
		r.store[keyType] = holder
		log.Debug().Str("type", keyType.String()).Msg("Dependency registered")
		registeredSomething = true
	}

	// Notify waiting goroutines *if* something was actually registered
	if registeredSomething {
		r.cond.Broadcast() // Wake up all waiters
	}
}

// Get retrieves dependencies from the global registry and assigns them to the target pointers.
// Targets must be non-nil pointers to variables of the desired types (e.g., &myVar).
// It searches for exact type matches first. If a target is an interface type and no
// exact match is found, it searches for a registered concrete type that implements the interface.
// Returns ErrDependencyNotFound if any dependency cannot be found.
// Returns ErrAmbiguousInterface if multiple registered types implement a requested interface.
// Returns ErrInvalidTargetType if any target is not a valid pointer.
func (r *DependencyRegistry) Get(targets ...any) error {
	if len(targets) == 0 {
		return nil
	}

	r.mu.RLock() // Use read lock for potentially faster concurrent Gets
	defer r.mu.RUnlock()

	return r.resolveTargets(targets)
}

// MustGet is like Get but panics if any dependency cannot be found or assigned.
// Useful for essential dependencies during application startup.
func (r *DependencyRegistry) MustGet(targets ...any) {
	if err := r.Get(targets...); err != nil {
		log.Panic().Err(err).Msg("Failed to get required dependencies")
	}
}

// GetWait is like Get but waits for dependencies to become available if they are not
// immediately found. It waits up to the specified timeout duration.
// If the timeout is zero or negative, it behaves like Get (no waiting).
// Returns ErrWaitTimeout if the timeout is reached before all dependencies are found.
// Other errors are the same as Get (ErrDependencyNotFound, ErrAmbiguousInterface, ErrInvalidTargetType).
func (r *DependencyRegistry) GetWait(timeout time.Duration, targets ...any) error {
	if len(targets) == 0 {
		return nil
	}

	// If no timeout, behave like Get
	if timeout <= 0 {
		return r.Get(targets...)
	}

	startTime := time.Now()
	deadline := startTime.Add(timeout)

	r.mu.Lock() // Need full lock for condition variable Wait
	defer r.mu.Unlock()

	for {
		err := r.resolveTargets(targets)
		if err == nil {
			// Success! All dependencies found and assigned.
			return nil
		}

		// Only wait if the error is specifically "not found" for *some* target.
		// Other errors (invalid target, ambiguity) should return immediately.
		if !errors.Is(err, ErrDependencyNotFound) {
			return err
		}

		// Check timeout *before* waiting
		if time.Now().After(deadline) {
			log.Warn().Err(err).Dur("timeout", timeout).Msg("Timed out waiting for dependencies")
			// Return the original error (likely ErrDependencyNotFound) wrapped or a specific timeout error
			return fmt.Errorf("%w: %w", ErrWaitTimeout, err)
		}

		// Wait for a signal from Set.
		// cond.Wait() atomically unlocks r.mu, waits, and relocks r.mu before returning.
		log.Debug().Msg("Dependency not found, waiting...")
		r.cond.Wait()
		log.Debug().Msg("Woke up from wait, rechecking dependencies...")
		// Loop continues to re-check dependencies
	}
}

// resolveTargets attempts to find and assign all dependencies to the target pointers.
// This function MUST be called with the appropriate lock held (read for Get/MustGet, write for GetWait).
func (r *DependencyRegistry) resolveTargets(targets []any) error {
	missingDeps := []reflect.Type{}

	for _, target := range targets {
		targetVal := reflect.ValueOf(target)

		// 1. Validate Target: Must be a non-nil pointer
		if targetVal.Kind() != reflect.Ptr || targetVal.IsNil() {
			return fmt.Errorf("%w: received %T", ErrInvalidTargetType, target)
		}

		// 2. Get Target Element Type: The type of variable the pointer points to
		targetElem := targetVal.Elem()
		targetType := targetElem.Type()

		// 3. Find Dependency
		holder, err := r.findDependencyByType(targetType)
		if err != nil {
			// Handle specific errors from findDependencyByType
			if errors.Is(err, ErrDependencyNotFound) {
				missingDeps = append(missingDeps, targetType) // Collect missing type
				continue                                      // Check next target
			}
			return err // Propagate other errors (e.g., ambiguity)
		}

		// 4. Check Assignability (Safety Check)
		if !holder.reflectType.AssignableTo(targetType) {
			// This case is less likely with the interface check in findDependencyByType,
			// but good for robustness, especially if holder.reflectType != targetType (interface case)
			// Re-check implementation for interfaces explicitly here if needed, but find should cover it.
			if targetType.Kind() == reflect.Interface && holder.reflectType.Implements(targetType) {
				// This is the expected case for interfaces - the concrete type implements the interface
			} else {
				// This indicates a logic error or unexpected type mismatch
				log.Error().Str("targetType", targetType.String()).Str("foundType", holder.reflectType.String()).Msg("Type mismatch during assignment check")
				return fmt.Errorf("internal error: found dependency type %s is not assignable to target type %s", holder.reflectType, targetType)
			}
		}

		// 5. Assign Value
		if !targetElem.CanSet() {
			// This usually means the original variable passed to Get (e.g., in Get(&myVar))
			// was not addressable, which shouldn't happen if it's a pointer to a variable.
			// Could happen with pointers to map elements or unexported struct fields from another package.
			log.Error().Str("targetType", targetType.String()).Msg("Cannot set target value (is it addressable?)")
			return fmt.Errorf("cannot set target value for type %s", targetType)
		}
		targetElem.Set(holder.reflectVal)
		// log.Debug().Str("type", targetType.String()).Msg("Dependency assigned")

	} // End target loop

	if len(missingDeps) > 0 {
		return fmt.Errorf("%w: missing types %v", ErrDependencyNotFound, missingDeps)
	}

	return nil // All targets resolved successfully
}

// findDependencyByType searches the registry for a value assignable to the targetType.
// Handles exact type matches and interface implementations.
// This function MUST be called with the appropriate lock held.
func (r *DependencyRegistry) findDependencyByType(targetType reflect.Type) (dependencyHolder, error) {
	// 1. Exact Type Match
	if holder, found := r.store[targetType]; found {
		// Found direct match (concrete type or interface type if registered explicitly)
		return holder, nil
	}

	// 2. Interface Implementation Match
	if targetType.Kind() == reflect.Interface {
		var foundHolder dependencyHolder
		var foundCount int

		for _, holder := range r.store {
			// Check if the *stored concrete type* implements the *target interface type*
			if holder.reflectType.Implements(targetType) {
				// Also check if the stored value itself is non-nil interface that is assignable
				// This handles cases where an interface was registered holding nil explicitly.
				// Generally, we rely on the concrete type check above.
				// Optional: Add check here if needed: holder.reflectVal.IsValid() && !holder.reflectVal.IsNil()

				foundHolder = holder
				foundCount++
			}
		}

		if foundCount == 1 {
			log.Debug().Str("interface", targetType.String()).Str("implementation", foundHolder.reflectType.String()).Msg("Found unique implementation for interface")
			return foundHolder, nil
		}
		if foundCount > 1 {
			log.Error().Str("interface", targetType.String()).Int("count", foundCount).Msg("Multiple implementations found for interface")
			return dependencyHolder{}, fmt.Errorf("%w: interface %s has multiple implementations registered", ErrAmbiguousInterface, targetType.String())
		}
	}

	// 3. Not Found
	log.Debug().Str("type", targetType.String()).Msg("Dependency not found")
	return dependencyHolder{}, ErrDependencyNotFound
}
