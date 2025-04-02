package redlb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/resolver"
)

// Scheme defines the URI scheme for the redlb resolver.
const Scheme = "redlb"

// Ensure implementation matches interface.
var _ resolver.Builder = (*ResolverBuilder)(nil)
var _ resolver.Resolver = (*redisResolver)(nil)

// ResolverBuilder implements gRPC's resolver.Builder.
type ResolverBuilder struct {
	registry Registry
}

// NewResolverBuilder creates a gRPC resolver builder using the provided redlb Registry.
func NewResolverBuilder(registry Registry) resolver.Builder {
	if registry == nil {
		// Or panic? Builder creation should likely succeed.
		log.Error().Msg("cannot create resolver builder with nil registry")
		// Return a builder that always errors?
		return &ResolverBuilder{} // Or return an error if signature allowed
	}
	return &ResolverBuilder{registry: registry}
}

// Build creates a new gRPC resolver.Resolver for the given target.
// Target format expected: "redlb:///service-name" or "redlb://authority/service-name"
// The authority part is parsed but currently ignored by this implementation.
func (b *ResolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	if b.registry == nil {
		return nil, errors.New("resolver builder created with nil registry")
	}

	serviceName := ""
	// Prefer Path for service name extraction as per gRPC spec (e.g., "dns:///service")
	if target.URL.Path != "" {
		serviceName = strings.TrimPrefix(target.URL.Path, "/")
	} else if target.Endpoint() != "" {
		// Fallback for older formats or manual target specification without scheme
		// Might be ambiguous if endpoint contains slashes.
		log.Warn().Str("target_endpoint", target.Endpoint()).Str("target_url", target.URL.String()).Msg("using endpoint as service name due to empty url path")
		serviceName = target.Endpoint()
	}

	if serviceName == "" {
		err := fmt.Errorf("target %q must specify a service name in the path (e.g., %s:///your-service)", target.URL.String(), Scheme)
		log.Error().Err(err).Msg("invalid resolver target")
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	r := &redisResolver{
		registry:    b.registry,
		serviceName: serviceName,
		cc:          cc,
		ctx:         ctx,
		cancel:      cancel,
		// watchCh and wg are initialized in startWatching
	}

	// Start the background watcher
	if err := r.startWatching(); err != nil {
		cancel() // Clean up context if start fails
		log.Error().Err(err).Str("service", serviceName).Msg("failed to start resolver watcher")
		return nil, fmt.Errorf("failed to start watcher for %s: %w", serviceName, err)
	}

	log.Info().Str("service", serviceName).Str("target", target.URL.String()).Msg("grpc resolver built")
	return r, nil
}

// Scheme returns the scheme supported by this builder ("redlb").
func (b *ResolverBuilder) Scheme() string {
	return Scheme
}

// redisResolver implements gRPC's resolver.Resolver.
type redisResolver struct {
	registry    Registry
	serviceName string
	cc          resolver.ClientConn
	ctx         context.Context
	cancel      context.CancelFunc
	watchCh     <-chan []*ServiceInstance
	wg          sync.WaitGroup
}

// startWatching initializes the registry watch and starts the goroutine
// to push updates to the gRPC ClientConn.
func (r *redisResolver) startWatching() error {
	var err error
	// Use the resolver's context for the watch
	r.watchCh, err = r.registry.Watch(r.ctx, r.serviceName)
	if err != nil {
		return err // Error already logged by registry.Watch or NewRedisRegistry
	}

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		log.Debug().Str("service", r.serviceName).Msg("resolver watch goroutine started")
		for {
			select {
			case <-r.ctx.Done():
				log.Info().Str("service", r.serviceName).Msg("resolver watch goroutine stopping")
				return
			case instances, ok := <-r.watchCh:
				if !ok {
					// Watch channel closed, likely due to registry Close() or unrecoverable error.
					// Report an error to the ClientConn? gRPC might handle this by trying to reconnect/re-resolve.
					log.Warn().Str("service", r.serviceName).Msg("resolver watch channel closed")
					r.cc.ReportError(fmt.Errorf("registry watch channel closed for service %s", r.serviceName))
					return // Exit goroutine
				}

				// Convert ServiceInstances to resolver.Address, including metadata
				addresses := make([]resolver.Address, 0, len(instances))
				for _, inst := range instances {
					addr := resolver.Address{
						Addr:       inst.Address,
						ServerName: inst.Name, // Can be useful for SNI/TLS verification
						// Attach metadata using our helper and key
						Attributes: AttachMetadata(nil, inst.Metadata),
					}
					addresses = append(addresses, addr)
				}

				// Update gRPC client connection state
				state := resolver.State{Addresses: addresses}
				err := r.cc.UpdateState(state)
				if err != nil {
					// Log error, gRPC might retry or handle internally
					log.Error().Err(err).Str("service", r.serviceName).Msg("failed to update grpc client connection state")
				} else {
					log.Debug().Str("service", r.serviceName).Int("count", len(addresses)).Msg("grpc client connection state updated")
				}
			}
		}
	}()
	return nil
}

// ResolveNow is called by gRPC to request an immediate resolution update.
// Since our watcher polls periodically, this is often a no-op, but
// we could optionally trigger an immediate Discover here if needed.
func (r *redisResolver) ResolveNow(opts resolver.ResolveNowOptions) {
	log.Trace().Str("service", r.serviceName).Interface("options", opts).Msg("resolve now requested")
	// Potentially trigger an immediate Discover and update ClientConn here,
	// but the polling watcher usually handles this implicitly soon enough.
}

// Close stops the resolver, canceling the watcher context and waiting for cleanup.
func (r *redisResolver) Close() {
	log.Info().Str("service", r.serviceName).Msg("closing grpc resolver")
	r.cancel()  // Signal the watcher goroutine to stop
	r.wg.Wait() // Wait for the watcher goroutine to fully exit
	log.Debug().Str("service", r.serviceName).Msg("grpc resolver closed")
}

// RegisterService is a convenience function for gRPC servers to register themselves.
// It generates a UUID if instanceID is empty.
func RegisterService(ctx context.Context, registry Registry, serviceName, instanceID, listenAddress string, metadata map[string]string) (deregister func(context.Context) error, err error) {
	if instanceID == "" {
		instanceID = uuid.NewString()
	}
	instance := &ServiceInstance{
		ID:       instanceID,
		Name:     serviceName,
		Address:  listenAddress, // The address clients should connect to
		Metadata: metadata,
	}
	log.Info().Stringer("instance", instance).Msg("attempting to register grpc service")
	return registry.Register(ctx, instance)
}
