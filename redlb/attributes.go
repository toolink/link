package redlb

import "google.golang.org/grpc/attributes"

// metadataKey is the type used for storing ServiceInstance.Metadata
// in gRPC resolver.Address attributes. Using a dedicated type avoids collisions.
type metadataKey struct{}

// AttachMetadata attaches the service metadata to gRPC attributes.
func AttachMetadata(attr *attributes.Attributes, md map[string]string) *attributes.Attributes {
	// Make a copy to avoid modifying the original ServiceInstance map if attr is shared/cached.
	mdCopy := make(map[string]string, len(md))
	for k, v := range md {
		mdCopy[k] = v
	}
	return attr.WithValue(metadataKey{}, mdCopy)
}

// GetMetadataFromAttributes extracts service metadata from gRPC attributes.
// Returns nil if no metadata is found.
func GetMetadataFromAttributes(attr *attributes.Attributes) map[string]string {
	if attr == nil {
		return nil
	}
	md, _ := attr.Value(metadataKey{}).(map[string]string)
	// Return the map (which might be nil if not found or wrong type)
	return md
}
