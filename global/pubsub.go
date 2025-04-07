package global

import (
	"sync/atomic"

	"github.com/toolink/link/pubsub"
)

func defaultBroker() *atomic.Value {
	v := &atomic.Value{}
	v.Store(pubsub.New())
	return v
}

var globalBroker = defaultBroker()

// SetBroker sets the global broker instance.
func SetBroker(b *pubsub.Broker) {
	globalBroker.Store(b)
}

// GetBroker retrieves the current global broker instance.
func GetBroker() *pubsub.Broker {
	return globalBroker.Load().(*pubsub.Broker)
}
