package global

import (
	"sync/atomic"

	"github.com/toolink/link/pubsub"
)

func defaultBroker() *atomic.Value {
	v := &atomic.Value{}
	// Initialize with default (memory) broker. Panic on error.
	broker, err := pubsub.New()
	if err != nil {
		panic("failed to initialize default global pubsub broker: " + err.Error())
	}
	v.Store(broker)
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
