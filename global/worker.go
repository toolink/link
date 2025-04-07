package global

import (
	"sync/atomic"

	"github.com/toolink/link/worker"
)

var globalConsumerManager = &atomic.Value{}

// SetConsumerManager sets the global consumer manager instance.
func SetConsumerManager(cm *worker.ConsumerManager) {
	globalConsumerManager.Store(cm)
}

// GetConsumerManager retrieves the current global consumer manager instance.
func GetConsumerManager() *worker.ConsumerManager {
	return globalConsumerManager.Load().(*worker.ConsumerManager)
}
