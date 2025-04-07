package global

import (
	"sync/atomic"

	"github.com/toolink/link/extension"
)

func defaultExtensionManager() *atomic.Value {
	v := &atomic.Value{}
	v.Store(extension.New())
	return v
}

var globalExtensionManager = defaultExtensionManager()

// SetExtensionManager sets the global extension manager.
func SetExtensionManager(m *extension.ExtensionManager) {
	globalExtensionManager.Store(m)
}

// GetExtensionManager retrieves the current global extension manager.
func GetExtensionManager() *extension.ExtensionManager {
	return globalExtensionManager.Load().(*extension.ExtensionManager)
}
