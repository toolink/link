package limiter

// LimitBy types
const (
	LimitByIP       = "ip"
	LimitByDeviceID = "device_id"
	LimitByUserID   = "user_id"
)

// Storage types
const (
	StorageMemory = "memory"
	StorageRedis  = "redis"
)
