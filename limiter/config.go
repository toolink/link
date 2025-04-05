package limiter

import (
	"fmt"
	"regexp"

	"github.com/rs/zerolog/log"
)

// Valid LimitBy types
var validLimitBy = map[string]bool{
	LimitByIP:       true,
	LimitByDeviceID: true,
	LimitByUserID:   true,
}

// Rule defines a single rate limiting rule.
type Rule struct {
	Path    string   // request path (can be regex if IsRegex is true)
	IsRegex bool     // indicates if Path is a regex
	Rate    float64  // number of allowed requests (tokens)
	Period  float64  // time window in seconds
	LimitBy []string `mapstructure:"limit_by"` // list of identifiers to limit by ("ip", "device_id", "user_id")

	// internal fields
	compiledRegex *regexp.Regexp // compiled regex for performance
	ratePerSecond float64        // calculated rate / period
}

// Config holds the overall rate limiter configuration.
type Config struct {
	StorageType string `yaml:"storage_type"` // "memory" or "redis"
	Rules       []Rule `yaml:"rules"`
}

// ValidateAndPrepare processes the raw config, validates it, and prepares internal fields.
func (c *Config) ValidateAndPrepare() error {
	if c.StorageType != StorageMemory && c.StorageType != StorageRedis {
		return fmt.Errorf("invalid storage_type: %s, must be '%s' or '%s'", c.StorageType, StorageMemory, StorageRedis)
	}

	if len(c.Rules) == 0 {
		log.Warn().Msg("no rate limit rules defined in config")
	}

	seenPaths := make(map[string]bool)
	for i := range c.Rules {
		rule := &c.Rules[i] // operate on pointer to modify original slice element

		// validate path uniqueness
		if seenPaths[rule.Path] {
			return fmt.Errorf("duplicate path definition found: %s", rule.Path)
		}
		seenPaths[rule.Path] = true

		// validate rate and per
		if rule.Rate <= 0 {
			return fmt.Errorf("rule for path '%s' has invalid rate: %f, must be positive", rule.Path, rule.Rate)
		}
		if rule.Period <= 0 {
			return fmt.Errorf("rule for path '%s' has invalid period: %f, must be positive", rule.Path, rule.Period)
		}
		rule.ratePerSecond = rule.Rate / rule.Period // pre-calculate

		// compile regex if needed
		if rule.IsRegex {
			re, err := regexp.Compile(rule.Path)
			if err != nil {
				return fmt.Errorf("failed to compile regex for path '%s': %w", rule.Path, err)
			}
			rule.compiledRegex = re
		}

		// validate LimitBy types
		if len(rule.LimitBy) == 0 {
			return fmt.Errorf("rule for path '%s' must have at least one limit_by type", rule.Path)
		}
		for _, lb := range rule.LimitBy {
			if !validLimitBy[lb] {
				return fmt.Errorf("rule for path '%s' has invalid limit_by type: '%s'", rule.Path, lb)
			}
		}
	}
	return nil
}
