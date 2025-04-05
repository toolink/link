package limiter

import (
	"context"
	"fmt"

	"github.com/rs/zerolog/log"
)

// RateLimiter contains the configuration and store for rate limiting.
type RateLimiter struct {
	config       *Config
	store        Store
	extractValue Extractor // function to extract identifier value from context
}

// NewRateLimiter creates a new RateLimiter instance.
func NewRateLimiter(cfg *Config, store Store) *RateLimiter {
	return &RateLimiter{
		config: cfg,
		store:  store,
	}
}

// SetExtractor sets the function to extract identifier values from the context.
func (rl *RateLimiter) SetExtractor(extractor Extractor) {
	rl.extractValue = extractor
}

// Limit checks if the request is allowed based on the rate limiting rules.
func (rl *RateLimiter) Limit(ctx context.Context, path string) bool {
	if rl.extractValue == nil {
		log.Error().Msg("extractor function not set, cannot limit requests")
		return false // stop processing, request is allowed
	}

	for _, rule := range rl.config.Rules {
		if rl.pathMatches(path, &rule) {
			log.Debug().Str("path", path).Str("rule_path", rule.Path).Msg("matched rule")

			// apply limit for each LimitBy type in the matched rule
			limited, err := rl.applyRuleLimits(ctx, &rule)
			if err != nil {
				// log error from store and deny request (fail-closed)
				log.Error().Err(err).Str("path", path).Str("rule_path", rule.Path).Msg("rate limit check failed")
				return false // stop processing, request is allowed
			}

			if limited {
				// rate limit exceeded for one of the LimitBy types
				log.Warn().Str("path", path).Str("rule_path", rule.Path).Msg("rate limit triggered for rule")
				return true // stop processing, request is denied
			}
		}
	}

	return false
}

// pathMatches checks if the request path matches the rule's path (exact or regex).
func (rl *RateLimiter) pathMatches(requestPath string, rule *Rule) bool {
	if rule.IsRegex {
		return rule.compiledRegex != nil && rule.compiledRegex.MatchString(requestPath)
	}
	return rule.Path == requestPath
}

// applyRuleLimits checks limits for all LimitBy types specified in the rule.
// Returns true if rate limited, false otherwise. Error if store fails.
func (rl *RateLimiter) applyRuleLimits(ctx context.Context, rule *Rule) (bool, error) {
	for _, limitType := range rule.LimitBy {
		value := rl.extractValue(ctx, limitType)
		if value == "" {
			// if identifier is missing (e.g., header not present), skip limiting by this type for this request
			log.Debug().Str("rule_path", rule.Path).Str("limit_by", limitType).Msg("identifier value missing, skipping this limit type")
			continue
		}

		// generate a unique key for the store
		key := generateStoreKey(rule, limitType, value)

		// check allowance with the store
		allowed, err := rl.store.Allow(ctx, key, rule.Rate, rule.Period)
		if err != nil {
			// Propagate store error
			return false, fmt.Errorf("store error for key %s: %w", key, err)
		}

		if !allowed {
			// limit exceeded for this specific type and value
			log.Warn().Str("key", key).Str("limit_type", limitType).Str("value", value).Str("rule_path", rule.Path).Float64("rate", rule.Rate).Float64("period", rule.Period).Msg("rate limit exceeded for identifier")
			return true, nil // limited
		}
	}
	// passed all limit checks for this rule
	return false, nil // not limited
}

type Extractor func(ctx context.Context, limitType string) string

// generateStoreKey creates a unique string key for the store.
// Format: rule:<Rule.Path>|by:<LimitType>|val:<Value>
// Ensure characters in Path/Value don't conflict with separators or are sanitized/encoded if necessary.
// Using "|" as separator here.
func generateStoreKey(rule *Rule, limitType string, value string) string {
	// Basic key generation. Consider URL encoding or hashing parts if they can contain unsafe characters.
	// Keeping it simple here.
	// Using rule.Path provides uniqueness between rules.
	return fmt.Sprintf("rule:%s|by:%s|val:%s", rule.Path, limitType, value)
}
