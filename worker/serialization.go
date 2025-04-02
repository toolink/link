package worker

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
)

// --- Serialization ---

// serializeArgs converts arguments to JSON bytes.
func serializeArgs(args ...any) ([]byte, error) {
	return json.Marshal(args)
}

// deserializeArgs converts JSON bytes back to a slice of any.
func deserializeArgs(data []byte) ([]any, error) {
	var args []any
	// Important: Use json.Unmarshal with *[]any to handle diverse types correctly.
	err := json.Unmarshal(data, &args)
	return args, err
}

// --- Reflection Helper (same as before) ---

// prepareArgForCall attempts to make the deserialized argument `arg` compatible with `targetType`.
// Handles common JSON number issues (float64 -> int).
func prepareArgForCall(arg any, targetType reflect.Type) (reflect.Value, error) {
	if arg == nil {
		// Check if target type is nillable
		k := targetType.Kind()
		if k >= reflect.Chan && k <= reflect.Slice || k == reflect.Interface || k == reflect.Ptr {
			return reflect.Zero(targetType), nil // Use zero value for nillable types
		}
		return reflect.Value{}, fmt.Errorf("nil argument provided for non-nillable type %s", targetType)
	}

	argVal := reflect.ValueOf(arg)
	argType := argVal.Type()

	// Direct assignability check first
	if argType.AssignableTo(targetType) {
		return argVal, nil
	}

	// Common case: JSON number (float64) needs to be converted to int/uint etc.
	if argType.Kind() == reflect.Float64 {
		targetKind := targetType.Kind()
		if targetKind >= reflect.Int && targetKind <= reflect.Uint64 {
			floatVal := argVal.Float()
			// Basic check for non-integer float to prevent data loss
			if floatVal != float64(int64(floatVal)) {
				return reflect.Value{}, fmt.Errorf("cannot assign non-integer float64 (%f) to integer type %s", floatVal, targetType)
			}
			// Ensure value fits within target type's range (example for int64)
			// Add checks for other int/uint sizes as needed
			if targetKind == reflect.Int64 && (floatVal < float64(math.MinInt64) || floatVal > float64(math.MaxInt64)) {
				return reflect.Value{}, fmt.Errorf("float64 value %f out of range for %s", floatVal, targetType)
			}
			// ... add checks for Int32, Uint64 etc.

			// Convert via reflect after checks
			// Use Convert instead of casting directly to handle different int/uint sizes via reflection
			// Need to handle potential conversion panic if checks aren't exhaustive, though unlikely after range checks.
			convertedVal := reflect.ValueOf(floatVal).Convert(targetType)
			return convertedVal, nil
		}
	}

	// General convertibility check (use with caution)
	// if argType.ConvertibleTo(targetType) {
	//     log.Warn().Str("from", argType.String()).Str("to", targetType.String()).Msg("performing reflection type conversion")
	//     return argVal.Convert(targetType), nil
	// }

	return reflect.Value{}, fmt.Errorf("type mismatch: cannot assign %s to %s", argType, targetType)
}
