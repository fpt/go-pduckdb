package convert

import (
	"fmt"
	"strings"
)

// ToBoolean converts a value to a boolean
func ToBoolean(value any) (bool, error) {
	switch v := value.(type) {
	case bool:
		return v, nil
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		// Convert numeric types to boolean (0 = false, non-zero = true)
		return v != 0, nil
	case string:
		// Accept "true"/"false" or "1"/"0"
		v = strings.ToLower(strings.TrimSpace(v))
		return v == "true" || v == "1" || v == "t" || v == "yes" || v == "y", nil
	default:
		return false, fmt.Errorf("cannot convert %T to boolean", value)
	}
}
