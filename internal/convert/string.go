package convert

import (
	"fmt"
)

// ToString converts a value to a string
func ToString(value any) (string, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	case fmt.Stringer:
		return v.String(), nil
	default:
		// Use fmt.Sprint as a last resort
		return fmt.Sprint(value), nil
	}
}
