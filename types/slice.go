package types

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strings"
)

// StringSlice represents a slice of strings that can be scanned from a DuckDB array/list
type StringSlice []string

// Scan implements the sql.Scanner interface for StringSlice
func (ss *StringSlice) Scan(value any) error {
	// Add debug printing
	fmt.Printf("StringSlice.Scan() value type: %T, value: %v\n", value, value)

	if value == nil {
		*ss = StringSlice{}
		return nil
	}

	// For string values coming from DuckDB (e.g., VARCHAR[] or LIST types)
	switch v := value.(type) {
	case string:
		// Handle empty strings
		if v == "" {
			*ss = StringSlice{}
			return nil
		}

		// Try first as JSON format (used when arrays/lists contain complex values)
		var result []string
		err := json.Unmarshal([]byte(v), &result)
		if err == nil {
			*ss = result
			return nil
		}

		// If JSON parsing fails, try to parse DuckDB's array string format: ['value1', 'value2', 'value3']
		if v[0] == '[' && v[len(v)-1] == ']' {
			// Remove outer brackets
			content := v[1 : len(v)-1]

			// If array is empty
			if strings.TrimSpace(content) == "" {
				*ss = StringSlice{}
				return nil
			}

			// Split into elements and handle quotes
			var elements []string

			// Simple parsing: split by commas and handle quoted strings
			// This is a basic implementation and may not handle all edge cases
			inQuote := false
			currentElement := ""
			for _, char := range content {
				if char == '\'' || char == '"' {
					inQuote = !inQuote
					continue
				}

				if char == ',' && !inQuote {
					elements = append(elements, strings.TrimSpace(currentElement))
					currentElement = ""
					continue
				}

				currentElement += string(char)
			}

			// Add the last element
			if currentElement != "" {
				elements = append(elements, strings.TrimSpace(currentElement))
			}

			*ss = elements
			return nil
		}

		// If we get here, the format is not recognized
		return fmt.Errorf("cannot parse string slice from: %s", v)

	case []byte:
		// Handle []byte by converting to string
		return ss.Scan(string(v))

	case []any:
		// Handle array of interfaces (might come from database/sql)
		result := make([]string, len(v))
		for i, item := range v {
			if item == nil {
				result[i] = ""
			} else {
				result[i] = fmt.Sprintf("%v", item)
			}
		}
		*ss = result
		return nil

	default:
		return fmt.Errorf("unsupported Scan, storing driver.Value type %T into type StringSlice", value)
	}
}

// Value implements the driver.Valuer interface for StringSlice
func (ss StringSlice) Value() (driver.Value, error) {
	if ss == nil {
		return nil, nil
	}

	// For parameter binding, we'll use DuckDB's array literal format instead of JSON
	// This format is: ['value1', 'value2', 'value3']
	if len(ss) == 0 {
		return "[]", nil
	}

	// Escape single quotes in values and wrap each in single quotes
	parts := make([]string, len(ss))
	for i, s := range ss {
		// Replace single quotes with two single quotes (SQL escape)
		escaped := strings.ReplaceAll(s, "'", "''")
		parts[i] = fmt.Sprintf("'%s'", escaped)
	}

	return fmt.Sprintf("[%s]", strings.Join(parts, ", ")), nil
}

// Interface guards
var (
	_ driver.Valuer = StringSlice{}
	_ sql.Scanner   = (*StringSlice)(nil)
)
