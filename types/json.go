package types

import (
	"database/sql/driver"
)

// JSON represents a DuckDB JSON value
type JSON struct {
	value string
}

// NewJSON creates a new JSON with the specified string value
func NewJSON(value string) *JSON {
	return &JSON{
		value: value,
	}
}

// String returns the string representation of the JSON value
func (j *JSON) String() string {
	return j.value
}

// MarshalJSON implements the json.Marshaler interface
func (j *JSON) MarshalJSON() ([]byte, error) {
	return []byte(j.value), nil
}

// UnmarshalJSON implements the json.Unmarshaler interface
func (j *JSON) UnmarshalJSON(data []byte) error {
	j.value = string(data)
	return nil
}

// Value implements the driver.Valuer interface.
func (j *JSON) Value() (driver.Value, error) {
	return j.value, nil
}
