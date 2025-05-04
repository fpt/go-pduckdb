package pduckdb

import (
	"database/sql/driver"
	"reflect"
	"time"
)

// ErrDuckDB represents an error from DuckDB operations
type ErrDuckDB struct {
	Message string
}

func (e ErrDuckDB) Error() string {
	return e.Message
}

// Date represents a DuckDB date
type Date struct {
	Days int32
}

// NewDate creates a new Date with the specified number of days
func NewDate(days int32) *Date {
	return &Date{Days: days}
}

// ToTime converts a DuckDB Date to a Go time.Time
func (d Date) ToTime() time.Time {
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	return epoch.AddDate(0, 0, int(d.Days))
}

// Value implements the driver.Valuer interface.
func (d Date) Value() (driver.Value, error) {
	return d.ToTime(), nil
}

// Time represents a DuckDB time
type Time struct {
	Micros int64
}

// NewTime creates a new Time with the specified number of microseconds
func NewTime(micros int64) *Time {
	return &Time{Micros: micros}
}

// ToTime converts a DuckDB Time to a Go time.Time
func (t Time) ToTime() time.Time {
	return time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).
		Add(time.Duration(t.Micros) * time.Microsecond)
}

// Value implements the driver.Valuer interface.
func (t Time) Value() (driver.Value, error) {
	return t.ToTime(), nil
}

// Timestamp represents a DuckDB timestamp
type Timestamp struct {
	Micros int64
}

// NewTimestamp creates a new Timestamp with the specified number of microseconds
func NewTimestamp(micros int64) *Timestamp {
	return &Timestamp{Micros: micros}
}

// ToTime converts a DuckDB Timestamp to a Go time.Time
func (ts Timestamp) ToTime() time.Time {
	return time.Unix(0, ts.Micros*1000).UTC()
}

// Value implements the driver.Valuer interface.
func (ts Timestamp) Value() (driver.Value, error) {
	return ts.ToTime(), nil
}

// Interval is not supported.

// HugeInt represents a DuckDB huge integer (128-bit)
type HugeInt struct {
	Lower uint64
	Upper int64
}

// NewHugeInt creates a new HugeInt
func NewHugeInt(lower uint64, upper int64) *HugeInt {
	return &HugeInt{
		Lower: lower,
		Upper: upper,
	}
}

// UHugeInt represents a DuckDB unsigned huge integer (128-bit)
type UHugeInt struct {
	Lower uint64
	Upper uint64
}

// NewUHugeInt creates a new UHugeInt
func NewUHugeInt(lower, upper uint64) *UHugeInt {
	return &UHugeInt{
		Lower: lower,
		Upper: upper,
	}
}

// Decimal represents a DuckDB decimal
type Decimal struct {
	Width uint8
	Scale uint8
	Value HugeInt
}

// NewDecimal creates a new Decimal
func NewDecimal(width, scale uint8, value HugeInt) *Decimal {
	return &Decimal{
		Width: width,
		Scale: scale,
		Value: value,
	}
}

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

// ColumnType represents a database column type
type ColumnType struct {
	name         string
	databaseType string
	length       int64
	nullable     bool
	scanType     reflect.Type
	precision    int64
	scale        int64
}

// Name returns the name of the column
func (ct *ColumnType) Name() string {
	return ct.name
}

// DatabaseTypeName returns the database type name of the column
func (ct *ColumnType) DatabaseTypeName() string {
	return ct.databaseType
}

// Length returns the length of the column type
func (ct *ColumnType) Length() (length int64, ok bool) {
	return ct.length, ct.length > 0
}

// DecimalSize returns the precision and scale of a decimal type
func (ct *ColumnType) DecimalSize() (precision, scale int64, ok bool) {
	if ct.databaseType == "DECIMAL" {
		return ct.precision, ct.scale, true
	}
	return 0, 0, false
}

// Nullable returns whether the column is nullable
func (ct *ColumnType) Nullable() (nullable, ok bool) {
	return ct.nullable, true
}

// ScanType returns the Go type used to scan values of this column
func (ct *ColumnType) ScanType() reflect.Type {
	return ct.scanType
}
