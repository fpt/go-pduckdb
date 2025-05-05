package types

// NOTE: Interval is not supported due to prego limitations.

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
