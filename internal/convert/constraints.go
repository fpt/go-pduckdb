package convert

// integer is the set of integer types the numeric converters target. It mirrors
// golang.org/x/exp/constraints.Integer so that dependency is not needed.
type integer interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr
}

// float is the set of floating-point types, mirroring constraints.Float.
type float interface {
	~float32 | ~float64
}
