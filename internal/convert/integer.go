package convert

import (
	"fmt"
	"math"
	"strconv"

	"golang.org/x/exp/constraints"
)

// ToInt8 converts a value to an int8
func ToInt8(value any) (int8, error) {
	return ToIntX[int8](value, math.MinInt8, math.MaxInt8)
}

// ToInt16 converts a value to an int16
func ToInt16(value any) (int16, error) {
	return ToIntX[int16](value, math.MinInt16, math.MaxInt16)
}

// ToInt32 converts a value to an int32
func ToInt32(value any) (int32, error) {
	return ToIntX[int32](value, math.MinInt32, math.MaxInt32)
}

// ToInt64 converts a value to an int64
func ToInt64(value any) (int64, error) {
	return ToIntX[int64](value, math.MinInt64, math.MaxInt64)
}

// ToUint8 converts a value to a uint8
func ToUint8(value any) (uint8, error) {
	return ToIntX[uint8](value, 0, math.MaxUint8)
}

// ToUint16 converts a value to a uint16
func ToUint16(value any) (uint16, error) {
	return ToIntX[uint16](value, 0, math.MaxUint16)
}

// ToUint32 converts a value to a uint32
func ToUint32(value any) (uint32, error) {
	return ToIntX[uint32](value, 0, math.MaxUint32)
}

// ToUint64 converts a value to a uint64
func ToUint64(value any) (uint64, error) {
	return ToIntX[uint64](value, 0, math.MaxUint64)
}

// ToIntX is a generic function to convert a value to an integer type
func ToIntX[T constraints.Integer](value any, minValue, maxValue T) (T, error) {
	switch v := value.(type) {
	case int8:
		if int64(v) < int64(minValue) || int64(v) > int64(maxValue) {
			return 0, fmt.Errorf("value %d out of range for %T", v, minValue)
		}
		return T(v), nil
	case int16:
		if int64(v) < int64(minValue) || int64(v) > int64(maxValue) {
			return 0, fmt.Errorf("value %d out of range for %T", v, minValue)
		}
		return T(v), nil
	case int32:
		if int64(v) < int64(minValue) || int64(v) > int64(maxValue) {
			return 0, fmt.Errorf("value %d out of range for %T", v, minValue)
		}
		return T(v), nil
	case int64:
		if v < int64(minValue) || v > int64(maxValue) {
			return 0, fmt.Errorf("value %d out of range for %T", v, minValue)
		}
		return T(v), nil
	case int:
		if int64(v) < int64(minValue) || int64(v) > int64(maxValue) {
			return 0, fmt.Errorf("value %d out of range for %T", v, minValue)
		}
		return T(v), nil
	case uint8:
		if uint64(v) < uint64(minValue) || uint64(v) > uint64(maxValue) {
			return 0, fmt.Errorf("value %d out of range for %T", v, minValue)
		}
		return T(v), nil
	case uint16:
		if uint64(v) < uint64(minValue) || uint64(v) > uint64(maxValue) {
			return 0, fmt.Errorf("value %d out of range for %T", v, minValue)
		}
		return T(v), nil
	case uint32:
		if uint64(v) < uint64(minValue) || uint64(v) > uint64(maxValue) {
			return 0, fmt.Errorf("value %d out of range for %T", v, minValue)
		}
		return T(v), nil
	case uint64:
		if v < uint64(minValue) || v > uint64(maxValue) {
			return 0, fmt.Errorf("value %d out of range for %T", v, minValue)
		}
		return T(v), nil
	case uint:
		if uint64(v) < uint64(minValue) || uint64(v) > uint64(maxValue) {
			return 0, fmt.Errorf("value %d out of range for %T", v, minValue)
		}
		return T(v), nil
	case float32:
		if v < float32(minValue) || v > float32(maxValue) || float32(T(v)) != v {
			return 0, fmt.Errorf("value %f cannot be exactly represented as %T", v, minValue)
		}
		return T(v), nil
	case float64:
		if v < float64(minValue) || v > float64(maxValue) || float64(T(v)) != v {
			return 0, fmt.Errorf("value %f cannot be exactly represented as %T", v, minValue)
		}
		return T(v), nil
	case string:
		i, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("cannot convert string '%s' to %T: %v", v, minValue, err)
		}
		if i < int64(minValue) || i > int64(maxValue) {
			return 0, fmt.Errorf("value %d out of range for %T", i, minValue)
		}
		return T(i), nil
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to %T", value, minValue)
	}
}
