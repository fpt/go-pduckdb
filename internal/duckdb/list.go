package duckdb

import (
	"fmt"
	"reflect"
)

// IsListParam reports whether v is a Go slice that BindParameter binds as a
// LIST value: a slice (not []byte, which is a BLOB) of bool, integers, floats,
// strings, or such slices.
func IsListParam(v any) bool {
	t := reflect.TypeOf(v)
	return t != nil && t.Kind() == reflect.Slice && t.Elem().Kind() != reflect.Uint8 &&
		listElemType(t.Elem()) != DuckDBTypeInvalid
}

// listElemType maps a Go element type to the DuckDB type of a list element.
// It returns DuckDBTypeList for a nested slice and DuckDBTypeInvalid for a
// type that cannot be a list element.
func listElemType(t reflect.Type) DuckDBType {
	switch t.Kind() {
	case reflect.Bool:
		return DuckDBTypeBoolean
	case reflect.Int8:
		return DuckDBTypeTinyint
	case reflect.Int16:
		return DuckDBTypeSmallint
	case reflect.Int32:
		return DuckDBTypeInteger
	case reflect.Int, reflect.Int64:
		return DuckDBTypeBigint
	case reflect.Uint8:
		return DuckDBTypeUTinyint
	case reflect.Uint16:
		return DuckDBTypeUSmallint
	case reflect.Uint32:
		return DuckDBTypeUInteger
	case reflect.Uint, reflect.Uint64:
		return DuckDBTypeUBigint
	case reflect.Float32:
		return DuckDBTypeFloat
	case reflect.Float64:
		return DuckDBTypeDouble
	case reflect.String:
		return DuckDBTypeVarchar
	case reflect.Slice:
		// A nested []byte would be a BLOB element, which is not supported.
		if t.Elem().Kind() != reflect.Uint8 && listElemType(t.Elem()) != DuckDBTypeInvalid {
			return DuckDBTypeList
		}
	}
	return DuckDBTypeInvalid
}

// listLogicalType creates the logical type of a list whose elements have Go
// type t. The caller destroys it.
func (db *DB) listLogicalType(t reflect.Type) DuckDBLogicalType {
	if t.Kind() != reflect.Slice {
		return db.CreateLogicalType(listElemType(t))
	}
	child := db.listLogicalType(t.Elem())
	defer db.DestroyLogicalType(&child)
	return db.CreateListType(child)
}

// createListValue builds a LIST value from the slice v, recursively for nested
// slices. The caller destroys it.
func (db *DB) createListValue(v reflect.Value) (DuckDBValue, error) {
	elemType := db.listLogicalType(v.Type().Elem())
	defer db.DestroyLogicalType(&elemType)

	// duckdb_create_list_value copies the children, so they are destroyed here.
	// It also rejects a NULL values array, so an empty list gets a one-slot one.
	children := make([]DuckDBValue, v.Len(), max(v.Len(), 1))
	defer func() {
		for i := range children {
			if children[i] != nil {
				db.DestroyValue(&children[i])
			}
		}
	}()
	for i := range v.Len() {
		child, err := db.createElemValue(v.Index(i))
		if err != nil {
			return nil, err
		}
		children[i] = child
	}

	list := db.CreateListValue(elemType, &children[:1][0], int64(v.Len()))
	if list == nil {
		return nil, fmt.Errorf("failed to create a LIST value from %s", v.Type())
	}
	return list, nil
}

func (db *DB) createElemValue(v reflect.Value) (DuckDBValue, error) {
	var out DuckDBValue
	switch v.Kind() {
	case reflect.Bool:
		out = db.CreateBool(v.Bool())
	case reflect.Int8:
		out = db.CreateInt8(int8(v.Int()))
	case reflect.Int16:
		out = db.CreateInt16(int16(v.Int()))
	case reflect.Int32:
		out = db.CreateInt32(int32(v.Int()))
	case reflect.Int, reflect.Int64:
		out = db.CreateInt64(v.Int())
	case reflect.Uint8:
		out = db.CreateUint8(uint8(v.Uint()))
	case reflect.Uint16:
		out = db.CreateUint16(uint16(v.Uint()))
	case reflect.Uint32:
		out = db.CreateUint32(uint32(v.Uint()))
	case reflect.Uint, reflect.Uint64:
		out = db.CreateUint64(v.Uint())
	case reflect.Float32:
		out = db.CreateFloat(float32(v.Float()))
	case reflect.Float64:
		out = db.CreateDouble(v.Float())
	case reflect.String:
		out = db.CreateVarchar(v.String())
	case reflect.Slice:
		return db.createListValue(v)
	default:
		return nil, fmt.Errorf("unsupported list element type %s", v.Type())
	}
	if out == nil {
		return nil, fmt.Errorf("failed to create a value from %s", v.Type())
	}
	return out, nil
}
