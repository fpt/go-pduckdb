package types

import "reflect"

// ColumnType represents a database column type
type ColumnType struct {
	name         string
	databaseType string
	length       int64
	scanType     reflect.Type
	precision    int64
	scale        int64
}

func NewColumnType(name, databaseType string, length int64, scanType reflect.Type, precision, scale int64) *ColumnType {
	return &ColumnType{
		name:         name,
		databaseType: databaseType,
		length:       length,
		scanType:     scanType,
		precision:    precision,
		scale:        scale,
	}
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
	// In DuckDB, all columns are nullable by default
	return true, true
}

// ScanType returns the Go type used to scan values of this column
func (ct *ColumnType) ScanType() reflect.Type {
	return ct.scanType
}
