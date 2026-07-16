package duckdb

import (
	"unsafe"
)

// DataChunk wraps a duckdb_data_chunk handle together with its row count.
//
// A chunk is columnar: it holds up to STANDARD_VECTOR_SIZE (2048) tuples, and
// each column is a Vector. Chunks are obtained by streaming a result with
// duckdb_fetch_chunk and must be released with Destroy.
type DataChunk struct {
	handle DuckDBDataChunk
	db     *DB
	size   int
}

// Size returns the number of rows (tuples) held in the chunk.
func (c *DataChunk) Size() int { return c.size }

// Vector returns the Vector for the given column, caching its data and
// validity pointers so per-row reads are pure pointer arithmetic.
func (c *DataChunk) Vector(col int64) *Vector {
	h := c.db.DataChunkGetVector(c.handle, col)
	return &Vector{
		handle:   h,
		db:       c.db,
		data:     c.db.VectorGetData(h),
		validity: c.db.VectorGetValidity(h),
	}
}

// Destroy releases the chunk's memory. Safe to call more than once.
func (c *DataChunk) Destroy() {
	if c.handle != nil {
		c.db.DestroyDataChunk(&c.handle)
		c.handle = nil
	}
}

// Vector is a single column within a DataChunk. data points at the column's
// physical array; validity points at the (optional) validity bitmask.
type Vector struct {
	handle   DuckDBVector
	db       *DB
	data     unsafe.Pointer
	validity *uint64
}

// RowValid reports whether row i is non-NULL. A nil validity mask means every
// row is valid, matching DuckDB's convention.
func (v *Vector) RowValid(i int) bool {
	if v.validity == nil {
		return true
	}
	word := *(*uint64)(unsafe.Add(unsafe.Pointer(v.validity), (i/64)*8))
	return (word>>(uint(i)%64))&1 == 1
}

// vectorValue reads a fixed-width value of type T for row i out of the vector's
// physical array. The caller must ensure T matches the column's physical type.
func vectorValue[T any](v *Vector, i int) T {
	var zero T
	return *(*T)(unsafe.Add(v.data, i*int(unsafe.Sizeof(zero))))
}

// bytesAt decodes the duckdb_string_t at row i (used by both VARCHAR and BLOB).
//
// duckdb_string_t is 16 bytes: a uint32 length, then either the inlined bytes
// (when length <= 12) or a 4-byte prefix followed by an 8-byte pointer to the
// remaining bytes. The referenced memory is owned by the chunk, so we copy it
// out to a Go slice that outlives the chunk.
func (v *Vector) bytesAt(i int) []byte {
	base := unsafe.Add(v.data, i*sizeofStringT)
	length := *(*uint32)(base)
	if length == 0 {
		return []byte{}
	}
	var src unsafe.Pointer
	if length <= stringInlineMax {
		src = unsafe.Add(base, 4)
	} else {
		src = *(*unsafe.Pointer)(unsafe.Add(base, 8))
	}
	out := make([]byte, length)
	copy(out, unsafe.Slice((*byte)(src), length))
	return out
}

// intervalAt decodes the duckdb_interval (int32 months, int32 days, int64 micros).
func (v *Vector) intervalAt(i int) Interval {
	base := unsafe.Add(v.data, i*sizeofInterval)
	return Interval{
		Months: *(*int32)(base),
		Days:   *(*int32)(unsafe.Add(base, 4)),
		Micros: *(*int64)(unsafe.Add(base, 8)),
	}
}

// hugeintAt decodes the duckdb_hugeint (uint64 lower, int64 upper) at row i.
func (v *Vector) hugeintAt(i int) Hugeint {
	base := unsafe.Add(v.data, i*sizeofHugeint)
	return Hugeint{
		Lower: *(*uint64)(base),
		Upper: *(*int64)(unsafe.Add(base, 8)),
	}
}

const (
	// sizeofStringT is sizeof(duckdb_string_t).
	sizeofStringT = 16
	// stringInlineMax is the largest string length stored inline in duckdb_string_t.
	stringInlineMax = 12
	// sizeofInterval is sizeof(duckdb_interval).
	sizeofInterval = 16
	// sizeofHugeint is sizeof(duckdb_hugeint).
	sizeofHugeint = 16
)
