package duckdb

import (
	"encoding/binary"
	"fmt"
	"math/big"
	"time"
	"unsafe"
)

// cachedChunk is a fetched data chunk together with the absolute row index at
// which it starts and its lazily-built per-column vectors.
type cachedChunk struct {
	chunk   *DataChunk
	baseRow int64
	vectors []*Vector
}

// Result wraps a raw DuckDB result and reads values through the data-chunk /
// vector API (duckdb_fetch_chunk), which replaces the deprecated duckdb_value_*
// family. duckdb_query materializes the result, so streaming its chunks with
// fetch_chunk is cheap; we cache every chunk we pull (fetch_chunk is a
// one-shot stream) so random (col,row) access stays correct.
type Result struct {
	Raw DuckDBResultRaw
	Db  *DB

	// Column metadata, loaded once, lazily.
	metaLoaded bool
	colCount   int64
	colLogical []DuckDBLogicalType // owned; destroyed in Close
	colAlias   []string            // e.g. "JSON" for JSON-typed VARCHAR columns

	// Streaming chunk cache.
	chunks    []*cachedChunk
	totalRows int64
	exhausted bool

	closed bool
}

// newResult creates a new Result from a database and raw result
func newResult(db *DB, raw DuckDBResultRaw) *Result {
	return &Result{
		Raw: raw,
		Db:  db,
	}
}

// loadMeta fetches and caches column count, logical types and type aliases.
func (r *Result) loadMeta() {
	if r.metaLoaded {
		return
	}
	r.metaLoaded = true
	r.colCount = r.Db.ColumnCount(&r.Raw)
	r.colLogical = make([]DuckDBLogicalType, r.colCount)
	r.colAlias = make([]string, r.colCount)
	for i := int64(0); i < r.colCount; i++ {
		lt := r.Db.ColumnLogicalType(&r.Raw, i)
		r.colLogical[i] = lt
		if lt != nil && r.Db.LogicalTypeGetAlias != nil {
			if alias := r.Db.LogicalTypeGetAlias(lt); alias != nil {
				r.colAlias[i] = GoString(alias)
				if r.Db.Free != nil {
					r.Db.Free(unsafe.Pointer(alias))
				}
			}
		}
	}
}

// fetchMore pulls the next chunk from the stream and appends it to the cache.
// Returns false once the result is exhausted.
func (r *Result) fetchMore() bool {
	if r.exhausted {
		return false
	}
	r.loadMeta()
	h := r.Db.FetchChunk(r.Raw)
	if h == nil {
		r.exhausted = true
		return false
	}
	size := int(r.Db.DataChunkGetSize(h))
	if size == 0 {
		(&DataChunk{handle: h, db: r.Db}).Destroy()
		r.exhausted = true
		return false
	}
	r.chunks = append(r.chunks, &cachedChunk{
		chunk:   &DataChunk{handle: h, db: r.Db, size: size},
		baseRow: r.totalRows,
		vectors: make([]*Vector, r.colCount),
	})
	r.totalRows += int64(size)
	return true
}

// locate maps an absolute row index to its cached chunk and in-chunk offset,
// fetching more chunks as needed.
func (r *Result) locate(row int64) (*cachedChunk, int, bool) {
	if row < 0 {
		return nil, 0, false
	}
	for row >= r.totalRows && !r.exhausted {
		r.fetchMore()
	}
	if row >= r.totalRows {
		return nil, 0, false
	}
	// Binary search for the last chunk whose baseRow <= row.
	lo, hi := 0, len(r.chunks)-1
	for lo < hi {
		mid := (lo + hi + 1) / 2
		if r.chunks[mid].baseRow <= row {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	cc := r.chunks[lo]
	return cc, int(row - cc.baseRow), true
}

// cell resolves the Vector and in-chunk offset for an absolute (col, row).
func (r *Result) cell(col int64, row int32) (*Vector, int, bool) {
	cc, off, ok := r.locate(int64(row))
	if !ok || col < 0 || col >= r.colCount {
		return nil, 0, false
	}
	if cc.vectors[col] == nil {
		cc.vectors[col] = cc.chunk.Vector(col)
	}
	return cc.vectors[col], off, true
}

// ColumnCount returns the number of columns in the result
func (r *Result) ColumnCount() int64 {
	r.loadMeta()
	return r.colCount
}

// RowCount returns the number of rows in the result. This materializes all
// chunks (duckdb_query has already computed them), replacing the deprecated
// duckdb_row_count.
func (r *Result) RowCount() int64 {
	for !r.exhausted {
		r.fetchMore()
	}
	return r.totalRows
}

// RowsChanged returns the number of rows changed by an INSERT/UPDATE/DELETE.
func (r *Result) RowsChanged() int64 {
	if r.Db.RowsChanged != nil {
		return r.Db.RowsChanged(&r.Raw)
	}
	return 0
}

// ColumnName returns the name of the column at the given index
func (r *Result) ColumnName(column int64) string {
	return GoString(r.Db.ColumnName(&r.Raw, column))
}

// ColumnNames returns the names of all columns.
func (r *Result) ColumnNames() []string {
	names := make([]string, r.ColumnCount())
	for i := range names {
		names[i] = r.ColumnName(int64(i))
	}
	return names
}

// ColumnType returns the type of the column at the given index
func (r *Result) ColumnType(column int64) DuckDBType {
	return r.Db.ColumnType(&r.Raw, column)
}

// ColumnLogicalType returns the (cached) logical type of the column.
func (r *Result) ColumnLogicalType(column int64) DuckDBLogicalType {
	r.loadMeta()
	if column < 0 || column >= int64(len(r.colLogical)) {
		return nil
	}
	return r.colLogical[column]
}

// ColumnAlias returns the type alias of a column (e.g. "JSON"), or "".
func (r *Result) ColumnAlias(column int64) string {
	r.loadMeta()
	if column < 0 || column >= int64(len(r.colAlias)) {
		return ""
	}
	return r.colAlias[column]
}

// ValueNull returns true if the value at the given column and row is NULL.
func (r *Result) ValueNull(column int64, row int32) bool {
	v, off, ok := r.cell(column, row)
	if !ok {
		return true
	}
	return !v.RowValid(off)
}

// ValueString returns the value at the given column and row rendered as a
// string. Non-VARCHAR columns are converted to text, matching the behavior of
// the deprecated duckdb_value_varchar this API used to wrap.
func (r *Result) ValueString(column int64, row int32) (string, bool) {
	colType := r.ColumnType(column)
	switch colType {
	case DuckDBTypeVarchar, DuckDBTypeBlob:
		b, ok := r.valueBytes(column, row)
		if !ok {
			return "", false
		}
		return string(b), true
	}
	val, ok := r.ValueNested(column, row)
	if !ok {
		return "", false
	}
	return stringifyValue(val, colType), true
}

// stringifyValue renders a decoded Go value as DuckDB-style text.
func stringifyValue(val any, colType DuckDBType) string {
	switch v := val.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	case time.Time:
		switch colType {
		case DuckDBTypeDate:
			return v.Format("2006-01-02")
		case DuckDBTypeTime:
			return v.Format("15:04:05.999999")
		default: // timestamps
			return v.Format("2006-01-02 15:04:05.999999")
		}
	default:
		return fmt.Sprintf("%v", v)
	}
}

// ValueVarchar returns the raw bytes of a VARCHAR/JSON value.
func (r *Result) ValueVarchar(column int64, row int32) ([]byte, bool) {
	if r.ColumnType(column) != DuckDBTypeVarchar {
		return nil, false
	}
	return r.valueBytes(column, row)
}

// ValueBlob returns the raw bytes of a BLOB value.
func (r *Result) ValueBlob(column int64, row int32) ([]byte, bool) {
	if r.ColumnType(column) != DuckDBTypeBlob {
		return nil, false
	}
	return r.valueBytes(column, row)
}

// valueBytes reads a duckdb_string_t cell (VARCHAR/BLOB physical layout). The
// caller must have verified the column's type: interpreting any other physical
// layout as a string_t reads garbage and may dereference a wild pointer.
func (r *Result) valueBytes(column int64, row int32) ([]byte, bool) {
	v, off, ok := r.cell(column, row)
	if !ok || !v.RowValid(off) {
		return nil, false
	}
	return v.bytesAt(off), true
}

// ValueBoolean returns the boolean value at the given column and row
func (r *Result) ValueBoolean(column int64, row int32) (bool, bool) {
	v, off, ok := r.cell(column, row)
	if !ok || !v.RowValid(off) {
		return false, false
	}
	return vectorValue[bool](v, off), true
}

// ValueInt8 returns the int8 value at the given column and row
func (r *Result) ValueInt8(column int64, row int32) (int8, bool) {
	v, off, ok := r.cell(column, row)
	if !ok || !v.RowValid(off) {
		return 0, false
	}
	return vectorValue[int8](v, off), true
}

// ValueInt16 returns the int16 value at the given column and row
func (r *Result) ValueInt16(column int64, row int32) (int16, bool) {
	v, off, ok := r.cell(column, row)
	if !ok || !v.RowValid(off) {
		return 0, false
	}
	return vectorValue[int16](v, off), true
}

// ValueInt32 returns the int32 value at the given column and row
func (r *Result) ValueInt32(column int64, row int32) (int32, bool) {
	v, off, ok := r.cell(column, row)
	if !ok || !v.RowValid(off) {
		return 0, false
	}
	return vectorValue[int32](v, off), true
}

// ValueInt64 returns the int64 value at the given column and row
func (r *Result) ValueInt64(column int64, row int32) (int64, bool) {
	v, off, ok := r.cell(column, row)
	if !ok || !v.RowValid(off) {
		return 0, false
	}
	return vectorValue[int64](v, off), true
}

// ValueUint8 returns the uint8 value at the given column and row
func (r *Result) ValueUint8(column int64, row int32) (uint8, bool) {
	v, off, ok := r.cell(column, row)
	if !ok || !v.RowValid(off) {
		return 0, false
	}
	return vectorValue[uint8](v, off), true
}

// ValueUint16 returns the uint16 value at the given column and row
func (r *Result) ValueUint16(column int64, row int32) (uint16, bool) {
	v, off, ok := r.cell(column, row)
	if !ok || !v.RowValid(off) {
		return 0, false
	}
	return vectorValue[uint16](v, off), true
}

// ValueUint32 returns the uint32 value at the given column and row
func (r *Result) ValueUint32(column int64, row int32) (uint32, bool) {
	v, off, ok := r.cell(column, row)
	if !ok || !v.RowValid(off) {
		return 0, false
	}
	return vectorValue[uint32](v, off), true
}

// ValueUint64 returns the uint64 value at the given column and row
func (r *Result) ValueUint64(column int64, row int32) (uint64, bool) {
	v, off, ok := r.cell(column, row)
	if !ok || !v.RowValid(off) {
		return 0, false
	}
	return vectorValue[uint64](v, off), true
}

// ValueFloat returns the float32 value at the given column and row
func (r *Result) ValueFloat(column int64, row int32) (float32, bool) {
	v, off, ok := r.cell(column, row)
	if !ok || !v.RowValid(off) {
		return 0, false
	}
	return vectorValue[float32](v, off), true
}

// ValueDouble returns the float64 value at the given column and row
func (r *Result) ValueDouble(column int64, row int32) (float64, bool) {
	v, off, ok := r.cell(column, row)
	if !ok || !v.RowValid(off) {
		return 0, false
	}
	return vectorValue[float64](v, off), true
}

// ValueDate returns the date value at the given column and row.
// DuckDB stores DATE as the number of days since 1970-01-01.
func (r *Result) ValueDate(column int64, row int32) (time.Time, bool) {
	v, off, ok := r.cell(column, row)
	if !ok || !v.RowValid(off) {
		return time.Time{}, false
	}
	return dateDaysToTime(vectorValue[int32](v, off)), true
}

// ValueTime returns the time value at the given column and row.
// DuckDB stores TIME as microseconds since midnight.
func (r *Result) ValueTime(column int64, row int32) (time.Time, bool) {
	v, off, ok := r.cell(column, row)
	if !ok || !v.RowValid(off) {
		return time.Time{}, false
	}
	return timeMicrosToTime(vectorValue[int64](v, off)), true
}

// ValueTimestamp returns the timestamp value at the given column and row.
// DuckDB stores TIMESTAMP as microseconds since the Unix epoch.
func (r *Result) ValueTimestamp(column int64, row int32) (time.Time, bool) {
	v, off, ok := r.cell(column, row)
	if !ok || !v.RowValid(off) {
		return time.Time{}, false
	}
	return timestampMicrosToTime(vectorValue[int64](v, off)), true
}

// ValueInterval returns the interval value at the given column and row.
func (r *Result) ValueInterval(column int64, row int32) (Interval, bool) {
	v, off, ok := r.cell(column, row)
	if !ok || !v.RowValid(off) {
		return Interval{}, false
	}
	return v.intervalAt(off), true
}

// ValueHugeint returns the 128-bit integer value at the given column and row.
func (r *Result) ValueHugeint(column int64, row int32) (Hugeint, bool) {
	v, off, ok := r.cell(column, row)
	if !ok || !v.RowValid(off) {
		return Hugeint{}, false
	}
	return v.hugeintAt(off), true
}

// ValueDecimalString returns a DECIMAL value formatted as a decimal string,
// preserving full precision. The physical storage is chosen by DuckDB from the
// declared precision (int16/int32/int64/hugeint).
func (r *Result) ValueDecimalString(column int64, row int32) (string, bool) {
	v, off, ok := r.cell(column, row)
	if !ok || !v.RowValid(off) {
		return "", false
	}
	return decodeDecimal(r.Db, v, off, r.ColumnLogicalType(column)), true
}

// ValueUUIDString returns a UUID value formatted as its canonical string.
func (r *Result) ValueUUIDString(column int64, row int32) (string, bool) {
	v, off, ok := r.cell(column, row)
	if !ok || !v.RowValid(off) {
		return "", false
	}
	return uuidString(v.hugeintAt(off)), true
}

// ValueEnumString returns an ENUM value's label by looking up the dictionary.
func (r *Result) ValueEnumString(column int64, row int32) (string, bool) {
	v, off, ok := r.cell(column, row)
	if !ok || !v.RowValid(off) {
		return "", false
	}
	return decodeEnum(r.Db, v, off, r.ColumnLogicalType(column)), true
}

// DecimalInfo returns the precision and scale for decimal types
func (r *Result) DecimalInfo(column int64) (precision, scale int64, ok bool) {
	if r.ColumnType(column) != DuckDBTypeDecimal {
		return 0, 0, false
	}
	logicalType := r.ColumnLogicalType(column)
	if logicalType == nil {
		return 0, 0, false
	}
	if r.Db.DecimalWidth != nil && r.Db.DecimalScale != nil {
		return int64(r.Db.DecimalWidth(logicalType)), int64(r.Db.DecimalScale(logicalType)), true
	}
	return 0, 0, false
}

// Close destroys the result and frees associated resources. Idempotent.
func (r *Result) Close() {
	if r.closed {
		return
	}
	r.closed = true
	for _, cc := range r.chunks {
		cc.chunk.Destroy()
	}
	r.chunks = nil
	for i := range r.colLogical {
		if r.colLogical[i] != nil && r.Db.DestroyLogicalType != nil {
			r.Db.DestroyLogicalType(&r.colLogical[i])
		}
	}
	r.colLogical = nil
	r.Db.DestroyResult(&r.Raw)
}

// formatDecimal renders a scaled integer as a decimal string.
func formatDecimal(bi *big.Int, scale int) string {
	neg := bi.Sign() < 0
	digits := new(big.Int).Abs(bi).String()
	var out string
	if scale <= 0 {
		out = digits
	} else {
		for len(digits) <= scale {
			digits = "0" + digits
		}
		out = digits[:len(digits)-scale] + "." + digits[len(digits)-scale:]
	}
	if neg {
		return "-" + out
	}
	return out
}

// hugeintToBigInt converts a DuckDB hugeint (upper<<64 + lower) to a big.Int.
func hugeintToBigInt(h Hugeint) *big.Int {
	bi := big.NewInt(h.Upper)
	bi.Lsh(bi, 64)
	return bi.Add(bi, new(big.Int).SetUint64(h.Lower))
}

// String renders the signed 128-bit HUGEINT value in base 10.
func (h Hugeint) String() string { return hugeintToBigInt(h).String() }

// UString renders the 128 bits interpreted as an unsigned UHUGEINT, base 10.
func (h Hugeint) UString() string {
	bi := new(big.Int).SetUint64(uint64(h.Upper))
	bi.Lsh(bi, 64)
	return bi.Add(bi, new(big.Int).SetUint64(h.Lower)).String()
}

// uuidString formats DuckDB's UUID storage (a hugeint with a flipped sign bit)
// as the canonical 8-4-4-4-12 hex string.
func uuidString(h Hugeint) string {
	hi := uint64(h.Upper) ^ 0x8000000000000000
	var b [16]byte
	binary.BigEndian.PutUint64(b[0:8], hi)
	binary.BigEndian.PutUint64(b[8:16], h.Lower)
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
