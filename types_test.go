package pduckdb

import (
	"database/sql"
	"testing"

	"github.com/fpt/go-pduckdb/internal/duckdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// openConn opens an in-memory database and a native connection, cleaned up
// automatically when the test ends.
func openConn(t *testing.T) *duckdb.Connection {
	t.Helper()
	db, err := NewDuckDB(":memory:")
	require.NoError(t, err)
	conn, err := db.Connect()
	require.NoError(t, err)
	t.Cleanup(func() {
		conn.Close()
		db.Close()
	})
	return conn
}

// TestMultiChunkResult exercises random access across data-chunk boundaries.
// DuckDB emits at most STANDARD_VECTOR_SIZE (2048) rows per chunk, so a 5000-row
// result spans three chunks; the cache + locate() must stitch them together.
func TestMultiChunkResult(t *testing.T) {
	conn := openConn(t)
	res, err := conn.Query("SELECT i FROM range(5000) t(i)")
	require.NoError(t, err)
	defer res.Close()

	assert.Equal(t, int64(5000), res.RowCount(), "row count across chunks")

	for _, row := range []int32{0, 1, 2047, 2048, 2049, 4095, 4096, 4999} {
		v, ok := res.ValueInt64(0, row)
		assert.True(t, ok, "row %d present", row)
		assert.Equal(t, int64(row), v, "value at row %d", row)
	}
}

// TestMultiChunkSQLScan iterates a multi-chunk result through database/sql.
func TestMultiChunkSQLScan(t *testing.T) {
	sqlDB, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	defer func() { assert.NoError(t, sqlDB.Close()) }()

	rows, err := sqlDB.Query("SELECT i FROM range(5000) t(i)")
	require.NoError(t, err)
	defer func() { assert.NoError(t, rows.Close()) }()

	var count int
	for rows.Next() {
		var v int64
		require.NoError(t, rows.Scan(&v))
		require.Equal(t, int64(count), v)
		count++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 5000, count)
}

// TestBlobValue covers BLOB decoding, including the inline (<=12 bytes) and
// pointer (>12 bytes) forms of duckdb_string_t, and NULL. BLOB was previously
// unsupported due to purego struct-return limits.
func TestBlobValue(t *testing.T) {
	conn := openConn(t)
	res, err := conn.Query(
		"SELECT 'hi'::BLOB AS a, " +
			"'this string is longer than twelve chars'::BLOB AS b, " +
			"NULL::BLOB AS c")
	require.NoError(t, err)
	defer res.Close()

	a, ok := res.ValueBlob(0, 0)
	assert.True(t, ok)
	assert.Equal(t, []byte("hi"), a)

	b, ok := res.ValueBlob(1, 0)
	assert.True(t, ok)
	assert.Equal(t, []byte("this string is longer than twelve chars"), b)

	_, ok = res.ValueBlob(2, 0)
	assert.False(t, ok, "NULL blob")
	assert.True(t, res.ValueNull(2, 0))
}

// TestBlobSQLScan round-trips a BLOB through database/sql into []byte.
func TestBlobSQLScan(t *testing.T) {
	sqlDB, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	defer func() { assert.NoError(t, sqlDB.Close()) }()

	var b []byte
	require.NoError(t, sqlDB.QueryRow("SELECT 'DE:AD:BE:EF'::BLOB").Scan(&b))
	assert.Equal(t, []byte("DE:AD:BE:EF"), b)
}

// TestIntervalValue covers INTERVAL, previously unsupported due to purego
// struct-return limits.
func TestIntervalValue(t *testing.T) {
	conn := openConn(t)
	res, err := conn.Query("SELECT INTERVAL '1 year 2 months 3 days 04:05:06' AS iv")
	require.NoError(t, err)
	defer res.Close()

	iv, ok := res.ValueInterval(0, 0)
	require.True(t, ok)
	assert.Equal(t, int32(14), iv.Months) // 1 year + 2 months
	assert.Equal(t, int32(3), iv.Days)
	assert.Equal(t, int64(14706)*1_000_000, iv.Micros) // 04:05:06
}

// TestIntervalSQLScan checks the database/sql string rendering matches DuckDB.
func TestIntervalSQLScan(t *testing.T) {
	sqlDB, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	defer func() { assert.NoError(t, sqlDB.Close()) }()

	var s string
	require.NoError(t, sqlDB.QueryRow(
		"SELECT INTERVAL '1 year 2 months 3 days 04:05:06'").Scan(&s))
	assert.Equal(t, "1 year 2 months 3 days 04:05:06", s)
}

// TestNullInChunk verifies the validity bitmask is read correctly for a mix of
// NULL and non-NULL rows within one chunk.
func TestNullInChunk(t *testing.T) {
	conn := openConn(t)
	res, err := conn.Query("SELECT CASE WHEN i % 2 = 0 THEN i END AS x FROM range(10) t(i)")
	require.NoError(t, err)
	defer res.Close()

	for row := int32(0); row < 10; row++ {
		v, ok := res.ValueInt64(0, row)
		if row%2 == 0 {
			assert.True(t, ok, "row %d non-null", row)
			assert.Equal(t, int64(row), v)
			assert.False(t, res.ValueNull(0, row))
		} else {
			assert.False(t, ok, "row %d null", row)
			assert.True(t, res.ValueNull(0, row))
		}
	}
}

// TestVarcharBoundary checks the inline/pointer boundary of duckdb_string_t at
// exactly 12 and 13 characters.
func TestVarcharBoundary(t *testing.T) {
	conn := openConn(t)
	res, err := conn.Query("SELECT '123456789012' AS a, '1234567890123' AS b, '' AS c")
	require.NoError(t, err)
	defer res.Close()

	a, ok := res.ValueString(0, 0)
	assert.True(t, ok)
	assert.Equal(t, "123456789012", a) // 12 bytes: inlined

	b, ok := res.ValueString(1, 0)
	assert.True(t, ok)
	assert.Equal(t, "1234567890123", b) // 13 bytes: pointer

	c, ok := res.ValueString(2, 0)
	assert.True(t, ok)
	assert.Equal(t, "", c) // empty string is not NULL
}

// TestExtendedTypesSQL covers HUGEINT/DECIMAL/UUID rendering through database/sql.
func TestExtendedTypesSQL(t *testing.T) {
	sqlDB, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	defer func() { assert.NoError(t, sqlDB.Close()) }()

	var h, hneg, d, dneg, u string
	require.NoError(t, sqlDB.QueryRow(`SELECT
		170141183460469231731687303715884105727::HUGEINT,
		(-170141183460469231731687303715884105727)::HUGEINT,
		123.45::DECIMAL(10,2),
		(-6.789)::DECIMAL(5,3),
		'550e8400-e29b-41d4-a716-446655440000'::UUID`).
		Scan(&h, &hneg, &d, &dneg, &u))

	assert.Equal(t, "170141183460469231731687303715884105727", h)
	assert.Equal(t, "-170141183460469231731687303715884105727", hneg)
	assert.Equal(t, "123.45", d)
	assert.Equal(t, "-6.789", dneg)
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", u)
}

// TestHugeintFitsInt64 checks that small HUGEINT values come back as int64.
func TestHugeintFitsInt64(t *testing.T) {
	sqlDB, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	defer func() { assert.NoError(t, sqlDB.Close()) }()

	var pos, neg int64
	require.NoError(t, sqlDB.QueryRow(
		"SELECT 42::HUGEINT, (-42)::HUGEINT").Scan(&pos, &neg))
	assert.Equal(t, int64(42), pos)
	assert.Equal(t, int64(-42), neg)
}

// TestColumnTypeDatabaseTypeName verifies the driver reports database type
// names (not column names) through database/sql's ColumnTypes().
func TestColumnTypeDatabaseTypeName(t *testing.T) {
	sqlDB, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	defer func() { assert.NoError(t, sqlDB.Close()) }()

	rows, err := sqlDB.Query("SELECT 1::INTEGER AS i, 'x'::VARCHAR AS s, true AS b, '{}'::JSON AS j")
	require.NoError(t, err)
	defer func() { assert.NoError(t, rows.Close()) }()

	cts, err := rows.ColumnTypes()
	require.NoError(t, err)
	require.Len(t, cts, 4)
	assert.Equal(t, "INTEGER", cts[0].DatabaseTypeName())
	assert.Equal(t, "VARCHAR", cts[1].DatabaseTypeName())
	assert.Equal(t, "BOOLEAN", cts[2].DatabaseTypeName())
	assert.Equal(t, "JSON", cts[3].DatabaseTypeName())
}

// TestNestedTypes covers recursive decoding of LIST/ARRAY/STRUCT/MAP.
func TestNestedTypes(t *testing.T) {
	conn := openConn(t)

	cases := []struct {
		name string
		sql  string
		want any
	}{
		{"List", "SELECT [1, 2, 3] AS l", []any{int32(1), int32(2), int32(3)}},
		{"NestedList", "SELECT [[1,2],[3]] AS l", []any{[]any{int32(1), int32(2)}, []any{int32(3)}}},
		{"ListWithNull", "SELECT [1, NULL, 3] AS l", []any{int32(1), nil, int32(3)}},
		{"Array", "SELECT [1,2,3]::INTEGER[3] AS a", []any{int32(1), int32(2), int32(3)}},
		{"Struct", "SELECT {'a': 1, 'b': 'x'} AS s", map[string]any{"a": int32(1), "b": "x"}},
		{
			"StructWithList",
			"SELECT {'name': 'bob', 'nums': [7,8,9]} AS s",
			map[string]any{"name": "bob", "nums": []any{int32(7), int32(8), int32(9)}},
		},
		{
			"Map",
			"SELECT MAP {'k1': 10, 'k2': 20} AS m",
			[]duckdb.MapEntry{{Key: "k1", Value: int32(10)}, {Key: "k2", Value: int32(20)}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := conn.Query(tc.sql)
			require.NoError(t, err)
			defer res.Close()
			v, ok := res.ValueNested(0, 0)
			require.True(t, ok)
			assert.Equal(t, tc.want, v)
		})
	}

	t.Run("NullList", func(t *testing.T) {
		res, err := conn.Query("SELECT NULL::INTEGER[] AS l")
		require.NoError(t, err)
		defer res.Close()
		_, ok := res.ValueNested(0, 0)
		assert.False(t, ok, "NULL list should report absent")
	})
}

// TestNestedSQLScan checks nested values flow through database/sql into any.
func TestNestedSQLScan(t *testing.T) {
	sqlDB, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	defer func() { assert.NoError(t, sqlDB.Close()) }()

	var l any
	require.NoError(t, sqlDB.QueryRow("SELECT [10, 20, 30]").Scan(&l))
	assert.Equal(t, []any{int32(10), int32(20), int32(30)}, l)

	var s any
	require.NoError(t, sqlDB.QueryRow("SELECT {'x': 1, 'y': 2}").Scan(&s))
	assert.Equal(t, map[string]any{"x": int32(1), "y": int32(2)}, s)
}

// TestEnumValue covers ENUM dictionary decoding.
func TestEnumValue(t *testing.T) {
	conn := openConn(t)
	require.NoError(t, conn.Execute("CREATE TYPE mood AS ENUM ('sad','ok','happy')"))
	res, err := conn.Query("SELECT 'happy'::mood AS m")
	require.NoError(t, err)
	defer res.Close()

	v, ok := res.ValueEnumString(0, 0)
	require.True(t, ok)
	assert.Equal(t, "happy", v)
}
