package pduckdb

import (
	"database/sql"
	"database/sql/driver"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListParameters covers slice arguments bound as LIST values.
func TestListParameters(t *testing.T) {
	sqlDB, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	defer func() { assert.NoError(t, sqlDB.Close()) }()

	cases := []struct {
		name string
		arg  any
		want any
	}{
		{"Strings", []string{"a", "b"}, []any{"a", "b"}},
		{"Int64s", []int64{1, 2, 3}, []any{int64(1), int64(2), int64(3)}},
		{"Ints", []int{-1, 7}, []any{int64(-1), int64(7)}},
		{"Int32s", []int32{4}, []any{int32(4)}},
		{"Int8s", []int8{-8}, []any{int8(-8)}},
		{"Uint16s", []uint16{65535}, []any{uint16(65535)}},
		{"Uint64s", []uint64{1 << 63}, []any{uint64(1 << 63)}},
		{"Float64s", []float64{1.5, -2}, []any{1.5, -2.0}},
		{"Float32s", []float32{0.25}, []any{float32(0.25)}},
		{"Bools", []bool{true, false}, []any{true, false}},
		{"Empty", []string{}, []any{}},
		{"Nested", [][]int16{{1, 2}, {}}, []any{[]any{int16(1), int16(2)}, []any{}}},
		{"NamedType", namedStrings{"x"}, []any{"x"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var got any
			require.NoError(t, sqlDB.QueryRow("SELECT ?", c.arg).Scan(&got))
			assert.Equal(t, c.want, got)
		})
	}
}

type namedStrings []string

// TestListParameterUsage covers the cases from issue #44: a list fed to
// unnest(?) and to a table function, where DuckDB cannot infer the type.
func TestListParameterUsage(t *testing.T) {
	sqlDB, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	defer func() { assert.NoError(t, sqlDB.Close()) }()

	_, err = sqlDB.Exec("CREATE TABLE t AS SELECT * FROM (VALUES ('a', 1), ('b', 2), ('c', 3)) v(k, n)")
	require.NoError(t, err)

	var n int
	require.NoError(t, sqlDB.QueryRow(
		"SELECT count(*) FROM t WHERE k IN (SELECT unnest(?))", []string{"a", "b"},
	).Scan(&n))
	assert.Equal(t, 2, n)

	// A typed parameter: the bound BIGINT[] is cast to the column's INTEGER[].
	require.NoError(t, sqlDB.QueryRow(
		"SELECT count(*) FROM t WHERE list_contains(?::INTEGER[], n)", []int64{1, 3},
	).Scan(&n))
	assert.Equal(t, 2, n)

	dir := t.TempDir()
	paths := []string{filepath.Join(dir, "a.jsonl"), filepath.Join(dir, "b.jsonl")}
	require.NoError(t, os.WriteFile(paths[0], []byte("{\"x\":1}\n{\"x\":2}\n"), 0o600))
	require.NoError(t, os.WriteFile(paths[1], []byte("{\"x\":3}\n"), 0o600))
	require.NoError(t, sqlDB.QueryRow("SELECT count(*) FROM read_json(?)", paths).Scan(&n))
	assert.Equal(t, 3, n)
}

// TestCheckNamedValue guards which arguments bypass the default converter:
// []byte must stay a BLOB rather than become a list of UTINYINT, and a
// driver.Valuer slice must still go through Value().
func TestCheckNamedValue(t *testing.T) {
	c := &Conn{}
	for _, v := range []any{[]byte{1, 2}, valuerStrings{"a"}, [][]byte{{1}}, []any{1}, "s", 1} {
		assert.ErrorIs(t, c.CheckNamedValue(&driver.NamedValue{Value: v}), driver.ErrSkip, "%T", v)
	}
	for _, v := range []any{[]string{"a"}, []int{1}, [][]float64{{1}}, namedStrings{}} {
		assert.NoError(t, c.CheckNamedValue(&driver.NamedValue{Value: v}), "%T", v)
	}

	sqlDB, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	defer func() { assert.NoError(t, sqlDB.Close()) }()

	var s string
	require.NoError(t, sqlDB.QueryRow("SELECT ?::VARCHAR", valuerStrings{"a", "b"}).Scan(&s))
	assert.Equal(t, "a,b", s)
}

type valuerStrings []string

func (v valuerStrings) Value() (driver.Value, error) {
	out := ""
	for i, s := range v {
		if i > 0 {
			out += ","
		}
		out += s
	}
	return out, nil
}

// TestUnsupportedSliceIsRejected keeps the database/sql error for slices whose
// elements cannot be list elements.
func TestUnsupportedSliceIsRejected(t *testing.T) {
	sqlDB, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	defer func() { assert.NoError(t, sqlDB.Close()) }()

	var v any
	err = sqlDB.QueryRow("SELECT ?", []any{1, "a"}).Scan(&v)
	assert.ErrorContains(t, err, "unsupported type")
}
