package pduckdb

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestUntypedParameters covers parameters whose type DuckDB cannot infer at
// prepare time (issue #43); they are bound by the Go value's type.
func TestUntypedParameters(t *testing.T) {
	sqlDB, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	defer func() { assert.NoError(t, sqlDB.Close()) }()

	path := filepath.Join(t.TempDir(), "a.jsonl")
	require.NoError(t, os.WriteFile(path, []byte("{\"x\":1}\n{\"x\":2}\n"), 0o600))

	var n int
	require.NoError(t, sqlDB.QueryRow("SELECT count(*) FROM read_json(?)", path).Scan(&n))
	assert.Equal(t, 2, n)

	var v any
	require.NoError(t, sqlDB.QueryRow(`SELECT from_json(?, '["VARCHAR"]')`, `["a","b"]`).Scan(&v))
	assert.Equal(t, []any{"a", "b"}, v)

	ts := time.Date(2024, 5, 6, 7, 8, 9, 123456000, time.UTC)
	cases := []struct {
		name     string
		arg      any
		wantType string
		want     any
	}{
		{"Bool", true, "BOOLEAN", true},
		{"Int64", int64(-5), "BIGINT", int64(-5)},
		{"Float64", 1.5, "DOUBLE", 1.5},
		{"String", "s", "VARCHAR", "s"},
		{"Bytes", []byte{0, 1, 2}, "BLOB", []byte{0, 1, 2}},
		{"EmptyBytes", []byte{}, "BLOB", []byte{}},
		{"Time", ts, "TIMESTAMP", ts},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// typeof takes ANY, so DuckDB leaves the parameter's type open.
			var typ string
			var got any
			require.NoError(t, sqlDB.QueryRow("SELECT typeof(?), ?", c.arg, c.arg).Scan(&typ, &got))
			assert.Equal(t, c.wantType, typ)
			assert.Equal(t, c.want, got)
		})
	}
}

// TestBlobParameter binds []byte to a BLOB column, which was not supported.
func TestBlobParameter(t *testing.T) {
	sqlDB, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	defer func() { assert.NoError(t, sqlDB.Close()) }()

	_, err = sqlDB.Exec("CREATE TABLE b (v BLOB)")
	require.NoError(t, err)
	_, err = sqlDB.Exec("INSERT INTO b VALUES (?), (?)", []byte{0xde, 0xad, 0x00, 0xbe, 0xef}, []byte{})
	require.NoError(t, err)

	rows, err := sqlDB.Query("SELECT v FROM b ORDER BY octet_length(v) DESC")
	require.NoError(t, err)
	defer func() { assert.NoError(t, rows.Close()) }()
	var got [][]byte
	for rows.Next() {
		var b []byte
		require.NoError(t, rows.Scan(&b))
		got = append(got, b)
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, [][]byte{{0xde, 0xad, 0x00, 0xbe, 0xef}, {}}, got)
}
