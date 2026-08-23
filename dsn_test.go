package pduckdb

import (
	"database/sql"
	"path/filepath"
	"reflect"
	"testing"
)

func TestSplitDSN(t *testing.T) {
	for _, c := range []struct {
		name, dsn, path string
		settings        map[string]string
	}{
		{"a plain path", "warehouse.duckdb", "warehouse.duckdb", nil},
		{"in memory", ":memory:", ":memory:", nil},
		{"read only", "w.duckdb?access_mode=READ_ONLY", "w.duckdb",
			map[string]string{"access_mode": "READ_ONLY"}},
		{"several options", "w.duckdb?access_mode=READ_ONLY&threads=2", "w.duckdb",
			map[string]string{"access_mode": "READ_ONLY", "threads": "2"}},
		// The LAST ? separates, so a path containing one is still openable.
		{"a path with a question mark", "odd?name.duckdb?access_mode=READ_ONLY",
			"odd?name.duckdb", map[string]string{"access_mode": "READ_ONLY"}},
		{"a trailing question mark is part of the path", "odd?name.duckdb?",
			"odd?name.duckdb?", nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			path, settings := splitDSN(c.dsn)
			if path != c.path || !reflect.DeepEqual(settings, c.settings) {
				t.Errorf("splitDSN(%q) = %q, %v; want %q, %v", c.dsn, path, settings, c.path, c.settings)
			}
		})
	}
}

// A read-only database must refuse a write. Intending not to write is not the
// same as being unable to, and for a process whose only job is to read, the
// difference is the whole point of asking.
func TestReadOnlyRefusesAWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "w.duckdb")

	writable, err := sql.Open("duckdb", path)
	if err != nil {
		t.Skipf("no DuckDB library available: %v", err)
	}
	if _, err := writable.Exec("CREATE TABLE t (a INTEGER)"); err != nil {
		t.Skipf("no DuckDB library available: %v", err)
	}
	if _, err := writable.Exec("INSERT INTO t VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	if err := writable.Close(); err != nil {
		t.Fatal(err)
	}

	readonly, err := sql.Open("duckdb", path+"?access_mode=READ_ONLY")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = readonly.Close() }()

	var n int
	if err := readonly.QueryRow("SELECT COUNT(*) FROM t").Scan(&n); err != nil {
		t.Fatalf("a read-only database should still read: %v", err)
	}
	if n != 1 {
		t.Errorf("read %d rows, want 1", n)
	}
	if _, err := readonly.Exec("INSERT INTO t VALUES (2)"); err == nil {
		t.Error("a read-only database accepted a write")
	}
}

func TestUnknownSettingIsReported(t *testing.T) {
	db, err := sql.Open("duckdb", filepath.Join(t.TempDir(), "w.duckdb")+"?not_a_setting=1")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if err := db.Ping(); err == nil {
		t.Error("a setting DuckDB does not know should not open silently")
	}
}
