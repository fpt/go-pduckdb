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
		{
			"read only", "w.duckdb?access_mode=READ_ONLY", "w.duckdb",
			map[string]string{"access_mode": "READ_ONLY"},
		},
		{
			"several options", "w.duckdb?access_mode=READ_ONLY&threads=2", "w.duckdb",
			map[string]string{"access_mode": "READ_ONLY", "threads": "2"},
		},
		// The LAST ? separates, so a path containing one is still openable.
		{
			"a path with a question mark", "odd?name.duckdb?access_mode=READ_ONLY",
			"odd?name.duckdb",
			map[string]string{"access_mode": "READ_ONLY"},
		},
		// A trailing ? says "no options", so this names the file odd?name.duckdb.
		{
			"a trailing question mark escapes the path", "odd?name.duckdb?",
			"odd?name.duckdb", nil,
		},
		// ...and doubling it names a file that really does end in ?.
		{
			"a doubled question mark is a path ending in one", "odd?name.duckdb??",
			"odd?name.duckdb?", nil,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			path, settings, err := splitDSN(c.dsn)
			if err != nil {
				t.Fatalf("splitDSN(%q) returned an unexpected error: %v", c.dsn, err)
			}
			if path != c.path || !reflect.DeepEqual(settings, c.settings) {
				t.Errorf("splitDSN(%q) = %q, %v; want %q, %v", c.dsn, path, settings, c.path, c.settings)
			}
		})
	}
}

// A repeated option is refused rather than resolved. A DSN built by
// concatenation can end up asking for two different things, and quietly
// honouring one of them is how a read-only database becomes writable.
func TestSplitDSNRejectsARepeatedOption(t *testing.T) {
	for _, dsn := range []string{
		"w.duckdb?access_mode=READ_ONLY&access_mode=READ_WRITE",
		"w.duckdb?threads=2&threads=2",
		"w.duckdb?threads=1&access_mode=READ_ONLY&threads=8",
	} {
		if _, _, err := splitDSN(dsn); err == nil {
			t.Errorf("splitDSN(%q) accepted a repeated option; want an error", dsn)
		}
	}
}

// A malformed query is refused rather than quietly becoming a filename. The
// alternative is creating a file named after the typo and never reporting the
// option that was actually wrong.
func TestSplitDSNRejectsAMalformedQuery(t *testing.T) {
	for _, dsn := range []string{
		"w.duckdb?access_mode=%ZZ",
		"w.duckdb?%",
		"w.duckdb?a=1;b=2&%GG=x",
	} {
		path, settings, err := splitDSN(dsn)
		if err == nil {
			t.Errorf("splitDSN(%q) = %q, %v, nil; want an error", dsn, path, settings)
		}
	}
}

// The error has to reach the caller of sql.Open, not just splitDSN.
func TestOpenRejectsARepeatedOption(t *testing.T) {
	db, err := sql.Open("duckdb", filepath.Join(t.TempDir(), "w.duckdb")+
		"?access_mode=READ_ONLY&access_mode=READ_WRITE")
	if err != nil {
		return // refused this early is fine too
	}
	defer func() { _ = db.Close() }()
	if err := db.Ping(); err == nil {
		t.Error("sql.Open accepted a DSN setting access_mode twice; want an error")
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

// Closing a database must release the file, not merely stop using it. DuckDB
// keeps the instance alive while a connection to it exists, so a driver that
// closes without disconnecting leaves the file open -- unnoticeable on POSIX,
// but the next open fails on Windows.
func TestCloseReleasesTheFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "reopen.duckdb")
	for i := range 3 {
		db, err := sql.Open("duckdb", path)
		if err != nil {
			t.Skipf("no DuckDB library available: %v", err)
		}
		if _, err := db.Exec("CREATE TABLE IF NOT EXISTS t (a INTEGER)"); err != nil {
			t.Fatalf("open %d: %v", i, err)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("close %d: %v", i, err)
		}
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
