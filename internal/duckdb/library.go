// Package duckdb provides internal implementation details for the go-pduckdb driver.

//go:build darwin || freebsd || linux || netbsd || windows

package duckdb

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"unsafe"
)

// LoadDuckDBLibrary attempts to load the DuckDB library from various locations based on the platform
func LoadDuckDBLibrary() (uintptr, error) {
	// First check if the library path is specified via environment variable
	if envPath := os.Getenv("DUCKDB_LIBRARY_PATH"); envPath != "" {
		lib, err := openLibrary(envPath)
		if err == nil {
			return lib, nil
		}
		// If the explicitly provided path fails, return that error directly
		// as the user would expect the specified library to work
		return 0, fmt.Errorf("failed to load DuckDB library from DUCKDB_LIBRARY_PATH (%s): %w", envPath, err)
	}

	// Get platform-specific library paths
	locations := getLibraryPaths()

	// Try each location
	var lastErr error
	for _, location := range locations {
		lib, err := openLibrary(location)
		if err == nil {
			return lib, nil
		}
		lastErr = err
	}

	return 0, fmt.Errorf("failed to load DuckDB library from any standard location, last error: %w", lastErr)
}

// getLibraryPaths returns a list of paths to search for the DuckDB library based on the platform
func getLibraryPaths() []string {
	var locations []string

	switch runtime.GOOS {
	case "darwin":
		locations = getMacOSLibraryPaths()
	case "linux":
		locations = getLinuxLibraryPaths()
	case "windows":
		// Windows standard locations - prioritize current directory as it's the most likely location
		currentDir, err := os.Getwd()
		if err == nil {
			locations = append(locations, filepath.Join(currentDir, "duckdb.dll"))
		}
		// Then add other standard locations
		locations = append(locations,
			"duckdb.dll", // Current directory (relative path)
			filepath.Join(os.Getenv("ProgramFiles"), "DuckDB", "duckdb.dll"),
			filepath.Join(os.Getenv("ProgramFiles(x86)"), "DuckDB", "duckdb.dll"),
		)
	}

	return locations
}

// getMacOSLibraryPaths returns a list of paths to search for the DuckDB library on macOS
func getMacOSLibraryPaths() []string {
	locations := []string{}

	// First check DYLD_LIBRARY_PATH
	if libPaths := os.Getenv("DYLD_LIBRARY_PATH"); libPaths != "" {
		for _, path := range filepath.SplitList(libPaths) {
			locations = append(locations, filepath.Join(path, "libduckdb.dylib"))
		}
	}

	// Then add standard macOS locations
	standardPaths := []string{
		"/opt/homebrew/lib/libduckdb.dylib",         // Apple Silicon Homebrew
		"/usr/local/lib/libduckdb.dylib",            // Intel Homebrew
		"/usr/local/opt/duckdb/lib/libduckdb.dylib", // Alternative Homebrew location
		"/usr/lib/libduckdb.dylib",                  // System location
		"./libduckdb.dylib",                         // Current directory
	}

	locations = append(locations, standardPaths...)

	return locations
}

// getLinuxLibraryPaths returns a list of paths to search for the DuckDB library on Linux
func getLinuxLibraryPaths() []string {
	locations := []string{}

	// First check LD_LIBRARY_PATH
	if libPaths := os.Getenv("LD_LIBRARY_PATH"); libPaths != "" {
		for _, path := range filepath.SplitList(libPaths) {
			locations = append(locations, filepath.Join(path, "libduckdb.so"))
		}
	}

	// Then add standard Linux locations
	standardPaths := []string{
		"/usr/lib/libduckdb.so",
		"/usr/local/lib/libduckdb.so",
		"/usr/lib/x86_64-linux-gnu/libduckdb.so",  // Debian/Ubuntu for amd64
		"/usr/lib/aarch64-linux-gnu/libduckdb.so", // Debian/Ubuntu for arm64
		"/usr/lib64/libduckdb.so",                 // Fedora/RHEL/CentOS
		"./libduckdb.so",                          // Current directory
	}

	locations = append(locations, standardPaths...)

	return locations
}

// GoBytes converts a C string to a Go byte slice
func GoBytes(c *byte) []byte {
	if c == nil {
		return nil
	}

	// Use a safer approach with slices
	var bytes []byte
	// Iterate one byte at a time until null terminator
	for i := 0; ; i++ {
		// Get byte at index i
		b := *(*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(c)) + uintptr(i)))
		if b == 0 {
			break // null terminator
		}
		bytes = append(bytes, b)
	}

	return bytes
}

// GoString converts a C string to a Go string
func GoString(c *byte) string {
	return string(GoBytes(c))
}

// ToCString converts a Go string to a C string
func ToCString(s string) *byte {
	// Add 1 for null terminator
	cstr := make([]byte, len(s)+1)
	copy(cstr, s)
	cstr[len(s)] = 0 // null terminator
	return &cstr[0]
}

// FreeCString frees a C string (no-op in Go)
func FreeCString(cstr *byte) {
	// Nothing to do, Go handles garbage collection
}
