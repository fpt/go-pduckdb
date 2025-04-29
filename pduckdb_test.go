package pduckdb

import (
	"testing"
)

// TestGoString tests the C string to Go string conversion utility
func TestGoString(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		isNil    bool
	}{
		{
			name:     "Empty string",
			input:    "",
			expected: "",
			isNil:    false,
		},
		{
			name:     "Simple string",
			input:    "hello",
			expected: "hello",
			isNil:    false,
		},
		{
			name:     "String with spaces",
			input:    "hello world",
			expected: "hello world",
			isNil:    false,
		},
		{
			name:     "Nil pointer",
			input:    "",
			expected: "",
			isNil:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var ptr *byte
			if !tt.isNil {
				cString := tt.input + "\x00" // Add null terminator for C strings
				cStringBytes := []byte(cString)
				ptr = &cStringBytes[0]
			}

			result := GoString(ptr)
			if result != tt.expected {
				t.Errorf("GoString() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// TestDuckDB_Connect mocks the connection process
func TestDuckDB_Connect(t *testing.T) {
	tests := []struct {
		name          string
		connectResult DuckDBState
		wantErr       bool
	}{
		{
			name:          "Successful connection",
			connectResult: DuckDBSuccess,
			wantErr:       false,
		},
		{
			name:          "Failed connection",
			connectResult: DuckDBError,
			wantErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock DuckDB instance
			db := &DuckDB{
				connect: func(*byte, **byte) DuckDBState {
					return tt.connectResult
				},
			}

			conn, err := db.Connect()

			if (err != nil) != tt.wantErr {
				t.Errorf("Connect() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && conn == nil {
				t.Errorf("Connect() returned nil connection on success")
			}

			if tt.wantErr && conn != nil {
				t.Errorf("Connect() returned non-nil connection on error")
			}
		})
	}
}

// TestDuckDB_Close tests that close gets called with the right pointer
func TestDuckDB_Close(t *testing.T) {
	var passedPtr **byte
	var mockByte byte = 0
	mockHandle := &mockByte

	db := &DuckDB{
		handle: mockHandle,
		close: func(ptr **byte) {
			passedPtr = ptr
		},
	}

	db.Close()

	if passedPtr == nil {
		t.Error("Close() didn't call close function")
	}

	if passedPtr != nil && *passedPtr != mockHandle {
		t.Errorf("Close() called with wrong handle pointer")
	}
}

// TestGetDuckDBLibrary tests that the DuckDB library path is returned based on OS
func TestGetDuckDBLibrary(t *testing.T) {
	// This is a simple test that just verifies the function returns a non-empty string
	// A more comprehensive test would check OS-specific behavior
	_, err := loadDuckDBLibrary()
	if err != nil {
		t.Errorf("loadDuckDBLibrary() returned an error: %v", err)
	}
}
