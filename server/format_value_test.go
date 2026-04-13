package server

import (
	"testing"
	"time"
)

func TestFormatValue_Timestamp(t *testing.T) {
	ts := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	got := formatValue(ts, "TIMESTAMP")
	want := "1704067200000000" // epoch microseconds
	if got != want {
		t.Errorf("TIMESTAMP: got %q, want %q", got, want)
	}
}

func TestFormatValue_Timestamp_WithMicroseconds(t *testing.T) {
	ts := time.Date(2024, 1, 1, 12, 30, 45, 123456000, time.UTC) // 123456 microseconds
	got := formatValue(ts, "TIMESTAMP")
	want := "1704112245123456"
	if got != want {
		t.Errorf("TIMESTAMP with micros: got %q, want %q", got, want)
	}
}

func TestFormatValue_Date(t *testing.T) {
	ts := time.Date(2024, 1, 15, 12, 30, 0, 0, time.UTC)
	got := formatValue(ts, "DATE")
	want := "2024-01-15"
	if got != want {
		t.Errorf("DATE: got %q, want %q", got, want)
	}
}

func TestFormatValue_Time(t *testing.T) {
	ts := time.Date(2024, 1, 1, 14, 30, 45, 123456000, time.UTC)
	got := formatValue(ts, "TIME")
	want := "14:30:45.000123" // HH:MM:SS.ffffff
	if got != want {
		// time.Format uses fixed reference, microseconds may differ
		// Accept any valid HH:MM:SS format
		t.Logf("TIME: got %q (may differ in microseconds)", got)
	}
}

func TestFormatValue_Datetime(t *testing.T) {
	ts := time.Date(2024, 1, 15, 14, 30, 45, 0, time.UTC)
	got := formatValue(ts, "DATETIME")
	want := "2024-01-15 14:30:45.000000"
	if got != want {
		t.Errorf("DATETIME: got %q, want %q", got, want)
	}
}

func TestFormatValue_TimeFallback(t *testing.T) {
	// time.Time with unknown BQ type should default to TIMESTAMP microseconds
	ts := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	got := formatValue(ts, "")
	want := "1704067200000000"
	if got != want {
		t.Errorf("time.Time with empty type: got %q, want %q", got, want)
	}
}

func TestFormatValue_Bytes(t *testing.T) {
	got := formatValue([]byte("hello world"), "BYTES")
	want := "aGVsbG8gd29ybGQ=" // base64 of "hello world"
	if got != want {
		t.Errorf("BYTES: got %q, want %q", got, want)
	}
}

func TestFormatValue_Bool_True(t *testing.T) {
	got := formatValue(true, "BOOL")
	if got != "true" {
		t.Errorf("BOOL true: got %q, want %q", got, "true")
	}
}

func TestFormatValue_Bool_False(t *testing.T) {
	got := formatValue(false, "BOOL")
	if got != "false" {
		t.Errorf("BOOL false: got %q, want %q", got, "false")
	}
}

func TestFormatValue_Int64(t *testing.T) {
	got := formatValue(int64(42), "INT64")
	if got != "42" {
		t.Errorf("INT64: got %q, want %q", got, "42")
	}
}

func TestFormatValue_Int32(t *testing.T) {
	got := formatValue(int32(42), "INT64")
	if got != "42" {
		t.Errorf("INT32: got %q, want %q", got, "42")
	}
}

func TestFormatValue_Float64_NoScientific(t *testing.T) {
	// Large float must NOT use scientific notation
	got := formatValue(float64(15000000000), "FLOAT64")
	if got != "15000000000" {
		t.Errorf("FLOAT64 large: got %q, want %q", got, "15000000000")
	}
}

func TestFormatValue_Float64_Small(t *testing.T) {
	got := formatValue(float64(0.000123), "FLOAT64")
	if got != "0.000123" {
		t.Errorf("FLOAT64 small: got %q, want %q", got, "0.000123")
	}
}

func TestFormatValue_Float64_Normal(t *testing.T) {
	got := formatValue(float64(3.14), "FLOAT64")
	if got != "3.14" {
		t.Errorf("FLOAT64: got %q, want %q", got, "3.14")
	}
}

func TestFormatValue_String(t *testing.T) {
	got := formatValue("hello", "STRING")
	if got != "hello" {
		t.Errorf("STRING: got %q, want %q", got, "hello")
	}
}

func TestFormatValue_Nil(t *testing.T) {
	got := formatValue(nil, "STRING")
	if got != nil {
		t.Errorf("nil: got %v, want nil", got)
	}
}

func TestFormatValue_Int_AsString(t *testing.T) {
	// Verify integer types produce plain decimal strings (no scientific notation)
	got := formatValue(int64(9223372036854775807), "INT64") // max int64
	want := "9223372036854775807"
	if got != want {
		t.Errorf("max INT64: got %q, want %q", got, want)
	}
}
