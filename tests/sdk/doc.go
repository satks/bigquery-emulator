// Package sdk contains compatibility tests for Google Cloud BigQuery SDKs.
//
// These tests verify that the emulator works correctly with the official
// Google Cloud client libraries by exercising the same HTTP API endpoints
// with the exact JSON formats the SDKs expect.
//
// Go SDK tests (using net/http to simulate SDK behavior):
//
//	go test -race -v ./tests/sdk/...
//
// Prerequisites for real SDK testing (not required for these tests):
//
//	go get cloud.google.com/go/bigquery
//
// Node.js SDK tests (manual):
//
//	cd tests/sdk/nodejs && npm install && node test.js
package sdk
