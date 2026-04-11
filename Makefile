.PHONY: build test test-race bench lint clean docker docker-multiarch

# Build the binary
build:
	go build -o bigquery-emulator ./cmd/bigquery-emulator/

# Run all tests
test:
	go test ./...

# Run tests with race detector
test-race:
	go test -race -count=1 ./...

# Run benchmarks
bench:
	go test -bench=. -benchmem ./tests/benchmark/...

# Run linter
lint:
	golangci-lint run ./...

# Clean build artifacts
clean:
	rm -f bigquery-emulator
	go clean -testcache

# Build Docker image
docker:
	docker build -t bigquery-emulator .

# Build multi-platform Docker image (amd64 + arm64)
docker-multiarch:
	docker buildx build --platform linux/amd64,linux/arm64 -t bigquery-emulator:latest .

# Run the emulator locally
run: build
	./bigquery-emulator --project=test-project --port=9050

# Show test coverage
coverage:
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
