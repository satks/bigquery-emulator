# Build stage
FROM golang:1.24-bookworm AS builder

WORKDIR /build

# CGO is needed for DuckDB
ENV CGO_ENABLED=1

# Copy go mod files first for caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source
COPY . .

# Build
RUN go build -o /bigquery-emulator ./cmd/bigquery-emulator/

# Runtime stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /bigquery-emulator /usr/local/bin/bigquery-emulator

EXPOSE 9050

ENTRYPOINT ["bigquery-emulator"]
CMD ["--project=default", "--port=9050"]
