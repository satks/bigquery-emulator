# Build stage
FROM --platform=$BUILDPLATFORM golang:1.24-bookworm AS builder

ARG TARGETPLATFORM
ARG BUILDPLATFORM
ARG TARGETOS
ARG TARGETARCH

WORKDIR /build

# CGO is needed for DuckDB
ENV CGO_ENABLED=1

# Copy go mod files first for caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source
COPY . .

# Build with cross-compilation support
# For CGO cross-compilation, we need the appropriate toolchain
RUN if [ "$TARGETARCH" = "arm64" ] && [ "$BUILDPLATFORM" != "linux/arm64" ]; then \
        apt-get update && apt-get install -y gcc-aarch64-linux-gnu && \
        CC=aarch64-linux-gnu-gcc GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
        go build -o /bigquery-emulator ./cmd/bigquery-emulator/; \
    elif [ "$TARGETARCH" = "amd64" ] && [ "$BUILDPLATFORM" != "linux/amd64" ]; then \
        apt-get update && apt-get install -y gcc-x86-64-linux-gnu && \
        CC=x86_64-linux-gnu-gcc GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
        go build -o /bigquery-emulator ./cmd/bigquery-emulator/; \
    else \
        go build -o /bigquery-emulator ./cmd/bigquery-emulator/; \
    fi

# Runtime stage
FROM --platform=$TARGETPLATFORM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

LABEL org.opencontainers.image.title="BigQuery Emulator (DuckDB backend)" \
      org.opencontainers.image.description="Local Google Cloud BigQuery emulator: REST API v2 + BigQuery SQL translation on DuckDB. For integration tests, CI, and offline development — no GCP credentials needed." \
      org.opencontainers.image.source="https://github.com/satks/bigquery-emulator" \
      org.opencontainers.image.documentation="https://github.com/satks/bigquery-emulator/blob/main/README.md" \
      org.opencontainers.image.licenses="MIT"

COPY --from=builder /bigquery-emulator /usr/local/bin/bigquery-emulator

EXPOSE 9050

ENTRYPOINT ["bigquery-emulator"]
CMD ["--project=default", "--port=9050"]
