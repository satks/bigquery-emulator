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

COPY --from=builder /bigquery-emulator /usr/local/bin/bigquery-emulator

EXPOSE 9050

ENTRYPOINT ["bigquery-emulator"]
CMD ["--project=default", "--port=9050"]
