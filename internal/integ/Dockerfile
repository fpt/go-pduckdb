FROM golang:1.24.2 AS builder

# Add build arguments for architecture
ARG GOARCH=amd64
ARG LIBARCH=amd64

RUN apt-get update && apt-get install -y curl unzip

WORKDIR /app

COPY . .

# Set GOARCH for the build
ENV GOARCH=${GOARCH}

# Download and install DuckDB library before running tests
# NOTE: Official DuckDB builds require glibc. Not musl.
RUN curl -sSL https://github.com/duckdb/duckdb/releases/download/v1.2.2/libduckdb-linux-${LIBARCH}.zip -o archive.zip \
    && unzip -j archive.zip libduckdb.so \
    && rm archive.zip \
    && cp libduckdb.so /usr/local/lib/ \
    && ldconfig

RUN go get .

# Run the tests
RUN go test -v ./...

RUN ./internal/integ/build.sh

FROM debian:bookworm-slim AS tester

WORKDIR /app
COPY --from=builder /app/out ./out
COPY --from=builder /app/internal/integ/run.sh .
COPY --from=builder /usr/local/lib/libduckdb.so /usr/local/lib/

RUN ldconfig

RUN chmod +x /app/run.sh

ENTRYPOINT [ "/app/run.sh" ]
