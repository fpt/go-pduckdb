FROM golang:1.25 AS builder

# Add build arguments for architecture
ARG GOARCH=amd64
ARG LIBARCH=amd64

RUN apt-get update && apt-get install -y curl

WORKDIR /app

COPY . .

# Set GOARCH for the build
ENV GOARCH=${GOARCH}

# Download and install DuckDB library before running tests
# NOTE: Official DuckDB builds require glibc. Not musl.
RUN curl -sSfL https://github.com/duckdb/duckdb/releases/download/v2.0.0/duckdb-shared-libs-linux-${LIBARCH}.tar.gz -o archive.tar.gz \
    && tar xzf archive.tar.gz libduckdb.so \
    && rm archive.tar.gz \
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
