FROM golang:1.24.2 AS builder
RUN apt-get update && apt-get install -y curl unzip

WORKDIR /app

COPY . .

RUN go get .

RUN ./internal/integ/build.sh

# NOTE: Official DuckDB builds require glibc. Not musl.
RUN curl -sSL https://github.com/duckdb/duckdb/releases/download/v1.2.2/libduckdb-linux-amd64.zip -o archive.zip \
    && unzip -j archive.zip libduckdb.so \
    && rm archive.zip

FROM debian:bookworm-slim AS tester

WORKDIR /app
COPY --from=builder /app/out ./out
COPY --from=builder /app/internal/integ/run.sh .
COPY --from=builder /app/libduckdb.so /usr/local/lib

RUN ldconfig

RUN chmod +x /app/run.sh

ENTRYPOINT [ "/app/run.sh" ]
