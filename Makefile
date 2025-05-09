.PHONY: integ

run: ## Run the application
	CGO_ENABLED=0 go run example/simple/main.go
	CGO_ENABLED=0 go run example/databasesql/main.go
	CGO_ENABLED=0 go run example/databasesql2/main.go
	CGO_ENABLED=0 go run example/columntypes/main.go
	CGO_ENABLED=0 go run example/enhancedtypes/main.go
	CGO_ENABLED=0 go run example/json/main.go
	CGO_ENABLED=0 go run example/multistatement/main.go

test: ## Run unit tests
	go test -v ./...

fmt: ## Run format
	gofumpt -extra -w .

lint: ## Run lint
	golangci-lint run

integ: ## Run integration tests
	docker build --platform linux/amd64 -t go-pduckdb/integ -f internal/integ/Dockerfile . && \
	docker run --rm go-pduckdb/integ

integ-arm64: ## Run integration tests on arm64
	docker build --platform linux/arm64 --build-arg GOARCH=arm64 --build-arg LIBARCH=aarch64 -t go-pduckdb/integ-arm64 -f internal/integ/Dockerfile . && \
	docker run --rm go-pduckdb/integ-arm64

help: ## Display this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "%-20s %s\n", $$1, $$2}'
