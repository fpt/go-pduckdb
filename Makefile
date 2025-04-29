run: ## Run the application
	CGO_ENABLED=0 go run example/main.go

test: ## Run unit tests
	go test -v ./...

fmt: ## Run format
	gofumpt -extra -w .

lint: ## Run lint
	golangci-lint run

inspect: ## Run in MCP inspector
	npx @modelcontextprotocol/inspector go run ./godevmcp/main.go serve

help: ## Display this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "%-20s %s\n", $$1, $$2}'
