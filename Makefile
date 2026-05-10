GO ?= go
GOBIN ?= $(shell $(GO) env GOPATH)/bin
GOLANGCI_LINT ?= $(GOBIN)/golangci-lint
GOLANGCI_LINT_VERSION ?= v2.12.2

.PHONY: help
help: ## Show available targets.
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z_-]+:.*## / {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: test
test: ## Run unit tests.
	$(GO) test ./...

.PHONY: test-race
test-race: ## Run unit tests with the race detector.
	$(GO) test -race ./...

.PHONY: vet
vet: ## Run go vet.
	$(GO) vet ./...

.PHONY: lint
lint: ## Run golangci-lint (install with `make tools`).
	@if [ ! -x "$(GOLANGCI_LINT)" ] && ! command -v golangci-lint >/dev/null 2>&1; then \
		echo "golangci-lint not found. Run 'make tools' to install $(GOLANGCI_LINT_VERSION)." >&2; \
		exit 1; \
	fi
	@if [ -x "$(GOLANGCI_LINT)" ]; then $(GOLANGCI_LINT) run ./...; else golangci-lint run ./...; fi

.PHONY: fmt
fmt: ## Format the code (gofmt -s).
	gofmt -s -w .

.PHONY: fmt-check
fmt-check: ## Verify gofmt is clean.
	@diffs=$$(gofmt -s -l .); \
	if [ -n "$$diffs" ]; then \
		echo "gofmt issues in:" >&2; echo "$$diffs" >&2; \
		echo "Run 'make fmt' to fix." >&2; exit 1; \
	fi

.PHONY: tidy
tidy: ## Run go mod tidy.
	$(GO) mod tidy

.PHONY: tidy-check
tidy-check: ## Verify go.mod / go.sum are tidy.
	@diff=$$($(GO) mod tidy -diff 2>&1) ; \
	if [ -n "$$diff" ]; then \
		echo "go.mod is not tidy:" >&2; echo "$$diff" >&2; \
		echo "Run 'make tidy' to fix." >&2; exit 1; \
	fi

.PHONY: cover
cover: ## Run tests with a coverage report.
	$(GO) test -coverprofile=coverage.out ./...
	$(GO) tool cover -func=coverage.out | tail -1

.PHONY: cover-html
cover-html: cover ## Open the HTML coverage report.
	$(GO) tool cover -html=coverage.out

.PHONY: ci
ci: fmt-check tidy-check vet lint test-race ## Run the full CI pipeline locally.

.PHONY: tools
tools: ## Install required dev tools (golangci-lint $(GOLANGCI_LINT_VERSION)).
	$(GO) install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

.PHONY: hooks
hooks: ## Install pre-commit hooks (requires the `pre-commit` Python tool).
	@command -v pre-commit >/dev/null 2>&1 || { \
		echo "pre-commit not found. Install with: pip install pre-commit  (or your package manager)" >&2; \
		exit 1; \
	}
	pre-commit install
	@echo "Pre-commit hooks installed (.pre-commit-config.yaml)."

.PHONY: hooks-uninstall
hooks-uninstall: ## Uninstall pre-commit hooks.
	@command -v pre-commit >/dev/null 2>&1 || exit 0
	pre-commit uninstall
	@echo "Pre-commit hooks uninstalled."

.PHONY: hooks-run
hooks-run: ## Run all pre-commit hooks against the whole tree (CI-style).
	pre-commit run --all-files

.DEFAULT_GOAL := help
