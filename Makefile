GO ?= go
GOFMT ?= gofmt
BINARY := dist/via
PACKAGES := ./...
WEBMANAGER_DIR := webmanager
WEB_NODE_MODULES := $(WEBMANAGER_DIR)/node_modules/.package-lock.json

.PHONY: fmt fmt-check vet test race build build-go production webmanager-build webmanager-check test-network clean

fmt:
	$(GOFMT) -w .

fmt-check:
	@test -z "$$($(GOFMT) -l .)" || { $(GOFMT) -l .; echo "Go files are not formatted"; exit 1; }

vet:
	$(GO) vet $(PACKAGES)

test:
	$(GO) test $(PACKAGES)

race:
	$(GO) test -race $(PACKAGES)

$(WEB_NODE_MODULES): $(WEBMANAGER_DIR)/package.json $(WEBMANAGER_DIR)/package-lock.json
	cd $(WEBMANAGER_DIR) && npm ci --no-audit --no-fund

webmanager-build: $(WEB_NODE_MODULES)
	cd $(WEBMANAGER_DIR) && npm run build

webmanager-check: $(WEB_NODE_MODULES)
	cd $(WEBMANAGER_DIR) && npm run check

build: webmanager-build build-go

build-go:
	mkdir -p dist
	CGO_ENABLED=0 $(GO) build -trimpath -o $(BINARY) ./cmd/via

production: webmanager-check fmt-check vet test race build-go

test-network:
	./build-scripts/test-network.sh

clean:
	rm -rf dist coverage.out
