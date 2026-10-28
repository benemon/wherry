GO_LICENSES_VERSION := v1.6.0
GO_LICENSES         := $(shell go env GOPATH)/bin/go-licenses
CYCLONEDX_VERSION   := v1.11.0
CYCLONEDX           := $(shell go env GOPATH)/bin/cyclonedx-gomod

VERSION   ?= dev
DIST      ?= dist
PLATFORMS ?= linux/amd64 linux/arm64 darwin/amd64 darwin/arm64

LICENCE_ALLOWLIST := MIT,Apache-2.0,BSD-2-Clause,BSD-3-Clause,ISC,MPL-2.0

.PHONY: help
help: ## List targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "%-16s %s\n", $$1, $$2}'

.PHONY: build
build: ## Build the wherry binary
	go build -trimpath -o wherry ./cmd/wherry

.PHONY: check-fmt
check-fmt: ## Fail on Go files gofmt would change
	@out=$$(gofmt -l $$(git ls-files '*.go')); \
	if [ -n "$$out" ]; then echo "$$out"; echo "FAIL: gofmt"; exit 1; fi

.PHONY: vet
vet: ## Vet both modules
	go vet ./...
	cd pluginsdk && go vet ./...

.PHONY: test
# Two packages at a time: more exhaust macOS's ephemeral ports with the in-process Kafka connections the tests open.
# The timeout bounds each package's test binary: internal/receiver, the slowest, runs up to 12 minutes on a 4-core
# arm64 host whose CPU lacks SHA-512 instructions, past the 10-minute default.
test: ## Test both modules
	go test -p 2 -timeout 30m ./...
	cd pluginsdk && go test ./...

$(GO_LICENSES):
	go install github.com/google/go-licenses@$(GO_LICENSES_VERSION)

$(CYCLONEDX):
	go install github.com/CycloneDX/cyclonedx-gomod/cmd/cyclonedx-gomod@$(CYCLONEDX_VERSION)

.PHONY: check-licenses
# A failure means a dependency changed licence on upgrade: investigate the
# module, do not widen the list to get green.
check-licenses: $(GO_LICENSES) ## Fail on dependency licences outside the allowlist
	$(GO_LICENSES) check ./... --allowed_licenses=$(LICENCE_ALLOWLIST)
	cd pluginsdk && $(GO_LICENSES) check ./... --allowed_licenses=$(LICENCE_ALLOWLIST)

.PHONY: check-markers
# wherry integrates agent harnesses by name, so product names are not
# markers here; attribution is. The patterns are assembled from fragments so
# this recipe never contains the strings it hunts and the Makefile is
# scanned like every other tracked file.
check-markers: ## Fail on tool-attribution markers in tracked files
	@p="co-auth""ored-by|gener""ated with|ai-gen""erated|claude\.ai/co""de|noreply@anthro""pic"; \
	if hits=$$(git grep -n -i -E "$$p" -- .); then \
		echo "$$hits"; echo "FAIL: tool-attribution markers in tracked files"; exit 1; \
	fi; \
	echo "no tool-attribution markers in tracked files"

.PHONY: check
check: check-fmt vet test check-licenses check-markers ## Run every CI gate

.PHONY: docs
docs: ## Build the documentation site into docs-site/.vitepress/dist (npm ci in docs-site first)
	cd docs-site && npm run build

.PHONY: sbom
# app mode describes what the wherry binary links, not the module graph with
# its test-only dependencies. The tool reads tags through go-git, so run it
# from a plain clone with tags fetched.
sbom: $(CYCLONEDX) ## Write a CycloneDX SBOM for the wherry binary
	mkdir -p $(DIST)
	$(CYCLONEDX) app -json -licenses -main cmd/wherry -output $(DIST)/wherry.cdx.json .

.PHONY: dist
dist: ## Cross-compile release archives and checksums.txt into DIST
	rm -rf $(DIST)/wherry_*
	mkdir -p $(DIST)
	@set -e; for p in $(PLATFORMS); do \
		os=$${p%/*}; arch=$${p#*/}; name=wherry_$(VERSION)_$${os}_$${arch}; \
		mkdir -p $(DIST)/$$name; \
		CGO_ENABLED=0 GOOS=$$os GOARCH=$$arch go build -trimpath -o $(DIST)/$$name/wherry ./cmd/wherry; \
		cp LICENSE README.md $(DIST)/$$name/; \
		mkdir -p $(DIST)/$$name/contrib && cp -R contrib/observability $(DIST)/$$name/contrib/; \
		tar -C $(DIST) -czf $(DIST)/$$name.tar.gz $$name; \
		rm -rf $(DIST)/$$name; \
	done
	cd $(DIST) && sha256sum wherry_*.tar.gz > checksums.txt
