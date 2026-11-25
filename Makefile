GO_LICENSES_VERSION := v1.6.0
GO_LICENSES         := $(shell go env GOPATH)/bin/go-licenses

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

.PHONY: check-names
# The provisioning role is the operator and its record topic wherry.operator;
# every command is scoped by its role word. The old names went in a clean
# break. CHANGELOG-style history keeps them. The patterns are assembled from
# fragments, as in check-markers.
check-names: ## Fail on the old provisioning-role and command names in tracked files
	@p="wherry[ .]adm""in|WHERRY_TRUST_ADM""IN[A-Z_]*|wherry (k""ey|clean""up)|dev re""set|receiver (a""dd|up""date|re""voke)|pool (cre""ate|up""date)|host re""voke"; \
	q="#wherry-adm""in-|internal/adm""in"; \
	hits=$$(git grep -n -w -E "$$p" -- . ':!*CHANGELOG*'; git grep -n -E "$$q" -- . ':!*CHANGELOG*'); \
	r='wherry (host )?wa''it\b|host-wa''it-run-id'; \
	hits="$$hits$$(git grep -n -E "$$r" -- docs/ plugins/ test/ README.md '*.go' ':!*CHANGELOG*' ':!docs/upgrade.md' ':!*_test.go')"; \
	if [ -n "$$hits" ]; then \
		echo "$$hits"; echo "FAIL: old provisioning-role or command names in tracked files"; exit 1; \
	fi; \
	echo "no old provisioning-role or command names in tracked files"

.PHONY: check-ui-source
# The operator view renders tenant and agent text as React text nodes alone; the pattern is assembled from fragments
# so this recipe never contains the string it hunts.
check-ui-source: ## Fail on raw HTML rendering in the operator view's app source
	@p="dangerously""SetInnerHTML"; \
	if hits=$$(git grep -n -E "$$p" -- internal/view/web/src); then \
		echo "$$hits"; echo "FAIL: raw HTML rendering in the operator view's app source"; exit 1; \
	fi; \
	echo "no raw HTML rendering in the operator view's app source"

.PHONY: check
check: check-fmt vet test check-licenses check-markers check-names check-ui-source ## Run every CI gate

.PHONY: ui
# The built page is committed under internal/view/static, so go build and the Go jobs need no Node; ci.yml's ui job
# rebuilds it from the lockfile and fails when the committed output differs. The build also writes
# internal/view/static/THIRD_PARTY_NOTICES and fails on a bundled licence outside its list. npm dependency bumps are
# made here by hand, the lockfile regenerated on Linux (docker run node:22 npm install --package-lock-only), since a
# bump without a rebuild would fail the ui job: Dependabot has no npm entry.
ui: ## Rebuild the operator view's page into internal/view/static (needs Node 22)
	cd internal/view/web && npm ci && npm run check && npm run build

.PHONY: ui-smoke
ui-smoke: ## Run the operator view's browser smoke against the committed page (needs npx playwright install chromium)
	cd internal/view/web && npm run smoke

.PHONY: docs
docs: ## Build the documentation site into docs-site/.vitepress/dist (npm ci in docs-site first)
	cd docs-site && npm run build
