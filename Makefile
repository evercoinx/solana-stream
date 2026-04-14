# ── Rust ──────────────────────────────────────────────────────────────────────

.PHONY: build build-release check lint format format-check test \
        doc clean update audit fix

build:
	cargo build

build-release:
	cargo build --release --locked

check:
	cargo check

lint:
	cargo clippy --locked --all-targets -- -D warnings

format:
	cargo fmt

format-check:
	cargo fmt -- --check

test:
	cargo test --locked

doc:
	cargo doc --open

clean:
	cargo clean

update:
	cargo update

audit:
	cargo audit

fix:
	cargo fix --allow-dirty --allow-staged

# ── Release ───────────────────────────────────────────────────────────────────

BUMP           ?= auto
RELEASE_BRANCH := release

.PHONY: release

release:
	@if ! git diff --quiet || ! git diff --cached --quiet; then \
		echo "Error: working tree is dirty — commit or stash changes first"; exit 1; \
	fi
	git checkout main
	git pull --ff-only origin main
	git branch -D $(RELEASE_BRANCH) 2>/dev/null || true
	git checkout -b $(RELEASE_BRANCH)
	@set -e; \
	if [ "$(BUMP)" = "auto" ]; then \
		RESOLVED=$$(git cliff --bumped-version 2>/dev/null | sed 's/^v//'); \
		if [ -z "$$RESOLVED" ]; then RESOLVED=patch; fi; \
	else \
		RESOLVED="$(BUMP)"; \
	fi; \
	echo "Releasing: $$RESOLVED"; \
	cargo release $$RESOLVED --execute --no-confirm; \
	NEW_VERSION=$$(grep '^version' Cargo.toml | head -1 | sed 's/version = "//;s/"//'); \
	PREV_TAG=$$(git tag --sort=-version:refname | grep "^v" | grep -v "^v$$NEW_VERSION$$" | head -1); \
	BODY=$$(printf "## Release v$$NEW_VERSION\n\n**Changes since $$PREV_TAG:**\n\n%s" \
		"$$(git log $$PREV_TAG..HEAD^ --oneline --no-merges)"); \
	gh pr create \
		--base main \
		--head $(RELEASE_BRANCH) \
		--title "chore: bump project to version $$NEW_VERSION" \
		--body "$$BODY"
