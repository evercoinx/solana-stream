# ── Rust ──────────────────────────────────────────────────────────────────────

.PHONY: build build-release check lint format format-check test \
        doc clean update audit fix bump tag

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

LEVEL          ?= auto
RELEASE_BRANCH := release

.PHONY: bump tag

bump:
	@if ! git diff --quiet || ! git diff --cached --quiet; then \
		echo "Error: working tree is dirty — commit or stash changes first"; exit 1; \
	fi
	git checkout main
	git pull --ff-only origin main
	git fetch --tags --quiet
	git branch -D $(RELEASE_BRANCH) 2>/dev/null || true
	git checkout -b $(RELEASE_BRANCH)
	@set -e; \
	BASE=$$(git log HEAD --grep='^chore: bump project to version' --format=%H | head -1); \
	RANGE=$${BASE:+$$BASE..HEAD}; \
	if [ "$(LEVEL)" = "auto" ]; then \
		if [ -n "$$RANGE" ] && git log $$RANGE --format='%B' | grep -qE '(BREAKING CHANGE|^[a-z]+(\([^)]+\))?!:)'; then \
			LEVEL=major; \
		elif [ -n "$$RANGE" ] && git log $$RANGE --format='%s' | grep -qE '^feat(\(|:)'; then \
			LEVEL=minor; \
		else \
			LEVEL=patch; \
		fi; \
	else \
		LEVEL="$(LEVEL)"; \
	fi; \
	CUR_VERSION=$$(grep '^version' Cargo.toml | head -1 | sed 's/version = "//;s/"//'); \
	echo "Releasing: $$LEVEL (from $$CUR_VERSION)"; \
	cargo release $$LEVEL --execute --no-confirm; \
	NEW_VERSION=$$(grep '^version' Cargo.toml | head -1 | sed 's/version = "//;s/"//'); \
	PREV_TAG=$$(git tag --sort=-version:refname | grep "^v" | head -1); \
	BODY_RANGE=$${BASE:+$$BASE..HEAD^}; \
	BODY=$$(printf "## Release v$$NEW_VERSION\n\n**Changes since $$PREV_TAG:**\n\n%s" \
		"$$(git log $$BODY_RANGE --oneline --no-merges)"); \
	gh pr create \
		--base main \
		--head $(RELEASE_BRANCH) \
		--title "chore: bump project to version $$NEW_VERSION" \
		--body "$$BODY"

tag:
	@if ! git diff --quiet || ! git diff --cached --quiet; then \
		echo "Error: working tree is dirty — commit or stash changes first"; exit 1; \
	fi
	git checkout main
	git pull --ff-only origin main
	git fetch --tags --quiet
	@set -e; \
	VERSION=$$(grep '^version' Cargo.toml | head -1 | sed 's/version = "//;s/"//'); \
	TAG="v$$VERSION"; \
	if git tag --list "$$TAG" | grep -q .; then \
		echo "Tag $$TAG already exists — nothing to do"; exit 0; \
	fi; \
	echo "Creating tag $$TAG on $$(git rev-parse --short HEAD) (main)"; \
	git tag -s "$$TAG" -m "Release $$TAG"; \
	git push origin "$$TAG"; \
	echo "Pushed $$TAG to origin"
