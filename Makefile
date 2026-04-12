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
