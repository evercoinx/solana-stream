# AGENTS.md

## Project Overview

Rust + TypeScript monorepo for streaming real-time Solana blockchain data via Geyser gRPC, Shreds gRPC, and UDP Shreds. Rust crates are the primary focus.

## Workspace Layout

```
crate/
  solana-stream-sdk/   # Core library (published to crates.io)
  r2-uploader/         # R2 upload utility
client/
  geyser-rs/           # Geyser gRPC client
  shreds-rs/           # Shreds gRPC client
  shreds-udp-rs/       # UDP Shreds client (lowest latency)
package/
  solana-shreds-client/   # TypeScript Shreds client (NAPI-RS)
  solana-entry-decoder/   # TypeScript entry decoder (NAPI-RS)
```

Workspace dependencies are declared in the root `Cargo.toml`. Always use `{ workspace = true }` — never pin versions per-crate.

## Toolchain

- Rust `1.94`, edition `2024` (see `rust-toolchain.toml`)
- Node via `pnpm` for TypeScript packages

## Common Commands

```bash
make build           # cargo build (dev)
make build-release   # cargo build --release --locked
make check           # cargo check
make lint            # cargo clippy --locked --all-targets -- -D warnings
make format          # cargo fmt
make format-check    # cargo fmt -- --check
make test            # cargo test --locked
make audit           # cargo audit
make fix             # cargo fix --allow-dirty --allow-staged
```

## CI Requirements

All PRs must pass (in order): **format → lint + test + build**. Run before pushing:

```bash
make format-check && make lint && make test && make build-release
```

## Rust Guidelines

Follow the attached `rust-skills` conventions. Key priorities:

- **Errors**: use `thiserror` in library crates, `anyhow` in clients. Never `.unwrap()` in production; propagate with `?`.
- **Ownership**: borrow over clone; accept `&[T]`/`&str` not `&Vec<T>`/`&String`.
- **Async**: Tokio runtime. Never hold `Mutex`/`RwLock` across `.await`. Use bounded channels for backpressure.
- **Memory**: `with_capacity()` when size is known; avoid `format!()` in hot paths.
- **API**: `#[must_use]` on `Result`-returning functions; implement `From` not `Into`.
- **Naming**: `UpperCamelCase` types, `snake_case` functions, `SCREAMING_SNAKE_CASE` constants.
- **No `.unwrap()`** in non-test code — use `.expect("reason")` only for programmer errors.

## Code Style

- Imports at the top of each file — no inline imports.
- Exhaustive `match` on enums and unions; no wildcard arms that swallow variants.
- `pub(crate)` for internal APIs; `pub(super)` for parent-only visibility.
- All public items must have `///` doc comments.
