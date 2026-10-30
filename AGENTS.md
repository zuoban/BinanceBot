# Repository Guidelines

## Project Structure & Module Organization

This repository contains a Rust Binance Futures grid-trading backend and a
React/TypeScript console. Backend code lives in `src/`: `strategy/` implements
grid behavior, `exchange/` handles Binance connectivity, `server/` exposes
HTTP and WebSocket APIs, and `db.rs` manages SQLite persistence. Integration
tests live in `tests/`; module-specific Rust tests also live alongside source.
The frontend lives in `frontend/src/`, organized into `pages/`, `components/`,
`lib/`, and `test/`. Supporting design documents are in `docs/`, `DESIGN.md`,
and `PRODUCT.md`.

## Build, Test, and Development Commands

Use Rust 1.88+ and Node.js 22.12+.

- `cargo run --release`: start the backend with its default SQLite database.
- `cargo test`: run Rust unit and integration tests.
- `cargo fmt --check`: check Rust formatting; use `cargo fmt` to apply it.
- `cd frontend && npm ci`: install the locked frontend dependencies.
- `cd frontend && npm run dev`: start Vite; API requests proxy to
  `127.0.0.1:8080` by default.
- `cd frontend && npm test`: run the Vitest suite.
- `cd frontend && npm run build`: type-check and build the production console.
- `docker compose up -d --build`: build and start both services.

## Coding Style & Naming Conventions

Follow `rustfmt` defaults for Rust (four-space indentation and `snake_case`
functions and modules). Frontend files use TypeScript, two-space indentation,
single quotes, and semicolons; use `PascalCase` for React components and
`camelCase` for functions and variables. Preserve existing UI patterns and
Chinese user-facing copy. Use decimal types for prices and quantities rather
than introducing floating-point trading calculations.

## Testing Guidelines

Add focused tests for behavior changes, particularly order reconciliation,
trading safety, persistence, and API responses. Rust uses built-in tests and
integration files named `*_test.rs` or `*_tests.rs`. Frontend tests use Vitest
and Testing Library in `frontend/src/test/`, named `*.test.ts` or
`*.test.tsx`. Run the relevant suite and frontend build before submitting.

## Commit & Pull Request Guidelines

Recent commits commonly use Conventional Commit prefixes such as `feat`,
`fix`, and optional scopes, for example `fix(frontend): prevent account title
flicker`. Keep commits focused and write imperative summaries. Pull requests
should describe the behavior change, relevant validation, and any trading or
migration implications; link related issues and include screenshots for
visible UI changes.

## Security & Configuration

Never commit API keys, Telegram tokens, or SQLite databases. Runtime settings
and credentials are stored in SQLite and managed through the console; use
paper trading for development. Preserve existing safeguards around live orders,
pause state, and exchange reconciliation.
