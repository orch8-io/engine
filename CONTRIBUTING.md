# Contributing to Orch8 Engine

Run commands from the repository root unless a section explicitly changes it.
Use an isolated development database: integration and E2E tests create and
modify durable records.

## Prerequisites

- Rust from `rust-toolchain.toml` (1.97.0), including rustfmt and Clippy.
- A native C/C++ build toolchain and the Protocol Buffers compiler (`protoc`).
- PostgreSQL for backend and HTTP integration coverage; the root Compose file
  supplies PostgreSQL 16 on host port 5434.
- Node.js 22 and npm for dashboard and TypeScript E2E checks.
- Python 3.11+ and bash for the documentation check.

The root [README development example](README.md#development) starts the database
and exports its connection URL. SQLite-only checks do not establish PostgreSQL
conformance. Follow the relevant platform guide for Android, iOS, WASM, or a
native Node/Python binding.

## Rust checks

```sh
cargo fmt --check
cargo check --locked --workspace
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace
RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps --document-private-items

# Exercise the actual benchmark workloads without collecting timing results.
cargo bench --locked -p orch8-engine --bench engine_bench --profile test -- --test
cargo bench --locked -p orch8-storage --bench storage_bench --profile test -- --test
```

CI also checks migrations, generated contracts, supply-chain policy, packaging,
and the supported feature/platform matrix. See [the workflow](.github/workflows/ci.yml)
for the exact commands. Optional trigger builds use server features described
in [Triggers](docs/TRIGGERS.md).

The native bindings and fuzz package are separate Cargo workspaces. Changes
there need their own locked checks:

```sh
cargo check --locked --manifest-path packages/node-native/Cargo.toml
cargo check --locked --manifest-path packages/python-native/Cargo.toml
cargo check --locked --manifest-path fuzz/Cargo.toml
```

Keep one Cargo target directory for related local checks. Reusing it avoids
recompiling the same dependency graph in multiple `/private/tmp` build trees.
Remove completed build artifacts after verification; preserve active builds
and source worktrees. Set `CARGO_INCREMENTAL=0` and `CARGO_PROFILE_TEST_DEBUG=0`
when disk capacity is limited, as CI does.

## HTTP end-to-end tests

Build `orch8-server` first and make PostgreSQL available through
`ORCH8_DATABASE_URL` (default: `postgres://orch8:orch8@localhost:5434/orch8`).
The harness prefers release builds over debug builds under the root `target/`
directory, so remove or rebuild an older release binary before testing a new
debug build. It starts
its own server on port 18080, and stops it afterward. It does not build the
binary. Leave that port free or set `ORCH8_E2E_PORT`.

```sh
cargo build --locked --bin orch8-server
(cd tests/e2e && npm ci && npm run typecheck && npm test)
```

For a single suite or parallel isolated servers, consult
[`tests/e2e/run-standalone.ts`](tests/e2e/run-standalone.ts). Each standalone
suite uses its own PostgreSQL database; provisioning requires `psql` and a
role that can create databases.

## Dashboard checks

```sh
(cd dashboard && npm ci && npm run typecheck && npm test && npm run lint && npm run build)
```

See [Dashboard](docs/DASHBOARD.md) for local connection and authentication.

## Documentation maintenance

Use [the documentation index](docs/README.md) to find the nearest existing guide.
Update that guide in the same change as the behavior it describes. Keep
historical audit findings dated and link their closure reports; do not turn
old snapshots into current guarantees.

```sh
python3 scripts/check-docs.py
```

This check covers all Git-tracked Markdown and unignored new Markdown in the
checkout. It checks local link targets and heading anchors, balanced code
fences, JSON and TOML syntax, and shell parsing with `bash -n`. It never executes
example commands, follows external links, or certifies API behavior. Label
incomplete or annotated JSON as `jsonc` or `text`; use `http` for a request line
followed by a JSON body. Quote angle-bracket placeholders in shell examples
and replace them with real values before running a command.

Format the same inventory with pinned Prettier, excluding the generated error
catalog. This command uses Python to pass each path as a separate argument:

```sh
python3 - <<'PY'
import subprocess
files = subprocess.check_output(
    ['git', 'ls-files', '-z', '--cached', '--others', '--exclude-standard']
).decode().split('\0')
files = sorted({p for p in files if p.endswith('.md') and p != 'docs/ERRORS.md'})
subprocess.run(['npm', 'exec', '--yes', '--package=prettier@3.6.2', '--',
                'prettier', '--prose-wrap', 'preserve', '--check', *files], check=True)
PY
```

Replace `--check` with `--write` to apply formatting. npm needs registry access
on its first run; the documentation syntax/link check itself runs offline.

Generated files have different owners:

| Document                                | Source and update procedure                                                                                                                              |
| --------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `docs/ERRORS.md`                        | `orch8-types/src/error_catalog.rs`; regenerate with `UPDATE_ERRORS_MD=1 cargo test --locked -p orch8-types errors_md_is_in_sync`. Do not hand-format it. |
| `contracts/openapi.json`                | Generate from API annotations with `cargo run --locked -p orch8-api --example export_openapi -- contracts/openapi.json`; then run the contract checks.   |
| `contracts/sequence*.schema.json`       | Rust sequence types; export with the same contract script.                                                                                               |
| `deploy/helm/orch8/templates/NOTES.txt` | Helm template; validate with `scripts/helm-test.sh`. It is not Markdown.                                                                                 |

Current CLI argument declarations, typed configuration, and generated API/schema
contracts take precedence over prose. Source checks and syntax checks are useful,
but run changed tutorial flows against a matching current binary before claiming
that they have been exercised end to end.
