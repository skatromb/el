# `transferred` — Plan

Versioned roadmap and progress. Architecture and contracts in [DESIGN.md](./DESIGN.md).

Legend: `[x]` done · `[~]` in progress · `[ ]` pending.

## 0.0.1 — first publishable, ergonomics test

Goal: end-to-end Python wheel with Parquet round-trip published to PyPI + corresponding Rust crates published to crates.io. Validates the FFI seam and the publish pipeline, not connector breadth.

**Scope:**

- Parquet source + destination only. No Postgres, no BigQuery.
- Python API: `Transfer(source=..., destination=...).run() -> RunReport`. Accepts `Parquet` source/destination and `pa.Table` / `pa.RecordBatchReader` sources (zero-copy).
- `Iter` source deferred to 0.0.2.
- Schema model: destination-owned, `schema=` (full) and `schema_overrides=` (partial). Parquet vocabulary = polars/arrow-shorthand strings or `pa.Schema`. Trait surface from §Architecture.
- `RunReport`, `ElError` hierarchy surfaced into Python.
- License: MIT, `LICENSE` file at repo root.
- Workspace version shared across crates. Untie later if cadence diverges.

**Tasks:**

- [x] Workspace skeleton (`Cargo.toml`, `rust-toolchain.toml`, per-crate dirs).
- [x] `transferred-core` traits (`Source`, `Destination`, `Transfer`, `ElError`, `RunReport`, `BatchStream`).
- [x] `transferred-parquet` source (async).
- [x] `transferred-parquet` destination (async, atomic tmp+rename, zstd).
- [x] Parquet round-trip dogfood test (wide schema, AAA structure).
- [x] `dev` feature flag with `TestSource` / `TestDestination`.
- [x] **Rename crates `el-*` → `transferred-*`** (workspace, paths, imports).
- [ ] **Reserve names** on crates.io and PyPI (publish 0.0.0 placeholders or rely on first-publish reservation).
- [x] **LICENSE file** at repo root (MIT).
- [x] **Per-crate `description`, `readme`, `keywords`, `categories`** in Cargo.toml — crates.io rejects without them.
- [ ] **Schema redesign** — destination-owned vocab, trait additions (`parse_user_schema`, `apply_overrides`, `to_destination_schema`). Implement for Parquet first.
- [ ] **Coercion engine** — runtime cast from inferred Arrow schema to canonical schema. Tier 1 auto, Tier 2 warn, Tier 3 fail.
- [ ] **`transferred-py` crate** — PyO3 module, mixed-mode maturin layout (`python/transferred/`).
  - [ ] `Transfer` Python class wrapping Rust `Transfer`.
  - [ ] `Parquet` source + destination Python wrappers.
  - [ ] `RunReport` Python class (attribute access, `__repr__`).
  - [ ] `ElError` Python exception hierarchy (`transferred.ElError` root + subclasses for source/destination/schema failures).
  - [ ] Source coercion dispatcher (`pa.Table` / `pa.RecordBatchReader` → Arrow source; reject other types until 0.0.2).
  - [ ] `_native.pyi` stubs, `py.typed` marker.
- [ ] **`pyproject.toml`** — maturin config, wheel targets cp314 + cp314t. No cp313.
- [ ] **CI: PR gate workflow** (`.github/workflows/ci.yml`).
  - [ ] `cargo fmt --check`, `cargo clippy --workspace --tests --all-features -- -D warnings`.
  - [ ] `cargo test --workspace --all-features`.
  - [ ] `ruff check`, `ty` (or `mypy`), `pytest`.
  - [ ] rust-cache for incremental builds.
- [ ] **CI: release workflow** (`.github/workflows/release.yml`, tag-triggered).
  - [ ] Cargo publish each workspace crate in dep order: core → parquet → py.
  - [ ] Build wheels via `cibuildwheel` or maturin matrix (Linux x86_64/aarch64, macOS x86_64/arm64).
  - [ ] Publish to PyPI via Trusted Publishers (OIDC, no token in repo).
  - [ ] Pre-register pending publisher on PyPI before first tag push.
- [ ] **Cut 0.0.1 tag.**

**Open decisions to lock pre-tag:**

- Trusted Publisher requires repo + workflow filename pre-registered on PyPI. Workflow filename: `release.yml` (lock before pre-registration).
- Crate ownership on crates.io: personal account first, transfer to org later if/when one exists.
- Name reservation strategy: 0.0.0 placeholder publish on both crates.io and PyPI, or wait for real 0.0.1? Placeholder guards against squatting between now and first publish.

## 0.0.2 — custom Source from any iterable

Goal: load API responses and Python-native data without forcing the user through pyarrow.

**Scope:**

- `Iter` source — Python-side dispatcher accepts `Iterable[dict | dataclass | tuple]`, batches into `pa.RecordBatch`, hands to Rust.
- Auto-coercion in `Transfer(source=...)` — wrap raw iterables in `Iter` without explicit wrapper.
- Schema inference from first batch via `pa.RecordBatch.from_pylist`.
- Destination schema applies as coercion target (Iter has no native schema of its own).
- Bounded queue (`maxsize=2`) between Python producer and Rust consumer.
- Document: pyarrow becomes a hard runtime dep.

**Tasks:**

- [ ] `Iter` source class (Python).
- [ ] Source coercion dispatcher: `Iterable` → `Iter`, `pa.Table`/`pa.RecordBatchReader` → `Arrow`, `Source` → passthrough.
- [ ] Per-chunk pyarrow conversion + drop of source list.
- [ ] Bounded inter-thread queue.
- [ ] Tests: list-of-dicts, generator, dataclass, mixed-null columns, schema coercion to destination types.
- [ ] Docs: memory profile, batch_size tuning, when to use `Arrow` for zero-copy.

## 0.1.0 — Postgres source → BigQuery destination

Goal: original thesis. Atomic full load from PG to BQ.

**Scope:**

- `transferred-postgres` source: `COPY (SELECT ...) TO STDOUT (FORMAT BINARY)` → Arrow `RecordBatch`. Both `table=` and `query=` compile to COPY. Docker PG+PostGIS fixture for tests.
- `transferred-bigquery` destination: Storage Write API in `pending` mode against transient staging table → server-side copy job `WRITE_TRUNCATE` from staging into final → `DROP TABLE staging`. No GCS staging.
- BQ schema vocabulary in Python (`"INT64"`, `"NUMERIC(18, 4)"`, `"GEOGRAPHY"`, `bigquery.SchemaField`).
- Schema inference from `information_schema`.
- Auth via `gcp_auth` (ADC, service-account JSON, gcloud, workload identity).
- Type registry initial coverage: primitives, `arrow.json`, `arrow.uuid`, ranges (expand), `geography(_, 4326)` → BQ `GEOGRAPHY` (Tier 1), `geometry(_, 4326)` no Z/M → BQ `GEOGRAPHY` (Tier 2 warn). Other tier-3 cases refused with `columns=`/`skip_columns=` workaround.
- `tracing` → Python `logging` bridge.

**Tasks:**

- [ ] `transferred-postgres` connect + COPY binary parser.
- [ ] PG → Arrow type mapping (per DESIGN.md coverage table).
- [ ] Integration test: docker-compose PG+PostGIS fixture.
- [ ] `transferred-bigquery` Storage Write client (tonic + googleapis).
- [ ] Atomic staging-table + copy-replace + drop-staging flow.
- [ ] Auth integration (`gcp_auth`).
- [ ] BQ env-gated integration test.
- [ ] CI: docker PG service for PR gate.
- [ ] Logging bridge crate.

## 0.1.1 — Postgres destination, BigQuery source

**Scope:**

- `transferred-postgres` destination: `COPY ... FROM STDIN`, atomic swap via `BEGIN; DROP target; RENAME staging; COMMIT;`. Client-side schema compare needed (no server-side enforcement like BQ).
- `transferred-bigquery` source: Storage Read API.
- Round-trip integration tests (PG ↔ BQ).

## 0.2 — widen the matrix

- S3 destination (Parquet) via `object_store`.
- GCS destination (Parquet) — nearly free once S3 works.
- `mode="append"` where atomic-replace is wrong.
- Partitioned Parquet directory destination (enables true partition parallelism).
- Type registry expansion driven by new connectors.
- Concurrent transfers in one process — task-count cap, optional byte-aware semaphore.

## Later — deliberately deferred

- Incremental loads. Deferred; model TBD.
- CRS reprojection (`proj` FFI), `ST_MakeValid`, Z/M handling.
- Hstore / ltree / composite promotion from `arrow.opaque` to structured Arrow forms.
- `strict_mode` flag.
- Resumability after partial failure.
- Transformations beyond what type mapping forces.
- CLI and YAML/TOML config.
- Streaming and CDC.
- Byte-aware memory semaphore (when partition parallelism reveals skew issues).
