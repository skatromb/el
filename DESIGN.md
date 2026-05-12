# `el` — Design

Status: pre-0.1.

## Why

Existing ETL tools each hit at least one dealbreaker:

- **Airbyte** — clunky UX, heavy operationally.
- **Pandas** — memory overhead, single-threaded by default.
- **Polars** — fast and lean, but Parquet support has gaps that bite in practice.
- **dlt** — mixes the issues above.

I want the tool I'd actually use. Small, fast, opinionated. Built in Rust because I like Rust and it pays off here.

## What

A simple and pleasant tool that moves a table-shaped dataset from one system to another. Not T for Transformation, only extract and load.
Transformations are someone else's job (dbt, warehouse SQL, Polars after landing).

Properties it must have:

- **Simple Python API.** A transfer fits on a screen.
- **Lean.** No per-row Python overhead. Arrow end-to-end.
- **Mature on formats.** Parquet read/write covers what Polars currently misses (gaps catalogued as we hit them).
- **Boring where it matters.** Full-load only. No streaming. No DAG orchestrator. No web UI.

Explicit non-goals for 0.1:

- No transofrmations.
- No Streaming / CDC.
- No Scheduling — keep that for Airflow/Dagster/cron.
- No CLI — Python entrypoint only.
- Incremental loads — architecture leaves room, planned for later release.

## How

### API surface (Python, code-first)

```python
from el import Transfer
from el.sources import Postgres
from el.destinations import Parquet, BigQuery

# Local parquet destination — primary 0.1 path, no cloud creds needed.
Transfer(
    source=Postgres(dsn="postgres://...", table="public.orders"),
    destination=Parquet(path="./out/orders.parquet"),
).run()

# BigQuery destination — also 0.1.
Transfer(
    source=Postgres(dsn="postgres://...", table="public.orders"),
    destination=BigQuery(project="my-proj", dataset="raw", table="orders"),
).run()
```

Source accepts `table=` OR `query=` (mutually exclusive):

```python
Postgres(dsn="...", query="SELECT id, total FROM orders WHERE region = 'EU'")
```

Internally both compile to `COPY (SELECT ...) TO STDOUT`. `table=` is sugar.

Column-level overrides:

```python
source=Postgres(
    dsn="...",
    table="public.orders",
    schema_overrides={"amount": "decimal(18,4)"},
    skip_columns=["legacy_blob"],
)
```

`.run()` returns a `RunReport`:

```python
report = transfer.run()
report.rows            # 12_481_902
report.bytes_written   # 1_503_948_211
report.duration        # timedelta
report.coercions       # list[Coercion] — column, original type, target, level
report.staging         # transient artifacts (deleted unless keep_staging=True)
```

No row-level Python callbacks in 0.1. The FFI boundary is crossed once per transfer, not per row.

### Architecture

```
+----------+     bounded mpsc      +----------+
|  Source  |  ===================>  |   Destination   |
|  reader  |   Arrow RecordBatch    |  writer  |
+----------+                        +----------+
     |                                   ^
     v                                   |
 inferred schema  --> overrides -->  resolved schema
```

- Single Rust process. `el` Python module is a PyO3 extension.
- `Source` trait yields `Stream<Item = RecordBatch>` (async).
- `Destination` trait consumes the same stream.
- Backpressure via bounded `tokio::mpsc` between reader and writer tasks.
- Schema resolution happens once, before the stream starts.

### Runtime contract

- **Atomic loads.** Each backend uses its own native atomic primitive.
    - BQ: Storage Write API in `pending` mode against a transient staging table in the destination dataset, then a server-side copy job with `WRITE_TRUNCATE` from staging into the final table, then `DROP TABLE staging`. Atomicity comes from the copy job; the Storage Write commit makes the staging table whole, the copy-replace makes the final table whole. Partitioning, clustering, description, labels, IAM on the final table are preserved (data replaced, table object not recreated). Schema enforcement is server-side: AppendRows rejects mismatched rows, the copy job rejects mismatched schemas. No client-side staging in GCS, no Parquet encoding, no `staging_bucket` knob on the public API. Errors surfaced as `ElError` subclasses.
    - Postgres (0.2): `BEGIN; DROP target; RENAME staging; COMMIT;`. Client-side schema compare needed here since there's no equivalent server-side enforcement.
  Transfers never leave the destination half-written. `mode="append"` and `mode="upsert"` are out of scope for 0.1. `on_schema_change="replace"` to opt into destructive schema replacement is a deferred kwarg.
- **Source filter surface.** `table=` and `query=` are the two ways to bound the extract. No partial filter DSL on top — keeps the API one knob wide. Incremental loads (later) reuse `query=` plus a `WatermarkSpec` (?).
- **Credentials.** All GCP auth delegates to `gcp_auth`: Application Default Credentials, `GOOGLE_APPLICATION_CREDENTIALS` service-account JSON, gcloud user creds, workload identity. Postgres uses standard DSN-embedded creds or libpq env vars.
- **Run report.** `RunReport` returned by `.run()` is the canonical post-run record. Logs are for trace; `RunReport` is for programs.
- **Logging.** Rust uses `tracing`. A bridge layer emits events into Python's `logging` so users get one config story (`logging.getLogger("el").setLevel(...)`).
- **Batching.** Default target batch size: 16 MiB. Source yields batches sized to that target (row count varies by row width). Tunable per source.
- **Concurrency.** Reader and writer run as separate tasks on a tokio runtime owned by the Rust side. PyO3 releases the GIL on every entry. Supported interpreter: Python 3.14 free-threaded build (`cp314t`) — see Tech stack.

### Type mapping

Arrow covers most primitives directly. The tricky types — geometry, JSON, UUID, ranges, intervals, vendor-specific — go through a registry, not ad-hoc per-connector code.

**Lookup order for any source-native type:**

1. Native Arrow type if one matches (`int4` → `Int32`, `numeric(p,s)` → `Decimal128(p,s)`, `interval` → `Interval(MonthDayNano)`, `timestamptz` → `Timestamp(Microsecond, "UTC")`, …).
2. Canonical Arrow extension if one exists (`uuid` → `arrow.uuid`, `json`/`jsonb` → `arrow.json`).
3. Community extension we trust (PostGIS `geometry`/`geography` → `geoarrow.wkb` with CRS metadata).
4. Private `el.*` extension over the most structured storage type that loses nothing (Postgres ranges → `el.pg_range` over a `Struct{lower, upper, lower_inc, upper_inc, empty}`).
5. `arrow.opaque` as last resort, carrying raw bytes. Destinations that can't decode it refuse, loudly.

**Destinations declare capability per extension:**

```rust
enum ExtensionSupport {
    Native,                       // destination maps directly to its own type
    FallbackOnly(Fallback),       // destination can't represent it; apply Fallback
    Unknown,                      // destination doesn't know this extension at all
}

enum Fallback {
    Expand,   // range → multiple columns; struct → flatten
    Text,     // serialize to canonical string form
    Refuse,   // fail with diagnostic
}
```

**Coercion safety tiers.** Not every coercion is equally safe. The runtime classifies each and picks a default:

| Tier                 | What it covers                                           | Default in 0.1        | Reporting               |
| -------------------- | -------------------------------------------------------- | --------------------- | ----------------------- |
| Safe (lossless)      | Range → expand. JSON → `arrow.json`. UUID → `arrow.uuid`. Standard primitive widening. `geography(_, 4326)` → BQ `GEOGRAPHY`. | Auto-apply            | INFO, in run summary    |
| Lossy structural     | Unknown type → `arrow.opaque` (bytes). Composite → struct flatten. Hstore → JSON. `geometry(_, 4326)` no Z/M → BQ `GEOGRAPHY` (planar→geodesic edge reinterpretation). | Auto-apply            | WARN, in run summary    |
| Lossy semantic       | CRS reprojection. `ST_MakeValid`. Z/M drop. Decimal truncation. tz coercion.            | **Fail**              | ERROR, stops the run    |

User overrides choose the strategy per column when the default doesn't fit:

```python
column_overrides={
    "valid":  Range(strategy="expand"),    # already the default; override only if you want "text"
    "blob":   Coerce(to="bytes"),
}
```

**Tier 3 workaround in 0.1.** Lossy-semantic coercions are not implemented; the run fails on the offending column. Workaround = drop the column from the transfer via `columns=` or `skip_columns=` on the source. They are mutually exclusive.

**Run-summary contract.** Every coercion applied is recorded in `RunReport.coercions` and rendered in the end-of-run summary. Logs alone get ignored; the summary is where surprises become visible.

```
Coercions applied (3):
  [INFO] valid   tsrange → valid_lower, valid_upper, valid_lower_inc, valid_upper_inc, valid_empty
  [INFO] payload jsonb   → arrow.json
  [WARN] meta    address_t (composite) → STRUCT, field order preserved
```

Never silently coerce to `TEXT` or `BYTES` without a summary entry — that is the dlt failure mode and it produces broken pipelines that look green.

**Concrete coverage targets:**

| Source type (Postgres)        | Arrow representation                 | Notes                                  |
| ----------------------------- | ------------------------------------ | -------------------------------------- |
| `int2`/`int4`/`int8`          | `Int16`/`Int32`/`Int64`              | Native.                                |
| `numeric(p,s)`                | `Decimal128(p,s)` or `Decimal256`    | Native. `numeric` without precision → fail with override hint. |
| `text`/`varchar`              | `Utf8`                               | Native.                                |
| `bytea`                       | `Binary`                             | Native.                                |
| `bool`                        | `Boolean`                            | Native.                                |
| `date`                        | `Date32`                             | Native.                                |
| `timestamp`/`timestamptz`     | `Timestamp(Microsecond, tz)`         | Native. `tz=None` for `timestamp`.     |
| `interval`                    | `Interval(MonthDayNano)`             | Native, exact match.                   |
| `uuid`                        | `FixedSizeBinary(16)` + `arrow.uuid` | Canonical extension.                   |
| `json`/`jsonb`                | `Utf8` + `arrow.json`                | Canonical extension.                   |
| `geometry`/`geography` (PostGIS) | `Binary` + `geoarrow.wkb` + CRS   | Community extension. CRS from `geometry_columns`. |
| `tsrange`/`int4range`/...     | `Struct` + `el.pg_range`             | Private extension. Default destination fallback = expand. |
| `hstore`, `ltree`, composites | `arrow.opaque` initially             | 0.3+ promotion to structured forms.    |

### Tech stack

| Concern          | Choice                                              | Reason                                              |
| ---------------- | --------------------------------------------------- | --------------------------------------------------- |
| Core             | Rust                                                | Performance, types, no GC.                          |
| Python binding   | PyO3 + maturin, `cp314t` only                       | Free-threaded build (PEP 703). Forces concurrency correctness early. One-row wheel matrix. |
| Internal format  | Apache Arrow (`arrow-rs`)                           | Zero-copy to BQ, Parquet, Polars.                   |
| Async runtime    | Tokio                                               | Required by most cloud SDKs.                        |
| Postgres         | `tokio-postgres` + binary `COPY`                    | `COPY` is the fastest extract path.                 |
| BigQuery (0.1)   | REST Load Jobs, hand-rolled client                  | Rust BQ ecosystem is thin. Load Jobs are bulk-friendly and simple. |
| BigQuery (0.3+)  | Storage Write API via tonic + googleapis            | Direct write, no Parquet staging.                   |
| GCP auth         | `gcp_auth`                                          | ADC, service-account JSON, gcloud, workload identity. |
| Object storage   | `object_store` crate                                | Unified S3/GCS/Azure API.                           |
| Parquet          | `parquet` (arrow-rs)                                | Same family as Arrow. Audit gaps vs Polars.         |
| Errors           | `thiserror`, surfaced as `el.ElError`               | One root exception, typed subclasses.               |
| Logging          | `tracing` bridged into Python `logging`             | One config story for users.                         |
| License          | MIT                                                 | Liberal. Matches the rest of the analytical Python/Rust stack. |

### Known risks

- **BigQuery Rust support is thin.** Mitigation: build a minimal in-tree client against REST for 0.1, tonic+googleapis for Storage Write later. Don't depend on an unmaintained crate.
- **Parquet feature gaps.** Mitigation: keep a running list of formats Polars failed on; verify each against `arrow-rs` before claiming coverage. Upstream patches if we have to.
- **Schema drift between infer and override.** Mitigation: resolved schema is logged before the stream starts; destinations validate against it on write.
- **Tricky types silently broken.** Mitigation: type registry + destination capability declarations (see Type mapping). Default to refusal over coercion.
- **Free-threaded ecosystem maturity.** Mitigation: `el` itself only depends on PyO3 + arrow-rs across the FFI seam. Users on cp314t are responsible for their own dependency stack.

## Strategy

### 0.1 — prove the thesis

Minimum viable end-to-end path: **Postgres → Parquet (local file)**, then **Postgres → BigQuery**. Both atomic full load, schema inferred with override hooks. Parquet destination comes first — no cloud creds, validates Arrow→file path before tackling BQ Storage Write API.

1. Workspace skeleton:

   ```
   /Users/skatromb/code/el/
     Cargo.toml              # workspace manifest
     rust-toolchain.toml     # pin Rust edition / version
     crates/
       el-core/              # traits (Source, Destination), type registry, Arrow plumbing, RunReport. No connector deps.
       el-postgres/          # Postgres source. tokio-postgres + binary COPY.
       el-parquet/           # Local Parquet destination. arrow-rs `parquet` crate, atomic via tmp + rename.
       el-bigquery/          # BigQuery destination. Storage Write API via tonic + googleapis.
       el-py/                # PyO3 wrapper. Re-exports connectors via Cargo features.
     python/
       el/                   # Python source layout (mixed-mode maturin)
         __init__.py         # public API re-exports
         _typing.py          # TypeAlias / Protocol definitions
         _native.pyi         # hand-written stubs for the PyO3 extension
         py.typed            # PEP 561 marker
     pyproject.toml          # maturin config, cp314t target
     docker-compose.yml      # Postgres + PostGIS fixture for tests
     .github/workflows/ci.yml
     DESIGN.md
   ```

   Per-connector crates keep `el-core` free of cloud/database deps. `el-py` opts in via Cargo features. Built via maturin against `cp314t`.
2. `Source` and `Destination` traits. In-memory test implementations. Minimal surface for `full_load` only — incremental shapes will be added later, breaking changes are fine at this stage.
3. Postgres source via `COPY (SELECT ...) TO STDOUT (FORMAT BINARY)` → Arrow `RecordBatch`. Both `table=` and `query=` compile to this. Docker Postgres+PostGIS fixture used end-to-end.
4. Parquet destination: write to `<path>.tmp` via `arrow-rs` `parquet::arrow::ArrowWriter` → fsync → atomic rename to `<path>` on success. Single-file output for 0.1 (no partitioning, no directory layout). Compression default = `zstd`. Atomicity: POSIX rename within the same filesystem.
5. BigQuery destination: Storage Write API in `pending` mode against transient staging table → server-side copy job `WRITE_TRUNCATE` from staging into final → `DROP TABLE staging`. No GCS staging.
6. Schema inference from `information_schema`; user overrides merged in.
7. Type registry: primitives, `arrow.json`, `arrow.uuid` natively. Ranges auto-expanded (Tier 1). Composites/hstore/unknown → `arrow.opaque` or struct-flatten with WARN (Tier 2). Geo path: `geography(_, 4326)` → BQ `GEOGRAPHY` (Tier 1); `geometry(_, 4326)` without Z/M → BQ `GEOGRAPHY` with WARN about planar→geodesic edge reinterpretation (Tier 2); any other SRID, Z/M present, or reprojection-requiring case = Tier 3, refused. CRS read from PostGIS `geometry_columns`; WKB carried on the wire as `geoarrow.wkb`. Parquet destination preserves Arrow extension metadata in file metadata. BQ destination serializes WKB → WKT for Storage Write. Other Tier 3 cases (lossy decimal/tz coercions) also fail. Workaround = `columns=` / `skip_columns=` on the source.
8. `RunReport` returned by `.run()`. Coercions captured. End-of-run summary rendered.
9. Auth via `gcp_auth` (ADC, service-account JSON, gcloud, workload identity). Parquet destination needs no auth.
10. `tracing` → Python `logging` bridge.
11. Wheels: Linux x86_64, Linux aarch64, macOS x86_64, macOS arm64 — all `cp314t`. No Windows in 0.1.
12. Integration tests: real Postgres in Docker for both destinations; BQ behind an env-gated test.

### 0.2 — widen the matrix

- S3 destination (Parquet) via `object_store`.
- GCS destination (Parquet) — should come almost free once S3 works.
- BigQuery source via Storage Read API.
- Postgres destination via `COPY ... FROM STDIN`, atomic swap via `RENAME`.
- `mode="append"` where atomic-replace is wrong.
- Type registry expansion: any additions that fall out of new connectors.

### Later — deliberately deferred

- Incremental loads. Model:
  - User declares `Incremental(watermark="updated_at", primary_key=["id"])`. Watermark column and PK auto-inferable from `information_schema` and common naming heuristics; refused on ambiguity.
  - **Inserts and updates** detected via `WHERE updated_at > last_watermark`. Destination applies as `MERGE ON pk WHEN MATCHED UPDATE WHEN NOT MATCHED INSERT`. Soft deletes ride along as ordinary updates — `deleted_at` is just another column whose value changes.
  - **Hard deletes** detected via destination-side reconciliation: source emits a `PkSnapshot` batch (PK columns only, full current set), destination computes set-difference against its own PKs and deletes the missing ones. Expensive (full PK scan), so opt-in and runnable on a separate cadence from upsert runs.
  - Source emits two batch kinds — `Upsert(RecordBatch)` (full payload) and `PkSnapshot(RecordBatch)` (PK columns only) — issued inside a single source-side transaction when both are produced in one run, so the upsert tail and the snapshot are consistent.
  - Watermark state is passed in and out of `.run(state=...)` so the user picks where it persists (file, BQ side table, env). Helper `el.state.bq_table(...)` later.
  - Append mode (no replace, no MERGE) uses Storage Write `pending` streams directly against the final table — no staging.
- BQ Storage Write API path (no Parquet staging).
- CRS reprojection (`proj` FFI), `ST_MakeValid`, Z/M handling.
- Hstore / ltree / composite promotion from `arrow.opaque` to structured Arrow forms.
- `strict_mode` flag.
- Resumability after partial failure.
- Transformations beyond what type mapping forces.
- CLI and YAML/TOML config.
- Streaming and CDC.
