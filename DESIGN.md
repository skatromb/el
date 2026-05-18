# `transferred` — Design

Status: pre-0.0.1.

Package name `transferred` on both crates.io and PyPI. `el` was taken on both. Workspace is split into per-connector crates (`transferred-core`, `transferred-parquet`, `transferred-postgres`, `transferred-bigquery`) plus a Python binding crate. Workspace version is shared across all crates; untie only if release cadence diverges.

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

Explicit non-goals for the pre-1.0 line:

- No transformations.
- No streaming / CDC.
- No scheduling — keep that for Airflow/Dagster/cron.
- No CLI — Python entrypoint only.
- Incremental loads — architecture leaves room, planned for later release.

## How

### API surface (Python, code-first)

```python
from transferred import Transfer
from transferred.sources import Postgres
from transferred.destinations import Parquet, BigQuery

# Local parquet destination — primary 0.0.1 path, no cloud creds needed.
Transfer(
    source=Postgres(dsn="postgres://...", table="public.orders"),
    destination=Parquet(path="./out/orders.parquet"),
).run()

# BigQuery destination — 0.1.0.
Transfer(
    source=Postgres(dsn="postgres://...", table="public.orders"),
    destination=BigQuery(project="my-proj", dataset="raw", table="orders"),
).run()
```

**Source auto-coercion.** `source=` accepts anything iterable, not only `Source` instances. Python-side dispatcher normalises:

```python
# Generator of dicts — primary 0.0.2 path
def fetch_orders():
    for page in api.paginate("/orders"):
        yield from page["results"]

Transfer(source=fetch_orders(), destination=Parquet("out.parquet")).run()

# List of dicts
Transfer(source=[{"id": 1}, {"id": 2}], destination=Parquet("...")).run()

# Dataclasses — converted via dataclasses.asdict per chunk
Transfer(source=order_iter, destination=Parquet("...")).run()

# Existing pyarrow — zero-copy
Transfer(source=pa.table({...}), destination=Parquet("...")).run()
```

Dispatcher rules:

- `Source` instance → used directly.
- `pa.Table` / `pa.RecordBatchReader` → wrapped in `Arrow` source (zero-copy via Arrow C Data Interface).
- Any other `Iterable` → wrapped in `Iter` source. Chunks rows into `pa.RecordBatch` of `batch_size` (default 4096), one FFI crossing per batch, schema inferred from first batch.
- Anything else → `TypeError`.

Explicit wrappers (`Iter(...)`, `Arrow(...)`) stay available when caller needs to set `batch_size` or override inference.

Source accepts `table=` OR `query=` (mutually exclusive):

```python
Postgres(dsn="...", query="SELECT id, total FROM orders WHERE region = 'EU'")
```

Internally both compile to `COPY (SELECT ...) TO STDOUT`. `table=` is sugar.

Source-side column filtering — `columns=` or `skip_columns=` (mutually exclusive):

```python
source=Postgres(
    dsn="...",
    table="public.orders",
    skip_columns=["legacy_blob"],
)
```

No source-side typing API. Source schema is either inferred from the source itself, or bridged in reverse from the destination schema (Destination → Arrow → coerce source rows to match). All typing knobs live on the destination.

**Destination schema.** User-facing schema is destination-native; Arrow is implementation, never spelled in Python.

```python
# Full schema — destination-native types as strings
BigQuery(
    project="p", dataset="d", table="orders",
    schema={
        "id":         "INT64",
        "total":      "NUMERIC(18, 4)",
        "created_at": "TIMESTAMP",
        "tags":       "ARRAY<STRING>",
    },
)

# Or native lib objects, where the library ships them
BigQuery(
    project="p", dataset="d", table="orders",
    schema=[
        bigquery.SchemaField("id", "INT64"),
        bigquery.SchemaField("total", "NUMERIC", precision=18, scale=4),
    ],
)

# Inferred + partial overrides
BigQuery(
    project="p", dataset="d", table="orders",
    schema_overrides={"total": "NUMERIC(18, 4)"},
)
```

Rules:

- `schema=` is **full**: every column listed. Engine enforces strictly.
- `schema_overrides=` is **partial**: only listed columns are pinned; rest inferred.
- `schema=` and `schema_overrides=` mutually exclusive.
- Vocabulary is owned by each destination (BQ types for BigQuery, PG types for Postgres, polars/arrow-shorthand for Parquet, etc). No cross-destination DSL.
- Coercion follows the tier model in §Type mapping. Lossy-semantic conversions still fail by default.

`.run()` returns a `RunReport`:

```python
report = transfer.run()
report.rows            # 12_481_902
report.bytes_written   # 1_503_948_211
report.duration        # timedelta
report.coercions       # list[Coercion] — column, original type, target, level
report.staging         # transient artifacts (deleted unless keep_staging=True)
```

No row-level Python callbacks. The FFI boundary is crossed once per transfer (per-batch for `Iter` source), not per row.

### Architecture

```
+----------+    Vec<BatchStream>    +-------------+
|  Source  |  ===================>  | Destination |
|  reader  |  Arrow RecordBatch     |   writer    |
+----------+                        +-------------+
     ^                                   |
     |                                   v
     +- coerce-to -- Arrow schema --- user schema (destination-native)
                          ^
                          |
                  inferred (when user schema absent)
```

- Single Rust process. `transferred` Python module is a PyO3 extension.
- `Source` trait: `partitions(self) -> Result<Vec<BatchStream>>`. Each `BatchStream` = one partition's async `Stream<Item = RecordBatch>`. Non-partitionable sources return a one-element `Vec`.
- `Destination` trait consumes `Vec<BatchStream>`. Single-file destinations serialize partitions; partition-aware destinations (e.g., partitioned Parquet directory, BQ multi-stream) run them concurrently.
- Async end-to-end: native async I/O via `AsyncArrowWriter` and `ParquetRecordBatchStream`. No `spawn_blocking`, no internal mpsc channels.
- Backpressure happens naturally — `.next().await` on the source stream blocks until the writer is ready.
- Schema resolution happens once, before partitions are produced.

**Schema resolution flow.** Destination owns the user-facing vocabulary; Arrow is internal.

1. If `destination.schema=` is set: destination parses native types → Arrow schema. This is the canonical schema.
2. Else if `destination.schema_overrides=` is set: source produces its inferred Arrow schema, destination overrides parsed → applied per-column.
3. Else: source produces inferred Arrow schema, used as-is.
4. Source emits batches; engine coerces each batch to the canonical schema (Tier 1 auto, Tier 2 warn, Tier 3 fail).
5. Destination receives canonical Arrow batches, maps to destination-native representation on write.

Destination trait surface (Rust):

```rust
trait Destination {
    /// Parse user-provided schema (full) into Arrow.
    fn parse_user_schema(&self, schema: UserSchema) -> Result<ArrowSchema>;
    /// Apply partial overrides to an inferred Arrow schema.
    fn apply_overrides(&self, inferred: ArrowSchema, ovr: UserOverrides) -> Result<ArrowSchema>;
    /// Map Arrow → destination schema for write.
    fn to_destination_schema(&self, arrow: ArrowSchema) -> Result<DestinationSchema>;
    /// Existing write path.
    async fn write(self: Box<Self>, schema: SchemaRef, partitions: Vec<BatchStream>) -> Result<RunReport>;
}
```

`UserSchema` / `UserOverrides` are opaque carriers passed in from Python (string DSL, native lib objects, or mixed). Destination is sole interpreter.

### Memory model

Goal: keep per-worker memory consumption predictable, under a configured cap (default 256 MiB), without surprising OOMs.

Current model (serial, single partition):

- One batch in flight at any time. `source.next().await` yields one `RecordBatch`; `writer.write(&batch).await` consumes it; loop.
- No buffering between source and destination.
- Async readers don't prefetch; async writers buffer one row group internally (configure `WriterProperties::set_max_row_group_size` to keep this bounded — default is large).
- Peak per-pipeline memory ≈ `1 × batch_bytes + writer_row_group_buffer`.

Parallel partitions (deferred to partition feature):

- Per partition: same 1-batch-in-flight + writer buffer.
- Concurrency cap K via `stream::iter(partitions).buffered(K)`.
- Worst-case memory ≈ `K × (batch_bytes + writer_row_group_buffer)`.
- Default K = `min(parallelism_config, available_parallelism())`.
- Tune row group size down when K > 1.

Byte-aware budget (deferred):

- Currently no semaphore. Memory bounded by batch shape × K.
- If real workloads show skew (huge variable-width columns) blowing out the bound, introduce a byte-aware semaphore in `transferred-core` and have partitions acquire permits sized by `RecordBatch::get_array_memory_size()`.

Concurrent transfers in one process (deferred):

- Currently each transfer assumes it owns the worker's memory budget. Multiple `Transfer.run()` calls in one process compound memory.
- For now, run independent transfers in separate processes if isolation matters.

Python-side memory (`Iter` source):

- Generator sources stream one row at a time. Engine collects `batch_size` rows (default 4096) into a Python list, calls `pa.RecordBatch.from_pylist(chunk)`, drops the list, hands the batch to Rust via Arrow C Data Interface. One FFI crossing per batch.
- Peak Python-side memory per batch ≈ `batch_size × avg_row_bytes × 2` (list + Arrow buffers briefly co-resident).
- List/`pa.Table` sources: caller is responsible for what they materialise — engine cannot help if the user pre-builds a 10 GiB list. Documented as "use a generator if your data doesn't fit twice in RAM".
- Bounded queue between Python producer and Rust consumer (`maxsize=2`) provides backpressure on the generator when Rust is slow. Useful under free-threaded Python; near-no-op under GIL.
- `memory_budget_mb=` knob (deferred): translated to row count via running average row size, adjusts batch_size adaptively.
- PyArrow is a hard runtime dependency when `Iter` ships. Building Arrow buffers directly from Rust over `list[dict]` was considered and rejected — pyarrow's C path is faster than what we'd write, and pyarrow is the natural interop currency for `transferred` users.

### Runtime contract

- **Atomic loads.** Each backend uses its own native atomic primitive.
    - BQ: Storage Write API in `pending` mode against a transient staging table in the destination dataset, then a server-side copy job with `WRITE_TRUNCATE` from staging into the final table, then `DROP TABLE staging`. Atomicity comes from the copy job; the Storage Write commit makes the staging table whole, the copy-replace makes the final table whole. Partitioning, clustering, description, labels, IAM on the final table are preserved (data replaced, table object not recreated). Schema enforcement is server-side: AppendRows rejects mismatched rows, the copy job rejects mismatched schemas. No client-side staging in GCS, no Parquet encoding, no `staging_bucket` knob on the public API. Errors surfaced as `ElError` subclasses.
    - Postgres: `BEGIN; DROP target; RENAME staging; COMMIT;`. Client-side schema compare needed here since there's no equivalent server-side enforcement.
  Transfers never leave the destination half-written. `mode="append"` and `mode="upsert"` are out of scope for the pre-1.0 line. `on_schema_change="replace"` to opt into destructive schema replacement is a deferred kwarg.
- **Source filter surface.** `table=` and `query=` are the two ways to bound the extract. No partial filter DSL on top — keeps the API one knob wide. Incremental loads (later) reuse `query=` plus a `WatermarkSpec` (?).
- **Credentials.** All GCP auth delegates to `gcp_auth`: Application Default Credentials, `GOOGLE_APPLICATION_CREDENTIALS` service-account JSON, gcloud user creds, workload identity. Postgres uses standard DSN-embedded creds or libpq env vars.
- **Run report.** `RunReport` returned by `.run()` is the canonical post-run record. Logs are for trace; `RunReport` is for programs.
- **Logging.** Rust uses `tracing`. A bridge layer emits events into Python's `logging` so users get one config story (`logging.getLogger("transferred").setLevel(...)`).
- **Batching.** Reader batch size is its own default (Parquet ≈ 1024 rows). No bytes target enforced; in-flight memory bounded by 1 batch per partition (see Memory model).
- **Concurrency.** Async end-to-end on the tokio multi-thread runtime owned by the Rust side. Partitions within one transfer run concurrently (deferred); separate transfers run in separate processes. PyO3 releases the GIL on every entry. Supported interpreters: Python 3.14 standard and free-threaded (`cp314`, `cp314t`) — see Tech stack.

### Type mapping

User-facing vocabulary is **destination-native**. Arrow is the internal lingua franca, never spelled in the Python API. Each destination owns its own schema DSL parser (string forms like `"NUMERIC(18, 4)"`) and accepts native lib objects (`bigquery.SchemaField`, `pa.Field`) where the destination's ecosystem ships them. Cross-destination consistency is not a goal — `STRING` in BQ and `text` in PG and `Utf8` in Arrow are independent vocabularies.

Arrow covers most primitives directly. The tricky types — geometry, JSON, UUID, ranges, intervals, vendor-specific — go through a registry, not ad-hoc per-connector code.

**Lookup order for any source-native type:**

1. Native Arrow type if one matches (`int4` → `Int32`, `numeric(p,s)` → `Decimal128(p,s)`, `interval` → `Interval(MonthDayNano)`, `timestamptz` → `Timestamp(Microsecond, "UTC")`, …).
2. Canonical Arrow extension if one exists (`uuid` → `arrow.uuid`, `json`/`jsonb` → `arrow.json`).
3. Community extension we trust (PostGIS `geometry`/`geography` → `geoarrow.wkb` with CRS metadata).
4. Private `transferred.*` extension over the most structured storage type that loses nothing (Postgres ranges → `transferred.pg_range` over a `Struct{lower, upper, lower_inc, upper_inc, empty}`).
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

| Tier                 | What it covers                                           | Default               | Reporting               |
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

**Tier 3 workaround.** Lossy-semantic coercions are not implemented; the run fails on the offending column. Workaround = drop the column from the transfer via `columns=` or `skip_columns=` on the source. They are mutually exclusive.

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
| `tsrange`/`int4range`/...     | `Struct` + `transferred.pg_range`    | Private extension. Default destination fallback = expand. |
| `hstore`, `ltree`, composites | `arrow.opaque` initially             | Later promotion to structured forms.   |

### Tech stack

| Concern          | Choice                                              | Reason                                              |
| ---------------- | --------------------------------------------------- | --------------------------------------------------- |
| Core             | Rust                                                | Performance, types, no GC.                          |
| Python binding   | PyO3 + maturin, `cp314` + `cp314t`                  | Standard for adoption; free-threaded for concurrency correctness early. |
| Internal format  | Apache Arrow (`arrow-rs`)                           | Zero-copy to BQ, Parquet, Polars.                   |
| Async runtime    | Tokio                                               | Required by most cloud SDKs.                        |
| Postgres         | `tokio-postgres` + binary `COPY`                    | `COPY` is the fastest extract path.                 |
| BigQuery         | Storage Write API via tonic + googleapis            | Direct write, no Parquet/GCS staging.               |
| GCP auth         | `gcp_auth`                                          | ADC, service-account JSON, gcloud, workload identity. |
| Object storage   | `object_store` crate                                | Unified S3/GCS/Azure API.                           |
| Parquet          | `parquet` (arrow-rs)                                | Same family as Arrow. Audit gaps vs Polars.         |
| Errors           | `thiserror`, surfaced as `transferred.ElError`      | One root exception, typed subclasses.               |
| Logging          | `tracing` bridged into Python `logging`             | One config story for users.                         |
| License          | MIT                                                 | Liberal. Matches the rest of the analytical Python/Rust stack. |

### Known risks

- **BigQuery Rust support is thin.** Mitigation: hand-rolled Storage Write client over tonic + googleapis. Don't depend on an unmaintained crate.
- **Parquet feature gaps.** Mitigation: keep a running list of formats Polars failed on; verify each against `arrow-rs` before claiming coverage. Upstream patches if we have to.
- **Schema drift between infer and override.** Mitigation: resolved schema is logged before the stream starts; destinations validate against it on write.
- **Tricky types silently broken.** Mitigation: type registry + destination capability declarations (see Type mapping). Default to refusal over coercion.
- **Free-threaded ecosystem maturity.** Mitigation: `transferred` itself only depends on PyO3 + arrow-rs across the FFI seam. Users on cp314t are responsible for their own dependency stack.

## Strategy

Versioned roadmap, milestone scope, and what's currently done live in [PLAN.md](./PLAN.md). DESIGN.md covers architecture and contracts only.
