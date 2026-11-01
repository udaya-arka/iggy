# Guide: Parquet Output Format for Blob Sinks (S3, GCS, HDFS)

This document provides a comprehensive walkthrough of the Parquet output format implementation for Apache Iggy blob sinks. It explains the architectural design, step-by-step code modifications, system impact, and detailed instructions for manual and automated verification.

---

## 1. Architectural Overview & Design Rationale

### Why Parquet?
Blob sinks (such as Amazon S3, Google Cloud Storage, or HDFS) previously supported `json_lines`, `json_array`, and `raw` formats. While JSON is human-readable, it is inefficient for analytical queries (OLAP) over large data lakes. 

**Apache Parquet** provides:
- **Columnar storage**: Query engines (DuckDB, Athena, Spark, Trino, Snowflake) only read columns requested in the query.
- **High compression ratios**: Utilizes dictionary encoding, bit packing, and ZSTD block compression.
- **Embedded schema and statistics**: Enables min/max pushdown filtering without reading whole files.

### Why a Shared Formatter in `iggy_connector_sdk`?
Rather than hardcoding Parquet encoding inside `s3_sink`, the formatter was implemented as a shared module in `iggy_connector_sdk::formatters::parquet`:
1. **Reusability across Blob Sinks**: Future sinks (such as `gcs_sink` or `hdfs_sink`) buffer messages and finalize files in the exact same pattern. They can reuse `json_entries_to_parquet` directly without duplicating Arrow/Parquet dependencies.
2. **Zero Bloat for Lightweight Connectors**: Heavy dependencies (`parquet`, `arrow`, `arrow-json`) are placed behind an optional feature flag (`features = ["parquet"]`) in `iggy_connector_sdk`. Sinks like `stdout_sink` or `postgres_sink` do not pull in Arrow or Parquet dependencies or incur build-time overhead.
3. **Dynamic Schema Inference**: In streaming environments, messages may arrive with varied JSON schemas. The shared formatter dynamically infers the Arrow schema across buffered records and reconciles sparse or nested fields across the batch before writing the Parquet file.

---

## 2. Step-by-Step Code Modifications

### A. Connector SDK (`core/connectors/sdk`)

#### 1. `Cargo.toml`
- Added optional feature flag `parquet = ["dep:parquet", "dep:arrow", "dep:arrow-json"]`.
- Added optional dependencies using workspace versions:
  ```toml
  arrow = { workspace = true, optional = true }
  arrow-json = { workspace = true, optional = true }
  parquet = { workspace = true, optional = true }
  ```
- Added `bytes = { workspace = true }` under `[dev-dependencies]` for test verification.

#### 2. `src/formatters/mod.rs` & `src/formatters/parquet.rs`
- Created `core/connectors/sdk/src/formatters/parquet.rs` providing two functions:
  - `ndjson_to_parquet<R: BufRead + Seek>(mut reader, schema, props) -> Result<Vec<u8>, Error>`:
    1. If `schema` is not explicitly provided, calls `arrow_json::reader::infer_json_schema(&mut reader, None)`.
    2. Rewinds the reader to position 0 (`reader.seek(SeekFrom::Start(0))`).
    3. Builds an Arrow record batch stream via `arrow_json::ReaderBuilder`.
    4. Initializes `parquet::arrow::ArrowWriter` with ZSTD compression (or custom properties).
    5. Iteratively writes all Arrow `RecordBatch`es and closes the writer, returning the complete Parquet file bytes.
  - `json_entries_to_parquet(entries, schema, props) -> Result<Vec<u8>, Error>`:
    Concatenates an iterator of byte slices into newline-delimited JSON and delegates to `ndjson_to_parquet`.

#### 3. `src/lib.rs`
- Exported the module:
  ```rust
  pub mod formatters;
  ```

---

### B. S3 Sink Connector (`core/connectors/sinks/s3_sink`)

#### 1. `Cargo.toml`
- Enabled the `parquet` feature on the SDK dependency:
  ```toml
  iggy_connector_sdk = { workspace = true, features = ["parquet"] }
  ```
- Added `bytes` and `parquet` to `[dev-dependencies]`.

#### 2. `src/lib.rs`
- Added the `Parquet` variant to `OutputFormat`:
  ```rust
  #[derive(Debug, Clone, Copy, PartialEq, Eq)]
  pub(crate) enum OutputFormat {
      JsonLines,
      JsonArray,
      Raw,
      Parquet,
  }
  ```
- Added `"parquet"` string parser in `TryFrom<&str>` (case-insensitive).
- Mapped file extension to `"parquet"` in `OutputFormat::file_extension()`.

#### 3. `src/formatter.rs`
- In `format_message`:
  Messages formatted for Parquet share the structured JSON layout (`format_json_message`) with metadata and headers as configured.
- In `finalize_buffer`:
  Changed return signature to `Result<Vec<u8>, Error>` and added the Parquet arm:
  ```rust
  OutputFormat::Parquet => iggy_connector_sdk::formatters::parquet::json_entries_to_parquet(
      buffer.entries(),
      None,
      None,
  ),
  ```

#### 4. `src/sink.rs`
- In `extract_flush_payload`:
  Propagated errors cleanly when calling `formatter::finalize_buffer(buffer, resolved.output_format)?`.

---

### C. Integration Testing (`core/integration`)

#### 1. `fixtures/s3/fixture.rs` & `fixtures/s3/mod.rs`
- Defined `ENV_SINK_PLUGIN_OUTPUT_FORMAT = "IGGY_CONNECTORS_SINK_S3_PLUGIN_CONFIG_OUTPUT_FORMAT"`.
- Implemented `S3SinkParquetFixture` which overrides the sink's output format to `"parquet"`.

#### 2. `tests/connectors/s3/s3_sink.rs`
- Added `s3_sink_writes_parquet_with_correct_layout`:
  1. Spins up testcontainers with S3/MinIO and Iggy server harness.
  2. Sends messages through Iggy stream/topic.
  3. Waits for the S3 object upload.
  4. Asserts that the key ends with `.parquet`.
  5. Decodes the Parquet payload using `parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder` and validates row count and schema integrity.

---

## 3. Impact of These Changes

| Aspect | Before | After |
| ------ | ------ | ----- |
| **Output Formats** | `json_lines`, `json_array`, `raw` | `json_lines`, `json_array`, `raw`, `parquet` |
| **File Extension** | `.jsonl`, `.json`, `.bin` | `.jsonl`, `.json`, `.bin`, `.parquet` |
| **Storage Consumption** | Uncompressed text/JSON | Columnar compressed with ZSTD (typically 60-85% size reduction for event logs) |
| **Analytical Query Speed** | Requires full scan & JSON parsing | Column pruning + vectorised scans in DuckDB / Athena / Spark |
| **Backward Compatibility** | Default is `json_lines` | **100% backward compatible**; default remains `json_lines` |
| **Extensibility** | S3-only formatting logic | Shared `iggy_connector_sdk::formatters::parquet` ready for GCS, HDFS, Azure Blob |

---

## 4. How to Manually Test This Yourself

### Step 1: Run Unit Tests

Verify SDK Parquet encoding and S3 sink buffer finalization:

```bash
# Test the shared Parquet formatter in SDK
cargo test -p iggy_connector_sdk --features parquet

# Test the S3 sink with Parquet output
cargo test -p iggy_connector_s3_sink
```

### Step 2: Run Automated Integration Test (Requires Docker)

The integration test runs a real S3 container (via `testcontainers`) with MinIO:

```bash
cargo test -p integration --test mod -- connectors::s3::s3_sink_writes_parquet_with_correct_layout --nocapture
```

---

### Step 3: End-to-End Manual Testing with Local MinIO and DuckDB

You can manually inspect the generated Parquet files using a local MinIO instance and DuckDB.

#### 1. Start MinIO via Docker
```bash
docker run -d --name local-minio \
  -p 9000:9000 -p 9001:9001 \
  -e "MINIO_ROOT_USER=minioadmin" \
  -e "MINIO_ROOT_PASSWORD=minioadmin" \
  minio/minio server /data --console-address ":9001"
```

Create a bucket named `my-data-lake`:
- Open `http://localhost:9001` in your browser.
- Login with `minioadmin` / `minioadmin`.
- Create a bucket: `my-data-lake`.

#### 2. Start Apache Iggy Server
In one terminal:
```bash
cargo run --bin iggy-server
```

#### 3. Build the S3 Sink Plugin
```bash
cargo build --release -p iggy_connector_s3_sink
```

#### 4. Create Connector Configuration (`s3_parquet_test.toml`)
Create a file named `s3_parquet_test.toml`:

```toml
type = "sink"
key = "s3"
enabled = true
version = 0
name = "S3 sink"
path = "target/release/libiggy_connector_s3_sink"
verbose = true

[[streams]]
stream = "demo_stream"
topics = ["orders"]
schema = "json"
batch_length = 5
consumer_group = "s3_parquet_consumer"

[plugin_config]
bucket = "my-data-lake"
prefix = "lake/events"
region = "us-east-1"
endpoint = "http://localhost:9000"
access_key_id = "minioadmin"
secret_access_key = "minioadmin"
path_template = "{stream}/{topic}/{date}"
file_rotation = "messages"
max_messages_per_file = 5
output_format = "parquet"
include_metadata = true
include_headers = true
path_style = true
```

#### 5. Start the Connectors Runtime
In a second terminal:
```bash
cargo run --bin iggy-connectors -- --config s3_parquet_test.toml
```

#### 6. Produce Test Messages to Iggy
Using the `iggy` CLI or your preferred SDK:
```bash
cargo run --bin iggy -- stream create demo_stream
cargo run --bin iggy -- topic create demo_stream orders

# Send 5 JSON messages
cargo run --bin iggy -- message send demo_stream orders '{"order_id": 101, "customer": "Alice", "amount": 49.99}'
cargo run --bin iggy -- message send demo_stream orders '{"order_id": 102, "customer": "Bob", "amount": 89.50}'
cargo run --bin iggy -- message send demo_stream orders '{"order_id": 103, "customer": "Charlie", "amount": 15.00}'
cargo run --bin iggy -- message send demo_stream orders '{"order_id": 104, "customer": "Diana", "amount": 120.00}'
cargo run --bin iggy -- message send demo_stream orders '{"order_id": 105, "customer": "Evan", "amount": 7.25}'
```

#### 7. Verify the Upload in MinIO
Check the MinIO console (`http://localhost:9001/buckets/my-data-lake/browse`).
You should see a file uploaded under:
`lake/events/demo_stream/orders/<DATE>/00000-00000000000000000000-00000000000000000004.parquet`

#### 8. Query with DuckDB
Install DuckDB (`brew install duckdb` or download standalone binary), then run:

```bash
duckdb
```

Inside the DuckDB shell:
```sql
INSTALL httpfs;
LOAD httpfs;

SET s3_endpoint = 'localhost:9000';
SET s3_access_key_id = 'minioadmin';
SET s3_secret_access_key = 'minioadmin';
SET s3_use_ssl = false;
SET s3_url_style = 'path';

-- Query schema and records directly from MinIO:
SELECT 
    offset, 
    stream, 
    topic, 
    timestamp, 
    payload.order_id, 
    payload.customer, 
    payload.amount 
FROM read_parquet('s3://my-data-lake/lake/events/demo_stream/orders/*/*.parquet');
```

Output:
```text
┌────────┬─────────────┬────────┬──────────────────────┬──────────┬──────────┬────────┐
│ offset │   stream    │ topic  │      timestamp       │ order_id │ customer │ amount │
│ int64  │   varchar   │ varchar│       varchar        │  int64   │ varchar  │ double │
├────────┼─────────────┼────────┼──────────────────────┼──────────┼──────────┼────────┤
│      0 │ demo_stream │ orders │ 2026-10-30T10:00:00Z │      101 │ Alice    │  49.99 │
│      1 │ demo_stream │ orders │ 2026-10-30T10:00:01Z │      102 │ Bob      │  89.50 │
│      2 │ demo_stream │ orders │ 2026-10-30T10:00:02Z │      103 │ Charlie  │  15.00 │
│      3 │ demo_stream │ orders │ 2026-10-30T10:00:03Z │      104 │ Diana    │ 120.00 │
│      4 │ demo_stream │ orders │ 2026-10-30T10:00:04Z │      105 │ Evan     │   7.25 │
└────────┴─────────────┴────────┴──────────────────────┴──────────┴──────────┴────────┘
```
