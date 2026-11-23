# 08 — Medallion patterns on AWS (Bronze/Silver on S3, Gold in warehouse)

## Bronze (raw, S3)
- Land data **as received** (append-only), plus ingestion metadata: `_ingest_ts`, `_source_file`,
  `_batch_id`. Never transform business logic here.
- Partition by ingestion/event date (`dt=YYYY-MM-DD`, optionally `hr=`). Low-cardinality columns only.
- Format: Parquet (or Iceberg/Delta tables); raw JSON/CSV kept only if replay is needed.
- Register in Glue Data Catalog; partitions added by the job (or `MSCK REPAIR`/partition projection).

## Silver (clean, conformed, S3)
- Explicit schema, typed columns, standardized names (snake_case), UTC timestamps.
- **Deduplicate** by business key with deterministic rule (latest `updated_at`).
- Handle late/out-of-order data: reprocess a lookback window (e.g. last N days) idempotently.
- Conform reference data (mappings, dimensions) with uniqueness guaranteed.
- Privacy: hash/tokenize identifiers (salted SHA-256), drop PII not needed, KMS encryption.
- Prefer Iceberg/Delta for MERGE/UPSERT, deletes (GDPR/CCPA), time travel, schema evolution.

## Gold (business, warehouse e.g. Redshift)
- Aggregates/marts at the grain consumers need; stable column contracts.
- Load pattern: Spark writes Parquet to S3 staging → `COPY` into a staging table → transactional
  `DELETE+INSERT`/`MERGE` into target (or partition swap). Never partial loads into the target.
- Redshift design: DISTKEY on the most frequent large join key, SORTKEY on date filter columns,
  `ANALYZE` after loads; `VACUUM` when needed.
- Publish only after DQ passes (write–audit–publish).

## Cross-cutting
| Topic | Rule |
|---|---|
| Idempotency | Same input + same logical date ⇒ same output. Dynamic partition overwrite or MERGE. No `append` for reprocessable data |
| Backfills | Parameterize by date; run per partition; avoid `current_date()` |
| File sizing | 128 MB–1 GB per file; compaction job for small files |
| Partitioning | Filter-friendly, low cardinality (date); never user_id |
| Data quality | Schema check, row counts vs source, null/unique keys, accepted ranges, freshness, reconciliation of key sums Bronze→Silver→Gold. Tools: Deequ/PyDeequ, Great Expectations, custom asserts |
| Observability | Log input/output counts, runtime, resource ID; alert on runtime SLA and row-count drift |
| Governance | Glue Catalog + Lake Formation permissions; tags for PII columns; lineage |
| Cost | Read less (pruning/pushdown), Spot for task nodes, auto-termination, Serverless for spiky loads |
