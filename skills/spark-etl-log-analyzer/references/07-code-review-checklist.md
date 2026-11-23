# 07 — Code review & optimization checklist (PySpark, Scala, Spark SQL, Airflow)

Order findings: **correctness → performance → readability**. Severity: 🔴 critical (wrong data /
will fail), 🟠 important (major cost or risk), 🟡 minor (style, small gains).
Every finding: line/snippet, why it matters, rewritten snippet, expected impact.

## Contents
1. Correctness
2. Performance
3. Readability & structure
4. Language notes (Scala, Spark SQL)
5. Airflow DAG review
6. Optimize sub-mode procedure

---

## 1. Correctness

| Anti-pattern | Why | Fix |
|---|---|---|
| `write.mode("append")` in a daily batch | Re-runs/retries duplicate data | `spark.sql.sources.partitionOverwriteMode=dynamic` + `mode("overwrite")` by partition, or MERGE (Iceberg/Delta) |
| Join to a lookup without key uniqueness | Row multiplication | Dedup lookup / assert uniqueness before join |
| Inner join that silently drops facts | Missing dimension rows vanish | Left join + DQ count of unmatched keys |
| `F.current_date()` / `datetime.now()` inside logic | Backfills compute wrong day | Pass the logical date (`{{ ds }}`) as a parameter |
| Implicit casts / string dates | Nulls on bad formats, no pushdown | Explicit schema, `F.to_date(col, fmt)`, `try_to_timestamp` + DQ |
| `dropDuplicates()` without ordering rule | Non-deterministic survivor | `row_number()` over key ordered by `updated_at desc` |
| `inferSchema=true` / schema-on-read for prod | Schema drift breaks silently | Explicit `StructType` |
| Null join keys | Skew + unexpected matches/drops | Filter or handle nulls explicitly |
| Session timezone not set | Day boundaries shift | `spark.sql.session.timeZone=UTC` |

## 2. Performance

| Anti-pattern | Before | After |
|---|---|---|
| Python UDF for simple logic | `udf(lambda x: x.upper())` | `F.upper("x")`; complex → `F.expr`, `F.when`, higher-order funcs (`F.transform`, `F.filter`); last resort → pandas UDF |
| No early projection/filter | `df.join(dim, "k").select(...)` | `df.select(needed).filter(...)` **before** the join |
| Filter on a function of the partition column | `F.year("dt") == 2026` | `F.col("dt").between("2026-01-01","2026-12-31")` (enables partition pruning) |
| `count()` / `show()` for logging between steps | Each triggers a full job | Remove, or compute counts once from the written output / use `df.observe()` (Spark 3.3+) |
| `if df.count() > 0` | Full scan | `if not df.isEmpty()` (3.3+) or `df.limit(1).count()` |
| `collect()` / `toPandas()` of large data | Driver OOM | Keep distributed; write to storage |
| `cache()` used once / never `unpersist()` | Wasted memory, spill | Cache only DFs reused ≥ 2 times, then unpersist |
| Loops with `withColumn` (dozens) | Huge plans, slow analysis | Single `select([...])` or `withColumns({...})` (3.3+) |
| Loop of `union` over many small DFs | Deep plan | Read all paths in one `spark.read.parquet(*paths)` |
| `repartition(1)` / `coalesce(1)` before big write | Single task writes everything | `repartition(n)` or by partition columns; target 128 MB–1 GB files |
| No repartition before partitioned write | Thousands of tiny files per partition | `df.repartition("dt").write.partitionBy("dt")` (or by `dt` + salt for big days) |
| Global `autoBroadcastJoinThreshold` raised | Affects all joins, driver risk | Targeted `F.broadcast(small)` after trimming the small side |
| Skewed join/agg | Long tail tasks | AQE skew join; salt hot keys; split hot keys path |
| `distinct()` on all columns | Large shuffle | Dedup on key columns only |
| `orderBy` before write (global sort) | Full shuffle + range partitioning | `sortWithinPartitions` if only file-level ordering is needed |
| Reading the same source multiple times | Repeated I/O | Read once, reuse (cache if wide reuse) |
| Window without partitionBy | All data to one task | Always `Window.partitionBy(...)` |
| Hard-coded `shuffle.partitions=200` for any volume | Too many/too few tasks | Enable AQE coalesce; size by data (≈ shuffle bytes / 128 MB) |

## 3. Readability & structure

- Structure: `config` → `read_*()` → `validate_*()` → `transform_*()` (pure, DataFrame in/out) →
  `write_*()` → `main(args)`. Each transform testable in isolation.
- Use `df.transform(fn)` chains for readable pipelines:
  `df.transform(clean).transform(enrich(dim)).transform(aggregate)`.
- Column names in constants; no magic strings repeated; aliases explicit after joins
  (`df.alias("f").join(d.alias("d"), ...)`) to avoid ambiguous columns.
- One `select` with all derived columns > many `withColumn`.
- Parameters (date, env, paths) from CLI args / job config, not hard-coded.
- Logging with `logging` (counts from DQ step, paths, run date), not `print`.
- Type hints and docstrings on public functions; delete dead code and commented blocks.
- Unit tests with small DataFrames (pytest + local SparkSession / ScalaTest).

## 4. Language notes

**Scala Spark**: prefer built-in functions to Scala UDFs (UDFs are opaque to Catalyst even if JVM);
`Dataset[T]` with case classes for type safety in core models, but `map` on Datasets
deserializes objects (slower than column expressions); avoid `collect`; use `Option` for nullable
fields; package with `sbt-assembly`, mark Spark deps `% "provided"`.

**Spark SQL**: same rules — filter partitions with literal ranges, avoid `SELECT *`, use
`/*+ BROADCAST(d) */` hints, CTEs for readability, `INSERT OVERWRITE ... PARTITION` for idempotency,
`QUALIFY`-style dedup via `ROW_NUMBER()` in a subquery.

## 5. Airflow DAG review

- `retries` ≥ 1 with `retry_delay`, `execution_timeout` set, SLA / `on_failure_callback` alert.
- Idempotent tasks parameterized by `{{ ds }}` / `data_interval_start`; `catchup` intentional.
- No heavy processing in the worker (`PythonOperator` doing pandas on GBs).
- Sensors in `mode="reschedule"` or deferrable operators; sensible `poke_interval`.
- Use provider operators (EMR/Glue/Serverless) and wait for completion, so failures surface.
- Pass the resource ID via XCom / log it so the diagnosis chain works.
- Bronze → Silver → Gold dependencies explicit; DQ task gates Gold publishing.

## 6. Optimize sub-mode procedure

1. Restate what the ETL does (inputs, outputs, grain, business rules) — confirm if unsure.
2. List findings with this checklist (and with UI/plan evidence if provided).
3. Rewrite the full ETL preserving logic; keep public interfaces (paths, table names, columns)
   unless the user agrees to change them.
4. Equivalence notes: for each change, why the output is identical (or where it intentionally
   differs, e.g. fixing duplicates).
5. Change log with expected impact (less shuffle, fewer jobs, pruning, fewer files).
6. Validation plan: run on one partition in non-prod; compare row counts, key uniqueness, sums of
   key measures, and `exceptAll` diff between old and new outputs; compare runtime and shuffle in UI.
