# 09 — Build a new ETL (Mode C)

## 1. Intake — collect before writing code
Ask only for what is missing (use `templates/data-request.md`). **Blocking** items (do not write
code without them): target layer, grain, business keys, sources, approximate volumes.

| Item | Why |
|---|---|
| Purpose / consumer | Shapes grain and columns |
| Sources (paths/tables, format, partitioning, owner, arrival time) | Read strategy, sensors |
| Target layer (bronze/silver/gold) and table/path | Write pattern |
| **Grain** ("one row per household per day") | Dedup and aggregation logic |
| **Business keys** + uniqueness expectations | Joins, MERGE, DQ |
| Business rules / mappings / filters | Transform logic |
| Volumes (rows/GB per day, largest table, smallest lookup) | Join strategy, partitions, cluster size |
| Schedule, SLA, late data window | DAG, lookback reprocessing |
| Engine & language (EMR/Serverless/Glue; PySpark/Scala/SQL), Spark version, table format | Code flavor |
| Sensitive fields | Hashing, column exclusion |

If engine/language are unknown, default to **PySpark on EMR Serverless, Parquet/Iceberg on S3,
Airflow provider operators** and state it as an assumption.

## 2. Design steps (write them as a short design block before code)
1. Inputs → outputs table (paths, schema, partitions).
2. Grain and keys; dedup rule.
3. Join plan: for each join — sizes, expected strategy (broadcast if trimmed side small, else SMJ),
   key uniqueness check, skew risk (null/hot keys).
4. Write strategy: idempotent (dynamic overwrite by `dt` / MERGE), file sizing, partition columns.
5. DQ checks and thresholds (fail vs warn).
6. Orchestration: DAG tasks, sensors, retries, timeout, alerts, backfill approach.
7. Monitoring: counts and runtime logged; resource ID surfaced.

## 3. Deliverables
1. ETL code from `templates/etl-pyspark-skeleton.py` (or Scala skeleton / Spark SQL), filled in.
2. Airflow DAG from `templates/airflow-dag-skeleton.py`.
3. DQ checks inside the job (gate before publish).
4. README: purpose, inputs/outputs, parameters, how to run one date, how to backfill, DQ rules,
   expected runtime/volume, where to find logs (link to the Airflow → engine path).
5. Unit test examples for the main transform functions.

## 4. Optimization defaults baked in
- Explicit schemas; project + filter at read; partition filters on literal ranges.
- Native functions only; no Python UDFs unless justified.
- Broadcast hint only on a trimmed, uniqueness-checked lookup.
- AQE on; `partitionOverwriteMode=dynamic`; repartition by partition columns before write.
- No `count()`/`show()` debug actions; DQ counts computed once.
- Session timezone UTC; logical date passed in, never `now()`.
