# 05 — Recurring failure catalog

Match the **literal signature** you find in the logs. Each entry: *signature → meaning → likely
causes → fix → prevention*. If nothing matches, say so and ask for the full driver stderr.

## Contents
- A. Airflow (summary — details in 01)
- B. Cluster / YARN / engine
- C. Spark driver
- D. Spark executors, shuffle, memory
- E. Data / schema / Bronze (S3)
- F. Silver (transform correctness)
- G. Gold / Redshift
- H. Slow but not failing

---

## A. Airflow
| Signature | Meaning | Fix |
|---|---|---|
| `AirflowTaskTimeout` | `execution_timeout` hit | Check if job slower (engine) or timeout too tight |
| `AirflowSensorTimeout` | Data never arrived | Upstream DAG / S3 partition; `mode="reschedule"` |
| `Detected zombie job` | Worker lost heartbeat | Worker memory / restarts; avoid heavy work in worker |
| `EMR step failed` / `Job run failed` / `Glue job run failed` | Messenger only | Follow the resource ID to engine logs |

## B. Cluster / YARN / engine
| Signature | Meaning | Likely causes | Fix / prevention |
|---|---|---|---|
| `Container killed by YARN for exceeding physical memory limits` / `exceeding memory limits. X GB of Y GB physical memory used` | Off-heap usage exceeded `memoryOverhead` | Python workers (UDFs, pandas), Arrow, large shuffle buffers | Raise `spark.executor.memoryOverhead` (e.g. 10–20%), reduce cores/executor, remove Python UDFs |
| `ExecutorLostFailure (executor N exited caused by one of the running tasks) Reason: Container from a bad node` / `Container marked as failed ... Exit status: 137` | Executor killed (137 = SIGKILL, OOM) | Skewed partition, too-big partitions | More partitions, AQE skew, fix skew; see D |
| `Exit status: 143` | SIGTERM — often **Spot interruption** or decommission | Spot task nodes reclaimed | Use On-Demand for core, Spot only for task nodes; enable graceful decommission; idempotent retries |
| `Cluster terminated: ... BOOTSTRAP_FAILURE` / `VALIDATION_ERROR` | Bootstrap script or config failed | Bad bootstrap action, package install, wrong subnet/role | Node logs `node/<id>/bootstrap-actions/` |
| `Insufficient capacity` / `InsufficientInstanceCapacity` | No EC2 capacity for that type in that AZ | Instance type scarcity | Instance fleets with multiple types/AZs |
| `Application ... maximum capacity reached` (Serverless) | App max vCPU/memory cap | Too small max capacity | Raise application max capacity |
| `OOMKilled` (EKS) | Pod memory limit | Same as 137 | Raise memory/overhead, fix skew |
| `Accepted: waiting for AM container to be allocated` (long) | YARN queue full | Other jobs, too small cluster | Managed scaling, queues |

## C. Spark driver
| Signature | Meaning | Fix |
|---|---|---|
| `java.lang.OutOfMemoryError: Java heap space` **in driver log** | Driver out of heap | Remove `collect()`, `toPandas()`, large `broadcast`; raise `spark.driver.memory` |
| `Total size of serialized results of N tasks (X MB) is bigger than spark.driver.maxResultSize` | Too much data collected | Do not collect; write to storage; raise maxResultSize only if justified |
| `Could not execute broadcast in 300 secs` / `Futures timed out after [300 seconds]` | Broadcast too large/slow | Remove/limit broadcast, filter the small side, raise `spark.sql.broadcastTimeout` as mitigation |
| `Cannot broadcast the table that is larger than 8GB` | Hard broadcast limit | Do not broadcast; use SMJ |
| `org.apache.spark.sql.AnalysisException: ...cannot resolve 'col'` / `[UNRESOLVED_COLUMN]` | Column missing / renamed upstream | Schema change in source; explicit schema, contract |
| `Py4JJavaError` | Wrapper — the real Java exception is below | Read the `Caused by:` lines |
| `GC overhead limit exceeded` (driver) | Driver memory thrash | Too many tasks/partitions metadata, big plans; reduce files/partitions |

## D. Executors, shuffle, memory
| Signature | Meaning | Fix |
|---|---|---|
| `java.lang.OutOfMemoryError: Java heap space` in **executor** | Partition too big / skew | Increase partitions, AQE skew join, salting, less cores per executor |
| `org.apache.spark.shuffle.FetchFailedException` / `MetadataFetchFailedException: Missing an output location for shuffle` | Executor holding shuffle files died | Root cause is usually executor OOM/Spot loss; fix that; external shuffle service |
| `Stage X contains a task of very large size` | Big closure/variable captured | Broadcast variables, avoid capturing large objects |
| `Task not serializable` | Non-serializable object in closure | Create the object inside the function / `mapPartitions` |
| `SparkException: Job aborted due to stage failure: Task N in stage X failed 4 times` | Generic: read the *most recent failure* reason after it | — |
| `PythonException ... ` / `Python worker exited unexpectedly` | Error or crash inside Python UDF | Read the Python traceback; replace UDF with native functions |
| Large **Spill (Disk)** in Stage metrics (no error) | Not enough execution memory | More partitions, fix skew, raise memory |

## E. Data / schema / Bronze (S3)
| Signature | Meaning | Fix |
|---|---|---|
| `Path does not exist: s3://...` / `[PATH_NOT_FOUND]` | Input partition missing | Upstream late/failed; add sensor; handle empty days explicitly |
| `Unable to infer schema for Parquet. It must be specified manually` | Empty or no Parquet files at path | Same as above; explicit schema |
| `Parquet column cannot be converted ... Expected: bigint, Found: INT32` / `SchemaColumnConvertNotSupportedException` | Type changed between files | Fix writer; read with explicit schema; cast; Iceberg/Delta schema evolution |
| `AmazonS3Exception: Slow Down (Status Code: 503)` | S3 request throttling | Fewer, larger files; spread prefixes; EMRFS retry config |
| `AmazonS3Exception: Access Denied (403)` | IAM / bucket policy / KMS | Role permissions incl. `kms:Decrypt` |
| `FileNotFoundException ... It is possible the underlying files have been updated` | Files changed while reading | Do not overwrite input while reading; `REFRESH TABLE` |
| `ConcurrentAppendException` / `CommitFailedException` (Delta/Iceberg) | Concurrent writers | Serialize writers, retry, partition-scoped MERGE |
| Many tiny files (UI: thousands of files read) | Small-files problem | Compaction, `coalesce/repartition` before write, Iceberg `rewrite_data_files` / Delta `OPTIMIZE` |

## F. Silver (transform correctness — runs "succeed" but data is wrong)
| Symptom | Likely cause | Check |
|---|---|---|
| Row count after join ≫ before | Duplicate keys on lookup side | `groupBy(key).count().filter("count>1")` |
| Rows disappear after join | Inner join where left was needed; null keys; type mismatch | Anti-join to list unmatched keys |
| Duplicates in output after re-run | Non-idempotent write (`append`) | Dynamic partition overwrite / MERGE |
| Wrong day's data | Timezone (`spark.sql.session.timeZone`), late data | Filter on event date vs processing date |
| Nulls in a not-null column | Upstream schema change, failed cast returns null silently | DQ check; `try_cast` audit |

## G. Gold / Redshift
| Signature | Meaning | Fix |
|---|---|---|
| `Load into table 'x' failed. Check 'stl_load_errors'` / `sys_load_error_detail` | COPY row errors | Read `err_reason`, `colname`, `raw_line` |
| `String length exceeds DDL length` | Value longer than column | Widen column or truncate upstream |
| `Invalid timestamp format or value` / `Invalid digit` | Format mismatch | `TIMEFORMAT`, `DATEFORMAT`, clean upstream |
| `Serializable isolation violation on table` (1023) | Concurrent writes to same table | Serialize loads, use staging + swap |
| `Disk Full` / query disk-based steps | Redistribution/spill | DISTKEY/SORTKEY on join keys, VACUUM/ANALYZE |
| Query waits in WLM queue | Concurrency limits | WLM config, concurrency scaling |
| `permission denied for relation` | Grants | GRANT to ETL role |

## H. Slow but not failing (classify, then go to 03/04/07)
| Pattern in UI | Cause | First fix |
|---|---|---|
| Join changed BHJ → SMJ | Small side grew | 04 §5–6 |
| Max task ≫ median on a join/agg stage | Skew | AQE skew, salting, isolate hot/null keys |
| Scan reads all partitions | Missing partition filter / non-pushable filter (UDF, cast on partition column) | Filter on the partition column directly |
| Many jobs for one ETL | Repeated actions (`count`, `show`), no cache of reused DF | Remove debug actions; cache only if reused |
| Huge shuffle on `distinct`/`dropDuplicates`/window | Wide op on all columns | Dedup by key subset, earlier projection |
| Long write stage, thousands of files | Small files / partition explosion | `repartition(partition_cols)` before write |
| Cluster idle before Spark starts | Provisioning / queue | Pre-warm, Serverless initial capacity, pools |
