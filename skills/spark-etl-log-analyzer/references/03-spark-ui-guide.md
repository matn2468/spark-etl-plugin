# 03 — Reading the Spark UI

Mental model: **application → jobs (one per action: write, count, collect) → stages (split at
every shuffle) → tasks (one per partition)**. Time goes where tasks are slow or numerous; cost
goes where data is shuffled, spilled or re-read.

## Contents
1. Tab-by-tab walkthrough (what to look at, in order)
2. Health thresholds (what "bad" looks like)
3. How much data? How long? Where did the time go?
4. What to ask the user to capture

---

## 1. Tab-by-tab walkthrough

### Jobs
- Total duration of each job and which **action** triggered it (description shows the code line,
  e.g. `parquet at etl_silver.py:142`, `count at ...`).
- Many short jobs from `count()`, `show()`, `isEmpty()` calls = hidden extra passes over data.
- Failed job → click it → failed stage → failure reason.

### Stages  ← most diagnosis happens here
Sort by **Duration**. For the slowest stage open *Details* and read **Summary Metrics for
Completed Tasks** (Min / 25th / Median / 75th / Max):

| Column | Read it as |
|---|---|
| Duration | Max ≫ Median ⇒ **skew** (a few tasks do most of the work) |
| GC Time | > 10–20% of task time ⇒ memory pressure |
| Input Size / Records | Data read from source (S3/Parquet). Compare to expectations |
| Shuffle Read / Write | Data moved across the network. Large = wide transformation cost |
| Spill (Memory / Disk) | Partitions did not fit in execution memory ⇒ slower, disk I/O |
| Scheduler Delay / Task Deserialization | High ⇒ too many tiny tasks or huge closures/broadcasts |

Also: **number of tasks** (= partitions). 200 tasks for 5 GB ≈ 25 MB each (fine); 200 tasks for
2 TB ≈ 10 GB each (too big → spill/OOM); 50 000 tasks for 1 GB (too small → overhead).
*Failed tasks* list shows per-task errors (FetchFailed, ExecutorLostFailure, OOM).

### SQL / DataFrame  ← the plan, the joins and the row counts
- One entry per query/action. Open the slow one → **graph** with a box per operator, each showing
  *number of output rows*, *data size*, *time*.
- Follow rows top-down: a node whose output rows explode (e.g. join produces 10× its input) points
  to **duplicate join keys** or a missing join condition.
- Find **Exchange** nodes (= shuffle) and **join nodes** (see 04-join-strategies.md).
- `Scan parquet` node: *number of files read*, *size of files read*, *partitions read*,
  `PushedFilters`, `PartitionFilters`. Empty PartitionFilters on a date-partitioned table ⇒ full
  scan.
- With AQE you see `AdaptiveSparkPlan isFinalPlan=true` — always read the **final plan**.
- Bottom "Details" = full physical plan text (copyable).

### Executors
- Per executor: tasks, failed tasks, GC time, shuffle read/write, storage memory used.
- Many **dead/removed executors** ⇒ Spot interruptions, YARN kills (memory overhead), node loss.
- A single executor with most of the task time ⇒ skew or data locality.
- **Driver** row: high GC / memory ⇒ `collect()`, `toPandas()`, big broadcast, too many tasks.

### Storage
- Cached DataFrames, fraction cached, size. Cached but used once = wasted memory. "Fraction
  cached < 100%" = recomputed partially.

### Environment
- Effective configs — check `spark.sql.shuffle.partitions`, `spark.sql.adaptive.*`,
  `spark.sql.autoBroadcastJoinThreshold`, `spark.executor.memory/cores/memoryOverhead`,
  `spark.dynamicAllocation.*`, Spark version. Essential when comparing runs.

## 2. Health thresholds (rules of thumb, not laws)

| Signal | Healthy | Investigate |
|---|---|---|
| Task Max / Median duration | < 2× | > 3–5× (skew) |
| Shuffle partition size | 100–200 MB | < 10 MB (too many) or > 1 GB (too few) |
| Spill (disk) | 0 | any significant amount on the slow stage |
| GC time / task time | < 10% | > 20% |
| Output file size | 128 MB – 1 GB | thousands of files < 10 MB (small files) |
| Join output rows / larger input rows | ≈ 1 for lookups | ≫ 1 (row explosion) |
| Failed tasks / retried stages | 0 | any recurring (FetchFailed, executor lost) |

## 3. How much data? How long? Where did the time go?

- **Data volume**: SQL tab `Scan` nodes (rows + size read per table), Stages *Input*, final
  write node (*number of written files*, *rows*, *bytes*). For S3 the human can run
  `aws s3 ls s3://bucket/prefix/dt=2026-11-10/ --recursive --summarize`.
- **Duration**: application duration (History Server list) vs Airflow task duration — the gap is
  cluster startup/queueing (EMR provisioning, Serverless cold start, YARN queue).
- **Where the time went**: sum of the top 3 stages usually explains > 70% of runtime. For each,
  classify: *scan-bound* (input large, little shuffle → pruning/pushdown), *shuffle-bound* (large
  shuffle → join/aggregation strategy), *skew-bound* (max ≫ median), *spill/GC-bound*
  (memory), *write-bound* (many small files, S3 commit).

## 4. What to ask the user to capture

Ask for, in order of value:
1. SQL tab → the slow query → **copy the physical plan text** ("Details" at the bottom), or
   `df.explain("formatted")` output.
2. Stages tab → slowest stage → screenshot or copy of **Summary Metrics** table + number of tasks.
3. Executors tab screenshot (dead executors, GC).
4. Environment tab values for the configs listed above.
For a run comparison, the same four items from the good run.
