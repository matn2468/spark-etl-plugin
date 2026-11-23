# 06 — Comparing a run with a past successful run

Goal: find **what changed** — code, data, config, or environment — between a good run and a bad
run of the same ETL. Change one hypothesis at a time and back each with evidence.

## 1. Pick the right baseline
- Same DAG/task, same weekday if data is seasonal, the **most recent successful** run before the
  regression, ideally similar input volume.
- Note the Spark application IDs of both runs.

## 2. Collect the same artifacts for both runs
1. Airflow: duration, try count, start/end times (queueing vs execution).
2. Spark UI: application duration; Jobs list; top 3 stages (duration, tasks, input, shuffle,
   spill, max/median task time); SQL-tab final plan of the main query; Environment configs.
3. Data: input size/rows per source (SQL tab scan nodes or `aws s3 ls --summarize`), output
   rows/files.
4. Code/config: git commit or artifact version deployed, cluster config (instance types, count,
   Spot vs On-Demand, Spark/EMR version).
Faster for large comparisons: Spark REST API `/api/v1/applications/<id>/stages` for both
applications, diff the JSON.

## 3. Comparison table (fill it in the report)

| Metric | Good run | Bad run | Δ | Comment |
|---|---|---|---|---|
| Airflow task duration | | | | |
| Spark app duration | | | | gap = startup/queue |
| Input rows / bytes (per source) | | | | data growth? |
| Output rows / files | | | | |
| # jobs / # stages | | | | new actions? |
| Slowest stage: duration | | | | |
| Slowest stage: # tasks | | | | partitioning change? |
| Slowest stage: shuffle read/write | | | | |
| Slowest stage: spill (disk) | | | | |
| Slowest stage: max / median task | | | | skew? |
| Join strategy of main join(s) | | | | BHJ→SMJ? |
| Dead executors / failed tasks | | | | Spot / OOM? |
| Key configs (shuffle.partitions, AQE, broadcast threshold, executor mem/cores) | | | | |
| Code version / EMR / Spark version | | | | deploy? |

## 4. Interpret the deltas
- **Input grew ~proportionally to runtime** → scaling issue: partitions, cluster size.
- **Input same, runtime up, plan changed** → strategy change (join, pruning lost) → 04.
- **Input same, plan same, max/median up** → new hot key → skew handling.
- **Same everything, dead executors up** → infrastructure (Spot, node loss).
- **Code version changed** → diff the code; review with 07.
- **Queue/startup time up only** → capacity, not the ETL.

## 5. Output
State: the single most likely cause, the evidence row(s) from the table, mitigation, durable fix,
and a monitor/alert that would have caught it (row-count drift, runtime SLA, plan check, input
size check).
