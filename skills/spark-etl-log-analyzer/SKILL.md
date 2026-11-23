---
name: spark-etl-log-analyzer
description: Diagnose, optimize and build Spark ETLs on AWS orchestrated by Airflow in a medallion (bronze/silver on S3, gold in a warehouse such as Redshift) architecture. Use whenever someone shares or asks about an Airflow task log, a failed/slow/stuck ETL or DAG, Spark UI or Spark History Server, EMR / EMR Serverless / EMR on EKS / Glue / Redshift logs, YARN container errors, OOM, shuffle, spill, skew, which join strategy Spark used (BroadcastHashJoin, SortMergeJoin), comparing a current run with a past successful run, reviewing or optimizing PySpark / Scala Spark / Spark SQL code, or creating a new ETL / pipeline with best practices — even if they do not mention Spark explicitly.
---

# Spark ETL Log Analyzer

Teach the human to read an ETL execution, find what went wrong or what is slow, and make the
ETL faster, simpler and easier to read. Explain the *why* in plain language so anyone on the
team — not only Spark experts — learns from each case.

## Ground rules

1. **Guidance only.** Never run AWS CLI, Airflow or git commands against the user's
   environment, never ask for or use credentials. Tell the human exactly what to fetch and how
   (click path, command *they* run, S3 path), then analyze what they paste.
2. **Evidence before conclusions.** Never state a root cause without a log line, metric or plan
   node that supports it. Quote the evidence. If it is missing, use the missing-data protocol.
3. **Airflow first.** Every execution diagnosis starts at the Airflow task log.
4. **Stack is not yet confirmed.** Until the user confirms it, consider all likely variants:
   EMR on EC2, EMR Serverless, EMR on EKS, AWS Glue; Spark 2.x vs 3.x (AQE on by default only
   from 3.2); PySpark or Scala; plain Parquet, Iceberg or Delta; Gold most likely in Redshift.
   Once the user states the stack, stick to it.
5. **Teach.** For each finding: *what you see → what it means → why it happens → what to do*.

## Pick the mode

| The user… | Mode |
|---|---|
| shares a log, a Spark UI screenshot, a plan, says "the ETL failed / is slow / is stuck", or wants to learn to read logs | **A — Diagnose an execution** |
| wants to compare today's run with a previous run | **A**, step 5 (run comparison) |
| shares ETL code and asks to review, find errors, improve, simplify or **optimize** it | **B — Review / optimize code** |
| describes a new process and wants the ETL created | **C — Build a new ETL** |

Several modes can chain: A finds the slow stage → B rewrites the code responsible.

## Mode A — Diagnose an execution

Follow these steps in order and stop at the layer where the failure lives.

1. **Airflow task log** → read `references/01-airflow-task-logs.md`.
   Identify: DAG, task, try number, operator, the **resource ID** it launched (EMR cluster/step
   ID, EMR Serverless application/job-run ID, Glue JobRunId, Redshift query ID, Spark
   application ID) and whether the failure is **in Airflow itself** (timeout, sensor, zombie,
   retries exhausted, credentials/import errors). If it is an Airflow-side failure, diagnose it
   there — do not send the user to the Spark UI.
2. **Engine logs** with that ID → read `references/02-engine-navigation.md` for the exact path
   per engine (EMR step stderr/controller, S3 log URI, Spark History Server, Glue CloudWatch,
   Redshift system views).
3. **Spark UI** → read `references/03-spark-ui-guide.md`: duration, input/output rows and bytes,
   slowest stage, task skew, shuffle, spill, GC, lost executors.
   **Join strategy** → read `references/04-join-strategies.md` (always read the *final* AQE plan).
4. **Classify** the failure → read `references/05-failure-catalog.md` (matched by log signature).
5. **Compare with a good run** → read `references/06-run-comparison.md`.
6. **Report** using `templates/diagnosis-report.md`.

## Mode B — Review / optimize code

Read `references/07-code-review-checklist.md` (and `references/08-medallion-patterns.md` for
layer-specific rules). Works for PySpark, Scala Spark, Spark SQL and Airflow DAGs.

- **Review:** findings ordered *correctness → performance → readability*, each with severity,
  evidence (line), why it matters, and a rewritten snippet. Use `templates/code-review-report.md`.
- **Optimize:** return the full rewritten ETL with the same business logic, a change log with
  expected impact, equivalence notes and risks to validate in a test run. Use
  `templates/optimization-report.md`.
- If a plan or Spark UI metrics are available, tie each performance finding to them.

## Mode C — Build a new ETL

Read `references/09-etl-build-playbook.md` and `references/08-medallion-patterns.md`.
First collect the essentials (sources, target layer, grain, keys, business rules, schedule,
volumes, SLA). If grain, keys or volumes are missing, **ask before writing code** — use
`templates/data-request.md`. Then deliver: ETL code from `templates/etl-pyspark-skeleton.py`
(or `templates/etl-scala-skeleton.scala` / Spark SQL on request), the Airflow DAG from
`templates/airflow-dag-skeleton.py`, built-in data-quality checks, and a short README.

## Missing-data protocol

When you need something the user has not provided, use `templates/data-request.md`:
list each artifact **in priority order**, why it is needed, and exactly how to get it.
Ask for the smallest set that unblocks the next step — usually one or two items.

Typical priority for a failed run: (1) full Airflow task log of the failed try,
(2) engine stderr / driver log for the resource ID, (3) Spark UI SQL tab plan of the slow query,
(4) Stages tab summary of the slowest stage, (5) the same items for the last good run,
(6) the ETL code.

## Reference index

| File | Read when |
|---|---|
| `references/01-airflow-task-logs.md` | Any execution diagnosis (always first) |
| `references/02-engine-navigation.md` | You have a resource ID and need engine logs / Spark UI |
| `references/03-spark-ui-guide.md` | Interpreting duration, volume, stages, tasks, executors |
| `references/04-join-strategies.md` | Identifying or changing the join strategy |
| `references/05-failure-catalog.md` | Matching an error message to a cause and fix |
| `references/06-run-comparison.md` | Good-vs-bad run comparison |
| `references/07-code-review-checklist.md` | Mode B review / optimize |
| `references/08-medallion-patterns.md` | Layer rules, idempotency, partitioning, DQ, formats |
| `references/09-etl-build-playbook.md` | Mode C new ETL |
