# spark-etl — Claude Code plugin

A Claude Code plugin that helps anyone on a data team **read, diagnose, optimize and build Spark ETLs on AWS** orchestrated by **Airflow**, in a **medallion** architecture (Bronze/Silver on S3, Gold in a warehouse such as Redshift).

It ships one skill, `spark-etl-log-analyzer`, that Claude loads automatically when you share an Airflow task log, a Spark UI plan, an EMR/Glue/Redshift error, or Spark code.

## What it does

| Mode | You bring | You get |
|---|---|---|
| **A — Diagnose an execution** | Airflow task log (always the first step), engine logs, Spark UI plan/metrics | Step-by-step navigation to the right log, failure classification, join-strategy analysis (final AQE plan), good-vs-bad run comparison, diagnosis report |
| **B — Review / optimize code** | PySpark, Scala Spark, Spark SQL or an Airflow DAG | Findings ordered correctness → performance → readability, with rewrites; full optimized ETL with change log and risks |
| **C — Build a new ETL** | A process description (sources, layer, grain, keys, volumes, SLA) | ETL code, Airflow DAG, data-quality checks and README, from best-practice skeletons |

Covers EMR on EC2, EMR Serverless, EMR on EKS, AWS Glue and Redshift; Spark 2.x/3.x; Parquet, Iceberg and Delta.

**Guidance only:** the plugin never runs AWS CLI, Airflow or git commands and never uses credentials. It tells you exactly what to fetch (click path, command you run, S3 path) and analyzes what you paste.

## Install

```shell
/plugin marketplace add matn2468/spark-etl-plugin
/plugin install spark-etl@spark-etl-tools
```

Or from a terminal:

```shell
claude plugin marketplace add matn2468/spark-etl-plugin
claude plugin install spark-etl@spark-etl-tools
```

## Usage

Just talk to Claude, for example:

- "My ETL failed last night, here is the Airflow log: …"
- "Why did the plan switch from BroadcastHashJoin to SortMergeJoin?"
- "Review / optimize this PySpark job: …"
- "Create an ETL that builds a gold table of daily conversions per campaign."

You can also invoke the skill explicitly with `/spark-etl:spark-etl-log-analyzer`.

## Layout

```
.claude-plugin/plugin.json        plugin manifest
.claude-plugin/marketplace.json   marketplace (this repo is installable directly)
skills/spark-etl-log-analyzer/    SKILL.md, references/, templates/
evals/                            eval cases for `claude plugin eval .`
```

## License

MIT
