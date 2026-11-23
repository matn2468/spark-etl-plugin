# 02 — Engine navigation: where the logs and the Spark UI live

Use the resource ID from the Airflow log. Each section gives the click path, the files to open
**first**, and the command the *human* can run (the skill never runs it).

## Contents
1. EMR on EC2 (YARN)
2. EMR Serverless
3. EMR on EKS
4. AWS Glue
5. Redshift (Gold)
6. Spark History Server / event logs (any engine)
7. Shortcuts: which file answers which question

---

## 1. EMR on EC2 (YARN) — IDs `j-...` (cluster), `s-...` (step), `application_..._NNNN`

**Console**: EMR → Clusters → `j-...` → **Steps** tab → the step → *View logs*:
- `controller` — how EMR launched the step (command, exit code). Open first if the step never ran.
- `stderr` — **spark-submit output, driver log in client mode, the final exception**. Usually the
  most useful file.
- `stdout` — prints from your code.
- `syslog` — YARN/Hadoop messages.

**Spark UI**: cluster → **Applications** tab → *Spark History Server* (persistent, works after
the cluster terminates) → find `application_..._NNNN` by name/time.
Also *YARN timeline server* for container-level status.

**S3 log URI** (set at cluster creation, e.g. `s3://<log-bucket>/logs/`), logs arrive ~5 min later:
```
<log-uri>/<cluster-id>/steps/<step-id>/{controller,stderr,stdout,syslog}.gz
<log-uri>/<cluster-id>/containers/application_<ts>_<n>/container_<...>_000001/stderr.gz   # driver (cluster mode) = first container
<log-uri>/<cluster-id>/containers/application_<ts>_<n>/container_<...>_00000N/stderr.gz   # executors
<log-uri>/<cluster-id>/node/<instance-id>/...                                             # node / bootstrap logs
```
Human-run commands:
```bash
aws emr describe-step --cluster-id j-XXX --step-id s-XXX          # state + failure reason
aws emr list-steps --cluster-id j-XXX --step-states FAILED
aws s3 cp s3://<log-uri>/j-XXX/steps/s-XXX/stderr.gz - | gunzip | tail -200
yarn logs -applicationId application_<ts>_<n> > app.log            # on the primary node, while cluster is alive
```
Deploy mode matters: `--deploy-mode cluster` → driver log is in the **first container**
(`..._000001`), not in step `stderr`. `client` → driver log is in step `stderr`.

## 2. EMR Serverless — application ID + job run ID

**Console**: EMR → EMR Serverless → EMR Studio → Applications → app → **Job runs** → run →
- *Spark UI* (live or persistent, launched from the job run page)
- *Driver logs* / *Executor logs* (stdout / stderr).

**S3 / CloudWatch** (if configured in `monitoringConfiguration`):
```
s3://<bucket>/<prefix>/applications/<app-id>/jobs/<job-run-id>/SPARK_DRIVER/{stderr,stdout}.gz
s3://<bucket>/<prefix>/applications/<app-id>/jobs/<job-run-id>/SPARK_EXECUTOR/<n>/stderr.gz
```
Human-run: `aws emr-serverless get-job-run --application-id <app> --job-run-id <run>`
(`stateDetails` often has the failure reason; also shows resource usage: vCPU-hours, memory-GB-hours).
Serverless-specific: application max capacity reached, worker cold start, job run timeout.

## 3. EMR on EKS — virtual cluster ID + job run ID

Console: EMR → EMR on EKS → Virtual clusters → job runs → *Spark UI*.
Logs: CloudWatch / S3 per `monitoringConfiguration`; pods via `kubectl logs <driver-pod> -n <ns>`.
Specific failures: pod `OOMKilled`, `Pending` pods (no node capacity), image pull errors.

## 4. AWS Glue — job name + `jr_...`

Console: Glue → ETL jobs → job → **Runs** tab → run:
- *Error message* column (often truncated — open logs)
- **Output logs**: CloudWatch `/aws-glue/jobs/output` (driver stdout)
- **Error logs**: CloudWatch `/aws-glue/jobs/error` (driver stderr, the stack trace)
- Continuous logging: `/aws-glue/jobs/logs-v2`
- *Spark UI*: needs `--enable-spark-ui true` and `--spark-event-logs-path s3://...`; open from the
  run page or a Spark History Server pointed at that path.
- Job run insights (`--enable-job-insights`) gives a root-cause line in `/aws-glue/jobs/logs-v2`.
Glue-specific: DPU/worker type too small (G.1X vs G.2X), job bookmarks skipping data,
`Glue ETL job timeout`, DynamicFrame vs DataFrame conversion cost.

## 5. Redshift (Gold) — query / statement ID

Run in the Redshift query editor (human):
```sql
-- recent failed or long queries (Redshift provisioned + serverless)
SELECT query_id, status, start_time, elapsed_time/1e6 AS sec, error_message, LEFT(query_text,200)
FROM sys_query_history
WHERE start_time > dateadd(hour,-24,getdate()) AND (status='failed' OR elapsed_time > 600e6)
ORDER BY start_time DESC;

-- COPY / load errors
SELECT * FROM sys_load_error_detail ORDER BY start_time DESC LIMIT 50;   -- or stl_load_errors (provisioned)

-- step-level plan of a slow query (provisioned)
SELECT * FROM svl_query_summary WHERE query = <id> ORDER BY stm, seg, step;

-- locks / blocking
SELECT * FROM svv_transactions; SELECT * FROM stv_locks;
```
Look for: `DS_BCAST_INNER` / `DS_DIST_BOTH` (redistribution = Redshift's equivalent of a
shuffle; fix with DISTKEY on join keys), disk-based steps (`is_diskbased='t'`), WLM queue waits,
`Serializable isolation violation`, `Invalid timestamp format`, `String length exceeds DDL length`.

## 6. Spark History Server / event logs (any engine)

If the UI is not reachable but event logs exist in S3 (`spark.eventLog.dir`), the human can
download the event log file (JSON lines) for the application and open it in a local Spark History
Server, or share the relevant parts (`SparkListenerStageCompleted`, `SparkListenerTaskEnd`
summaries). The Spark REST API (`/api/v1/applications/<id>/stages`) returns the same metrics as
JSON — ideal for run comparison.

## 7. Shortcuts — which file answers which question

| Question | Open |
|---|---|
| Why did the step/job fail? | EMR step `stderr` (client) / driver container `stderr` (cluster) / Glue error log / Serverless SPARK_DRIVER stderr |
| Did it fail before Spark started? | EMR `controller`, bootstrap/node logs, Airflow log |
| Why was an executor lost? | Executor container `stderr`, YARN `syslog` (`Container killed by YARN`), Executors tab |
| Why is it slow? | Spark UI → Stages + SQL tab (03, 04) |
| Which join was used? | Spark UI SQL tab plan or `df.explain("formatted")` (04) |
| How much data? | Spark UI SQL tab rows per node, Stages input/output, S3 prefix size (`aws s3 ls --summarize --recursive`) |
| Why did the Gold load fail? | Redshift `sys_query_history`, `sys_load_error_detail` |
