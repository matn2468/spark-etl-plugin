# 01 — Airflow task logs (always the first stop)

The Airflow task log tells you **what was launched, where, and whether Airflow or the job failed**.
Everything else in the diagnosis hangs from the resource ID you find here.

## Contents
1. How to open the task log
2. Anatomy of a task log
3. Operator → resource ID → where to go next
4. Airflow-side failures (stop here, do not go to Spark)
5. What to ask the user for

---

## 1. How to open the task log

**Airflow UI (2.x, Grid view)**
1. DAGs → search the DAG name → open it.
2. **Grid** view: columns = DAG runs (by logical date), rows = tasks. Red = failed,
   orange = up_for_retry, yellow = upstream_failed, light green = running, dark green = success.
3. Click the failed square → **Logs** tab. Use the **try number** selector: each retry has its
   own log. The *first* failed try often has the real error; later tries may show a different
   symptom (e.g. "cluster already terminated").
4. "Download" / "See more" gives the full log (the UI may truncate long logs).

**Airflow 3.x**: DAG → Runs → select run → Task Instances → task → Logs (same content).

**Amazon MWAA**: the UI works the same. Logs also go to CloudWatch:
- Log group: `airflow-<environment-name>-Task`
- Log stream: `dag_id=<dag>/run_id=<run>/task_id=<task>/attempt=<n>.log`
  (older versions: `<dag>/<task>/<execution_date>/<try>.log`)
- Other groups: `-Scheduler`, `-Worker`, `-DAGProcessing`, `-WebServer` (useful when the task
  never started or the DAG does not appear).

**Self-hosted Airflow**: `$AIRFLOW_HOME/logs/dag_id=<dag>/run_id=<run>/task_id=<task>/attempt=<n>.log`,
or remote logging to S3 (`remote_base_log_folder` in `airflow.cfg`).

## 2. Anatomy of a task log

Read it top to bottom looking for these markers:

| Marker in the log | Meaning |
|---|---|
| `Dependencies all met for ...` / `Starting attempt 2 of 4` | Try number and retries configured |
| `Executing <Task(EmrAddStepsOperator): run_silver>` | **Operator class** → tells you the engine |
| `Running command: ...` / `spark-submit ...` | The exact command / args (job name, jar/py file, configs) |
| IDs: `j-XXXXXXXX`, `s-XXXXXXXX`, `00f...` (job run), `jr_...` (Glue), `application_1700..._0042` | **Resource ID** to follow |
| `Job flow / step state: PENDING → RUNNING → FAILED` | Polling from sensors/operators |
| `Traceback (most recent call last):` | Python exception — read the **last** line first, then upward |
| `Marking task as FAILED` / `UP_FOR_RETRY` / `SUCCESS` | Final state of this try |
| `Task exited with return code 1` / `-9` / `Negsignal.SIGKILL` | Process exit: 1 = error raised; -9/SIGKILL = killed (memory on worker or timeout) |

Rule: the **Airflow traceback is often only the messenger** — e.g. `AirflowException: EMR step
failed` means the real error is in the EMR step logs.

## 3. Operator → resource ID → where to go next

| Operator / hook (examples) | Resource ID to copy | Next stop (see 02-engine-navigation.md) |
|---|---|---|
| `EmrCreateJobFlowOperator`, `EmrAddStepsOperator`, `EmrStepSensor`, `EmrJobFlowSensor` | Cluster `j-...`, step `s-...` | **EMR on EC2** |
| `EmrServerlessStartJobOperator`, `EmrServerlessJobSensor` | Application ID + job run ID | **EMR Serverless** |
| `EmrContainerOperator`, `EmrContainerSensor` | Virtual cluster ID + job run ID | **EMR on EKS** |
| `GlueJobOperator`, `GlueJobSensor` | Glue job name + `jr_...` run ID | **AWS Glue** |
| `SparkSubmitOperator` | `application_<ts>_<n>` (YARN) or driver pod | **EMR on EC2 / YARN** or Kubernetes |
| `RedshiftSQLOperator`, `RedshiftDataOperator`, `SQLExecuteQueryOperator`, `S3ToRedshiftOperator` | Query ID / statement ID | **Redshift (Gold)** |
| `S3KeySensor`, `ExternalTaskSensor`, `*Sensor` | — | Usually an upstream/data-arrival issue (section 4) |
| `PythonOperator` / `@task` running Spark locally | — | Read the traceback here; it is the job log |

## 4. Airflow-side failures (diagnose here, do not go to Spark)

| Log signature | Meaning | Typical fix |
|---|---|---|
| `AirflowTaskTimeout` / `Timeout, PID ...` / `execution_timeout` | Task exceeded `execution_timeout` | Check whether the job is genuinely slower (then diagnose the engine with the ID) or the timeout is too tight |
| `AirflowSensorTimeout` / `Sensor has timed out` | Data/upstream never arrived | Check upstream DAG / S3 prefix / partition date; use `mode="reschedule"` |
| `Detected zombie job` / `was killed externally` / heartbeat errors | Worker died or lost heartbeat (memory pressure on worker, autoscaling, restart) | Do not do heavy work in the worker; check worker logs/CloudWatch `-Worker` |
| `upstream_failed` state, no log | A dependency failed | Go to the first failed upstream task |
| `ModuleNotFoundError`, `ImportError`, `Broken DAG` | Packaging / requirements issue | MWAA `requirements.txt`, plugins.zip, provider versions |
| `NoCredentialsError`, `AccessDenied`, `ExpiredToken`, `UnrecognizedClientException` | IAM / connection problem | Check the Airflow connection (`aws_default`) and the role permissions |
| `Task exited with return code Negsignal.SIGKILL` | Worker process killed (OOM on the Airflow worker) | Move processing off the worker; increase worker size |
| Task stuck in `queued` / `scheduled` | Pool/slots/concurrency or executor capacity | Check pools, `max_active_tasks`, MWAA worker scaling |
| `ThrottlingException`, `Rate exceeded` | Too many AWS API polls | Increase sensor `poke_interval`, use deferrable operators |
| Retries succeed later | Transient failure (Spot loss, S3 throttling) or race with upstream | Confirm in engine logs; make the job idempotent |

## 5. What to ask the user for

If the user only says "the ETL failed", request (in this order):
1. The **full log of the first failed try** (download it; paste the last ~150 lines plus any
   line containing an ID like `j-`, `s-`, `jr_`, `application_`).
2. DAG id, task id, logical date / run id, and whether previous runs succeeded.
3. If the log has no resource ID (truncated, or the task never started), the **scheduler/worker
   logs** around that time, or a screenshot of the Grid view showing task states.
Never guess the engine from the DAG name — confirm it from the operator class in the log.
