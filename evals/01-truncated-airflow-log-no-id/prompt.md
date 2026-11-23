---
max_turns: 10
allowed_tools: [Read, Glob, Grep, Skill]
---

My ETL failed last night. Here is the end of the Airflow log:
[2026-11-10 03:42:11] ERROR - Task failed with exception
airflow.exceptions.AirflowException: Step failed
[2026-11-10 03:42:11] INFO - Marking task as UP_FOR_RETRY. dag_id=sales_medallion_daily, task_id=bronze_to_silver
What happened?
