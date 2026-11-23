"""Airflow DAG skeleton: bronze -> silver -> gold with EMR Serverless and Redshift.

Swap the Spark operator for your engine:
  EMR on EC2   -> EmrAddStepsOperator + EmrStepSensor
  Glue         -> GlueJobOperator(wait_for_completion=True)
  EMR on EKS   -> EmrContainerOperator
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

ENV = "prod"
APP_ID = "<emr-serverless-application-id>"
EXEC_ROLE = "arn:aws:iam::<account>:role/<emr-serverless-job-role>"
CODE = f"s3://<bucket>-{ENV}/code/etl"
LOGS = f"s3://<bucket>-{ENV}/logs/emr-serverless/"


def notify_failure(context):
    ti = context["task_instance"]
    # Replace with Slack/SNS/email. Include the link so anyone can start the diagnosis.
    print(f"FAILED {ti.dag_id}.{ti.task_id} {context['ds']} try={ti.try_number} log={ti.log_url}")


def spark_job(task_id: str, script: str) -> EmrServerlessStartJobOperator:
    return EmrServerlessStartJobOperator(
        task_id=task_id,
        application_id=APP_ID,
        execution_role_arn=EXEC_ROLE,
        name=f"{task_id}-{{{{ ds }}}}",
        job_driver={
            "sparkSubmit": {
                "entryPoint": f"{CODE}/{script}",
                "entryPointArguments": ["--run-date", "{{ ds }}", "--env", ENV],
                "sparkSubmitParameters": "--conf spark.sql.adaptive.enabled=true",
            }
        },
        configuration_overrides={
            "monitoringConfiguration": {"s3MonitoringConfiguration": {"logUri": LOGS}}
        },
        wait_for_completion=True,  # failure surfaces in this task; job run ID is logged
    )


with DAG(
    dag_id="<domain>_medallion_daily",
    start_date=datetime(2026, 11, 1),
    schedule="0 3 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-eng",
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
        "execution_timeout": timedelta(hours=2),
        "on_failure_callback": notify_failure,
    },
    tags=["medallion", "spark"],
) as dag:
    wait_source = S3KeySensor(
        task_id="wait_bronze_source",
        bucket_key=f"s3://<bucket>-{ENV}/bronze/<source>/dt={{{{ ds }}}}/_SUCCESS",
        mode="reschedule",
        poke_interval=300,
        timeout=60 * 60 * 3,
    )

    silver = spark_job("bronze_to_silver", "silver_<entity>.py")
    # Stage job writes Parquet to gold_stage/<mart>/dt=<ds>/ WITHOUT partitionBy, so the dt
    # column stays inside the files (COPY does not read partition values from the path).
    gold_stage = spark_job("silver_to_gold_stage", "gold_<mart>_stage.py")

    publish_gold = SQLExecuteQueryOperator(
        task_id="publish_gold_redshift",
        conn_id="redshift_default",
        sql="""
            BEGIN;
            DELETE FROM staging.<mart> WHERE dt = '{{ ds }}';
            COPY staging.<mart>
              FROM 's3://<bucket>-prod/gold_stage/<mart>/dt={{ ds }}/'
              IAM_ROLE '<redshift-copy-role-arn>' FORMAT AS PARQUET;
            DELETE FROM gold.<mart> WHERE dt = '{{ ds }}';
            INSERT INTO gold.<mart> SELECT * FROM staging.<mart> WHERE dt = '{{ ds }}';
            COMMIT;
        """,
    )

    wait_source >> silver >> gold_stage >> publish_gold
