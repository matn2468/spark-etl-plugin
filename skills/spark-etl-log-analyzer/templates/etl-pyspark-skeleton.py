"""<etl_name>: <one-line purpose>.

Layer: <bronze|silver|gold>   Grain: <one row per ...>   Keys: <business keys>
Inputs:  <paths/tables>
Output:  <path/table>, partitioned by dt
Run:     spark-submit etl.py --run-date 2026-11-10 --env prod
"""
from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

log = logging.getLogger("<etl_name>")


# ---------------------------------------------------------------- config
@dataclass(frozen=True)
class Config:
    run_date: str
    env: str
    source_path: str
    lookup_path: str
    target_path: str
    lookback_days: int = 0


SOURCE_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType(), False),
    T.StructField("key_id", T.StringType(), True),
    T.StructField("amount", T.DecimalType(18, 2), True),
    T.StructField("event_ts", T.TimestampType(), True),
    T.StructField("updated_at", T.TimestampType(), True),
    T.StructField("dt", T.DateType(), False),
])


def parse_args() -> Config:
    p = argparse.ArgumentParser()
    p.add_argument("--run-date", required=True)
    p.add_argument("--env", default="dev")
    p.add_argument("--lookback-days", type=int, default=0)
    a = p.parse_args()
    root = f"s3://<bucket>-{a.env}"
    return Config(
        run_date=a.run_date,
        env=a.env,
        source_path=f"{root}/bronze/<source>/",
        lookup_path=f"{root}/silver/<lookup>/",
        target_path=f"{root}/silver/<target>/",
        lookback_days=a.lookback_days,
    )


def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


# ---------------------------------------------------------------- read
def read_source(spark: SparkSession, cfg: Config) -> DataFrame:
    start = F.date_sub(F.lit(cfg.run_date).cast("date"), cfg.lookback_days)
    return (
        spark.read.schema(SOURCE_SCHEMA).parquet(cfg.source_path)
        .filter(F.col("dt").between(start, F.lit(cfg.run_date).cast("date")))  # partition pruning
        .select("event_id", "key_id", "amount", "event_ts", "updated_at", "dt")  # early projection
    )


def read_lookup(spark: SparkSession, cfg: Config) -> DataFrame:
    return (
        spark.read.parquet(cfg.lookup_path)
        .filter(F.col("is_current"))
        .select("key_id", "attribute")  # only what the join needs
    )


# ---------------------------------------------------------------- validate
def assert_unique(df: DataFrame, keys: list[str], name: str) -> None:
    dupes = df.groupBy(*keys).count().filter("count > 1").limit(5).collect()
    if dupes:
        raise ValueError(f"{name}: duplicate keys {keys}: {dupes}")


# ---------------------------------------------------------------- transform (pure functions)
def deduplicate(df: DataFrame) -> DataFrame:
    w = Window.partitionBy("event_id").orderBy(F.col("updated_at").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def enrich(lookup: DataFrame):
    def _enrich(df: DataFrame) -> DataFrame:
        # Broadcast only because lookup is trimmed and uniqueness-checked.
        return df.join(F.broadcast(lookup), "key_id", "left")
    return _enrich


def finalize(df: DataFrame) -> DataFrame:
    return df.select(
        "event_id", "key_id", "attribute", "amount", "event_ts",
        F.current_timestamp().alias("_processed_ts"), "dt",
    )


# ---------------------------------------------------------------- data quality
def run_dq(df: DataFrame) -> dict:
    stats = df.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(F.when(F.col("attribute").isNull(), 1).otherwise(0)).alias("unmatched"),
        F.countDistinct("event_id").alias("distinct_ids"),
    ).first().asDict()
    if stats["rows"] == 0:
        raise ValueError("DQ: output is empty")
    if stats["rows"] != stats["distinct_ids"]:
        raise ValueError(f"DQ: duplicate event_id in output {stats}")
    if stats["unmatched"] / stats["rows"] > 0.01:  # threshold agreed with owners
        raise ValueError(f"DQ: >1% rows without lookup match {stats}")
    return stats


# ---------------------------------------------------------------- write
def write_output(df: DataFrame, cfg: Config) -> None:
    (
        df.repartition("dt")  # one writer group per partition -> fewer, larger files
        .write.mode("overwrite")  # dynamic partition overwrite => idempotent re-runs
        .partitionBy("dt")
        .parquet(cfg.target_path)
    )


# ---------------------------------------------------------------- main
def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = parse_args()
    spark = build_spark(f"<etl_name>-{cfg.run_date}")
    log.info("Start run_date=%s env=%s app_id=%s", cfg.run_date, cfg.env,
             spark.sparkContext.applicationId)

    lookup = read_lookup(spark, cfg)
    assert_unique(lookup, ["key_id"], "lookup")

    result = (
        read_source(spark, cfg)
        .transform(deduplicate)
        .transform(enrich(lookup))
        .transform(finalize)
    ).cache()  # reused by DQ and write

    stats = run_dq(result)
    log.info("DQ passed: %s", stats)
    write_output(result, cfg)
    result.unpersist()
    log.info("Done")


if __name__ == "__main__":
    main()
