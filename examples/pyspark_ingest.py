#!/usr/bin/env python3
"""Batch ingestion: a PySpark job writes Parquet files, then Tidelog imports
them into a table in a single atomic commit, and a range query runs on top.

usage: python examples/pyspark_ingest.py [--rows 2000000] [--table /tmp/tidelog-events]
Requires: pip install pyspark, and `mvn package` (for target/tidelog.jar).
"""
import argparse
import glob
import pathlib
import shutil
import subprocess
import tempfile

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = pathlib.Path(__file__).resolve().parent.parent
JAR = ROOT / "target" / "tidelog.jar"
SCHEMA = "event_id:long,region:string,amount:double,is_test:boolean"


def tidelog(*args):
    out = subprocess.run(["java", "-jar", str(JAR), *args], check=True, capture_output=True, text=True).stdout
    print(out.strip())
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=2_000_000)
    ap.add_argument("--files", type=int, default=16)
    ap.add_argument("--table", default="/tmp/tidelog-events")
    args = ap.parse_args()

    spark = SparkSession.builder.master("local[2]").appName("tidelog-ingest").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    regions = F.array(*[F.lit(r) for r in ["us-east", "us-west", "eu-central", "ap-south"]])
    events = (
        spark.range(0, args.rows)
        .select(
            # Shuffle event ids so files are not already range-partitioned.
            (F.abs(F.xxhash64("id")) % (args.rows * 10)).alias("event_id"),
            F.element_at(regions, (F.col("id") % 4 + 1).cast("int")).alias("region"),
            F.round(F.rand(seed=7) * 1000, 2).alias("amount"),
            (F.col("id") % 50 == 0).alias("is_test"),
        )
        .repartition(args.files)
    )
    staging = tempfile.mkdtemp(prefix="spark-out-")
    events.write.mode("overwrite").parquet(staging)
    spark.stop()
    files = sorted(glob.glob(f"{staging}/*.parquet"))
    print(f"spark wrote {len(files)} parquet files")

    shutil.rmtree(args.table, ignore_errors=True)
    table = f"file://{args.table}"
    tidelog("create", "--table", table, "--schema", SCHEMA)
    tidelog("import", "--table", table, *files)
    lo = args.rows  # a 10% slice of the key space
    where = f"event_id BETWEEN {lo} AND {lo * 2}"
    tidelog("scan", "--table", table, "--where", where, "--limit", "3")
    tidelog("compact", "--table", table, "--target-rows", str(args.rows // args.files), "--cluster-by", "event_id")
    tidelog("scan", "--table", table, "--where", where, "--limit", "3")
    tidelog("history", "--table", table)
    shutil.rmtree(staging, ignore_errors=True)


if __name__ == "__main__":
    main()
