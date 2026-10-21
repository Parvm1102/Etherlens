#!/usr/bin/env python3
"""Run Hive queries locally via Spark SQL with HiveSupport."""
import os
import sys
from pathlib import Path

sys.path.insert(0, "src")

from etherlens.config import get_paths
from etherlens.spark_session import get_spark

paths = get_paths("local")

# Render templates
silver_path = str(Path(paths["silver"]).resolve())
gold_path = str(Path(paths["gold"]).resolve())

def render(sql: str) -> str:
    return sql.replace("__SILVER_PATH__", silver_path).replace("__GOLD_PATH__", gold_path)

spark = get_spark("local", "hive-local", hive=True)
spark.sparkContext.setLogLevel("WARN")

out_dir = Path("results/hive")
out_dir.mkdir(parents=True, exist_ok=True)

# Run create_tables
for stmt in render(Path("hive/create_tables.hql").read_text()).split(";"):
    stmt = stmt.strip()
    if stmt:
        spark.sql(stmt)

# Run queries
queries_sql = render(Path("hive/queries.hql").read_text())
q_num = 0
current = []
for line in queries_sql.splitlines():
    stripped = line.strip()
    if stripped.startswith("-- Q"):
        if current:
            sql = " ".join(current).strip().rstrip(";")
            if sql:
                df = spark.sql(sql)
                if q_num > 0:   # q_num 0 is the "USE etherlens" preamble, not a query
                    df.toPandas().to_csv(out_dir / f"q{q_num}.txt", index=False)
        q_num += 1
        current = []
    else:
        current.append(stripped)

if current:
    sql = " ".join(current).strip().rstrip(";")
    if sql:
        df = spark.sql(sql)
        df.toPandas().to_csv(out_dir / f"q{q_num}.txt", index=False)

spark.stop()
print(f"Hive queries written to {out_dir}/")
