"""Tests for silver job — Phase 3."""
import json
import os
import tempfile

import pytest

from etherlens.mock.generate_batch import generate


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    s = (SparkSession.builder
         .appName("test-silver")
         .master("local[2]")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.driver.memory", "2g")
         .config("spark.sql.warehouse.dir", "/tmp/test-warehouse")
         .config("spark.driver.extraJavaOptions", "-Dderby.system.home=/tmp/test-derby")
         .getOrCreate())
    yield s
    s.stop()


@pytest.fixture(scope="session")
def silver_dataset(spark, tmp_path_factory):
    tmp = tmp_path_factory.mktemp("silver")
    bronze_dir = str(tmp / "bronze" / "tx")
    silver_dir = str(tmp / "silver" / "tx")
    edges_dir = str(tmp / "edges")
    results_dir = str(tmp / "results")
    manifest_dir = str(tmp / "mock")
    os.makedirs(manifest_dir, exist_ok=True)

    manifest = generate(seed=42, preset="tiny", start_date="2025-01-01", out_dir=bronze_dir)
    # Write manifest where silver.py expects it
    manifest_path = str(tmp / "mock" / "manifest.json")
    import json
    with open(manifest_path, "w") as f:
        json.dump(manifest, f)

    from etherlens.batch.silver import run
    stats = run(
        env="local",
        bronze_path=bronze_dir,
        spark=spark,
    )
    # Override paths to use our tmp dirs
    stats["_bronze_dir"] = bronze_dir
    stats["_silver_dir"] = silver_dir
    stats["_manifest"] = manifest
    stats["_tmp"] = str(tmp)
    return stats


def test_bad_rows_match_manifest(silver_dataset):
    m = silver_dataset["_manifest"]
    assert silver_dataset["bad_rows"] == m["n_bad_rows"], (
        f"bad_rows {silver_dataset['bad_rows']} != manifest {m['n_bad_rows']}"
    )


def test_duplicates_match_manifest(silver_dataset):
    m = silver_dataset["_manifest"]
    assert silver_dataset["duplicates_removed"] == m["n_duplicates"], (
        f"duplicates_removed {silver_dataset['duplicates_removed']} != manifest {m['n_duplicates']}"
    )


def test_failed_match_manifest(silver_dataset):
    m = silver_dataset["_manifest"]
    assert silver_dataset["failed_removed"] == m["n_failed"], (
        f"failed_removed {silver_dataset['failed_removed']} != manifest {m['n_failed']}"
    )


def test_contract_creations_match_manifest(silver_dataset):
    m = silver_dataset["_manifest"]
    assert silver_dataset["contract_creations"] == m["n_contract_creations"], (
        f"contract_creations {silver_dataset['contract_creations']} != manifest {m['n_contract_creations']}"
    )


def test_rows_out_positive(silver_dataset):
    assert silver_dataset["rows_out"] > 0


def test_days_match_preset(silver_dataset):
    m = silver_dataset["_manifest"]
    assert silver_dataset["days"] == m["days"], (
        f"days {silver_dataset['days']} != preset {m['days']}"
    )


def test_silver_partitions_exist(silver_dataset, spark):
    """Silver Parquet can be read back and has correct schema."""
    silver = spark.read.parquet("data/silver/tx")
    from etherlens.schemas import SILVER_SCHEMA
    assert set(f.name for f in silver.schema.fields) == set(f.name for f in SILVER_SCHEMA.fields)


def test_edge_count_equals_non_null_to(silver_dataset):
    """Edge count equals silver rows with non-null to_address."""
    assert silver_dataset["edge_count"] > 0
    rows_with_to = silver_dataset["rows_out"] - silver_dataset["contract_creations"]
    assert silver_dataset["edge_count"] == rows_with_to
