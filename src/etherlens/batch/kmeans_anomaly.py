"""MLlib KMeans anomaly detection — Phase 4.6, notes §2.14."""
from __future__ import annotations

import math
import os
import time

import pyspark.sql.functions as F
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import PCA, StandardScaler, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.storagelevel import StorageLevel

from etherlens import io_utils
from etherlens.config import get_paths
from etherlens.spark_session import get_spark

FEATURE_COLS = [
    "log_tx_out", "log_tx_in", "log_eth_out", "log_counterparties",
    "log_max_nonce", "avg_gas_gwei", "plain_share", "log_active_hours",
]


def run(env: str, bucket: str | None = None, spark: SparkSession | None = None) -> dict:
    t0 = time.time()
    paths = get_paths(env, bucket)
    _spark = spark or get_spark(env, "etherlens-kmeans")

    gold = paths["gold"]
    results_dir = paths["results"]
    io_utils.makedirs(results_dir)

    _spark.sparkContext.setJobDescription("kmeans: read address_features")
    af = _spark.read.parquet(os.path.join(gold, "address_features"))

    # Join central_addresses from PageRank if available
    central_path = os.path.join(gold, "central_addresses")
    if io_utils.exists(_spark, os.path.join(central_path, "_SUCCESS")):
        central = _spark.read.parquet(central_path).withColumn("is_central", F.lit(True))
        af = af.drop("is_central").join(central, "address", "left") \
               .withColumn("is_central", F.coalesce(F.col("is_central"), F.lit(False)))

    # Compute log features
    af = (
        af
        .withColumn("log_tx_out", F.log1p(F.col("tx_out").cast(DoubleType())))
        .withColumn("log_tx_in", F.log1p(F.col("tx_in").cast(DoubleType())))
        .withColumn("log_eth_out", F.log1p(F.col("eth_out").cast(DoubleType())))
        .withColumn("log_counterparties", F.log1p(F.col("distinct_counterparties").cast(DoubleType())))
        .withColumn("log_max_nonce", F.log1p(F.col("max_nonce").cast(DoubleType())))
        .withColumn("log_active_hours", F.log1p(F.col("active_hours").cast(DoubleType())))
    )

    sample = af if env == "local" else af.sample(0.05, seed=42)

    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="raw_features",
                                handleInvalid="skip")
    scaler = StandardScaler(inputCol="raw_features", outputCol="features",
                            withMean=True, withStd=True)

    assembled = assembler.transform(sample)
    scaler_model = scaler.fit(assembled)
    scaled = scaler_model.transform(assembled)

    # Choose k by silhouette
    _spark.sparkContext.setJobDescription("kmeans: k selection")
    best_k, best_sil = 4, -999.0
    k_results = []
    for k in range(4, 11):
        km = KMeans(k=k, seed=42, featuresCol="features")
        m = km.fit(scaled)
        preds = m.transform(scaled)
        sil = ClusteringEvaluator(featuresCol="features").evaluate(preds)
        k_results.append({"k": k, "silhouette": round(sil, 4)})
        if sil > best_sil:
            best_sil, best_k = sil, k

    io_utils.write_json(_spark, os.path.join(results_dir, "kmeans_k_selection.json"),
                        {"k_results": k_results, "best_k": best_k,
                         "best_silhouette": round(best_sil, 4)})

    # Final model on full data
    _spark.sparkContext.setJobDescription("kmeans: fit final model")
    assembled_full = assembler.transform(af)
    scaler_full = scaler.fit(assembled_full)
    scaled_full = scaler_full.transform(assembled_full)

    final_km = KMeans(k=best_k, seed=42, featuresCol="features")
    model = final_km.fit(scaled_full)
    clustered = model.transform(scaled_full)

    # Distance to centroid UDF
    centers = model.clusterCenters()

    @F.udf(DoubleType())
    def dist_to_centroid(cluster, features):
        if cluster is None or features is None:
            return None
        c = centers[cluster]
        return float(math.sqrt(sum((a - b) ** 2 for a, b in zip(features.toArray(), c))))

    clustered = clustered.withColumn(
        "dist_to_centroid",
        dist_to_centroid(F.col("prediction"), F.col("features"))
    ).withColumnRenamed("prediction", "cluster")
    # Cached because the steps below reuse it several times
    clustered = clustered.persist(StorageLevel.MEMORY_AND_DISK)
    clustered_cached = clustered

    # Flag: rare cluster (< 1% of addresses)
    n_total = clustered.count()
    cluster_counts = clustered.groupBy("cluster").count()
    rare_threshold = n_total * 0.01
    rare_clusters = set(
        row["cluster"]
        for row in cluster_counts.filter(F.col("count") < rare_threshold).collect()
    )

    # Flag: far from centroid (top 0.5%). The relative error must be well below
    # 0.5%, otherwise approxQuantile may return the maximum and flag nobody.
    dist_threshold = clustered.approxQuantile("dist_to_centroid", [0.995], 0.001)[0]

    _spark.sparkContext.setJobDescription("kmeans: write address_clusters")
    clustered = clustered.withColumn(
        "is_flagged",
        F.col("cluster").isin(list(rare_clusters)) | (F.col("dist_to_centroid") > dist_threshold)
    ).withColumn(
        "flag_reason",
        F.when(F.col("cluster").isin(list(rare_clusters)), F.lit("rare_cluster"))
         .when(F.col("dist_to_centroid") > dist_threshold, F.lit("far_from_centroid"))
         .otherwise(F.lit(None).cast("string"))
    )

    addr_clusters = clustered.select(
        "address", "cluster", "dist_to_centroid", "is_flagged", "flag_reason"
    )
    addr_clusters.write.mode("overwrite").parquet(os.path.join(gold, "address_clusters"))

    flagged = clustered.filter(F.col("is_flagged")).select(
        F.col("address"),
        F.col("flag_reason").alias("reason"),
        F.col("dist_to_centroid").alias("score"),
    )
    flagged.write.mode("overwrite").parquet(os.path.join(gold, "flagged_addresses"))

    # Cluster profiles (mean of raw features per cluster)
    _spark.sparkContext.setJobDescription("kmeans: cluster profiles")
    profile_cols = ["tx_out", "tx_in", "eth_out", "distinct_counterparties",
                    "max_nonce", "avg_gas_gwei", "plain_share", "active_hours"]
    profiles = clustered.groupBy("cluster").agg(
        *[F.avg(c).alias(c) for c in profile_cols if c in clustered.columns]
    ).orderBy("cluster")
    io_utils.write_text(_spark, os.path.join(results_dir, "kmeans_clusters.csv"),
                        profiles.toPandas().to_csv(index=False))

    # PCA (k=2) for visualisation
    from pyspark.ml.functions import vector_to_array
    _spark.sparkContext.setJobDescription("kmeans: PCA viz")
    pca = PCA(k=2, inputCol="features", outputCol="pcs")
    pca_model = pca.fit(scaled_full)
    viz_df = pca_model.transform(scaled_full) \
                      .withColumn("pcs_arr", vector_to_array(F.col("pcs")))
    viz_df = viz_df.join(addr_clusters.select("address", "cluster", "is_flagged"), "address", "left")

    # Sample 5000 + all flagged
    flagged_viz = viz_df.filter(F.col("is_flagged") == True)
    sample_viz = viz_df.filter(F.col("is_flagged") != True).limit(5000).union(flagged_viz)
    sample_viz = sample_viz.withColumn("pc1", F.col("pcs_arr").getItem(0)) \
                           .withColumn("pc2", F.col("pcs_arr").getItem(1)) \
                           .select("address", "pc1", "pc2", "cluster", "is_flagged")
    sample_viz.write.mode("overwrite").parquet(os.path.join(gold, "cluster_viz"))

    # Mock evaluation: precision/recall of planted entities
    flagged_addrs = set(row["address"] for row in flagged.collect())

    stats = {
        "best_k": best_k,
        "best_silhouette": round(best_sil, 4),
        "n_addresses": n_total,
        "n_flagged": len(flagged_addrs),
        "runtime_s": round(time.time() - t0, 2),
    }
    # Only the mock world has ground truth (the planted entities in the manifest)
    if env == "local" and io_utils.exists(_spark, paths["manifest"]):
        manifest = io_utils.read_json(_spark, paths["manifest"])
        evaluable = set(r["address"] for r in af.select("address").collect())
        stats["mock_eval"] = _evaluate_planted(flagged_addrs, manifest, evaluable)
    io_utils.write_json(_spark, os.path.join(results_dir, "kmeans_eval_mock.json"), stats)
    clustered_cached.unpersist()

    print(f"KMeans: best_k={best_k}, flagged={len(flagged_addrs)}")
    return stats


def _evaluate_planted(flagged: set, manifest: dict, evaluable: set) -> dict:
    """Precision/recall of the flagged set against the planted bots, wash ring and whales.

    Only planted addresses that made it into address_features (enough transactions)
    can be flagged at all, so recall is computed over those.
    """
    groups = {name: {a.lower() for a in manifest.get(name, [])} & evaluable
              for name in ("bots", "wash_ring", "whales")}
    planted = set().union(*groups.values())
    hits = flagged & planted
    result = {
        "planted_evaluable": len(planted),
        "true_positives": len(hits),
        "precision": round(len(hits) / len(flagged), 4) if flagged else 0.0,
        "recall": round(len(hits) / len(planted), 4) if planted else 0.0,
    }
    for name, members in groups.items():
        result[f"recall_{name}"] = round(len(flagged & members) / len(members), 4) if members else None
    return result

