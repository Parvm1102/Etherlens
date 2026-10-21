# Report outline — Big Data Analytics

Section headings, what each section must say, and which figure and numbers to use. Numbers come
from `results_gcp/summary.md` (real January 2025 run) and `results/summary.md` (mock run) unless
another file is named. Figures are in `results_gcp/figures/` (real) and `results/figures/` (mock).

## 1. Introduction
- Problem: Ethereum produces about 1.2 M transactions a day; questions like "when is gas cheapest",
  "which addresses behave unusually" need distributed processing.
- Objective: batch + streaming analytics on Hadoop/Spark, with verifiable published reports.
- Contributions (bullets): medallion pipeline on Dataproc; MapReduce vs Spark comparison; PageRank
  and KMeans at scale; streaming with sketches; four analytics types on real data.

## 2. Background
- Big data V's and data lake vs warehouse (`docs/architecture.md` §3).
- Hadoop stack: HDFS (blocks, replication), YARN, MapReduce model (map, shuffle, reduce, combiner).
- Spark: RDDs, DAG, lazy evaluation, narrow vs wide transformations, DataFrames, MLlib,
  Structured Streaming.
- Streaming algorithms: reservoir sampling, Count-Min Sketch, HyperLogLog, Bloom filter.
- Ethereum data basics needed for the analysis: gas, gwei, nonce, plain transfers (21,000 gas).

## 3. Data
- Source: BigQuery `crypto_ethereum.transactions` (a view since September 2026; partition
  pruning still works), 13 columns (list them).
- Slice: 2025-01-01 to 2025-01-31. `check_range`: 31 days, 1.04–1.40 M tx per day.
- Export: 9.93 GB scanned (free tier), 117 Parquet files, 7.02 GB.
- Mock data: synthetic world with planted bots, wash ring, whales (why: ground truth, free testing).
- Table: rows in 37,865,791 → out 37,323,780; failed 542,011; contract creations 50,601;
  duplicates 0; bad rows 0.

## 4. System design
- Architecture diagram (mermaid from `docs/architecture.md`, or a redrawn version).
- Medallion layers with paths and sizes.
- Cluster: Dataproc 2.2, 1 × e2-standard-4 + 4 × e2-highmem-4; executor layout and why (memory).
- Local-first development in Docker, `--env local|gcp` switch, one I/O helper for local/gs/hdfs.

## 5. Implementation
5.1 Silver: casts, accumulators in an action (cross-checked), dedup, persist, one file per day.
5.2 Gold aggregates (list the tables, `address_features` = 1,435,758 rows).
5.3 Hive: external partitioned table, the five queries (`hive/queries.hql`).
5.4 MapReduce (Hadoop Streaming) with and without combiner.
5.5 Spark RDD count; PageRank (class formula, 10 iterations, checkpoint every 5, last 7 days).
5.6 KMeans pipeline (features, scaling, silhouette k-selection, flags).
5.7 Four analytics types (correlation, chronological split, persistence baseline).
5.8 Structured Streaming: windows, watermark, foreachBatch sketches, alerts; checkpoints on HDFS.

## 6. Results
- **Scale and runtime**: job runtime table (`results_gcp/gcp_job_times.tsv`; total 41 min).
  Figure: the job-runtime bar chart from `dashboard/overview.html` or a table.
- **Descriptive**: `daily_tx.png`, `plain_vs_contract.png`; 1.2 M tx/day, 34% plain transfers.
- **Gas**: `gas_by_hour.png`; cheapest 04:00 UTC, 68.4% below the 15:00 peak; correlation 0.27.
- **Predictive**: `predictive_vs_baseline.png`; RMSE 2.64 vs baseline 2.24 (real), 0.208 vs 0.865
  (mock). Discuss why.
- **MapReduce vs Spark**: `mr_combiner_shuffle.png` (shuffle −46%), `hadoop_vs_spark.png`
  (median 144 s vs 74 s, 1.94×). Table of MR counters (`results_gcp/mapreduce_counters.json`).
- **PageRank**: `pagerank_top20.png`; USDT and USDC on top; iteration times show laziness.
- **KMeans**: `kmeans_k_selection.png`, `clusters_pca.png`; k = 10, silhouette 0.278, 7,417
  flagged; cluster profile table (`results_gcp/kmeans_clusters.csv`). Mock: recall 1.00,
  precision 0.25 (explain the budget ceiling).
- **Streaming**: `sketch_cms_error.png`, `sketch_hll_error.png` (mock); cluster run HLL 17,712
  vs 17,714, CMS within bound for 100% (bound 124.8, worst 34), Bloom with 7,417 addresses; live
  mainnet check 10 blocks at 12.0 s.
- **Validation**: Stage B table (cloud == local on mock) from `docs/PROGRESS.md`.

## 7. Engineering lessons (short, concrete)
- Small files on object storage (200 → 1 file per day; 888 s → 194 s).
- Executor memory vs node memory (earlyoom kills; headroom).
- Partition pruning and caching.
- Cost control: dry runs, `--max-idle`, delete clusters (total about $4 of credits).

## 8. Limitations and future work
- Prediction does not beat persistence; try features like mempool size or block fullness.
- KMeans clusters weakly separated; try density-based methods.
- Streaming sketches reset on restart (driver state); Kafka not used (file source instead).
- Scale to a year: estimated 120 GB scan.

## 9. Conclusion

## Appendix
- Commands to reproduce (`README.md`), deviations list (`docs/PROGRESS.md`), screenshots
  (`results/screenshots/`).
