# EtherLens — Progress Log

---

## Phase 1 — Mock data generator

**Date:** 2026-10-02

**What was built:**
- `src/etherlens/mock/entities.py` — deterministic synthetic world (20k users, 300 contracts, 1 hub, 8 bots, 6-address wash ring, 15 whales)
- `src/etherlens/mock/generate_batch.py` — bronze Parquet generator with planted anomalies; presets tiny/small/medium; writes `data/mock/manifest.json`
- `src/etherlens/mock/blocks.py` — `MockChain` yielding §6.4 stream records; bursts every 25th block (5×); 2% flagged tx; spammer every 100 blocks
- `tests/test_mock.py` — 11 tests: schema, row count, manifest counts (duplicates/bad_rows/failures), hub share, bot nonces, cheapest hour, nonce monotonicity, MockChain records, bursts

**Commands to reproduce:**
```
docker compose run --rm dev make mock-tiny      # 3 days, ~9k rows
docker compose run --rm dev pytest tests/test_mock.py -v
docker compose run --rm dev make mock           # 14 days, ~560k rows (small preset)
```

**Key numbers (tiny preset, seed=42):**
- 9,255 total rows, 3 days
- Duplicates: 8, bad rows: 3, failed: 172, contract creations: 19
- Hub share: ~8%, bots nonce ≥ 200,000, cheapest hour ≈ 3 UTC
# EtherLens — Progress Log

---


**Deviations from spec:**
- None. All §6.1 bronze columns present with correct pyarrow types.
- Nonces assigned in a two-pass approach (sort by block_timestamp first) to ensure per-sender monotonicity as required.

---

---

## Phase 2 — Core pure-Python libraries

**Date:** 2026-10-02

**What was built:**
- `src/etherlens/sketches/hashing.py` — `h64` (blake2b 8-byte digest → uint64), `h_pair` (16-byte digest → two uint64s)
- `src/etherlens/sketches/cms.py` — CountMinSketch(eps, delta); w=ceil(e/eps), d=ceil(ln(1/delta)); heavy-hitter candidate dict pruned at 10k
- `src/etherlens/sketches/hll.py` — HyperLogLog(p=14); alpha_m correction; linear counting for small range
- `src/etherlens/sketches/bloom.py` — BloomFilter(n_expected, fp_target); double hashing; theoretical_fp()
- `src/etherlens/sketches/reservoir.py` — Reservoir(s, seed); Algorithm R
- `src/etherlens/chain/merkle.py` — MerkleTree with sorted-pair keccak256; proof(i); verify_row; to_json; CLI
- `tests/test_sketches.py` — 7 tests (CMS, HLL, Bloom, Reservoir)
- `tests/test_merkle.py` — 23 tests including golden vector

**Commands to reproduce:**
```
docker compose run --rm dev pytest tests/test_sketches.py tests/test_merkle.py -v
```

**Key numbers:**
- All 30 Phase 2 tests pass in ~8.5s
- Golden vector ["a","b","c"] → root 0x5842148bc6ebeb52af882a317c765fccd3ae80589b21a9b8cbf21abb630e46a7 ✓
- Proofs verified for n ∈ {1,2,3,4,5,7,8,33,100}
- CMS: no underestimates; error fraction ≤ 2δ on 50k Zipf stream
- HLL: 200k distinct → <3% error; 100 distinct → <5% error
- Bloom: 0 false negatives; empirical FP within [0.5×, 2×] theoretical

**Deviations from spec:**
- None.

---

## Phase 7 — Figures, summary, and dashboard

**Date:** 2026-10-02

**What was built:**
- `scripts/make_figures.py` — 6 matplotlib figures (Agg backend, 150 dpi): daily_tx.png, gas_by_hour.png, plain_vs_contract.png, pagerank_top20.png, kmeans_k_selection.png, clusters_pca.png; skips gracefully on missing input
- `scripts/make_summary.py` — `results/summary.md` with all key numbers from every phase
- `dashboard/app.py` — Streamlit dashboard with 7 tabs: Overview, Gas & Prediction, Network (PageRank), Anomalies (KMeans), Live (streaming, auto-refresh 5s), Blockchain (on-chain verify demo), Big Data Internals; pyarrow for fast parquet reads; works with both local and gcp data sources
- `scripts/stream_local.sh` — starts mock poller + stream_job together with trap/cleanup on Ctrl-C

**Commands to reproduce:**
```
docker compose run --rm dev make figures
docker compose run --rm dev make summary
docker compose up -d dashboard   # → http://localhost:8501
# or:
docker compose run --rm dev make dashboard
```

**Key numbers:** 6 figures written to results/figures/; summary.md has 3000+ characters of results.

**Deviations from spec:**
- `gas_vs_rows.png` (7th figure) skipped gracefully — requires chain gas experiment data not yet produced; all other 6 figures produced.
- Streamlit `@st.fragment(run_every=...)` used for live tab auto-refresh.

---

## Phase 6 — On-chain Merkle registry

**Date:** 2026-10-02

**What was built:**
- `contracts/src/ReportRegistry.sol` — stores root per day, verifyRow with sorted-pair keccak256 proof; constant gas regardless of leaf count
- `contracts/test/ReportRegistry.t.sol` — Foundry tests; forge-std installed via `forge install foundry-rs/forge-std --no-git`
- `src/etherlens/chain/common.py` — get_w3(), get_account(), load_artifact(), send_tx()
- `src/etherlens/chain/deploy.py` — deploy contract, write results/chain/deployment_local.json
- `src/etherlens/chain/publish.py` — publish_day(), run_gas_experiment(); redeploys if anvil was restarted
- `src/etherlens/chain/verify.py` — verifyRow on-chain; --tamper flag
- `tests/test_chain_local.py` — 4 tests: all rows one day, random rows other days, tampered rows fail, golden vector on-chain

**Commands to reproduce:**
```
docker compose up -d chain
docker compose run --rm dev make contracts-build
docker compose run --rm dev make contracts-test
docker compose run --rm dev python -m etherlens.chain.deploy
docker compose run --rm dev python -m etherlens.chain.publish --all
docker compose run --rm dev pytest tests/test_chain_local.py -v -m chain
```

**Key numbers:**
- 4/4 chain tests pass (3.42s)
- Contract deployed at 0x5FbDB2315678afecb367f032d93F642f64180aa3 (local anvil)
- 3 Merkle roots published (2025-01-01 to 2025-01-03), 92 leaves each

**Deviations from spec:**
- forge-std not bundled in image; must be installed at runtime: `docker compose run --rm dev bash -c "cd contracts && forge install foundry-rs/forge-std --no-git"`. Added to setup instructions.

---

## Phase 5 — Structured Streaming

**Date:** 2026-10-02

**What was built:**
- `src/etherlens/stream/poller.py` — mock/replay/live modes; atomic writes; no pyspark import
- `src/etherlens/stream/stream_job.py` — readStream; Q1 tumbling 1-min windows; Q2 sliding 5-min/1-min; Q3 foreachBatch with CMS/HLL/Bloom/Reservoir sketches; z-score anomaly detection (ALERT_Z=3.0); writes latest_tumbling.json, latest_sliding.json, alerts.json, sketch_metrics.json

**Commands to reproduce:**
```
docker compose run --rm dev bash scripts/stream_local.sh   # mock mode, 10min
```

**Key numbers:** Q1/Q2/Q3 queries run in parallel; sketch state is module-level (driver-side).

**Deviations from spec:**
- `approx_count_distinct` used for streaming HLL (exact countDistinct not supported in streaming).

---

## Phase 4 — Gold aggregates, MapReduce, PageRank, KMeans, analytics

**Date:** 2026-10-02

**What was built:**
- `src/etherlens/batch/gold_aggregates.py` — daily_stats, hourly_gas, hour_of_day_profile, top_senders, whales, address_features (954 rows, min_tx=3)
- `src/etherlens/batch/count_per_sender_spark.py` — RDD textFile → map → reduceByKey → saveAsTextFile; spark_count_timing.json
- `src/etherlens/batch/pagerank.py` — verbatim class algorithm; 10 iterations; checkpoint every 5; top-100 central addresses written to gold/central_addresses.parquet
- `src/etherlens/batch/kmeans_anomaly.py` — log1p features; VectorAssembler→StandardScaler→KMeans; silhouette k-selection (4–10); PCA(2) viz; rare_cluster + far_from_centroid flags; 14 flagged addresses on tiny preset
- `src/etherlens/batch/analytics_types.py` — descriptive/diagnostic/prescriptive/predictive; LinearRegression; 80/20 chronological split; analytics_types.json
- `src/etherlens/batch/daily_report.py` — per-dt report.txt with sorted summary/top_sender/whale/flagged lines; floats %.6f
- `mapreduce/` — Hadoop streaming mapper/reducer; run_local.sh

**Commands to reproduce:**
```
docker compose run --rm dev make gold
docker compose run --rm dev make pagerank
docker compose run --rm dev make kmeans
docker compose run --rm dev make analytics
docker compose run --rm dev make daily-report
docker compose run --rm dev make merkle
```

**Key numbers (tiny preset, 3 days):**
- address_features: 954 rows
- PageRank top address: 0xead101875b9bc92a565dc9eda76530b6490125ba, rank 1.00
- KMeans best k=8, silhouette=0.3445, flagged=14 addresses
- Analytics: RMSE 0.5906 (R²=0.9512), cheapest hour 04:00 UTC, 57.1% savings
- Merkle: 3 trees built, 92 leaves each

**Deviations from spec:**
- PageRank does NOT overwrite address_features (would leave directory empty due to Spark local mode + Docker volume rename limitation). Instead writes top-100 to gold/central_addresses.parquet; kmeans joins it in. is_central semantics preserved.

---

## Phase 3 — Silver pipeline

**Date:** 2026-10-02

**What was built:**
- `src/etherlens/batch/silver.py` — reads bronze; Spark accumulators (bad_rows, contract_creations); cross-checks with DataFrame aggregates; dropDuplicates(["hash"]); filters receipt_status==1; derives dt/hour/dow/value_eth/gas_price_gwei/fee_eth/is_plain_transfer/is_contract_creation; partitioned write; edge CSV
- `tests/test_silver.py` — 8 tests (all passing)

**Commands to reproduce:**
```
docker compose run --rm dev make mock-tiny && docker compose run --rm dev make silver
docker compose run --rm dev pytest tests/test_silver.py -v
```

**Key numbers (tiny preset):**
- Rows in: 9,255 | Rows out: 9,072 | Days: 3 | Bronze bytes: 1,576,095
- Bad rows: 3, duplicates: 8, failed tx: 172, contract creations: 19
- Silver runtime: ~20s on local[2]
- All 8 tests pass in 33.9s

**Deviations from spec:**
- None.

---

## Phase 0 — Development environment

**Date:** 2026-10-02

**What was built:**
- `.gitattributes` (LF enforcement)
- `Dockerfile` (python:3.11-slim-bookworm + Java 17 + gcloud CLI + Foundry)
- `docker-compose.yml` (dev, chain, dashboard services + 3 named volumes)
- `.dockerignore`, `.gitignore`, `.env.example`, `.env`
- `CLAUDE.md` (operating rules)
- Full repository skeleton: `src/etherlens/`, `jobs/`, `mapreduce/`, `hive/`, `sql/`, `contracts/`, `infra/`, `scripts/`, `dashboard/`, `notebooks/`, `tests/`, `docs/`, `data/`, `results/`
- `src/etherlens/config.py`, `spark_session.py`, `io_utils.py`, `schemas.py`
- `scripts/check_env.sh`
- `Makefile` with all targets from the spec

**Commands to reproduce:**
```
docker compose build
docker compose run --rm dev make check-env
docker compose run --rm dev pytest
docker compose up -d chain
docker compose run --rm dev cast block-number --rpc-url http://chain:8545
docker compose down
```

**Deviations from spec:**
- Foundry installation method changed: the spec says `curl -L https://foundry.paradigm.xyz | bash && foundryup`, but `raw.githubusercontent.com` and `release-assets.githubusercontent.com` return `SSL_ERROR_SYSCALL` inside Docker build layers on this machine (GitHub CDN is blocked in the Docker build network). Fix: multi-stage build — copy forge/anvil/cast/chisel from the official `ghcr.io/foundry-rs/foundry:latest` image instead. Result is identical: same binary versions (1.8.1), same PATH entry.
- `docker-compose.yml` `build.network: host` added for the dev service (later found not needed for image pulls, but harmless).
- All acceptance checks pass: check-env green, pytest 11 skipped (0 failures), anvil returns block 0, docker compose down clean.

---

## Phase 8 — Google Cloud Dataproc (real data)

**Date:** 2026-10-10

**Scope agreed with the user:** Stage B (mock smoke test on the cluster) + Stage C (one month of real data, Jan 2025). Stage D (three months) not run. GCP project `bda-505305` (free trial, ~$300 credits), bucket `gs://etherlens-parvmittal` (US).

**What was built / changed:**
- `scripts/run_gcp_batch.sh` (+ `make gcp-batch`) — submits the whole batch chain in spec order (silver → gold → Hive → MapReduce ×2 → Spark count → PageRank → KMeans → analytics → daily report) and records each job's wall-clock time in `logs/gcp_job_times_*.tsv`. `SKIP="gold hive"` resumes a chain without re-running finished steps.
- `src/etherlens/io_utils.py` — cloud paths now go through Hadoop's FileSystem API (GCS connector on Dataproc, no extra Python libs). Added `makedirs`, `delete`, `size_bytes`, `is_cloud`.
- All batch jobs and the streaming job write their result files through `io_utils` (see deviations).

**Commands to reproduce:** (all from the host, project root)
```
# one-time: reuse the host's gcloud login inside the container (Linux host)
docker run --rm -v "$HOME/.config/gcloud:/src:ro" -v project_gcloud-config:/dst alpine sh -c 'cp -a /src/. /dst/'
docker compose run --rm dev bash infra/gcp_bootstrap.sh
docker compose run --rm dev bash infra/upload_code.sh
docker compose run --rm dev bash infra/create_cluster.sh            # billable, ~$0.27/h
# Stage B (mock)
docker compose run --rm dev make mock
docker compose run --rm dev bash -c 'gcloud storage rsync -r data/bronze/tx gs://$GCS_BUCKET/bronze_mock/tx && gcloud storage cp data/mock/manifest.json gs://$GCS_BUCKET/mock/'
docker compose run --rm dev bash -c 'bash scripts/run_gcp_batch.sh gs://$GCS_BUCKET/bronze_mock/tx'
# Stage C (real data, Jan 2025)
docker compose run --rm dev bash -c 'python scripts/render_template.py sql/check_range.sql | bq query --use_legacy_sql=false --location=US'
docker compose run --rm dev make gcp-estimate                         # dry run, shows bytes
docker compose run --rm dev make gcp-export                           # writes gs://$GCS_BUCKET/bronze/tx
docker compose run --rm dev make gcp-batch
docker compose run --rm dev make gcp-bench
docker compose run --rm dev bash infra/delete_cluster.sh
```

**Deviations from spec (and why):**
1. *Host is Linux, not Windows.* gcloud was already installed and logged in on the host, so its config was copied into the `gcloud-config` Docker volume instead of running the two interactive `gcloud auth login` commands. All GCP commands still run inside the container.
2. *`bigquery-public-data.crypto_ethereum.transactions` is now a VIEW* (the whole dataset was converted to views on 2026-09-01; definitions hidden). All 13 Appendix B columns exist with the expected types, and partition pruning still works through the view: dry run for Jan 2025 = 9.94 GB, for Jan–Mar 2025 = 28.77 GB.
3. *Dataproc 2.2 gives nodes internal IPs only by default.* The first cluster failed: the init action could not reach PyPI ("Network is unreachable") and SSH had no route. Added `--public-ip-address` to `create_cluster.sh`.
4. *Boot disks are `pd-balanced`* instead of the default `pd-standard`: a 50 GB pd-standard disk gives ~6 MB/s, far too slow for Spark shuffles (gcloud warns about this). Cost difference is cents.
5. *`openssh-client` added to the Dockerfile* — `gcloud compute ssh` (MapReduce output cleanup, poller scripts) needs it. SSH user is `etherlens@` because the container runs as root and GCE refuses root logins; HDFS cleanup uses `sudo hdfs dfs`.
6. *Hadoop Streaming jar* is at `/usr/lib/hadoop/hadoop-streaming.jar` on image 2.2 (not `/usr/lib/hadoop-mapreduce/`).
7. *`make package` uses Python's `shutil.make_archive`* — `zip` is not in the image.
8. *Result files were silently lost on the cluster.* gold, Spark count, PageRank, KMeans, analytics and daily report wrote JSON/CSV/report files with plain `open()` on `gs://` paths, which lands in the driver's temp directory. They now use `io_utils.write_json/write_text`. Existence checks on `gs://` (`os.path.isdir`) were always False, so KMeans never joined `central_addresses` and the daily report never joined `flagged_addresses`; they now check `<dir>/_SUCCESS` through `io_utils.exists`.
9. *`config.py`* imports `python-dotenv` only if installed (it is not on Dataproc).
10. *Merkle trees for GCP data are built locally.* `eth_utils` is not on the cluster, so `daily_report` only runs the Merkle CLI for `--env local`. After pulling, run `python -m etherlens.chain.merkle build --all --gold data_gcp/gold --results results_gcp` (new `--gold/--results` flags).
11. *Silver performance.* (a) The deduplicated rows are cached, so the dedup shuffle runs once instead of once per action. (b) `repartition("dt")` before the partitioned write → one Parquet file per day instead of 200 (one per shuffle partition); each small file costs a slow rename on GCS. Stage B silver went from 888 s to 210 s. (c) The edge CSV is `coalesce(32)`-ed so MapReduce gets ~block-sized input files instead of 200 tiny ones. (d) `bronze_bytes` is now measured on GCS too (was -1).
12. *Gold* counts rows from the written Parquet instead of re-running every aggregation.
13. *KMeans far-from-centroid flag never fired:* `approxQuantile(..., [0.995], 0.01)` — a 1% rank error on a 99.5th percentile can return the maximum, so `dist > threshold` flagged nobody (small preset: 0 flagged). Relative error is now 0.001 → 114 flagged on the small preset (0.6%, as designed). KMeans also caches the clustered table it reuses ~6 times.
14. *Spark count* deletes its output directory first (the 3× benchmark failed on the 2nd run otherwise).
15. *MapReduce benchmark* now compares MR **with combiner** against Spark `reduceByKey` (both combine map-side); before, MR time covered two jobs (both variants) vs one Spark job. `run_dataproc.sh` takes an optional variant argument. `parse_mr_counters.py` variant names fixed (`no_combiner`/`with_combiner`, was `no`/`with`).
16. `make gcp-hive` saves the query output to `logs/hive_queries_*.log`. `.gitignore` now also ignores `data_gcp/` and `results_gcp/`.
17. *Cluster shape for real data.* Stage B ran on the spec's 1 + 2 × e2-standard-2. On real data that shape kept losing executors to memory pressure even after deviation 20 (6 more losses in 8 minutes), so silver never finished. With the user's approval Stage C used **e2-standard-4 master + 4 × e2-highmem-4 workers** (20 vCPU, 128 GB on workers; ~$1.1/h; this free-trial account's quota is 32 vCPU / 24 E2 vCPU, not 8). The shape is now set in `.env` (`MASTER_TYPE`, `WORKER_TYPE`, `NUM_WORKERS`, `SPARK_PROPS`); `.env.example` keeps the spec defaults. Executors: 2 × (2 cores, 9 g heap + 3 g overhead) per worker = 24 GB of the 28 GB YARN gives, leaving ~8 GB per node for the OS and off-heap memory (Dataproc's own default would fill all 28 GB). Zero executors lost in Stage C.
18. *Daily report tie-breaking.* Top-50 senders / top-20 whales are now ordered by `(value desc, address asc)`. Before, which of several senders tied at the cut-off made the list depended on partition order, so the cloud and local reports (and their Merkle roots) differed for the same data. Verified: reports are now identical with 8 vs 3 shuffle partitions (except `median_gas_gwei`, see below).
19. *Stage B wipe skipped.* The spec says to delete `silver/`, `gold/`, `results/` in the bucket after Stage B; the delete was blocked by the Claude Code permission policy. Not needed in practice: Stage C covers 2025-01-01..31, a superset of the mock dates, silver overwrites whole `dt` partitions and every gold/result path is written in overwrite mode. Stage B outputs were pulled to `results_gcp/stage_b_mock/` first.
20. *Executor layout (`infra/submit.sh`).* On real data the first Stage C silver run lost 9 executors in 10 minutes ("Container killed on request. Exit code is 143"). The worker's `earlyoom` log showed available memory falling to 3.5 % (Dataproc memory protection): the default layout of two 1-core executors with 2.9 GB heap on each 8 GB worker leaves no room for off-heap memory. Jobs are now submitted with `spark.executor.cores=2, spark.executor.memory=3g, spark.executor.memoryOverhead=1g` (one 2-core executor per worker; same CPU count, one JVM's overhead instead of two). Available memory during silver went to ~47 % and no executors were lost. `SPARK_PROPS=...` overrides it.

### Stage B — mock smoke test on the cluster (small preset, 14 days)

Acceptance: row counts and key results equal the local run — **passed**.

| Check | Local | Cluster |
|---|---|---|
| silver rows in → out | 534,166 → 522,866 | 534,166 → 522,866 |
| bad / dup / failed / contract creations | 46 / 527 / 10,727 / 1,310 | same |
| days, edge_count, bronze bytes | 14, 521,583, 73,795,380 | same |
| gold daily_stats / hourly_gas / top_senders / whales | 14 / 336 / 1000 / 100 | same |
| PageRank top address, rank | `0xd6cd…cf82` (hub), 189.423158 | same; `pagerank_top.csv` byte-identical |
| analytics: cheapest hour, corr, savings vs peak, RMSE / R² | 03 UTC, 0.8293, 56.9 %, 0.208 / 0.9957 | same |
| MapReduce map input = edges; distinct senders | — | 521,583; 19,794 (both variants) |
| address_features (MIN_TX 3 local, 5 GCP — by design) | 19,275 | 18,287 |
| KMeans best k / silhouette / flagged (5 % sample on GCP — by design) | 6 / 0.354 / 114 | 4 / 0.368 / 109 |

Expected small differences: `median_gas_gwei` (12.612 vs 12.614) comes from `percentile_approx`, whose answer depends on how rows are partitioned.

MapReduce counters (Stage B): no combiner — reduce input 521,583 records, shuffle 24.5 MB; with combiner — combine 521,583 → 257,491, reduce input 257,491, shuffle 12.1 MB (combiner halves the shuffle). 32 map tasks after the `coalesce(32)` fix (200 before; one no-combiner run took >15 min on 200 tiny files vs ~3.5 min after).

Stage B job times (s, cluster 1 + 2 × e2-standard-2): silver 888 → 194 after the one-file-per-day fix; gold 319; Hive 1296 (5 queries over the old 2,800-file silver); MapReduce both variants 444; PageRank 118; KMeans 218; analytics 95; daily report 139.

### Stage C — real data, January 2025 (2026-10-11)

Input: `bigquery-public-data.crypto_ethereum.transactions`, 2025-01-01 … 2025-01-31. `check_range`: 31 days, 1.04 M – 1.40 M tx/day. Export: BigQuery billed 9.93 GB (free tier) → 117 Parquet files, **7.02 GB** bronze in `gs://etherlens-parvmittal/bronze/tx`.

Whole batch chain: **41 min** on 1 × e2-standard-4 + 4 × e2-highmem-4 (07:03 → 07:44 UTC).

| Job | Wall-clock (s) | Key output |
|---|---|---|
| silver | 340 | 37,865,791 rows in → 37,323,780 out; bad 0, duplicates 0, failed 542,011, contract creations 50,601; 31 days |
| gold | 285 | daily_stats 31, hourly_gas 744, address_features 1,435,758 (MIN_TX = 5) |
| Hive (5 queries) | 566 | output in `results_gcp/hive/queries_output.log` |
| MapReduce ×2 | 350 | 37,273,390 edges → 7,151,324 distinct senders; combiner: shuffle 1.75 GB → 0.94 GB (−46 %), reduce input 37.3 M → 20.1 M records |
| Spark count | 87 | `saveAsTextFile` action 51.9 s |
| PageRank (last 7 days) | 142 | #1 `0xdac17f…1ec7` (USDT), #2 `0xa0b869…eb48` (USDC) |
| KMeans | 301 | best k = 10, silhouette 0.278, 7,417 flagged (0.5 %) |
| analytics | 80 | cheapest hour 04:00 UTC, 68.4 % cheaper than peak (15:00 UTC); hourly tx↔gas corr 0.273 |
| daily report | 263 | 31 reports; Merkle trees built locally: 2,622 – ~3,000 leaves per day |

Honest caveats (useful for the viva):
- *Predictive model loses to the baseline on real data:* next-hour gas RMSE 2.64 vs 2.24 for "next hour = this hour" (R² 0.56). On mock data it won easily. Real gas prices behave close to a random walk, so persistence is a strong baseline — report this as a finding, not a failure.
- *KMeans structure is weak:* silhouettes for k = 4…10 all lie between 0.24 and 0.28 and the best k is at the top of the search range, so the "best k" is not sharply defined. The far-from-centroid flags are still meaningful as outliers.
- `median_gas_gwei` uses `percentile_approx` and can differ slightly between runs with different partitioning.

**Pull and local post-processing (as run):**
```
docker compose run --rm dev bash -c 'gcloud storage rsync -r gs://$GCS_BUCKET/results results_gcp --exclude="mapreduce/.*"'
docker compose run --rm dev bash -c 'for t in daily_stats hourly_gas hour_of_day_profile top_senders whales pagerank central_addresses cluster_viz flagged_addresses daily_report; do gcloud storage rsync -r gs://$GCS_BUCKET/gold/$t data_gcp/gold/$t; done'
docker compose run --rm dev python scripts/parse_mr_counters.py results_gcp/mapreduce_counters.json
docker compose run --rm dev python -m etherlens.chain.merkle build --all --gold data_gcp/gold --results results_gcp
docker compose run --rm dev python scripts/make_figures.py --root results_gcp --gold data_gcp/gold
docker compose run --rm dev python scripts/make_summary.py --results results_gcp --gold data_gcp/gold
docker compose run --rm dev bash infra/delete_cluster.sh
```
Produced: `results_gcp/summary.md`, 6 figures in `results_gcp/figures/`, 31 Merkle files in `results_gcp/merkle/`, `gcp_job_times.tsv`, `mapreduce_counters.json`.

### Remaining Phase 8 items (2026-10-11, second cluster session, ~45 min)

Same cluster shape. Silver and PageRank were re-run first (HDFS edges are lost with each cluster; their Spark DAGs are what 8.8 screenshots want) — identical results to the morning run.

- **MR vs Spark benchmark** (`make gcp-bench`, MR with combiner vs Spark `reduceByKey`, 37.27 M edges): MapReduce 137 / 167 / 144 s, Spark 71 / 78 / 74 s → median **144 s vs 74 s, Spark 1.94× faster**. `results_gcp/bench_mr_vs_spark.json`. The script crashed after the runs (pre-existing bug: times were pasted into Python as `[175 205 178]` without commas); fixed, and the six durations were taken from the Dataproc job records (submit → DONE) so both engines are measured the same way.
- **Streaming on the cluster (8.9):** mock poller on the master in tmux (`infra/start_poller.sh mock`, 806 blocks), Structured Streaming job for 6 minutes. 124,800 tx through the sketches; HyperLogLog 17,712 vs 17,714 exact (0.01 %); Count-Min 54 KB, 100 % of counts within the error bound; Bloom filter loaded with the 7,417 real KMeans-flagged addresses; 20 tumbling windows, 12 alerts (gas spikes, flagged addresses). Outputs in `results_gcp/stream/`.
- **Jupyter showcase notebook (8.10):** `notebooks/cluster_showcase.ipynb` (daily activity, partition-pruning `explain()`, gas-by-hour chart, PageRank, KMeans clusters). Validated by running its code as a Dataproc job on the real data; also copied to the cluster's Jupyter folder `gs://<dataproc-staging-bucket>/notebooks/jupyter/`, so it shows up in Jupyter on any new cluster.
- **Screenshots (8.8): taken on 2026-10-13** (separate ~30 min session: cluster + silver + PageRank re-run so the Spark History Server has their DAGs; identical results again). 9 images in `results/screenshots/`: `yarn_nodes`, `yarn_apps`, `hdfs_overview`, `hdfs_blocks`, `spark_silver_jobs`, `spark_silver_dag`, `spark_pagerank_jobs`, `spark_pagerank_dag`, `jupyter_notebook`. Shown in the dashboard's Big Data Internals tab (it now reads `results/screenshots/` for both data sources). The executed notebook (all 6 cells, no errors, with the gas-by-hour chart) is saved as `notebooks/cluster_showcase_executed.ipynb`.
  - *Component Gateway links gave HTTP 502 at Google's `/_signin` step in the user's browser.* The cluster side was healthy (gateway and Knox services running; the YARN URL returned 200 with the project account's OAuth token), so the cause is browser sign-in — typically a browser profile signed in with a different or several Google accounts. Fixes: open the links in a window signed in only as the project's account, or use an SSH tunnel (no Google sign-in), which was set up as a fallback:
    ```
    docker compose run --rm -p 127.0.0.1:8088:8088 -p 127.0.0.1:9870:9870 -p 127.0.0.1:18080:18080 dev \
      bash -c 'gcloud compute ssh "etherlens@${CLUSTER_NAME}-m" --zone="${GCP_ZONE}" --quiet -- -N \
        -L 0.0.0.0:8088:localhost:8088 -L 0.0.0.0:9870:localhost:9870 -L 0.0.0.0:18080:localhost:18080'
    # then open http://localhost:8088 (YARN), http://localhost:9870 (HDFS), http://localhost:18080 (Spark History)
    ```

Fixes made in this session (deviations, continued):
21. *Streaming sketch query crashed* on its first batch: `row.get(...)` — Spark `Row` has no `.get()`. Now `row["..."]`.
22. *Streaming checkpoints moved to HDFS* (`/etherlens/stream_checkpoints`, as spec 8.3 says) and the stream job uses 8 shuffle partitions. On GCS with 200 partitions a 5 s micro-batch took 67–86 s; now ≤ 16 s, and only while catching up a backlog. The path has no scheme because Structured Streaming rejects `hdfs:///…` ("Uri without authority").
23. *tmux is not in the Dataproc 2.2 image*; `infra/init_actions.sh` now installs it on the master (needed by `start_poller.sh`).
24. *Dashboard:* (a) crashed on the Anomalies tab — `plotly` was never in `requirements.txt`; the PCA scatter now uses Streamlit's built-in `st.scatter_chart` (flagged addresses drawn larger), so the Live/Blockchain/Internals tabs render again; (b) the `gcp` data source pointed at `gs://` paths but reads local files, so it showed nothing — it now reads the pulled copies `results_gcp/` and `data_gcp/gold/`; (c) the Chain dropdown was ignored (`get_w3()` reads `CHAIN` from the environment) — it now sets it; (d) the Blockchain tab lists only days whose root is published on the selected chain. Smoke-tested headlessly with `streamlit.testing` (all 7 tabs, both sources, no exceptions).
25. *Chain tools for real data:* `deploy`, `publish`, `verify` take `--results` (the real-data Merkle trees are in `results_gcp/merkle`); the `sepolia-*` Make targets pass `--results results_gcp`. Tx hashes are stored with `Web3.to_hex` (`.hex()` drops the `0x` in hexbytes ≥ 1, which broke Etherscan links). New `make sepolia-check` (RPC chain id, deployer address, balance — never prints the key). `publish` writes `results_gcp/chain/sepolia_links.md` automatically on Sepolia. Rehearsed the whole flow on a local anvil chain with the real Jan 2025 trees: 3 roots published (~105 k gas each), a real row verifies `True`, the same row tampered verifies `False`.
26. `make_summary.py` also reports gold table sizes, job runtimes, MapReduce counters, the benchmark and streaming sketch metrics when those files exist.

**Not done in Phase 8:** Stage D (3 months; outside the agreed scope).

Cost of Phase 8 so far: ≈ $3–4 of the free-trial credits (cluster hours; BigQuery scans were inside the free tier). Cluster deleted after every session; the bucket holds bronze (real + mock), code, silver/gold/results and stream files (a few GB, cents per month).

---

## Phase 9 — Sepolia testnet

**Date:** 2026-10-13

**What was done:**
- User set up a dev-only MetaMask account, an Alchemy RPC URL and 0.05 SepoliaETH (free faucet) in `.env`.
- `ReportRegistry` deployed to Sepolia; Merkle roots of the **3 most recent real days** (2025-01-29, -30, -31; real Jan 2025 data from Phase 8) published; rows verified on-chain.
- Links: `results_gcp/chain/sepolia_links.md` (written automatically by `publish`). Deployment/publish records: `results_gcp/chain/deployment_sepolia.json`, `published_sepolia.json`.

**Commands:**
```
docker compose run --rm dev make sepolia-check
docker compose run --rm dev make sepolia-deploy
docker compose run --rm dev make sepolia-publish
docker compose run --rm dev make sepolia-verify DT=2025-01-31 IDX=2958
docker compose run --rm dev make sepolia-verify DT=2025-01-31 IDX=2958 TAMPER=--tamper
```

**Key numbers:**
- Contract: [`0x3d6707d3Ca4d5b7F2A39B0FD6b3B68e561E1964A`](https://sepolia.etherscan.io/address/0x3d6707d3Ca4d5b7F2A39B0FD6b3B68e561E1964A) (deploy gas 5,010,731).
- Publish gas: 573,630 (first root; first writes to empty storage cost more), then 377,828 and 377,840 for trees of 2,942 and 3,029 leaves — gas does not grow with the number of rows, because only the 32-byte root is stored.
- Verify: `2025-01-31|summary|tx_count|1172912` (index 2958) → **True**; same row with the last digit changed → **False**; also `flagged_active` rows on 2025-01-29 and -31 → True. `verifyRow` is a free `view` call.
- Dashboard (data source `gcp`, chain `sepolia`): Blockchain tab lists the 3 published days; "✓ Verified on-chain" for a real row, "✗ Verification failed" with "Tamper" ticked (checked headlessly with `streamlit.testing`).
- Cost: 0.0000063 SepoliaETH (gas price ~0.001 gwei); 0.04999 left. ₹0 spent.
- **Acceptance:** real-data row verifies `true` on Sepolia; tampered row `false`; dashboard Blockchain tab works with `CHAIN=sepolia` — **passed**.

**Deviations / notes:**
1. *Wrong network in the first `.env`:* `SEPOLIA_RPC_URL` was the Alchemy **mainnet** URL (`eth-mainnet…`), so `sepolia-check` showed chain id 1 and balance 0 (the faucet funds were on Sepolia all along). Fixed by switching the host to `eth-sepolia.g.alchemy.com` (same key). To prevent a transaction ever going to mainnet by mistake, `chain/common.get_w3()` now **refuses to run** with `CHAIN=sepolia` unless the RPC reports chain id 11155111 (tested against a non-Sepolia chain).
2. Gas used on Sepolia is higher than on the local anvil chain for the same calls (anvil: ~105 k per publish) because the networks run different gas schedules; the "constant gas regardless of leaf count" result holds on both.
3. Contract source verification on Etherscan (P2, needs an Etherscan API key) not done.

---

## Phase 7 (completion) — missing spec items found and filled

**Date:** 2026-10-13

Earlier phases were marked done, but checking `PROJECT_SPEC.md` §4.6 and §7 against the outputs
showed gaps. Filled now:

- **KMeans mock evaluation** (`kmeans_anomaly.py` → `_evaluate_planted()`): precision/recall of the
  flagged set against the planted bots, wash ring and whales, written into
  `results/kmeans_eval_mock.json` (local/mock runs only; real data has no ground truth).
  Small preset: **recall 1.00** (29 of 29; bots 1.0, ring 1.0, whales 1.0; acceptance ≥ 0.8),
  precision 0.25 (flag budget ~0.5% = 114 addresses vs 29 true anomalies, so 0.25 is the ceiling).
- **Figures** added to `scripts/make_figures.py`: `mr_combiner_shuffle.png`, `hadoop_vs_spark.png`,
  `predictive_vs_baseline.png`, `sketch_cms_error.png`, `sketch_hll_error.png`; `gas_vs_rows.png`
  now has data. 10 figures per dataset (the MapReduce ones exist only for the cloud run; the
  per-minute HLL and gas-vs-rows ones only locally).
- **Gas experiment** (`publish.py --gas-experiment`, local chain): 10 rows 139,542 gas (first
  publish, fresh storage), 1,000 rows 105,428, 100,000 rows 105,464, 1,000,000 rows 105,476 →
  `results/chain/gas_vs_rows.json`. Foundry gas snapshot → `results/chain/gas_snapshot.txt`.
- **Stream job** records the top 50 Count-Min estimates and exact distinct senders per minute
  (to check Spark's `approx_count_distinct` against the truth).
- **Live mainnet poller check**: 10 real blocks (26183718–26183727), 2,855 tx, exactly 12.0 s
  apart → `results/stream_live_check.json`.
- **Local Hive**: `scripts/run_hive_local.py` read `build/hive/*.hql`, the copies already rendered
  for GCP (gs:// paths), and wrote an empty `q0.txt` for the `USE` statement. Now reads the
  templates in `hive/` and writes `results/hive/q1..q5.txt`.
- **`scripts/stream_local.sh` ignored its mode argument** (`make stream-replay` ran mock). It now
  supports `mock | replay | live` and starts from a clean stream folder.
- **Summaries** now include combiner reduction %, MR-vs-Spark speedup, mock precision/recall, the
  live check, gas-vs-rows and Sepolia Etherscan links.
- **Streamlit dashboard** (spec 7.3): precision/recall and cluster profiles on the Anomalies tab;
  MapReduce counters, combiner effect, Hadoop vs Spark and job runtimes on the Internals tab;
  Etherscan link per published day on the Blockchain tab. Screenshots are read from
  `results/screenshots/` for both data sources. Smoke-tested headlessly in both modes.
- **Docker image**: solc 0.8.24 is pre-installed in `/root/.svm` (forge downloaded it on every
  fresh container and failed when the download host was unreachable). Verified: a forced offline
  compile succeeds; `forge test` 9/9.

## Phase 10 — Documentation and presentation pack

**Date:** 2026-10-13

**What was built:**
- `README.md` — overview with headline results, docs index, architecture, repository layout,
  local quick start, cloud runbook, Sepolia steps, dashboards, troubleshooting (from problems
  actually hit), cost and safety.
- `docs/architecture.md` — diagram, where each part runs, medallion layers with real sizes, the
  V's, batch and streaming pipelines with timings, the oracle problem (integrity vs
  correctness), cluster configuration, design decisions.
- `docs/syllabus_mapping.md` — every row of the spec's two syllabus tables mapped to file →
  function (line) and the evidence to show; honest gaps; quick demo map.
- `docs/ml_explained.md` — KMeans and the gas regression from first principles (features,
  scaling, silhouette, flags, precision/recall on planted anomalies, chronological split,
  RMSE/MAE/R², baseline).
- `docs/demo_script.md` — 10-minute BDA and Blockchain demos with timings, commands and
  fallbacks (replay mode, offline overview page, local chain, Remix).
- `docs/viva_qa.md` — 41 short answers.
- `docs/report_outline_bda.md`, `docs/report_outline_blockchain.md` — section headings with the
  figures and numbers for each.
- **Project dashboard** `dashboard/overview.html` (built by `scripts/build_overview.py` from the
  result files; template `dashboard/overview_template.html`): one self-contained page, no
  external libraries, works offline. Headline numbers, pipeline, every analysis with hover
  tooltips and a table view, the source file under each chart, and an in-browser Merkle
  verifier (its Keccak-256 was checked against the standard test vectors and against
  `eth_utils`; all 6 sample rows verify against the Sepolia root, an edited row fails).
  Also published as a private claude.ai artifact.

**Commands:**
```
docker compose run --rm dev python scripts/build_overview.py   # rebuild the overview page
docker compose run --rm dev make figures summary               # mock outputs
docker compose run --rm dev python scripts/make_figures.py --root results_gcp --gold data_gcp/gold
docker compose run --rm dev python scripts/make_summary.py --results results_gcp --gold data_gcp/gold
```

**Not done:** Etherscan source verification (P2, needs an Etherscan API key); Kafka (P2);
Stage D (three months; outside the agreed scope).
