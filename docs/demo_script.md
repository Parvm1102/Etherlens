# Demo script

Two 10-minute demos: one for Big Data Analytics, one for Blockchain. Each step says what to show,
what to say, and what to do if something fails. Everything below works **without the cloud
cluster**: cloud results are already in `results_gcp/` and the screenshots in
`results/screenshots/`.

## Before the viva (15 minutes, the day before is better)

Run from the project folder on the laptop.

```bash
docker compose build                               # only if the image is missing
docker compose up -d chain dashboard               # local blockchain + Streamlit (http://localhost:8501)
docker compose run --rm dev make chain-local       # deploy + publish local roots, run chain tests
docker compose run --rm dev make sepolia-check     # Sepolia reachable? expect chain id 11155111
```

Open these and leave them in browser tabs:

1. `dashboard/overview.html` (double-click the file; works offline). Also available as a private
   claude.ai link.
2. http://localhost:8501 (Streamlit). Sidebar: **Data source = gcp**, **Chain = sepolia**.
3. https://sepolia.etherscan.io/address/0x3d6707d3Ca4d5b7F2A39B0FD6b3B68e561E1964A
4. `results/screenshots/` in the file browser.

Have a terminal open in the project folder.

**If the internet is down:** the overview page, the screenshots, the figures and the local chain
all work offline. Only Etherscan and the Sepolia verify need the network (fallbacks below).

---

## Demo A — Big Data Analytics (10 minutes)

| Time | Show | Say |
|---|---|---|
| 0:00 | `overview.html`, top: the six numbers | "Every Ethereum transaction from January 2025: 37.9 million rows, 7 GB, processed on a 5-node Hadoop/Spark cluster in 41 minutes." |
| 0:45 | `overview.html` → Pipeline | "Bronze is the raw export from BigQuery, never edited. Spark cleans it into silver, one Parquet file per day, and aggregates gold tables. Everything else reads gold. Streaming runs alongside." |
| 1:45 | `results/screenshots/yarn_nodes.png`, `hdfs_overview.png`, `hdfs_blocks.png` | "This is the real cluster: YARN with 4 worker nodes. HDFS stores the edge list in 128 MB blocks, each on two workers: replication factor 2." |
| 2:45 | `src/etherlens/batch/silver.py` (accumulators, `dropDuplicates`, `persist`, `repartition("dt")`), then `spark_silver_jobs.png` and `spark_silver_dag.png` | "Accumulators count bad rows inside an action. We cache after deduplication so the shuffle runs once. Job names you see in the Spark UI come from `setJobDescription`." |
| 3:45 | `mapreduce/mapper.py`, `reducer.py`; `overview.html` → Hadoop & Spark | "Map emits (sender, 1), reduce sums. The combiner pre-sums on each mapper: shuffle fell 46%, from 1.75 to 0.94 GB. Spark did the same count 1.94× faster, 74 s vs 144 s, because it keeps data in memory." |
| 5:00 | `hive/queries.hql`; Streamlit → Overview | "Hive gives SQL over the same Parquet files: an external partitioned table, schema-on-read." |
| 5:45 | `overview.html` → Gas | "Four analytics types. Descriptive: 1.2 M transactions a day. Diagnostic: busier hours cost more. Prescriptive: send at 04:00 UTC, 68% cheaper than 15:00. Predictive: our regression does not beat 'same as this hour' on real data; we report that honestly." |
| 7:00 | `overview.html` → Network; `spark_pagerank_dag.png` | "PageRank on RDDs, the class algorithm. USDT and USDC are the most central. Each iteration takes milliseconds because transformations are lazy; the work happens at the final action." |
| 7:45 | `overview.html` → Anomalies | "KMeans on 8 behaviour features, scaled. On mock data with 29 planted anomalies it found all 29. On real data it flags 7,417 outlying addresses." |
| 8:30 | **Live streaming** (below) or `overview.html` → Streaming | "Structured Streaming with 1-minute tumbling and 5-minute sliding windows. HyperLogLog counted 17,712 distinct senders against 17,714 exact, using kilobytes." |
| 9:30 | `overview.html` → Checks | "What we verified, and two honest limitations." |

### Live streaming step

```bash
docker compose run --rm --service-ports dev bash scripts/stream_local.sh mock
```

Then Streamlit → **Data source = local** → **Live** tab: windows, alerts and sketches refresh
every 5 s. The Spark UI is at http://localhost:4040. Stop with Ctrl-C.

- **Show real mainnet instead (needs internet):** put a mainnet RPC URL in `.env` as
  `ETH_RPC_URL=` (for Alchemy, the same key with `eth-mainnet` in the host), then run
  `bash scripts/stream_local.sh live`. A real block arrives every 12 seconds.
- **Fallback 1 (no internet):** `bash scripts/stream_local.sh replay` replays a day of local data.
- **Fallback 2:** `overview.html` → Streaming shows the cluster run's results and the live-block
  check (10 blocks, 12.0 s apart).

---

## Demo B — Blockchain (10 minutes)

| Time | Show | Say |
|---|---|---|
| 0:00 | `overview.html` → On-chain proof, first paragraph | "A contract cannot compute analytics over 37 million rows. That is the oracle problem: someone off-chain must publish. We make every published number checkable." |
| 1:00 | `results_gcp/merkle/2025-01-31.json`, a few `rows` | "Each day's report is sorted text rows. Each row is hashed with Keccak-256, pairs are hashed up to one 32-byte root." |
| 2:00 | `contracts/src/ReportRegistry.sol`: `Owned`, `publishRoot`, `AlreadyPublished`, `verifyRow` | "`ReportRegistry is Owned`: inheritance. Owner-only, append-only: the same day twice reverts. `verifyRow` recomputes the root from a row and its proof." |
| 3:30 | Etherscan tab: contract and the three publish transactions | "Deployed on Sepolia. Three real days published. Only the root goes on-chain: about 378,000 gas a day whether the report has 2,900 or 3,000 rows." |
| 4:30 | **Verify on Sepolia** in the terminal (below) | "The real row verifies true. Change one digit: false." |
| 6:00 | Streamlit → Blockchain (`gcp`, `sepolia`): pick 2025-01-31, row 2958, **Verify on-chain**; tick **Tamper**, verify again | Same check from the dashboard. |
| 7:00 | `overview.html` → "Check a report row yourself": edit the row, verify | "The browser does the same Keccak hash path itself. Anyone can check without trusting us." |
| 8:00 | `overview.html` → gas chart, or `results/figures/gas_vs_rows.png` | "10 rows or 1,000,000 rows: about 105,000 gas on a local chain. Storage cost is constant because only the root is stored." |
| 8:45 | `contracts/test/ReportRegistry.t.sol` and `docker compose run --rm dev make contracts-test` | "Foundry tests: golden vector, double publish reverts, non-owner reverts. 9 tests pass." |
| 9:30 | Wrap-up | "Integrity, not correctness: a root cannot be changed later, and since the source data is public anyone can rebuild the report and compare roots." |

### Verify on Sepolia

```bash
docker compose run --rm dev make sepolia-verify DT=2025-01-31 IDX=2958
docker compose run --rm dev make sepolia-verify DT=2025-01-31 IDX=2958 TAMPER=--tamper
```

Expected: `Row: 2025-01-31|summary|tx_count|1172912 … Result: True`, then the tampered row with
`Result: False`.

- **Fallback 1 (Sepolia RPC down):** the same contract on the local chain:
  `docker compose run --rm dev python -m etherlens.chain.verify --dt 2025-01-03 --index 0`
  (and with `--tamper`), which needs `docker compose up -d chain` and `make chain-local` first.
- **Fallback 2 (no network at all):** `overview.html` → "Check a report row yourself" runs fully
  in the browser against the stored root.
- **Fallback 3:** Remix (remix.ethereum.org) → paste `ReportRegistry.sol` → deploy in "Remix VM"
  → `publishRoot(20250131, <root>, 3029, "2025-01-31")` → `verifyRow` with a row and proof copied
  from `results_gcp/merkle/2025-01-31.json`.

---

## Likely follow-up questions

See `docs/viva_qa.md` (41 short answers) and `docs/syllabus_mapping.md` (where each syllabus
topic lives in the code).
