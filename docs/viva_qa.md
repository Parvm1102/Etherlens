# Viva questions and answers

Short answers you can say out loud, each tied to something in this project you can show.
Numbers are from the real January 2025 run unless marked "mock".

---

## Big data and Hadoop

**1. What is the project in one sentence?**
It processes every Ethereum transaction from January 2025 (37.9 million) on a Hadoop/Spark
cluster, analyses it in batch and in real time, and publishes a Merkle root of each day's report
to a smart contract so anyone can prove a published number was not changed.

**2. Why is this "big data"? 38 million rows fits on a laptop.**
One month is the slice we could afford to process; the source table holds billions of rows and
grows by about 1.2 million a day. The same code ran unchanged on a 5-node cluster, and it is the
techniques that matter for the course: distributed storage (HDFS, Cloud Storage), distributed
processing (MapReduce, Spark), streaming, and sketches that use kilobytes instead of a full table.

**3. What are bronze, silver and gold?**
Bronze is the raw export, never edited (7.02 GB Parquet). Silver is cleaned: typed,
deduplicated, failed transactions removed, one file per day (37.3 M rows). Gold is small
aggregate tables for analysis. If a bug is found, we rebuild from bronze. Show:
`docs/architecture.md`, section 3.

**4. Data lake or data warehouse?**
The bucket is a data lake: raw Parquet files, schema applied when read (Hive external tables are
schema-on-read). The gold tables play the warehouse role: clean, aggregated, query-ready.

**5. Why Parquet?**
It is columnar: a query that needs 3 of 13 columns reads only those 3. It is compressed per
column, stores the schema, and keeps min/max statistics so readers can skip data. The month was
7 GB as Parquet.

**6. What do HDFS and YARN do on the cluster?**
HDFS stores files as 128 MB blocks spread over the workers, each block copied to 2 workers
(replication factor 2 on our cluster; Hadoop's default is 3). If a worker dies, the copy is used.
YARN is the resource manager: it gives each job containers (CPU + memory) on the workers. Show:
`results/screenshots/hdfs_blocks.png` (block on w-2 and w-3) and `yarn_nodes.png`.

**7. Why is replication useful, and what does it cost?**
It gives fault tolerance and lets the scheduler run a task on a node that already holds the data
(data locality). It costs disk space (2× here) and network traffic when writing.

**8. Explain MapReduce with your job.**
Count transactions per sender. Map: read each edge line, emit (sender, 1). Shuffle: Hadoop groups
all pairs by sender and sends each group to one reducer. Reduce: add up the 1s. On real data,
37,273,390 map outputs became 7,151,324 sender counts. Code: `mapreduce/mapper.py`,
`mapreduce/reducer.py`.

**9. What does the combiner do, and how much did it help?**
It runs the reducer logic on each mapper's own output before the shuffle, so a mapper sends
(sender, 57) once instead of 57 × (sender, 1). Shuffled data fell from 1.75 GB to 0.94 GB
(−46%) and reduce input from 37.3 M to 20.1 M records. Show: `mr_combiner_shuffle.png`.

**10. Why is the combiner valid for a sum but not for a median?**
A combiner must be safe to apply to any subset in any order. Sums are associative and
commutative: the sum of partial sums is the total. A median of medians is not the median (the
median of {1,2,3} and {4,100} is 3, but the median of the medians 2 and 52 is 27). An average
needs the combiner to pass (sum, count) instead.

**11. Spark vs MapReduce: which was faster, and why?**
Spark: median 74 s; MapReduce with combiner: median 144 s, so Spark was 1.94× faster on the same
input. MapReduce writes map outputs to disk and starts a new JVM per task; Spark keeps
intermediate data in memory, reuses executors, and plans the whole job as one DAG. Show:
`hadoop_vs_spark.png`.

**12. What is an RDD, and what is lazy evaluation?**
An RDD is a distributed, immutable collection split into partitions. Transformations (`map`,
`join`, `reduceByKey`) only build a plan; nothing runs until an action (`saveAsTextFile`,
`count`, `top`). Evidence: in `pagerank_iters.json` each PageRank iteration "takes" 0.02–0.05 s,
because the loop only builds lineage; the real work runs at the final `top()` action.

**13. Narrow vs wide transformations?**
Narrow: each output partition needs one input partition (`map`, `filter`), so no data moves.
Wide: an output partition needs data from many partitions (`groupByKey`, `reduceByKey`, `join`),
so Spark shuffles data across the network and starts a new stage. In the Spark UI the DAG splits
into stages at each shuffle. Show: `spark_pagerank_dag.png`.

**14. Why `reduceByKey` instead of `groupByKey` for counting?**
`reduceByKey` combines values on each partition before the shuffle (like a combiner);
`groupByKey` ships every single value across the network. PageRank uses `groupByKey` once to
build the link lists, because the algorithm needs the whole list.

**15. Where do you use accumulators and broadcast variables?**
Accumulators in `silver.py`: count bad rows and contract creations while scanning, updated
inside an action (`foreach`), because updates inside transformations can be counted twice when a
task is retried. They are cross-checked against DataFrame counts. Broadcast in
`daily_report.py`: the 7,417 flagged addresses are broadcast to every executor, so the join with
a day's transactions needs no shuffle.

**16. What is partition pruning?**
Silver is stored as one folder per day (`dt=2025-01-15`). A query filtering on `dt` reads only
that folder. The Jupyter notebook shows `PartitionFilters: [dt = 2025-01-15]` in the plan. The
daily report reads one day at a time this way.

**17. What problems did you hit on the real cluster?**
(a) Small files: silver was first written as 200 files per day; every file costs a slow rename on
Cloud Storage, so we repartitioned by day (silver 888 s → 194 s on mock). (b) Memory: with two
executors packed onto each 8 GB worker, Dataproc's out-of-memory protection killed executors; we
moved to bigger workers and left headroom. (c) Nodes had no internet (internal IPs by default).
All are listed in `docs/PROGRESS.md`.

**18. What does Hive add?**
SQL over files in the lake: an external, partitioned table over the silver Parquet, five
HiveQL queries (daily activity, top senders, gas by hour, plain-transfer share, busiest hour
per day), run on Tez on the cluster. Show: `hive/queries.hql`, `results_gcp/hive/queries_output.log`.

## Streaming and sketches

**19. How does the streaming part work?**
A poller writes one file per block. Spark Structured Streaming reads the folder, groups
transactions by event time into 1-minute tumbling and 5-minute sliding windows, and feeds raw
rows to sketches. A 2-minute watermark lets it drop state for old windows. Live check: 10 real
mainnet blocks, exactly 12.0 s apart.

**20. Tumbling vs sliding windows?**
Tumbling windows do not overlap: 10:00–10:01, 10:01–10:02. Each event is in exactly one.
Sliding windows overlap: 5 minutes long, starting every minute, so each event is in 5 windows.
Sliding gives a smoother moving view.

**21. Why HyperLogLog instead of an exact distinct count?**
An exact distinct count must remember every address seen, and Spark does not support exact
`countDistinct` in streaming aggregations. HyperLogLog keeps a fixed set of registers (16,384 in
ours) and estimates the count from the longest run of leading zeros in hashes. Standard error is
1.04/√m ≈ 0.8%. Result on the cluster: 17,712 estimated vs 17,714 exact.

**22. Why does a Count-Min Sketch never underestimate?**
Each item increments one counter in each of d rows. Other items can collide into the same
counters and add to them, but nothing ever subtracts. So every counter is at least the true count,
and the estimate (the minimum over the rows) is too. With width e/ε, the overestimate is at most
ε·N with probability 1 − δ. Ours: bound 124.8, worst seen 34, 100% within bound, 54 KB.

**23. Why can a Bloom filter have false positives but no false negatives?**
Adding an item sets k bits. Checking an item looks at its k bits. If the item was added, all its
bits are set, so the answer is always "maybe yes": no false negatives. Other items may have set
the same bits by chance, so a never-added item can also look present: false positives, at a rate
we control with the filter size (target 1%).

**24. What is reservoir sampling for?**
Keeping a uniform random sample of fixed size (100) from a stream of unknown length: item i
replaces a random slot with probability s/i.

**25. What would a chain reorganisation (reorg) mean for the stream?**
Ethereum can replace the newest block or two. The poller stays 2 blocks behind the head, which
avoids most reorgs. A deeper reorg would leave an orphaned block's transactions in our counts;
for per-minute statistics that is a small, accepted error. A production system would track block
hashes and retract orphaned blocks.

## Machine learning and analytics

**26. How do you detect anomalies without labels?**
KMeans groups addresses by 8 behaviour features (log-scaled, then standardised). We pick k by
silhouette and flag addresses far from their cluster centre (top 0.5%) or in tiny clusters.
Real data: 7,417 flagged of 1,435,758. See `docs/ml_explained.md`.

**27. How do you know it works?**
On mock data we planted 8 bots, a 6-address wash ring and 15 whales. All 29 were flagged
(recall 1.00). Precision is 0.25 because the rule flags a fixed 0.5% budget (114 addresses) and
only 29 true anomalies exist, so 0.25 is the maximum possible.

**28. Your prediction model loses to the baseline. Is that a failure?**
It is a finding. Next-hour gas behaves almost like a random walk, so "same as this hour" is hard
to beat (RMSE 2.24 vs our 2.64). We tested on the last 20% of hours in time order, to avoid
leakage, and compared with a baseline; without that we would have reported R² = 0.56 as success.

**29. What are the four types of analytics in your project?**
Descriptive: 1.2 M transactions a day, 34% plain transfers. Diagnostic: busier hours have higher
gas (correlation 0.27). Predictive: next-hour gas regression. Prescriptive: send at 04:00 UTC,
68.4% cheaper than the 15:00 peak.

## Blockchain

**30. Why store a Merkle root and not the report itself?**
Storage on Ethereum is expensive and grows with size; a root is always 32 bytes. Publish gas was
about 105,000 for 1,000 rows and for 1,000,000 rows on a local chain, and about 378,000 per day
on Sepolia regardless of row count. Any row can still be proved with about 12 hashes
(log₂ of 3,000).

**31. How does verification work?**
Hash the row with Keccak-256 to get the leaf. Combine it with each sibling hash in the proof,
smaller one first, up the tree. If the result equals the stored root, the row is in the
report. `verifyRow` does this on-chain as a free read-only call. Change one digit and the root
differs: `2025-01-31|summary|tx_count|1172912` verifies true on Sepolia, the edited row false.

**32. Why sort the pair before hashing?**
So the proof does not need to say whether each sibling is on the left or right; the smaller hash
always goes first. The Python code (`chain/merkle.py`) and the contract use exactly the same rule.

**33. Keccak-256 vs SHA-3?**
Ethereum adopted Keccak before NIST finalised SHA-3; NIST then changed the padding byte (0x01 vs
0x06). Same algorithm otherwise, different outputs. Python's `hashlib.sha3_256` would not match
the contract, so we use `eth_utils.keccak`.

**34. What is the oracle problem and how do you handle it?**
A contract cannot fetch off-chain data or compute analytics over millions of rows, so a trusted
publisher must post results. We cannot make the publisher honest, but we make every published
number checkable: the report is deterministic and built from public data, so anyone can rerun it
and compare roots, and once published a root cannot be changed (append-only, `AlreadyPublished`).
It guarantees integrity, not correctness.

**35. What stops someone overwriting a published day?**
`publishRoot` is owner-only (`onlyOwner`, inherited from `Owned`) and reverts with
`AlreadyPublished` if the day already has a root. Tested in `contracts/test/ReportRegistry.t.sol`.

**36. What is gas, gwei and wei? Why 21,000?**
Gas measures computation; the sender pays gas used × gas price. 1 ETH = 10⁹ gwei = 10¹⁸ wei. A
plain ETH transfer always uses exactly 21,000 gas, so `gas_used == 21000` marks plain transfers
(34% of January's transactions); everything else calls a contract.

**37. EOA vs contract account, and what is the nonce?**
An externally owned account (EOA) has a private key and sends transactions; a contract account
has code and only acts when called. The nonce counts transactions an EOA has sent and prevents
replays. A huge nonce suggests a bot, which is why `max_nonce` is a KMeans feature.

**38. Why Sepolia and not mainnet?**
Same contract code and tools, public explorer, but test ether is free from faucets. The whole
project cost no real money. A safety check refuses to run if the RPC URL is not Sepolia.

**39. Where is Solidity inheritance?**
`contract ReportRegistry is Owned`: the `Owned` base contract holds `owner`, the `onlyOwner`
modifier and `transferOwnership`.

## Practical

**40. What did the cloud part cost?**
About $4 of free-trial credits in total (cluster hours). The BigQuery scan (9.93 GB) was inside
the free 1 TB per month. Clusters were deleted after every session.

**41. How would you scale to a year?**
Export 12 months (about 120 GB scanned, still inside the free 1 TB), add workers, keep one file per day in silver.
Gold and Merkle steps stay small. MapReduce and Spark scale out by adding nodes; HDFS and YARN
spread the work.
