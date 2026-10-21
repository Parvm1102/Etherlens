"""Generate results/summary.md — Phase 7.2."""
import argparse
import json
import os


def _read_json(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    with open(path) as f:
        return json.load(f)


def _fmt(v, decimals: int = 2) -> str:
    if v is None:
        return "N/A"
    if isinstance(v, float):
        return f"{v:.{decimals}f}"
    return str(v)


def run(results_dir: str = "results", gold_dir: str = "data/gold"):
    lines = ["# EtherLens — Results Summary", ""]

    # Dataset scale
    silver = _read_json(os.path.join(results_dir, "silver_stats.json"))
    if silver:
        lines += [
            "## Dataset Scale",
            f"- Rows in: {silver.get('rows_in', 'N/A'):,}" if isinstance(silver.get('rows_in'), int) else f"- Rows in: {silver.get('rows_in', 'N/A')}",
            f"- Rows out (silver): {silver.get('rows_out', 'N/A')}",
            f"- Days: {silver.get('days', 'N/A')}",
            f"- Bronze bytes: {silver.get('bronze_bytes', 'N/A')}"
            + (f" ({silver['bronze_bytes'] / 1e9:.2f} GB)" if isinstance(silver.get('bronze_bytes'), int) and silver['bronze_bytes'] > 0 else ""),
            f"- Removed: bad rows {silver.get('bad_rows', 'N/A')}, duplicates {silver.get('duplicates_removed', 'N/A')}, "
            f"failed tx {silver.get('failed_removed', 'N/A')}; contract creations {silver.get('contract_creations', 'N/A')}",
            f"- Silver runtime: {silver.get('runtime_s', 'N/A')}s",
            "",
        ]

    # Gold table sizes
    gold_stats = _read_json(os.path.join(results_dir, "gold_stats.json"))
    if gold_stats:
        lines += ["## Gold Tables (rows)"]
        lines += [f"- {k}: {v}" for k, v in gold_stats.items() if k != "runtime_s"]
        lines.append("")

    # Cloud job runtimes (written by scripts/run_gcp_batch.sh, copied next to the results)
    times_path = os.path.join(results_dir, "gcp_job_times.tsv")
    if os.path.exists(times_path):
        lines += ["## Cluster Job Runtimes (wall-clock incl. submission)"]
        with open(times_path) as f:
            for row in f:
                if row.strip():
                    name, secs = row.split("\t")
                    lines.append(f"- {name}: {int(secs)} s")
        lines.append("")

    # MapReduce counters and the MR vs Spark benchmark
    mr = _read_json(os.path.join(results_dir, "mapreduce_counters.json"))
    if mr:
        lines += ["## MapReduce Counters"]
        for variant, c in sorted(mr.items()):
            lines.append(f"- {variant}: map out {c.get('map_output_records')}, combine out "
                         f"{c.get('combine_output_records')}, reduce in {c.get('reduce_input_records')}, "
                         f"shuffle bytes {c.get('reduce_shuffle_bytes')}")
        try:
            cut = 1 - mr["with_combiner"]["reduce_shuffle_bytes"] / mr["no_combiner"]["reduce_shuffle_bytes"]
            lines.append(f"- Combiner reduces shuffle bytes by {cut * 100:.1f}%")
        except (KeyError, TypeError, ZeroDivisionError):
            pass
        lines.append("")
    bench = _read_json(os.path.join(results_dir, "bench_mr_vs_spark.json"))
    if bench:
        lines += [
            "## MapReduce vs Spark (count per sender)",
            f"- MapReduce (with combiner) median: {bench.get('mr_median_s')} s  runs={bench.get('mr_times_s')}",
            f"- Spark reduceByKey median: {bench.get('spark_median_s')} s  runs={bench.get('spark_times_s')}",
            f"- Speedup: Spark is {bench['mr_median_s'] / bench['spark_median_s']:.2f}x faster",
            "",
        ]

    # Analytics
    analytics = _read_json(os.path.join(results_dir, "analytics_types.json"))
    if analytics:
        d = analytics.get("descriptive", {})
        p = analytics.get("prescriptive", {})
        diag = analytics.get("diagnostic", {})
        pred = analytics.get("predictive", {})
        lines += [
            "## Analytics Results",
            "### Descriptive",
            f"- Total tx: {d.get('total_tx', 'N/A')}",
            f"- Avg tx/day: {d.get('avg_tx_per_day', 'N/A')}",
            f"- Total ETH: {_fmt(d.get('total_eth'))}",
            f"- Avg gas: {_fmt(d.get('avg_gas_gwei'), 3)} gwei",
            f"- Plain transfer share: {_fmt(d.get('plain_transfer_share'), 4)}",
            "",
            "### Diagnostic",
            f"- Hourly tx↔gas correlation: {_fmt(diag.get('hourly_tx_gas_corr'), 4)}",
            f"- {diag.get('interpretation', '')}",
            "",
            "### Prescriptive",
            f"- Cheapest hour: {p.get('cheapest_hour_utc', 'N/A'):02d}:00 UTC",
            f"- Savings vs peak: {p.get('savings_vs_peak_pct', 'N/A')}%",
            f"- {p.get('recommendation', '')}",
            "",
            "### Predictive",
            f"- RMSE: {_fmt(pred.get('rmse'), 4)} | MAE: {_fmt(pred.get('mae'), 4)} | R²: {_fmt(pred.get('r2'), 4)}",
            f"- Baseline RMSE: {_fmt(pred.get('baseline_rmse'), 4)}",
            f"- Beats baseline: {pred.get('beats_baseline', 'N/A')}",
            "",
        ]

    # PageRank
    pr = _read_json(os.path.join(results_dir, "pagerank_iters.json"))
    pr_csv = os.path.join(results_dir, "pagerank_top.csv")
    lines += ["## PageRank"]
    if pr:
        lines.append(f"- Top address: {pr.get('top_address', 'N/A')}")
        lines.append(f"- Top rank score: {_fmt(pr.get('top_rank'))}")
    if os.path.exists(pr_csv):
        with open(pr_csv) as f:
            rows = f.readlines()[1:6]
        lines.append("- Top 5:")
        for row in rows:
            lines.append(f"  - {row.strip()}")
    lines.append("")

    # KMeans
    km = _read_json(os.path.join(results_dir, "kmeans_eval_mock.json"))
    if km:
        lines += [
            "## KMeans Anomaly Detection",
            f"- Best k: {km.get('best_k', 'N/A')}",
            f"- Silhouette: {_fmt(km.get('best_silhouette'), 4)}",
            f"- Flagged addresses: {km.get('n_flagged', 'N/A')}",
        ]
        ev = km.get("mock_eval")
        if ev:
            lines += [
                f"- Mock ground truth: {ev['true_positives']}/{ev['planted_evaluable']} planted anomalies flagged "
                f"(recall {ev['recall']:.2f}; bots {ev['recall_bots']}, wash ring {ev['recall_wash_ring']}, "
                f"whales {ev['recall_whales']}); precision {ev['precision']:.2f}",
            ]
        prof = os.path.join(results_dir, "kmeans_clusters.csv")
        if os.path.exists(prof):
            lines.append("- Cluster profiles (mean raw features): see `kmeans_clusters.csv`")
        lines.append("")

    # Streaming sketches (Phase 5 locally, Phase 8.9 on the cluster)
    sk = _read_json(os.path.join(results_dir, "stream", "sketch_metrics.json"))
    if sk:
        hll, cms = sk.get("hll", {}), sk.get("cms", {})
        lines += [
            "## Streaming Sketches",
            f"- Transactions through the sketches: {sk.get('n_items', 'N/A')}",
            f"- HyperLogLog distinct senders: {hll.get('estimate')} est vs {hll.get('exact')} exact "
            f"(rel. error {hll.get('rel_error')})",
            f"- Count-Min: {cms.get('memory_bytes')} bytes, fraction within error bound {cms.get('fraction_within_bound')}",
            f"- Bloom filter: {sk.get('bloom', {}).get('n_inserted')} flagged addresses inserted",
            "",
        ]

    live = _read_json(os.path.join(results_dir, "stream_live_check.json"))
    if live:
        lines += [
            "## Live Mainnet Poller Check",
            f"- {len(live['blocks'])} live blocks, {live['tx_total']} transactions, mean gap {live['mean_gap_s']} s "
            f"(blocks {live['blocks'][0]['block']}..{live['blocks'][-1]['block']})",
            "",
        ]

    # Merkle / chain
    chain_dir = os.path.join(results_dir, "chain")
    merkle_dir = os.path.join(results_dir, "merkle")
    lines += ["## Blockchain / Merkle"]
    if os.path.isdir(merkle_dir):
        mfiles = sorted(f for f in os.listdir(merkle_dir) if f.endswith(".json"))
        lines.append(f"- Merkle trees built: {len(mfiles)}")
        for mf in mfiles[:3]:
            with open(os.path.join(merkle_dir, mf)) as f:
                md = json.load(f)
            lines.append(f"  - {md['dt']}: root={md['root'][:20]}… leaves={md['leaf_count']}")
    for chain in ["local", "sepolia"]:
        pub_path = os.path.join(chain_dir, f"published_{chain}.json")
        dep_path = os.path.join(chain_dir, f"deployment_{chain}.json")
        if os.path.exists(dep_path):
            with open(dep_path) as f:
                dep = json.load(f)
            lines.append(f"- Contract ({chain}): {dep.get('address', 'N/A')}")
        if os.path.exists(pub_path):
            with open(pub_path) as f:
                pub = json.load(f)
            lines.append(f"- Published roots ({chain}): {len(pub)}")
            if chain == "sepolia":
                base = "https://sepolia.etherscan.io"
                lines.append(f"  - Contract: {base}/address/{dep.get('address')}")
                for p in pub:
                    lines.append(f"  - {p['dt']}: root {p['root'][:18]}…, {p['leaf_count']} leaves, "
                                 f"{p['gas_used']:,} gas — {base}/tx/{p['tx_hash']}")
    gvr = _read_json(os.path.join(chain_dir, "gas_vs_rows.json"))
    if gvr:
        lines.append("- Publish gas vs report size (local chain): " +
                     ", ".join(f"{r['n_leaves']:,} rows → {r['gas_used']:,}" for r in gvr))
    lines.append("")

    out_path = os.path.join(results_dir, "summary.md")
    with open(out_path, "w", newline="\n") as f:
        f.write("\n".join(lines) + "\n")
    print(f"Summary written to {out_path}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--results", default="results")
    parser.add_argument("--gold", default="data/gold")
    args = parser.parse_args()
    run(args.results, args.gold)


if __name__ == "__main__":
    main()
