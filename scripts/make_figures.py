"""Generate result figures — Phase 7.1."""
import argparse
import json
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker


def _fig(title: str, out: str, fig=None, ax=None):
    if fig is None:
        fig, ax = plt.subplots(figsize=(10, 5))
    fig.suptitle(title, fontsize=12)
    plt.tight_layout()
    os.makedirs(os.path.dirname(out), exist_ok=True)
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"  {os.path.basename(out)}")


def _read_json(path: str):
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


def _load_parquet(path: str):
    import pyarrow.parquet as pq
    if not os.path.exists(path):
        return None
    return pq.read_table(path).to_pandas()


def run(results_dir: str = "results", gold_dir: str = "data/gold"):
    fig_dir = os.path.join(results_dir, "figures")
    os.makedirs(fig_dir, exist_ok=True)

    # --- daily_tx.png ---
    try:
        df = _load_parquet(os.path.join(gold_dir, "daily_stats"))
        if df is not None and len(df):
            df = df.sort_values("dt")
            fig, ax = plt.subplots(figsize=(10, 5))
            ax.bar(df["dt"], df["tx_count"], label="tx count")
            ax2 = ax.twinx()
            ax2.plot(df["dt"], df["active_senders"], color="orange", label="active senders")
            ax.set_xlabel("Date"); ax.set_ylabel("Transactions"); ax2.set_ylabel("Active senders")
            ax.tick_params(axis="x", rotation=45)
            fig.legend(loc="upper left", bbox_to_anchor=(0.1, 0.9))
            _fig("Daily transactions & active senders", os.path.join(fig_dir, "daily_tx.png"), fig, ax)
    except Exception as e:
        print(f"  Skipped daily_tx.png: {e}")

    # --- gas_by_hour.png ---
    try:
        df = _load_parquet(os.path.join(gold_dir, "hour_of_day_profile"))
        if df is not None and len(df):
            df = df.sort_values("hour")
            cheapest = int(df.loc[df["median_gas_gwei"].idxmin(), "hour"])
            fig, ax = plt.subplots(figsize=(10, 5))
            colors = ["red" if h == cheapest else "steelblue" for h in df["hour"]]
            ax.bar(df["hour"], df["median_gas_gwei"], color=colors)
            ax.set_xlabel("Hour (UTC)"); ax.set_ylabel("Median gas price (gwei)")
            ax.set_xticks(range(24))
            _fig(f"Gas price by hour (cheapest = {cheapest:02d}:00 UTC)",
                 os.path.join(fig_dir, "gas_by_hour.png"), fig, ax)
    except Exception as e:
        print(f"  Skipped gas_by_hour.png: {e}")

    # --- plain_vs_contract.png ---
    try:
        df = _load_parquet(os.path.join(gold_dir, "daily_stats"))
        if df is not None and len(df):
            df = df.sort_values("dt")
            fig, ax = plt.subplots(figsize=(10, 5))
            ax.stackplot(df["dt"],
                         df["plain_transfer_share"] * 100,
                         (1 - df["plain_transfer_share"]) * 100,
                         labels=["Plain transfers %", "Contract calls %"])
            ax.set_xlabel("Date"); ax.set_ylabel("Share (%)")
            ax.tick_params(axis="x", rotation=45)
            ax.legend(loc="upper right")
            _fig("Plain transfer vs contract call share", os.path.join(fig_dir, "plain_vs_contract.png"), fig, ax)
    except Exception as e:
        print(f"  Skipped plain_vs_contract.png: {e}")

    # --- pagerank_top20.png ---
    try:
        df = _load_parquet(os.path.join(gold_dir, "pagerank"))
        if df is not None and len(df):
            df = df.sort_values("rank", ascending=False).head(20)
            fig, ax = plt.subplots(figsize=(10, 6))
            labels = [a[:10] + "…" for a in df["address"]]
            ax.barh(labels[::-1], df["rank"][::-1])
            ax.set_xlabel("PageRank score")
            _fig("Top 20 addresses by PageRank", os.path.join(fig_dir, "pagerank_top20.png"), fig, ax)
    except Exception as e:
        print(f"  Skipped pagerank_top20.png: {e}")

    # --- kmeans_k_selection.png ---
    try:
        ks_path = os.path.join(results_dir, "kmeans_k_selection.json")
        if os.path.exists(ks_path):
            with open(ks_path) as f:
                ks = json.load(f)
            ks_data = ks.get("k_results", [])
            if ks_data:
                ks_list = sorted(ks_data, key=lambda x: x["k"])
                fig, ax = plt.subplots(figsize=(8, 4))
                ax.plot([r["k"] for r in ks_list], [r["silhouette"] for r in ks_list], "o-")
                ax.set_xlabel("k"); ax.set_ylabel("Silhouette score")
                _fig("KMeans k selection", os.path.join(fig_dir, "kmeans_k_selection.png"), fig, ax)
    except Exception as e:
        print(f"  Skipped kmeans_k_selection.png: {e}")

    # --- clusters_pca.png ---
    try:
        df = _load_parquet(os.path.join(gold_dir, "cluster_viz"))
        if df is not None and len(df):
            fig, ax = plt.subplots(figsize=(8, 6))
            clusters = df["cluster"].unique()
            for c in sorted(clusters):
                sub = df[df["cluster"] == c]
                ax.scatter(sub["pc1"], sub["pc2"], s=5, alpha=0.4, label=f"Cluster {c}")
            flagged = df[df["is_flagged"] == True]
            if len(flagged):
                ax.scatter(flagged["pc1"], flagged["pc2"], s=30, c="red", marker="x", label="Flagged")
            ax.set_xlabel("PC1"); ax.set_ylabel("PC2")
            ax.legend(markerscale=2)
            _fig("KMeans clusters PCA", os.path.join(fig_dir, "clusters_pca.png"), fig, ax)
    except Exception as e:
        print(f"  Skipped clusters_pca.png: {e}")

    # --- gas_vs_rows.png ---
    try:
        gvr_path = os.path.join(results_dir, "chain", "gas_vs_rows.json")
        if os.path.exists(gvr_path):
            with open(gvr_path) as f:
                gvr = json.load(f)
            fig, ax = plt.subplots(figsize=(8, 4))
            xs, ys = [r["n_leaves"] for r in gvr], [r["gas_used"] for r in gvr]
            ax.plot(xs, ys, "o-")
            for x, y in zip(xs, ys):
                ax.annotate(f"{y:,}", (x, y), textcoords="offset points", xytext=(0, 8), ha="center", fontsize=8)
            ax.set_xscale("log"); ax.set_ylim(0, max(ys) * 1.25)
            ax.set_xlabel("Report size (rows, log scale)"); ax.set_ylabel("Gas used")
            ax.text(0.01, 0.04, "first publish costs more: fresh storage slot", transform=ax.transAxes, fontsize=8)
            _fig("Publish gas vs report size", os.path.join(fig_dir, "gas_vs_rows.png"), fig, ax)
    except Exception as e:
        print(f"  Skipped gas_vs_rows.png: {e}")

    # --- mr_combiner_shuffle.png (gcp) ---
    try:
        mr = _read_json(os.path.join(results_dir, "mapreduce_counters.json"))
        if mr and "no_combiner" in mr and "with_combiner" in mr:
            names = ["no combiner", "with combiner"]
            shuffle_mb = [mr[v]["reduce_shuffle_bytes"] / 1e6 for v in ("no_combiner", "with_combiner")]
            reduce_in = [mr[v]["reduce_input_records"] / 1e6 for v in ("no_combiner", "with_combiner")]
            cut = (1 - shuffle_mb[1] / shuffle_mb[0]) * 100
            fig, (ax, ax2) = plt.subplots(1, 2, figsize=(10, 4))
            ax.bar(names, shuffle_mb, color=["grey", "seagreen"])
            ax.set_ylabel("Reduce shuffle (MB)")
            ax2.bar(names, reduce_in, color=["grey", "seagreen"])
            ax2.set_ylabel("Reduce input records (millions)")
            for a, vals in ((ax, shuffle_mb), (ax2, reduce_in)):
                for i, v in enumerate(vals):
                    a.annotate(f"{v:,.0f}" if v > 100 else f"{v:,.1f}", (i, v), ha="center", va="bottom", fontsize=9)
            _fig(f"MapReduce combiner effect: shuffle cut by {cut:.0f}%",
                 os.path.join(fig_dir, "mr_combiner_shuffle.png"), fig, ax)
    except Exception as e:
        print(f"  Skipped mr_combiner_shuffle.png: {e}")

    # --- hadoop_vs_spark.png (gcp) ---
    try:
        b = _read_json(os.path.join(results_dir, "bench_mr_vs_spark.json"))
        if b:
            fig, ax = plt.subplots(figsize=(7, 4))
            meds = [b["mr_median_s"], b["spark_median_s"]]
            ax.bar(["MapReduce\n(with combiner)", "Spark\nreduceByKey"], meds, color=["grey", "darkorange"], alpha=0.8)
            for i, runs in enumerate((b["mr_times_s"], b["spark_times_s"])):
                ax.scatter([i] * len(runs), runs, color="black", s=15, zorder=3, label="individual runs" if i == 0 else None)
            for i, v in enumerate(meds):
                ax.annotate(f"{v:.0f} s", (i, v), textcoords="offset points", xytext=(18, 0), va="center")
            ax.set_ylabel("Job time (s), median of 3"); ax.legend(loc="upper right")
            _fig(f"Count per sender: Spark {b['mr_median_s'] / b['spark_median_s']:.2f}x faster than MapReduce",
                 os.path.join(fig_dir, "hadoop_vs_spark.png"), fig, ax)
    except Exception as e:
        print(f"  Skipped hadoop_vs_spark.png: {e}")

    # --- predictive_vs_baseline.png ---
    try:
        a = _read_json(os.path.join(results_dir, "analytics_types.json"))
        pred = (a or {}).get("predictive", {})
        if "rmse" in pred:
            fig, ax = plt.subplots(figsize=(7, 4))
            vals = [pred["rmse"], pred["baseline_rmse"]]
            colors = ["seagreen" if pred["beats_baseline"] else "indianred", "grey"]
            ax.bar(["Linear regression", "Baseline:\nnext hour = this hour"], vals, color=colors)
            for i, v in enumerate(vals):
                ax.annotate(f"{v:.3f}", (i, v), ha="center", va="bottom")
            ax.set_ylabel("RMSE of next-hour avg gas (gwei)")
            verdict = "beats" if pred["beats_baseline"] else "does not beat"
            _fig(f"Next-hour gas prediction: model {verdict} the baseline (R\u00b2 = {pred['r2']:.2f})",
                 os.path.join(fig_dir, "predictive_vs_baseline.png"), fig, ax)
    except Exception as e:
        print(f"  Skipped predictive_vs_baseline.png: {e}")

    # --- sketch_cms_error.png ---
    try:
        sk = _read_json(os.path.join(results_dir, "stream", "sketch_metrics.json"))
        cms = (sk or {}).get("cms", {})
        top = cms.get("top50") or cms.get("top10")
        if top:
            ranks = range(1, len(top) + 1)
            exact = [r["exact"] for r in top]
            est = [r["estimate"] for r in top]
            fig, ax = plt.subplots(figsize=(10, 4))
            ax.fill_between(ranks, exact, [e + cms["error_bound"] for e in exact], color="orange", alpha=0.25,
                            label=f"allowed overestimate \u03b5\u00b7N = {cms['error_bound']:.0f}")
            ax.plot(ranks, exact, "k-", label="exact count")
            ax.plot(ranks, est, "o", color="darkorange", ms=4, label="Count-Min estimate")
            ax.set_xlabel("Sender rank (by exact count)"); ax.set_ylabel("Transactions")
            ax.legend()
            _fig(f"Count-Min Sketch: top {len(top)} senders, all within the \u03b5\u00b7N bound "
                 f"({cms['memory_bytes'] / 1024:.0f} KB of memory)",
                 os.path.join(fig_dir, "sketch_cms_error.png"), fig, ax)
    except Exception as e:
        print(f"  Skipped sketch_cms_error.png: {e}")

    # --- sketch_hll_error.png ---
    try:
        sk = _read_json(os.path.join(results_dir, "stream", "sketch_metrics.json"))
        tb = _read_json(os.path.join(results_dir, "stream", "latest_tumbling.json"))
        exact = (sk or {}).get("per_minute_exact_distinct")
        if exact and tb:
            spark_est = {w["window_start"]: w["distinct_senders"] for w in tb}
            minutes = sorted(set(exact) & set(spark_est))
            if minutes:
                xs = range(len(minutes))
                ex = [exact[m] for m in minutes]
                es = [spark_est[m] for m in minutes]
                fig, ax = plt.subplots(figsize=(10, 4))
                ax.fill_between(xs, [v * 0.95 for v in ex], [v * 1.05 for v in ex], color="steelblue", alpha=0.2,
                                label="\u00b15% (approx_count_distinct default rsd)")
                ax.plot(xs, ex, "k-", label="exact distinct senders")
                ax.plot(xs, es, "o", color="steelblue", ms=4, label="Spark HyperLogLog estimate")
                ax.set_xticks(list(xs)); ax.set_xticklabels([m[11:16] for m in minutes], rotation=45, fontsize=7)
                ax.set_xlabel("Minute (event time)"); ax.set_ylabel("Distinct senders")
                ax.legend()
                hll = sk.get("hll", {})
                _fig(f"HyperLogLog per minute vs exact (whole stream: {hll.get('estimate')} est vs {hll.get('exact')} exact)",
                     os.path.join(fig_dir, "sketch_hll_error.png"), fig, ax)
    except Exception as e:
        print(f"  Skipped sketch_hll_error.png: {e}")

    print(f"Figures written to {fig_dir}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default="results")
    parser.add_argument("--gold", default="data/gold")
    args = parser.parse_args()
    run(args.root, args.gold)


if __name__ == "__main__":
    main()
