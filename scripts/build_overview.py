"""Build dashboard/overview.html — a single-file project dashboard.

Reads the result files (real January 2025 run in results_gcp/ and data_gcp/gold/,
mock/local checks in results/), packs the numbers into JSON and injects them into
dashboard/overview_template.html. The output opens offline in any browser.

Usage (inside the dev container):  python scripts/build_overview.py
"""
import csv
import json
import os
import random

import pyarrow.parquet as pq

R, G, L = "results_gcp", "data_gcp/gold", "results"
TEMPLATE = "dashboard/overview_template.html"
OUT = "dashboard/overview.html"
ARTIFACT = "build/etherlens_overview.html"


def jread(path):
    with open(path) as f:
        return json.load(f)


def table(path):
    return pq.read_table(path).to_pandas()


def merkle_demo(day: str, wanted: list[str]) -> dict:
    """A few rows of one day's report with their proofs, for the in-page verifier."""
    m = jread(f"{R}/merkle/{day}.json")
    rows = []
    for key in wanted:
        idx = next(i for i, r in enumerate(m["rows"]) if key in r)
        rows.append({"index": idx, "row": m["rows"][idx], "proof": m["proofs"][idx]})
    return {"day": day, "root": m["root"], "leaf_count": m["leaf_count"], "rows": rows}


def main():
    silver = jread(f"{R}/silver_stats.json")
    gold_stats = jread(f"{R}/gold_stats.json")
    analytics = jread(f"{R}/analytics_types.json")
    bench = jread(f"{R}/bench_mr_vs_spark.json")
    mr = jread(f"{R}/mapreduce_counters.json")
    pr_iters = jread(f"{R}/pagerank_iters.json")
    ksel = jread(f"{R}/kmeans_k_selection.json")
    keval = jread(f"{R}/kmeans_eval_mock.json")
    mock_eval = jread(f"{L}/kmeans_eval_mock.json")
    sketch = jread(f"{R}/stream/sketch_metrics.json")
    tumbling = jread(f"{R}/stream/latest_tumbling.json")
    alerts = jread(f"{R}/stream/alerts.json")
    live = jread(f"{L}/stream_live_check.json")
    deployment = jread(f"{R}/chain/deployment_sepolia.json")
    published = jread(f"{R}/chain/published_sepolia.json")
    gas_vs_rows = jread(f"{L}/chain/gas_vs_rows.json")

    with open(f"{R}/gcp_job_times.tsv") as f:
        job_times = [{"job": a, "s": int(b)} for a, b in csv.reader(f, delimiter="\t")]
    with open(f"{R}/pagerank_top.csv") as f:
        pagerank = [{"address": r["address"], "rank": float(r["rank"])} for r in csv.DictReader(f)][:10]
    with open(f"{R}/kmeans_clusters.csv") as f:
        profiles = [{k: (float(v) if k != "cluster" else int(v)) for k, v in r.items()} for r in csv.DictReader(f)]

    daily = table(f"{G}/daily_stats").sort_values("dt")
    hod = table(f"{G}/hour_of_day_profile").sort_values("hour")

    # PCA scatter: all flagged points would hide the structure, so sample both groups
    viz = table(f"{G}/cluster_viz")
    rnd = random.Random(7)
    normal = viz[~viz["is_flagged"]].to_dict("records")
    flagged = viz[viz["is_flagged"]].to_dict("records")
    pts = rnd.sample(normal, min(2000, len(normal))) + rnd.sample(flagged, min(600, len(flagged)))

    data = {
        "silver": silver,
        "gold_stats": gold_stats,
        "job_times": job_times,
        "daily": [{"dt": str(r.dt), "tx": int(r.tx_count), "senders": int(r.active_senders),
                   "plain": round(float(r.plain_transfer_share), 4), "eth": round(float(r.total_eth)),
                   "median_gas": round(float(r.median_gas_gwei), 3), "creations": int(r.contract_creations)}
                  for r in daily.itertuples()],
        "hour_profile": [{"hour": int(r.hour), "median_gas": round(float(r.median_gas_gwei), 3),
                          "avg_tx": round(float(r.avg_tx_count))} for r in hod.itertuples()],
        "analytics": analytics,
        "bench": bench,
        "mr": mr,
        "pagerank": pagerank,
        "pagerank_iters": pr_iters,
        "kmeans": {"k_results": ksel["k_results"], "best_k": ksel["best_k"],
                   "best_silhouette": ksel["best_silhouette"], "n_flagged": keval["n_flagged"],
                   "n_addresses": gold_stats["address_features"], "profiles": profiles},
        "mock_eval": mock_eval,
        "pca": [{"x": round(p["pc1"], 3), "y": round(p["pc2"], 3), "f": bool(p["is_flagged"])} for p in pts],
        "pca_counts": {"normal": len(normal), "flagged": len(flagged)},
        "stream": {"n_items": sketch["n_items"], "hll": sketch["hll"],
                   "cms": {k: sketch["cms"][k] for k in ("eps", "delta", "w", "d", "memory_bytes", "error_bound",
                                                          "fraction_within_bound", "top10")},
                   "bloom": sketch["bloom"],
                   "tumbling": [{"t": w["window_start"][11:16], "tx": w["tx_count"],
                                 "gas": round(w["avg_gas_gwei"], 2), "senders": w["distinct_senders"]}
                                for w in sorted(tumbling, key=lambda w: w["window_start"])],
                   "alerts": {t: sum(1 for a in alerts if a["type"] == t) for t in sorted({a["type"] for a in alerts})}},
        "live": live,
        "chain": {"address": deployment["address"], "deploy_tx": deployment["tx_hash"],
                  "deploy_gas": deployment["gas_used"], "published": published,
                  "gas_vs_rows": gas_vs_rows},
        "merkle": merkle_demo("2025-01-31", ["|summary|tx_count|", "|summary|active_senders|",
                                             "|summary|median_gas_gwei|", "|top_sender|", "|whale|",
                                             "|flagged_active|"]),
    }

    with open(TEMPLATE) as f:
        html = f.read()
    blob = json.dumps(data, separators=(",", ":")).replace("</", "<\\/")
    html = html.replace("/*__DATA__*/{}", blob)
    # Standalone file for the repo (opens offline in any browser)
    standalone = ('<!doctype html>\n<html lang="en">\n<meta charset="utf-8">\n'
                  '<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">\n'
                  + html + "</html>\n")
    with open(OUT, "w", newline="\n") as f:
        f.write(standalone)
    # Body-only copy for publishing as a claude.ai artifact (the host adds the skeleton)
    os.makedirs("build", exist_ok=True)
    with open(ARTIFACT, "w", newline="\n") as f:
        f.write(html)
    print(f"Wrote {OUT} ({os.path.getsize(OUT) / 1024:.0f} KB, data {len(blob) / 1024:.0f} KB) and {ARTIFACT}")


if __name__ == "__main__":
    main()
