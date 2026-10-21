"""Streamlit dashboard — Phase 7.3."""
import json
import os

import streamlit as st

st.set_page_config(page_title="EtherLens", layout="wide")

# ---- Sidebar ----
with st.sidebar:
    st.title("EtherLens")
    env = st.selectbox("Data source", ["local", "gcp"], index=0,
                       help="gcp = real Jan 2025 data, pulled from the bucket into results_gcp/ and data_gcp/gold/")
    chain = st.selectbox("Chain", ["local", "sepolia"], index=0)

if env == "gcp":
    # The dashboard reads local files, so cloud results are pulled first
    # (gcloud storage rsync, see docs/PROGRESS.md Phase 8).
    GOLD = "data_gcp/gold"
    RESULTS = "results_gcp"
    STREAM_OUT = "results_gcp/stream"
else:
    from etherlens.config import get_paths
    paths = get_paths("local")
    GOLD = paths["gold"]
    RESULTS = paths["results"]
    STREAM_OUT = paths["stream_out"]


@st.cache_data(ttl=60)
def _read_parquet(path: str):
    import pyarrow.parquet as pq
    if not os.path.exists(path):
        return None
    try:
        return pq.read_table(path).to_pandas()
    except Exception:
        return None


def _read_json(path: str):
    if not os.path.exists(path):
        return None
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return None


# ---- Tabs ----
tabs = st.tabs(["Overview", "Gas & Prediction", "Network", "Anomalies", "Live", "Blockchain", "Big Data Internals"])

# ===== Tab 1: Overview =====
with tabs[0]:
    st.header("Overview")
    daily = _read_parquet(os.path.join(GOLD, "daily_stats"))
    silver_stats = _read_json(os.path.join(RESULTS, "silver_stats.json"))

    if silver_stats:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Rows processed", f"{silver_stats.get('rows_in', 0):,}")
        c2.metric("Days", silver_stats.get("days", 0))
        c3.metric("Avg tx/day", f"{silver_stats.get('rows_out', 0) // max(1, silver_stats.get('days', 1)):,}")
        c4.metric("Bronze bytes", f"{silver_stats.get('bronze_bytes', 0) / 1e6:.1f} MB")

    if daily is not None and len(daily):
        daily = daily.sort_values("dt")
        st.subheader("Daily transactions")
        st.bar_chart(daily.set_index("dt")[["tx_count"]])
        st.subheader("Plain transfer vs contract call share")
        daily["contract_share"] = 1 - daily["plain_transfer_share"]
        st.area_chart(daily.set_index("dt")[["plain_transfer_share", "contract_share"]])
    else:
        st.info("Run the pipeline first: make silver gold")

# ===== Tab 2: Gas & Prediction =====
with tabs[1]:
    st.header("Gas & Recommendation")
    analytics = _read_json(os.path.join(RESULTS, "analytics_types.json"))
    hodp = _read_parquet(os.path.join(GOLD, "hour_of_day_profile"))

    if hodp is not None and len(hodp):
        hodp = hodp.sort_values("hour")
        st.subheader("Median gas price by hour (UTC)")
        st.bar_chart(hodp.set_index("hour")[["median_gas_gwei"]])

    if analytics:
        p = analytics.get("prescriptive", {})
        st.success(p.get("recommendation", ""))
        c1, c2 = st.columns(2)
        c1.metric("Cheapest hour UTC", f"{p.get('cheapest_hour_utc', '?'):02d}:00")
        c2.metric("Savings vs peak", f"{p.get('savings_vs_peak_pct', 0)}%")

        pred = analytics.get("predictive", {})
        if "rmse" in pred:
            st.subheader("Predictive model (next-hour gas)")
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("RMSE", f"{pred['rmse']:.4f}")
            c2.metric("MAE", f"{pred['mae']:.4f}")
            c3.metric("R²", f"{pred['r2']:.4f}")
            c4.metric("Baseline RMSE", f"{pred['baseline_rmse']:.4f}")
            st.caption(pred.get("note", ""))

# ===== Tab 3: Network =====
with tabs[2]:
    st.header("Network — PageRank")
    pr_df = _read_parquet(os.path.join(GOLD, "pagerank"))
    if pr_df is not None and len(pr_df):
        top = pr_df.sort_values("rank", ascending=False).head(20)
        top["short_addr"] = top["address"].str[:12] + "…"
        st.bar_chart(top.set_index("short_addr")[["rank"]])
        st.dataframe(top[["address", "rank"]].reset_index(drop=True))
    else:
        st.info("Run make pagerank first.")

# ===== Tab 4: Anomalies =====
with tabs[3]:
    st.header("Anomaly Detection")
    clusters = _read_parquet(os.path.join(GOLD, "address_clusters"))
    flagged = _read_parquet(os.path.join(GOLD, "flagged_addresses"))
    cluster_viz = _read_parquet(os.path.join(GOLD, "cluster_viz"))
    km_eval = _read_json(os.path.join(RESULTS, "kmeans_eval_mock.json"))

    if km_eval:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Best k", km_eval.get("best_k", "?"))
        c2.metric("Flagged addresses", f"{km_eval.get('n_flagged', 0):,}")
        ev = km_eval.get("mock_eval")
        if ev:   # only the mock world has ground truth
            c3.metric("Recall (planted anomalies)", f"{ev['recall']:.2f}",
                      help=f"{ev['true_positives']} of {ev['planted_evaluable']} planted bots / wash ring / whales flagged")
            c4.metric("Precision", f"{ev['precision']:.2f}",
                      help="Flag budget is a fixed 0.5% of addresses, so precision is capped by how many true anomalies exist")

    profiles_path = os.path.join(RESULTS, "kmeans_clusters.csv")
    if os.path.exists(profiles_path):
        import pandas as pd
        st.subheader("Cluster profiles (mean of raw features)")
        st.dataframe(pd.read_csv(profiles_path).round(2), hide_index=True)

    if cluster_viz is not None and len(cluster_viz):
        # Built-in chart (plotly is not in the image); flagged addresses drawn larger
        st.caption("KMeans clusters (PCA, 2 components) — large points are flagged addresses")
        cluster_viz["cluster"] = cluster_viz["cluster"].astype(str)
        cluster_viz["point_size"] = cluster_viz["is_flagged"].map({True: 80, False: 10}).fillna(10)
        st.scatter_chart(cluster_viz, x="pc1", y="pc2", color="cluster", size="point_size")

    if flagged is not None and len(flagged):
        st.subheader("Flagged addresses")
        st.dataframe(flagged.head(50))

# ===== Tab 5: Live =====
with tabs[4]:
    st.header("Live stream")

    @st.fragment(run_every="5s")
    def _live():
        tumbling = _read_json(os.path.join(STREAM_OUT, "latest_tumbling.json"))
        sliding = _read_json(os.path.join(STREAM_OUT, "latest_sliding.json"))
        alerts = _read_json(os.path.join(STREAM_OUT, "alerts.json"))
        sketches = _read_json(os.path.join(STREAM_OUT, "sketch_metrics.json"))

        if tumbling:
            import pandas as pd
            tdf = pd.DataFrame(tumbling)
            st.subheader("Tumbling (1-min) windows")
            st.line_chart(tdf.set_index("window_start")[["tx_count", "avg_gas_gwei"]])

        if alerts:
            st.subheader("Alerts (newest first)")
            import pandas as pd
            st.dataframe(pd.DataFrame(alerts[::-1]).head(20))

        if sketches:
            st.subheader("Sketch metrics")
            cms = sketches.get("cms", {})
            hll = sketches.get("hll", {})
            c1, c2, c3 = st.columns(3)
            c1.metric("CMS N", sketches.get("n_items", 0))
            c2.metric("HLL estimate", hll.get("estimate", 0))
            c3.metric("HLL rel error", f"{hll.get('rel_error', 0):.4f}")
            if cms.get("top10"):
                import pandas as pd
                st.dataframe(pd.DataFrame(cms["top10"]).head(10))

    _live()

# ===== Tab 6: Blockchain =====
with tabs[5]:
    st.header("Blockchain")
    merkle_dir = os.path.join(RESULTS, "merkle")
    dep_path = os.path.join(RESULTS, "chain", f"deployment_{chain}.json")

    if os.path.exists(dep_path):
        dep = _read_json(dep_path)
        st.metric("Contract address", dep.get("address", "N/A"))

    if os.path.isdir(merkle_dir):
        dates = sorted(f.replace(".json", "") for f in os.listdir(merkle_dir) if f.endswith(".json"))
        # Only days whose root is on this chain can verify (e.g. 3 days on Sepolia)
        published = _read_json(os.path.join(RESULTS, "chain", f"published_{chain}.json"))
        if published:
            on_chain = {p["dt"] for p in published}
            dates = [d for d in dates if d in on_chain]
        if dates:
            dt = st.selectbox("Pick a day", dates)
            mdata = _read_json(os.path.join(merkle_dir, f"{dt}.json"))
            if mdata:
                st.metric("Root", mdata["root"])
                st.metric("Leaf count", mdata["leaf_count"])
                pub = next((p for p in (published or []) if p["dt"] == dt), None)
                if pub and chain == "sepolia":
                    st.markdown(f"Publish tx: [{pub['tx_hash'][:18]}…](https://sepolia.etherscan.io/tx/{pub['tx_hash']}) "
                                f"— {pub['gas_used']:,} gas")
                idx = st.number_input("Row index", 0, max(0, mdata["leaf_count"] - 1), 0)
                tamper = st.checkbox("Tamper with row (demo)")
                if st.button("Verify on-chain"):
                    row = mdata["rows"][idx]
                    if tamper:
                        row = row[:-1] + ("0" if row[-1] != "0" else "1")
                        st.warning(f"Tampered: {row}")
                    os.environ["CHAIN"] = chain   # get_w3() picks the RPC URL from CHAIN
                    from etherlens.chain.common import get_w3, load_artifact
                    w3 = get_w3()
                    depl = _read_json(dep_path)
                    abi, _ = load_artifact()
                    contract = w3.eth.contract(address=depl["address"], abi=abi)
                    proof = [bytes.fromhex(p[2:]) for p in mdata["proofs"][idx]]
                    day = int(dt.replace("-", ""))
                    result = contract.functions.verifyRow(day, row, proof).call()
                    if result:
                        st.success("✓ Verified on-chain")
                    else:
                        st.error("✗ Verification failed")

    gvr = _read_json(os.path.join(RESULTS, "chain", "gas_vs_rows.json"))
    if gvr:
        import pandas as pd
        df = pd.DataFrame(gvr)
        st.subheader("Gas cost vs report size (near-constant)")
        st.line_chart(df.set_index("n_leaves")[["gas_used"]])

# ===== Tab 7: Big Data Internals =====
with tabs[6]:
    st.header("Big Data Internals")
    pr_iters = _read_json(os.path.join(RESULTS, "pagerank_iters.json"))
    if pr_iters:
        import pandas as pd
        st.subheader("PageRank per-iteration times")
        idf = pd.DataFrame({"iteration": range(1, len(pr_iters["iter_times_s"]) + 1),
                             "seconds": pr_iters["iter_times_s"]})
        st.line_chart(idf.set_index("iteration"))

    import pandas as pd
    mr = _read_json(os.path.join(RESULTS, "mapreduce_counters.json"))
    if mr and "no_combiner" in mr and "with_combiner" in mr:
        st.subheader("MapReduce: effect of the combiner")
        mdf = pd.DataFrame({
            "variant": ["no combiner", "with combiner"],
            "shuffle MB": [mr[v]["reduce_shuffle_bytes"] / 1e6 for v in ("no_combiner", "with_combiner")],
            "reduce input (M records)": [mr[v]["reduce_input_records"] / 1e6 for v in ("no_combiner", "with_combiner")],
        }).set_index("variant")
        st.bar_chart(mdf[["shuffle MB"]])
        st.dataframe(mdf.round(1))

    bench = _read_json(os.path.join(RESULTS, "bench_mr_vs_spark.json"))
    if bench:
        st.subheader(f"Hadoop MapReduce vs Spark: Spark {bench['mr_median_s'] / bench['spark_median_s']:.2f}x faster")
        st.bar_chart(pd.DataFrame({"median seconds": [bench["mr_median_s"], bench["spark_median_s"]]},
                                  index=["MapReduce (combiner)", "Spark reduceByKey"]))

    times_path = os.path.join(RESULTS, "gcp_job_times.tsv")
    if os.path.exists(times_path):
        st.subheader("Cluster job runtimes (seconds)")
        tdf = pd.read_csv(times_path, sep="\t", names=["job", "seconds"]).set_index("job")
        st.bar_chart(tdf)

    ks = _read_json(os.path.join(RESULTS, "kmeans_k_selection.json"))
    if ks:
        import pandas as pd
        st.subheader("Silhouette vs k")
        kdf = pd.DataFrame(ks.get("k_results", []))
        if len(kdf):
            st.line_chart(kdf.set_index("k")[["silhouette"]])

    # Screenshot gallery
    ss_dir = os.path.join("results", "screenshots")   # cluster screenshots (spec 8.8), same for both sources
    if os.path.isdir(ss_dir):
        imgs = [f for f in os.listdir(ss_dir) if f.lower().endswith((".png", ".jpg"))]
        if imgs:
            st.subheader("Screenshots")
            for img in sorted(imgs):
                st.image(os.path.join(ss_dir, img), caption=img)
