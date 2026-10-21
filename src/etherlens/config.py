"""Environment profiles and path resolution."""
from __future__ import annotations

import os
from pathlib import Path

# python-dotenv is not installed on Dataproc; there the job args carry
# everything we need, so a missing package is fine.
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


def get_paths(env: str, bucket: str | None = None) -> dict[str, str]:
    """Return path dict for the given env (local|gcp)."""
    if env == "gcp":
        if not bucket:
            bucket = os.environ.get("GCS_BUCKET", "")
        b = f"gs://{bucket}"
        return {
            "bronze": f"{b}/bronze/tx",
            "silver": f"{b}/silver/tx",
            "gold": f"{b}/gold",
            "edges": "hdfs:///etherlens/edges_csv",
            "results": f"{b}/results",
            "stream_in": f"{b}/stream/incoming",
            "stream_out": f"{b}/stream/out",
            # Checkpoints on HDFS (spec 8.3): many small writes per micro-batch are
            # fast on HDFS but slow on GCS. Regenerable; deleted with the cluster.
            # No scheme: the cluster's default filesystem is HDFS, and streaming
            # rejects "hdfs:///..." ("Uri without authority").
            "stream_ckpt": "/etherlens/stream_checkpoints",
            "rdd_ckpt": "hdfs:///etherlens/checkpoints",
            "manifest": f"{b}/mock/manifest.json",
        }
    # local
    return {
        "bronze": "data/bronze/tx",
        "silver": "data/silver/tx",
        "gold": "data/gold",
        "edges": "data/hdfs_sim/edges_csv",
        "results": "results",
        "stream_in": "data/stream/incoming",
        "stream_out": "results/stream",
        "stream_ckpt": "data/stream/checkpoints",
        "rdd_ckpt": "data/checkpoints",
        "manifest": "data/mock/manifest.json",
    }
