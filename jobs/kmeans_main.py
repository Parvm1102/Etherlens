"""Entrypoint: KMeans anomaly detection job."""
import argparse
from etherlens.batch.kmeans_anomaly import run

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default="local")
    parser.add_argument("--bucket", default=None)
    args = parser.parse_args()
    run(args.env, args.bucket)
