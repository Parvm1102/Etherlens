"""Entrypoint: streaming job."""
import argparse
from etherlens.stream.stream_job import run

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default="local")
    parser.add_argument("--bucket", default=None)
    parser.add_argument("--max-minutes", type=int, default=None)
    args = parser.parse_args()
    run(args.env, args.bucket, max_minutes=args.max_minutes)
