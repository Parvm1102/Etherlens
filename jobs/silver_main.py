"""Entrypoint: silver job."""
import argparse
from etherlens.batch.silver import run

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default="local")
    parser.add_argument("--bucket", default=None)
    parser.add_argument("--bronze-path", default=None)
    args = parser.parse_args()
    run(args.env, args.bucket, bronze_path=args.bronze_path)
