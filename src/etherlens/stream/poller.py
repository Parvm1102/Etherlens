"""Stream poller — Phase 5.1. No pyspark imports."""
from __future__ import annotations

import argparse
import json
import os
import sys
import time


def _atomic_write_local(path: str, data: str):
    """Write atomically: write .tmp then os.replace."""
    dir_ = os.path.dirname(path)
    os.makedirs(dir_, exist_ok=True)
    tmp = os.path.join(dir_, "." + os.path.basename(path) + ".tmp")
    with open(tmp, "w", newline="\n") as f:
        f.write(data)
    os.replace(tmp, path)


def _atomic_write_gcs(path: str, data: str):
    """Write to GCS atomically (objects appear atomically)."""
    from google.cloud import storage
    parts = path[5:].split("/", 1)
    bucket_name, blob_name = parts[0], parts[1]
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(data, content_type="application/json")


def _write_block(out_dir: str, block_number: int, txs: list[dict]):
    name = f"blk_{block_number}.jsonl"
    content = "\n".join(json.dumps(tx) for tx in txs) + "\n"
    if out_dir.startswith("gs://"):
        _atomic_write_gcs(os.path.join(out_dir, name), content)
    else:
        _atomic_write_local(os.path.join(out_dir, name), content)


def run_mock(out_dir: str, interval: float, max_blocks: int | None, manifest_path: str | None):
    from etherlens.mock.blocks import MockChain

    flagged: list[str] = []
    if manifest_path and os.path.exists(manifest_path):
        with open(manifest_path) as f:
            m = json.load(f)
        flagged = m.get("bots", [])

    chain = MockChain(seed=42, flagged_addresses=flagged, interval_s=12)
    count = 0
    for block_number, txs in chain.blocks():
        _write_block(out_dir, block_number, txs)
        print(f"block {block_number}: {len(txs)} tx", flush=True)
        count += 1
        if max_blocks and count >= max_blocks:
            break
        time.sleep(interval)


def run_replay(out_dir: str, silver_path: str, dt: str, speed: float):
    import pyarrow.parquet as pq
    from collections import defaultdict

    part_path = os.path.join(silver_path, f"dt={dt}")
    if not os.path.exists(part_path):
        print(f"Silver partition {part_path} not found", file=sys.stderr)
        return

    import pyarrow as pa
    table = pq.read_table(part_path)
    df = table.to_pydict()
    n = len(df["hash"])

    # Group by block_number
    blocks = defaultdict(list)
    for i in range(n):
        ts = df["ts"][i]
        if hasattr(ts, "isoformat"):
            ts_str = ts.strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            ts_str = str(ts)
        blk = df["block_number"][i]
        blocks[blk].append({
            "hash": df["hash"][i],
            "block_number": int(blk),
            "block_timestamp": ts_str,
            "from_address": df["from_address"][i],
            "to_address": df["to_address"][i],
            "value_wei": str(int((df["value_eth"][i] or 0) * 1e18)),
            "gas_limit": int(df["gas_limit"][i] or 0),
            "gas_price_wei": int(round((df["gas_price_gwei"][i] or 0) * 1e9)),
            "nonce": int(df["nonce"][i] or 0),
            "tx_type": int(df["tx_type"][i] or 0),
        })

    delay = 12.0 / speed
    for blk in sorted(blocks):
        _write_block(out_dir, blk, blocks[blk])
        print(f"replay block {blk}: {len(blocks[blk])} tx", flush=True)
        time.sleep(delay)


def run_live(out_dir: str, rpc_url: str, max_blocks: int | None):
    from web3 import Web3
    import datetime

    w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 20}))
    latest = w3.eth.block_number - 2
    backoff = 4

    count = 0
    while True:
        try:
            current = w3.eth.block_number
            while latest <= current:
                block = w3.eth.get_block(latest, full_transactions=True)
                ts = datetime.datetime.fromtimestamp(block["timestamp"], tz=datetime.timezone.utc)
                ts_str = ts.strftime("%Y-%m-%dT%H:%M:%SZ")
                txs = []
                for tx in block["transactions"]:
                    txs.append({
                        "hash": Web3.to_hex(tx["hash"]),
                        "block_number": int(block["number"]),
                        "block_timestamp": ts_str,
                        "from_address": tx["from"].lower(),
                        "to_address": tx["to"].lower() if tx["to"] else None,
                        "value_wei": str(tx["value"]),
                        "gas_limit": int(tx["gas"]),
                        "gas_price_wei": int(tx.get("gasPrice", 0)),
                        "nonce": int(tx["nonce"]),
                        "tx_type": int(tx.get("type", 0) or 0),
                    })
                _write_block(out_dir, int(block["number"]), txs)
                print(f"live block {latest}: {len(txs)} tx", flush=True)
                latest += 1
                count += 1
                if max_blocks and count >= max_blocks:
                    return
            backoff = 4
            time.sleep(backoff)
        except Exception as e:
            print(f"Error: {e}; retrying in {backoff}s", file=sys.stderr)
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)


def main():
    parser = argparse.ArgumentParser(description="Stream poller")
    parser.add_argument("--mode", choices=["mock", "replay", "live"], default="mock")
    parser.add_argument("--out", default="data/stream/incoming")
    parser.add_argument("--interval", type=float, default=2.0)
    parser.add_argument("--dt", default=None)
    parser.add_argument("--speed", type=float, default=60.0)
    parser.add_argument("--max-blocks", type=int, default=None)
    parser.add_argument("--manifest", default="data/mock/manifest.json")
    parser.add_argument("--silver", default="data/silver/tx")
    parser.add_argument("--rpc-url", default=None)
    args = parser.parse_args()

    if args.mode == "mock":
        run_mock(args.out, args.interval, args.max_blocks, args.manifest)
    elif args.mode == "replay":
        run_replay(args.out, args.silver, args.dt, args.speed)
    elif args.mode == "live":
        rpc = args.rpc_url or os.environ.get("ETH_RPC_URL", "")
        run_live(args.out, rpc, args.max_blocks)


if __name__ == "__main__":
    main()
