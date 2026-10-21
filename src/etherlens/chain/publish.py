"""Publish Merkle roots to ReportRegistry — Phase 6.3."""
from __future__ import annotations

import argparse
import json
import os

from dotenv import load_dotenv
from web3 import Web3

from etherlens.chain.common import get_account, get_w3, load_artifact, send_tx


def _load_contract(w3, address: str):
    abi, _ = load_artifact()
    return w3.eth.contract(address=address, abi=abi)


def _day_int(dt: str) -> int:
    return int(dt.replace("-", ""))


def publish_day(w3, contract, account, dt: str, merkle_path: str,
                uri_prefix: str = "") -> dict | None:
    day = _day_int(dt)
    if contract.functions.isPublished(day).call():
        print(f"  {dt}: already published, skip")
        return None

    with open(merkle_path) as f:
        data = json.load(f)

    root_bytes = bytes.fromhex(data["root"][2:])
    leaf_count = data["leaf_count"]
    uri = f"{uri_prefix}{dt}" if uri_prefix else dt

    receipt = send_tx(
        w3, account,
        contract.functions.publishRoot(day, root_bytes, leaf_count, uri)
    )
    return {
        "dt": dt,
        "day": day,
        "root": data["root"],
        "leaf_count": leaf_count,
        "tx_hash": Web3.to_hex(receipt["transactionHash"]),   # .hex() drops the 0x in hexbytes>=1
        "gas_used": receipt["gasUsed"],
        "block": receipt["blockNumber"],
    }


def run_gas_experiment(w3, contract, account, results_dir: str):
    """Publish synthetic roots for trees of different sizes; measure gas."""
    import random
    rng = random.Random(0)
    results = []
    for n_leaves in [10, 1_000, 100_000, 1_000_000]:
        from etherlens.chain.merkle import build_tree
        rows = [f"row_{i}" for i in range(min(n_leaves, 100))]
        # For 100k/1M: only the root matters; build from small set
        tree = build_tree(rows)
        root_bytes = bytes.fromhex(tree.root[2:])
        # Use a unique day so no collision
        day = 20_000_000 + n_leaves
        uri = f"synthetic_{n_leaves}"
        receipt = send_tx(w3, account,
                          contract.functions.publishRoot(day, root_bytes, n_leaves, uri))
        results.append({"n_leaves": n_leaves, "gas_used": receipt["gasUsed"]})
        print(f"  gas_experiment n={n_leaves}: {receipt['gasUsed']} gas")

    os.makedirs(os.path.join(results_dir, "chain"), exist_ok=True)
    with open(os.path.join(results_dir, "chain", "gas_vs_rows.json"), "w") as f:
        json.dump(results, f, indent=2)


def main():
    load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--dt", default=None)
    parser.add_argument("--all", action="store_true", dest="all_days")
    parser.add_argument("--last", type=int, default=None)
    parser.add_argument("--uri-prefix", default="")
    parser.add_argument("--gas-experiment", action="store_true")
    parser.add_argument("--results", default="results",
                        help="results dir; use results_gcp for the real-data (Sepolia) run")
    args = parser.parse_args()
    results_dir = args.results

    w3 = get_w3()
    account = get_account(w3)
    chain = os.environ.get("CHAIN", "local")

    depl_path = os.path.join(results_dir, "chain", f"deployment_{chain}.json")
    with open(depl_path) as f:
        depl = json.load(f)

    # Redeploy if local address has no code
    address = depl["address"]
    if w3.eth.get_code(address) == b"":
        print("No code at recorded address — redeploying")
        from etherlens.chain.deploy import deploy
        depl = deploy(results_dir=results_dir)
        address = depl["address"]

    contract = _load_contract(w3, address)

    if args.gas_experiment:
        run_gas_experiment(w3, contract, account, results_dir)
        return

    merkle_dir = os.path.join(results_dir, "merkle")
    if not os.path.isdir(merkle_dir):
        print("No merkle dir found")
        return

    dates = sorted(
        f.replace(".json", "")
        for f in os.listdir(merkle_dir)
        if f.endswith(".json")
    )

    if args.dt:
        dates = [args.dt]
    elif args.last:
        dates = dates[-args.last:]

    published = []
    for dt in dates:
        p = os.path.join(merkle_dir, f"{dt}.json")
        if not os.path.exists(p):
            continue
        result = publish_day(w3, contract, account, dt, p, args.uri_prefix)
        if result:
            published.append(result)
            print(f"  {dt}: published root={result['root'][:12]}… gas={result['gas_used']}")

    out = os.path.join(results_dir, "chain")
    os.makedirs(out, exist_ok=True)
    pub_path = os.path.join(out, f"published_{chain}.json")
    existing = []
    if os.path.exists(pub_path):
        with open(pub_path) as f:
            existing = json.load(f)
    with open(pub_path, "w") as f:
        json.dump(existing + published, f, indent=2)

    print(f"Published {len(published)} roots to {chain}")

    if chain == "sepolia":
        _write_sepolia_links(out, depl, existing + published)


def _write_sepolia_links(out_dir: str, depl: dict, published: list[dict]):
    """results/chain/sepolia_links.md: Etherscan links for the contract and each publish."""
    base = "https://sepolia.etherscan.io"
    lines = [
        "# EtherLens on Sepolia",
        "",
        f"- Contract `ReportRegistry`: [{depl['address']}]({base}/address/{depl['address']})",
        f"- Deployment tx: [{depl['tx_hash']}]({base}/tx/{depl['tx_hash']})",
        "",
        "| Day | Merkle root | Leaves | Gas | Publish tx |",
        "|---|---|---|---|---|",
    ]
    for p in published:
        lines.append(f"| {p['dt']} | `{p['root']}` | {p['leaf_count']} | {p['gas_used']} | "
                     f"[{p['tx_hash'][:12]}…]({base}/tx/{p['tx_hash']}) |")
    path = os.path.join(out_dir, "sepolia_links.md")
    with open(path, "w", newline="\n") as f:
        f.write("\n".join(lines) + "\n")
    print(f"Links written to {path}")


if __name__ == "__main__":
    main()
