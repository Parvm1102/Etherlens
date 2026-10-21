"""Verify rows on-chain — Phase 6.3."""
from __future__ import annotations

import argparse
import json
import os

from dotenv import load_dotenv

from etherlens.chain.common import get_w3, load_artifact


def main():
    load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--dt", required=True)
    parser.add_argument("--index", type=int, required=True)
    parser.add_argument("--tamper", action="store_true")
    parser.add_argument("--results", default="results",
                        help="results dir; use results_gcp for the real-data (Sepolia) run")
    args = parser.parse_args()

    w3 = get_w3()
    chain = os.environ.get("CHAIN", "local")
    depl_path = os.path.join(args.results, "chain", f"deployment_{chain}.json")
    with open(depl_path) as f:
        depl = json.load(f)

    abi, _ = load_artifact()
    contract = w3.eth.contract(address=depl["address"], abi=abi)

    merkle_path = os.path.join(args.results, "merkle", f"{args.dt}.json")
    with open(merkle_path) as f:
        data = json.load(f)

    row = data["rows"][args.index]
    proof = [bytes.fromhex(p[2:]) for p in data["proofs"][args.index]]
    day = int(args.dt.replace("-", ""))

    if args.tamper:
        row = row[:-1] + ("0" if row[-1] != "0" else "1")
        print(f"Tampered row: {row}")

    result = contract.functions.verifyRow(day, row, proof).call()
    print(f"Row:    {row}")
    print(f"Proof:  {data['proofs'][args.index]}")
    print(f"Result: {result}")

    # Explorer link on sepolia
    if chain == "sepolia":
        pub_path = os.path.join(args.results, "chain", f"published_{chain}.json")
        if os.path.exists(pub_path):
            with open(pub_path) as f:
                pubs = json.load(f)
            match = next((p for p in pubs if p["dt"] == args.dt), None)
            if match:
                print(f"Explorer: https://sepolia.etherscan.io/tx/{match['tx_hash']}")


if __name__ == "__main__":
    main()
