"""Deploy ReportRegistry.sol — Phase 6.3."""
from __future__ import annotations

import json
import os

from web3 import Web3

from etherlens.chain.common import get_account, get_w3, load_artifact, send_tx


def deploy(env: str = "local", results_dir: str = "results") -> dict:
    w3 = get_w3()
    account = get_account(w3)
    abi, bytecode = load_artifact()

    contract = w3.eth.contract(abi=abi, bytecode=bytecode)
    receipt = send_tx(w3, account, contract.constructor())

    address = receipt["contractAddress"]
    result = {
        "address": address,
        "tx_hash": Web3.to_hex(receipt["transactionHash"]),   # .hex() drops the 0x in hexbytes>=1
        "block": receipt["blockNumber"],
        "chain_id": w3.eth.chain_id,
        "gas_used": receipt["gasUsed"],
        "deployer": account.address,
    }

    out = os.path.join(results_dir, "chain")
    os.makedirs(out, exist_ok=True)
    chain = os.environ.get("CHAIN", "local")
    path = os.path.join(out, f"deployment_{chain}.json")
    with open(path, "w") as f:
        json.dump(result, f, indent=2)

    print(f"Deployed ReportRegistry at {address} (chain={chain})")
    return result


def main():
    import argparse
    from dotenv import load_dotenv
    load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default="local")
    parser.add_argument("--results", default="results",
                        help="results dir; use results_gcp for the real-data (Sepolia) run")
    args = parser.parse_args()
    deploy(args.env, args.results)


if __name__ == "__main__":
    main()
