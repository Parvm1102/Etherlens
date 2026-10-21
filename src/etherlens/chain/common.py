"""Chain common utilities — web3.py v7, Phase 6.3."""
from __future__ import annotations

import json
import os

from dotenv import load_dotenv
from web3 import Web3

load_dotenv()

SEPOLIA_CHAIN_ID = 11155111

# Anvil default account #0 — local only, public test key
_ANVIL_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"

_CONTRACT_JSON = os.path.join(
    os.path.dirname(__file__),
    "../../../contracts/out/ReportRegistry.sol/ReportRegistry.json",
)


def get_w3() -> Web3:
    chain = os.environ.get("CHAIN", "local")
    if chain == "sepolia":
        rpc = os.environ.get("SEPOLIA_RPC_URL", "")
    else:
        rpc = os.environ.get("LOCAL_RPC_URL", "http://chain:8545")
    w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 30}))
    # Safety (₹0 rule): never let a mistyped URL send transactions to mainnet
    if chain == "sepolia" and w3.eth.chain_id != SEPOLIA_CHAIN_ID:
        raise RuntimeError(
            f"SEPOLIA_RPC_URL points at chain id {w3.eth.chain_id}, not Sepolia "
            f"({SEPOLIA_CHAIN_ID}). Use the Sepolia endpoint of your RPC provider."
        )
    return w3


def get_account(w3: Web3):
    chain = os.environ.get("CHAIN", "local")
    if chain == "sepolia":
        key = os.environ.get("DEPLOYER_PRIVATE_KEY", "")
    else:
        key = _ANVIL_KEY
    return w3.eth.account.from_key(key)


def load_artifact() -> tuple[list, str]:
    """Returns (abi, bytecode_hex)."""
    path = _CONTRACT_JSON
    if not os.path.exists(path):
        # Try relative to cwd
        path = os.path.join("contracts", "out", "ReportRegistry.sol", "ReportRegistry.json")
    with open(path) as f:
        data = json.load(f)
    return data["abi"], data["bytecode"]["object"]


def send_tx(w3: Web3, account, fn) -> dict:
    """Build, sign, send a contract function call. Returns receipt dict."""
    nonce = w3.eth.get_transaction_count(account.address, "pending")
    tx = fn.build_transaction({"from": account.address, "nonce": nonce})
    signed = account.sign_transaction(tx)
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
    return dict(receipt)
