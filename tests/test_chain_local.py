"""Chain integration tests — Phase 6.

Requires anvil running (docker compose up -d chain) and
deployment + publication already done via make chain-local.
Mark: chain (run with pytest -m chain).
"""
import json
import os
import random

import pytest

from etherlens.chain.merkle import verify_row


@pytest.mark.chain
def test_all_rows_one_day():
    """Every row of the first published day verifies on-chain."""
    w3, contract, pub = _load_setup()
    day_entry = pub[0]
    dt = day_entry["dt"]
    day = int(dt.replace("-", ""))
    merkle_path = os.path.join("results", "merkle", f"{dt}.json")
    with open(merkle_path) as f:
        data = json.load(f)

    for i, row in enumerate(data["rows"]):
        proof = [bytes.fromhex(p[2:]) for p in data["proofs"][i]]
        result = contract.functions.verifyRow(day, row, proof).call()
        assert result, f"Row {i} of {dt} failed on-chain verification"


@pytest.mark.chain
def test_random_rows_other_days():
    """Random rows from every other published day verify on-chain."""
    w3, contract, pub = _load_setup()
    rng = random.Random(42)
    for day_entry in pub[1:]:
        dt = day_entry["dt"]
        day = int(dt.replace("-", ""))
        merkle_path = os.path.join("results", "merkle", f"{dt}.json")
        if not os.path.exists(merkle_path):
            continue
        with open(merkle_path) as f:
            data = json.load(f)
        idxs = rng.sample(range(len(data["rows"])), min(5, len(data["rows"])))
        for i in idxs:
            proof = [bytes.fromhex(p[2:]) for p in data["proofs"][i]]
            result = contract.functions.verifyRow(day, data["rows"][i], proof).call()
            assert result, f"Row {i} of {dt} failed"


@pytest.mark.chain
def test_tampered_rows_fail():
    """Tampered rows (single char change) all fail on-chain."""
    w3, contract, pub = _load_setup()
    day_entry = pub[0]
    dt = day_entry["dt"]
    day = int(dt.replace("-", ""))
    merkle_path = os.path.join("results", "merkle", f"{dt}.json")
    with open(merkle_path) as f:
        data = json.load(f)

    rng = random.Random(7)
    idxs = rng.sample(range(len(data["rows"])), min(10, len(data["rows"])))
    for i in idxs:
        row = data["rows"][i]
        tampered = row[:-1] + ("0" if row[-1] != "0" else "1")
        proof = [bytes.fromhex(p[2:]) for p in data["proofs"][i]]
        result = contract.functions.verifyRow(day, tampered, proof).call()
        assert not result, f"Tampered row {i} should fail on-chain"


@pytest.mark.chain
def test_golden_vector_on_chain():
    """["a","b","c"] golden vector verifies on-chain."""
    from etherlens.chain.merkle import build_tree
    w3, contract, pub = _load_setup()

    rows = ["a", "b", "c"]
    expected_root = "0x5842148bc6ebeb52af882a317c765fccd3ae80589b21a9b8cbf21abb630e46a7"
    tree = build_tree(rows)
    assert tree.root == expected_root

    # Publish the golden vector to anvil under day 19700101
    from etherlens.chain.common import get_account, send_tx
    account = get_account(w3)
    day = 19700101
    root_bytes = bytes.fromhex(tree.root[2:])
    if not contract.functions.isPublished(day).call():
        send_tx(w3, account, contract.functions.publishRoot(day, root_bytes, 3, "golden"))

    for i, row in enumerate(rows):
        proof = [bytes.fromhex(p[2:]) for p in tree.proof(i)]
        result = contract.functions.verifyRow(day, row, proof).call()
        assert result, f"Golden vector row {i} failed"


def _load_setup():
    from dotenv import load_dotenv
    load_dotenv()
    from etherlens.chain.common import get_account, get_w3, load_artifact
    w3 = get_w3()
    chain = os.environ.get("CHAIN", "local")
    depl_path = os.path.join("results", "chain", f"deployment_{chain}.json")
    assert os.path.exists(depl_path), f"No deployment found at {depl_path}"
    with open(depl_path) as f:
        depl = json.load(f)
    abi, _ = load_artifact()
    contract = w3.eth.contract(address=depl["address"], abi=abi)
    pub_path = os.path.join("results", "chain", f"published_{chain}.json")
    assert os.path.exists(pub_path), f"No published records at {pub_path}"
    with open(pub_path) as f:
        pub = json.load(f)
    return w3, contract, pub
