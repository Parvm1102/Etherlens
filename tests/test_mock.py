"""Tests for mock data generator — Phase 1."""
import datetime
import json
import os
import tempfile

import pyarrow.parquet as pq
import pytest

# Bronze schema field names expected by §6.1
BRONZE_FIELDS = {
    "hash", "block_number", "block_timestamp", "from_address", "to_address",
    "value", "gas", "gas_price", "nonce", "receipt_gas_used",
    "receipt_effective_gas_price", "receipt_status", "transaction_type",
}

# Stream schema field names expected by §6.4
STREAM_FIELDS = {
    "hash", "block_number", "block_timestamp", "from_address", "to_address",
    "value_wei", "gas_limit", "gas_price_wei", "nonce", "tx_type",
}


@pytest.fixture(scope="module")
def tiny_dataset():
    """Generate tiny preset once; return (out_dir, manifest) tuple."""
    from etherlens.mock.generate_batch import generate

    with tempfile.TemporaryDirectory() as tmp:
        out_dir = os.path.join(tmp, "bronze", "tx")
        manifest = generate(seed=42, preset="tiny", start_date="2025-01-01", out_dir=out_dir)
        # Read all parquet files into combined list of row dicts
        files = sorted(f for f in os.listdir(out_dir) if f.endswith(".parquet"))
        all_rows = []
        for fname in files:
            table = pq.read_table(os.path.join(out_dir, fname))
            for batch in table.to_batches():
                d = batch.to_pydict()
                n = len(d["hash"])
                for i in range(n):
                    all_rows.append({k: d[k][i] for k in d})
        yield all_rows, manifest


def test_schema(tiny_dataset):
    rows, manifest = tiny_dataset
    assert len(rows) > 0
    assert set(rows[0].keys()) == BRONZE_FIELDS


def test_row_count(tiny_dataset):
    rows, manifest = tiny_dataset
    # manifest["total_rows"] is the ground truth (includes volume variation + duplicates + bad rows)
    assert len(rows) == manifest["total_rows"], (
        f"Loaded {len(rows)} rows but manifest says {manifest['total_rows']}"
    )
    # Also check it's within ±10% of the nominal target
    expected = manifest["tx_per_day_target"] * manifest["days"]
    assert abs(len(rows) - expected) / expected < 0.10, (
        f"Row count {len(rows)} more than 10% from nominal {expected}"
    )


def test_manifest_duplicates(tiny_dataset):
    rows, manifest = tiny_dataset
    hashes = [r["hash"] for r in rows if r["hash"] is not None]
    seen = {}
    dup_count = 0
    for h in hashes:
        if h in seen:
            dup_count += 1
        seen[h] = True
    assert dup_count == manifest["n_duplicates"], (
        f"duplicate count {dup_count} != manifest {manifest['n_duplicates']}"
    )


def test_manifest_bad_rows(tiny_dataset):
    rows, manifest = tiny_dataset
    bad = sum(1 for r in rows if r["from_address"] is None)
    assert bad == manifest["n_bad_rows"], (
        f"bad_row count {bad} != manifest {manifest['n_bad_rows']}"
    )


def test_manifest_failures(tiny_dataset):
    rows, manifest = tiny_dataset
    failed = sum(1 for r in rows if r["receipt_status"] == 0)
    assert failed == manifest["n_failed"], (
        f"failed count {failed} != manifest {manifest['n_failed']}"
    )


def test_hub_share(tiny_dataset):
    rows, manifest = tiny_dataset
    hub = manifest["hub"]
    total = len(rows)
    hub_rx = sum(1 for r in rows if r["to_address"] == hub)
    share = hub_rx / total
    assert 0.07 <= share <= 0.09, f"Hub share {share:.3f} not in [0.07, 0.09]"


def test_bots_nonce(tiny_dataset):
    rows, manifest = tiny_dataset
    bots = set(manifest["bots"])
    bot_rows = [r for r in rows if r["from_address"] in bots]
    assert len(bot_rows) > 0, "No bot transactions found"
    for r in bot_rows:
        assert r["nonce"] >= 200_000, (
            f"Bot {r['from_address']} has nonce {r['nonce']} < 200000"
        )


def test_cheapest_hour(tiny_dataset):
    rows, manifest = tiny_dataset
    # Cheapest hour by median gas_price should be 3 UTC
    # Collect gas prices per hour (use receipt_effective_gas_price)
    from collections import defaultdict
    hour_prices = defaultdict(list)
    for r in rows:
        if r["block_timestamp"] is not None and r["receipt_effective_gas_price"] is not None:
            ts = r["block_timestamp"]
            # ts is a datetime or integer microseconds
            if isinstance(ts, (int, float)):
                dt = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc) + datetime.timedelta(microseconds=ts)
            else:
                dt = ts
            hour_prices[dt.hour].append(r["receipt_effective_gas_price"])

    medians = {}
    for h, prices in hour_prices.items():
        prices.sort()
        mid = len(prices) // 2
        medians[h] = prices[mid]

    cheapest_hour = min(medians, key=medians.get)
    # Allow ±2 hours tolerance given noise in mock data
    assert abs(cheapest_hour - 3) <= 2, (
        f"Cheapest hour is {cheapest_hour}, expected ~3 UTC"
    )


def test_nonce_monotonic(tiny_dataset):
    rows, manifest = tiny_dataset
    from collections import defaultdict
    # Sort by block_timestamp then filter valid rows
    valid = [r for r in rows if r["from_address"] is not None and r["receipt_status"] == 1]

    def sort_key(r):
        ts = r["block_timestamp"]
        if isinstance(ts, (int, float)):
            return ts
        return ts.timestamp() if hasattr(ts, "timestamp") else 0

    valid.sort(key=sort_key)

    sender_nonces = defaultdict(list)
    for r in valid:
        sender_nonces[r["from_address"]].append(r["nonce"])

    for addr, nonces in sender_nonces.items():
        # nonces should be non-decreasing (strictly increasing for same sender)
        for i in range(1, len(nonces)):
            assert nonces[i] >= nonces[i - 1], (
                f"Non-monotonic nonce for {addr}: {nonces[i-1]} -> {nonces[i]}"
            )


def test_mockchain_valid_records():
    """MockChain produces valid §6.4 records."""
    from etherlens.mock.blocks import MockChain
    from etherlens.mock.entities import build_world

    world = build_world(42)
    flagged = world["bots"][:2]
    chain = MockChain(seed=42, flagged_addresses=flagged, interval_s=12)

    count = 0
    for blk_num, txs in chain.blocks():
        for tx in txs:
            assert set(tx.keys()) == STREAM_FIELDS, f"Missing fields: {STREAM_FIELDS - set(tx.keys())}"
            assert isinstance(tx["hash"], str) and tx["hash"].startswith("0x")
            assert isinstance(tx["block_number"], int)
            assert isinstance(tx["value_wei"], str)
            count += 1
        if count >= 500:
            break


def test_mockchain_burst():
    """Every 25th block has 5× the normal tx count."""
    from etherlens.mock.blocks import MockChain

    chain = MockChain(seed=42, flagged_addresses=[], interval_s=12)
    normal_counts = []
    burst_counts = []
    for blk_num, txs in chain.blocks():
        if blk_num % 25 == 0:
            burst_counts.append(len(txs))
        else:
            normal_counts.append(len(txs))
        if len(burst_counts) >= 3 and len(normal_counts) >= 20:
            break

    avg_normal = sum(normal_counts) / len(normal_counts)
    avg_burst = sum(burst_counts) / len(burst_counts)
    assert avg_burst >= avg_normal * 4, (
        f"Burst avg {avg_burst:.0f} should be ~5x normal {avg_normal:.0f}"
    )
