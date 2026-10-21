"""Mock batch data generator — produces bronze Parquet files + manifest.json."""
import argparse
import datetime
import json
import math
import os
import random
import time

import pyarrow as pa
import pyarrow.parquet as pq

from etherlens.mock.entities import build_world

PRESETS = {
    "tiny":   {"days": 3,  "tx_per_day": 3_000},
    "small":  {"days": 14, "tx_per_day": 40_000},
    "medium": {"days": 30, "tx_per_day": 200_000},
}

# Bronze pyarrow schema (§6.1)
BRONZE_PA_SCHEMA = pa.schema([
    pa.field("hash",                        pa.string()),
    pa.field("block_number",                pa.int64()),
    pa.field("block_timestamp",             pa.timestamp("us", tz="UTC")),
    pa.field("from_address",                pa.string()),
    pa.field("to_address",                  pa.string()),
    pa.field("value",                       pa.string()),
    pa.field("gas",                         pa.int64()),
    pa.field("gas_price",                   pa.int64()),
    pa.field("nonce",                       pa.int64()),
    pa.field("receipt_gas_used",            pa.int64()),
    pa.field("receipt_effective_gas_price", pa.int64()),
    pa.field("receipt_status",              pa.int64()),
    pa.field("transaction_type",            pa.int64()),
])

_SECONDS_PER_BLOCK = 12
_START_BLOCK = 21_500_000


def _intraday_weights():
    """24 relative weights per hour: cos curve, busiest at 15:00, cheapest at 03:00."""
    return [1.0 + 0.5 * math.cos(2 * math.pi * (h - 15) / 24) for h in range(24)]


def _gas_base_walk(rng, n_days):
    """Bounded random walk for daily base gas price [8,40] gwei."""
    base = rng.uniform(15, 25)
    bases = []
    for _ in range(n_days):
        bases.append(base)
        base = max(8, min(40, base + rng.gauss(0, 1.5)))
    return bases


def _generate_day(date_str, day_idx, world, rng, tx_per_day, base_gas_gwei):
    """Generate all transactions for one day. Returns (rows, n_duplicates, n_bad_rows, n_failed, n_cc)."""
    date = datetime.date.fromisoformat(date_str)
    is_weekend = date.weekday() >= 5
    volume_mult = (1 + rng.uniform(-0.1, 0.1)) * (0.85 if is_weekend else 1.0)
    n_tx = int(tx_per_day * volume_mult)

    hour_weights = _intraday_weights()
    hw_sum = sum(hour_weights)
    hw_norm = [w / hw_sum for w in hour_weights]

    day_start = datetime.datetime(date.year, date.month, date.day, tzinfo=datetime.timezone.utc)
    day_start_ts = int(day_start.timestamp())

    def assign_hour():
        return rng.choices(range(24), weights=hw_norm)[0]

    def block_info(hour):
        sec_offset = rng.randint(0, 3599)
        abs_sec = hour * 3600 + sec_offset
        block_sec = (abs_sec // _SECONDS_PER_BLOCK) * _SECONDS_PER_BLOCK
        ts_unix = day_start_ts + block_sec
        blk = _START_BLOCK + (day_idx * 86400 + block_sec) // _SECONDS_PER_BLOCK
        ts_us = ts_unix * 1_000_000
        return blk, ts_us

    def make_hash():
        return "0x" + rng.randbytes(32).hex()

    def wei_str(eth_val):
        return str(int(eth_val * 1e18))

    def gas_price_for_hour(h):
        m = 1.0 + 0.4 * math.cos(2 * math.pi * (h - 15) / 24)
        noise = math.exp(rng.gauss(0, 0.25))
        return max(1, int(base_gas_gwei * m * noise * 1e9))

    # Counts for spec-defined entity segments
    # Hub: 8% of total tx as plain transfers (senders from normal users)
    n_hub = int(n_tx * 0.08)
    # Bots: 8 × 1% = 8% of total (but capped so total stays near n_tx)
    n_bot_total = int(n_tx * 0.08)
    # Wash ring: ~20 hops per ring member per day
    n_ring = 20 * len(world["wash_ring"])
    # Whales: 1-5 tx/day each
    whale_tx_counts = {w: rng.randint(1, 5) for w in world["whales"]}
    n_whale = sum(whale_tx_counts.values())
    # Remaining budget goes to normal user transactions
    n_normal = max(0, n_tx - n_hub - n_bot_total - n_ring - n_whale)

    rows = []  # rows without nonces initially; nonces assigned in a second pass
    n_failed = 0
    n_contract_creations = 0

    # ---- Normal users: contract calls, contract creations, plain transfers to random users ----
    # Note: plain transfers go to random users only here; hub traffic is in the hub section below.
    for _ in range(n_normal):
        sender = rng.choices(world["users"], weights=world["user_weights"])[0]
        roll = rng.random()
        if roll < 0.45:
            to = rng.choices(world["users"], weights=world["user_weights"])[0]
            gas_used = 21000
            gas_limit = 21000
            is_creation = False
        elif roll < 0.997:
            to = rng.choices(world["contracts"], weights=world["contract_weights"])[0]
            gas_used = rng.randint(30_000, 300_000)
            gas_limit = int(gas_used * rng.uniform(1.0, 1.5))
            is_creation = False
        else:
            to = None
            gas_used = rng.randint(500_000, 3_000_000)
            gas_limit = int(gas_used * rng.uniform(1.0, 1.5))
            is_creation = True
            n_contract_creations += 1

        eth_val = min(1000.0, math.exp(rng.gauss(-2, 2)))
        h = assign_hour()
        gp = gas_price_for_hour(h)
        blk, ts_us = block_info(h)
        tx_type = 2 if rng.random() < 0.9 else 0
        status = 0 if rng.random() < 0.02 else 1
        if status == 0:
            n_failed += 1
        rows.append({
            "hash": make_hash(),
            "block_number": blk,
            "block_timestamp": ts_us,
            "from_address": sender,
            "to_address": to,
            "value": wei_str(eth_val),
            "gas": gas_limit,
            "gas_price": gp,
            "nonce": None,  # assigned in second pass
            "receipt_gas_used": gas_used,
            "receipt_effective_gas_price": gp,
            "receipt_status": status,
            "transaction_type": tx_type,
        })

    # ---- Hub: plain transfers from normal users to the hub (~8% of total) ----
    for _ in range(n_hub):
        sender = rng.choices(world["users"], weights=world["user_weights"])[0]
        eth_val = min(1000.0, math.exp(rng.gauss(-2, 2)))
        h = assign_hour()
        gp = gas_price_for_hour(h)
        blk, ts_us = block_info(h)
        tx_type = 2 if rng.random() < 0.9 else 0
        status = 0 if rng.random() < 0.02 else 1
        if status == 0:
            n_failed += 1
        rows.append({
            "hash": make_hash(),
            "block_number": blk,
            "block_timestamp": ts_us,
            "from_address": sender,
            "to_address": world["hub"],
            "value": wei_str(eth_val),
            "gas": 21000,
            "gas_price": gp,
            "nonce": None,
            "receipt_gas_used": 21000,
            "receipt_effective_gas_price": gp,
            "receipt_status": status,
            "transaction_type": tx_type,
        })

    # ---- Bots: even hour distribution, fixed targets, gas_used=65000, nonce>=200000 ----
    n_per_bot = n_bot_total // len(world["bots"])
    for b_idx, bot in enumerate(world["bots"]):
        for _ in range(n_per_bot):
            target = rng.choice(world["bot_targets"])
            eth_val = rng.uniform(0.0001, 0.01)
            h = rng.randint(0, 23)
            gp = gas_price_for_hour(h)
            blk, ts_us = block_info(h)
            status = 0 if rng.random() < 0.02 else 1
            if status == 0:
                n_failed += 1
            rows.append({
                "hash": make_hash(),
                "block_number": blk,
                "block_timestamp": ts_us,
                "from_address": bot,
                "to_address": target,
                "value": wei_str(eth_val),
                "gas": int(65000 * rng.uniform(1.0, 1.5)),
                "gas_price": gp,
                "nonce": None,  # assigned from bot's special starting nonce in second pass
                "_bot_index": b_idx,
                "receipt_gas_used": 65000,
                "receipt_effective_gas_price": gp,
                "receipt_status": status,
                "transaction_type": 2,
            })

    # ---- Wash ring: A→B→C→D→E→F→A, ~20 hops/day, ~50 ETH ----
    ring = world["wash_ring"]
    for i in range(len(ring)):
        sender = ring[i]
        receiver = ring[(i + 1) % len(ring)]
        for _ in range(20):
            eth_val = 50.0 * (1 + rng.uniform(-0.001, 0.001))
            h = assign_hour()
            gp = gas_price_for_hour(h)
            blk, ts_us = block_info(h)
            status = 0 if rng.random() < 0.02 else 1
            if status == 0:
                n_failed += 1
            rows.append({
                "hash": make_hash(),
                "block_number": blk,
                "block_timestamp": ts_us,
                "from_address": sender,
                "to_address": receiver,
                "value": wei_str(eth_val),
                "gas": 21000,
                "gas_price": gp,
                "nonce": None,
                "receipt_gas_used": 21000,
                "receipt_effective_gas_price": gp,
                "receipt_status": status,
                "transaction_type": 2,
            })

    # ---- Whales: 1-5 tx/day, 500-20000 ETH ----
    for whale, n_w in whale_tx_counts.items():
        for _ in range(n_w):
            eth_val = rng.uniform(500, 20_000)
            to = world["hub"] if rng.random() < 0.5 else rng.choice(world["users"])
            h = assign_hour()
            gp = gas_price_for_hour(h)
            blk, ts_us = block_info(h)
            status = 0 if rng.random() < 0.02 else 1
            if status == 0:
                n_failed += 1
            rows.append({
                "hash": make_hash(),
                "block_number": blk,
                "block_timestamp": ts_us,
                "from_address": whale,
                "to_address": to,
                "value": wei_str(eth_val),
                "gas": 21000,
                "gas_price": gp,
                "nonce": None,
                "receipt_gas_used": 21000,
                "receipt_effective_gas_price": gp,
                "receipt_status": status,
                "transaction_type": 2,
            })

    # ---- Second pass: assign nonces in (block_timestamp, random) order per sender ----
    # Add a random tie-break key temporarily
    for r in rows:
        r["_tie"] = rng.random()

    rows.sort(key=lambda r: (r["block_timestamp"], r["_tie"]))

    sender_nonce = {}
    # Initialize bots with their current nonces (updated each day)
    for bot in world["bots"]:
        sender_nonce[bot] = world["bot_nonces"][bot]

    for r in rows:
        sender = r["from_address"]
        if sender is None:
            r["nonce"] = 0
        else:
            if sender not in sender_nonce:
                # Normal users / whales / ring: look up starting nonce from world
                if sender in world["user_nonces"]:
                    sender_nonce[sender] = world["user_nonces"][sender]
                elif sender in world["ring_nonces"]:
                    sender_nonce[sender] = world["ring_nonces"][sender]
                elif sender in world["whale_nonces"]:
                    sender_nonce[sender] = world["whale_nonces"][sender]
                else:
                    sender_nonce[sender] = 0
            r["nonce"] = sender_nonce[sender]
            sender_nonce[sender] += 1

        # Clean up temp fields
        r.pop("_tie", None)
        r.pop("_bot_index", None)

    # Update world nonces for next day
    for addr, nonce in sender_nonce.items():
        if addr in world["user_nonces"]:
            world["user_nonces"][addr] = nonce
        elif addr in world["ring_nonces"]:
            world["ring_nonces"][addr] = nonce
        elif addr in world["whale_nonces"]:
            world["whale_nonces"][addr] = nonce
        elif addr in world["bot_nonces"]:
            world["bot_nonces"][addr] = nonce

    # ---- Plant duplicates (0.1% of successful non-null-to rows) ----
    successful = [r for r in rows if r["receipt_status"] == 1 and r["to_address"] is not None]
    n_dup = max(1, int(len(rows) * 0.001))
    dup_sources = rng.choices(successful, k=n_dup)
    dup_rows = []
    for src in dup_sources:
        dup_rows.append(dict(src))
    rows.extend(dup_rows)
    n_duplicates = n_dup

    # ---- Plant bad rows (0.01% with from_address=null) ----
    n_bad = max(1, int(len(rows) * 0.0001))
    bad_candidates = [i for i, r in enumerate(rows) if r["receipt_status"] == 1]
    bad_idxs = rng.choices(bad_candidates, k=n_bad)
    for idx in bad_idxs:
        rows[idx] = dict(rows[idx])
        rows[idx]["from_address"] = None

    return rows, n_duplicates, n_bad, n_failed, n_contract_creations


def _rows_to_table(rows):
    """Convert list of row dicts to a pyarrow Table with exact bronze schema."""
    cols = {field.name: [] for field in BRONZE_PA_SCHEMA}
    for r in rows:
        for field in BRONZE_PA_SCHEMA:
            cols[field.name].append(r.get(field.name))

    arrays = []
    for field in BRONZE_PA_SCHEMA:
        if field.name == "block_timestamp":
            arrays.append(pa.array(cols[field.name], type=pa.timestamp("us", tz="UTC")))
        else:
            arrays.append(pa.array(cols[field.name], type=field.type))
    return pa.table(arrays, schema=BRONZE_PA_SCHEMA)


def generate(seed: int, preset: str, start_date: str, out_dir: str):
    """Generate bronze Parquet files and manifest.json."""
    cfg = PRESETS[preset]
    days = cfg["days"]
    tx_per_day = cfg["tx_per_day"]

    world = build_world(seed)
    rng = random.Random(seed + 1)

    os.makedirs(out_dir, exist_ok=True)

    gas_bases = _gas_base_walk(rng, days)

    start = datetime.date.fromisoformat(start_date)
    totals = {"duplicates": 0, "bad_rows": 0, "failed": 0, "contract_creations": 0}
    total_rows = 0

    for d in range(days):
        date = start + datetime.timedelta(days=d)
        date_str = date.isoformat()
        rows, n_dup, n_bad, n_fail, n_cc = _generate_day(
            date_str, d, world, rng, tx_per_day, gas_bases[d]
        )
        totals["duplicates"] += n_dup
        totals["bad_rows"] += n_bad
        totals["failed"] += n_fail
        totals["contract_creations"] += n_cc
        total_rows += len(rows)

        table = _rows_to_table(rows)
        out_path = os.path.join(out_dir, f"part-{date_str}.parquet")
        pq.write_table(table, out_path, compression="snappy")

    manifest = {
        "seed": seed,
        "preset": preset,
        "start_date": start_date,
        "end_date": (start + datetime.timedelta(days=days - 1)).isoformat(),
        "days": days,
        "tx_per_day_target": tx_per_day,
        "total_rows": total_rows,
        "hub": world["hub"],
        "bots": world["bots"],
        "bot_targets": world["bot_targets"],
        "wash_ring": world["wash_ring"],
        "whales": world["whales"],
        "contracts_top": world["contracts"][:10],
        "spammer": None,
        "cheapest_hour_utc": 3,
        "peak_hour_utc": 15,
        "n_duplicates": totals["duplicates"],
        "n_bad_rows": totals["bad_rows"],
        "n_failed": totals["failed"],
        "n_contract_creations": totals["contract_creations"],
    }

    manifest_dir = os.path.join(os.path.dirname(os.path.dirname(out_dir)), "mock")
    os.makedirs(manifest_dir, exist_ok=True)
    manifest_path = os.path.join(manifest_dir, "manifest.json")
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    return manifest


def main():
    parser = argparse.ArgumentParser(description="Generate mock bronze Parquet data")
    parser.add_argument("--preset", choices=PRESETS.keys(), default="small")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--start-date", default="2025-01-01")
    parser.add_argument("--out", default="data/bronze/tx")
    args = parser.parse_args()

    t0 = time.time()
    manifest = generate(args.seed, args.preset, args.start_date, args.out)
    elapsed = time.time() - t0

    print(f"Generated {manifest['total_rows']:,} rows in {elapsed:.1f}s")
    print(f"  Days: {manifest['days']}, preset: {manifest['preset']}")
    print(f"  Duplicates: {manifest['n_duplicates']}, bad_rows: {manifest['n_bad_rows']}, "
          f"failed: {manifest['n_failed']}, contract_creations: {manifest['n_contract_creations']}")
    print(f"  Manifest: data/mock/manifest.json")


if __name__ == "__main__":
    main()
