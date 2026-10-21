"""Merkle tree — must match ReportRegistry.sol bit for bit.

Rules:
1. leaf_i = keccak256(utf8(row_i))                    [eth_utils keccak, NOT hashlib.sha3_256]
2. parent(a, b) = keccak256(min(a,b) || max(a,b))     [sorted pairs, bytes comparison]
3. Bottom-up; odd count → promote last unchanged
4. One leaf → root = leaf; zero leaves → raise
5. proof(i): sibling hashes bottom-up; promoted node → skip
6. verify: fold with parent
7. Hex: "0x" + b.hex()
"""
import argparse
import json
import os

from eth_utils import keccak


def _keccak(data: bytes) -> bytes:
    return keccak(primitive=data)


def _leaf(row: str) -> bytes:
    return _keccak(row.encode("utf-8"))


def _parent(a: bytes, b: bytes) -> bytes:
    lo, hi = (a, b) if a < b else (b, a)
    return _keccak(lo + hi)


class MerkleTree:
    def __init__(self, rows: list[str]):
        if not rows:
            raise ValueError("Cannot build Merkle tree from empty list")
        self._rows = rows
        self._levels: list[list[bytes]] = []
        self._build()

    def _build(self):
        level = [_leaf(r) for r in self._rows]
        self._levels.append(level)
        while len(level) > 1:
            next_level = []
            for i in range(0, len(level) - 1, 2):
                next_level.append(_parent(level[i], level[i + 1]))
            if len(level) % 2 == 1:
                next_level.append(level[-1])  # promote last
            self._levels.append(next_level)
            level = next_level

    @property
    def root(self) -> str:
        return "0x" + self._levels[-1][0].hex()

    @property
    def leaves(self) -> list[str]:
        return ["0x" + b.hex() for b in self._levels[0]]

    def proof(self, i: int) -> list[str]:
        siblings = []
        idx = i
        for level in self._levels[:-1]:
            if idx % 2 == 0:
                # idx is left child; sibling is idx+1 (if it exists and is not a promoted node)
                if idx + 1 < len(level):
                    siblings.append("0x" + level[idx + 1].hex())
                # else: idx is a promoted lone node — append nothing
            else:
                # idx is right child; sibling is idx-1
                siblings.append("0x" + level[idx - 1].hex())
            idx //= 2
        return siblings


def build_tree(rows: list[str]) -> MerkleTree:
    return MerkleTree(rows)


def verify_row(row: str, proof: list[str], root: str) -> bool:
    current = _leaf(row)
    for sibling_hex in proof:
        sibling = bytes.fromhex(sibling_hex[2:] if sibling_hex.startswith("0x") else sibling_hex)
        current = _parent(current, sibling)
    return "0x" + current.hex() == root


def to_json(dt: str, rows: list[str]) -> dict:
    tree = build_tree(rows)
    return {
        "dt": dt,
        "root": tree.root,
        "leaf_count": len(rows),
        "rows": rows,
        "proofs": [tree.proof(i) for i in range(len(rows))],
    }


def main():
    parser = argparse.ArgumentParser(description="Build Merkle trees from daily reports")
    parser.add_argument("action", choices=["build"])
    parser.add_argument("--dt", help="Specific date yyyy-MM-dd")
    parser.add_argument("--all", action="store_true", dest="all_dates")
    parser.add_argument("--env", default="local")
    parser.add_argument("--bucket", default=None)
    parser.add_argument("--gold", default=None,
                        help="Override gold dir, e.g. data_gcp/gold after pulling GCP output")
    parser.add_argument("--results", default=None,
                        help="Override results dir, e.g. results_gcp")
    args = parser.parse_args()

    from etherlens.config import get_paths
    paths = get_paths(args.env, args.bucket)
    gold_dir = args.gold or paths["gold"]
    results_dir = args.results or paths["results"]

    report_base = os.path.join(gold_dir, "daily_report")
    out_dir = os.path.join(results_dir, "merkle")
    os.makedirs(out_dir, exist_ok=True)

    dates = []
    if args.all_dates:
        if os.path.isdir(report_base):
            for entry in os.listdir(report_base):
                if entry.startswith("dt="):
                    dates.append(entry[3:])
    elif args.dt:
        dates = [args.dt]

    for dt in sorted(dates):
        report_path = os.path.join(report_base, f"dt={dt}", "report.txt")
        if not os.path.exists(report_path):
            print(f"Skipping {dt}: report not found")
            continue
        with open(report_path) as f:
            rows = [line.rstrip("\n") for line in f if line.strip()]
        data = to_json(dt, rows)
        out_path = os.path.join(out_dir, f"{dt}.json")
        with open(out_path, "w") as f:
            json.dump(data, f, indent=2)
        print(f"{dt}: root={data['root']}, leaves={data['leaf_count']}")


if __name__ == "__main__":
    main()
