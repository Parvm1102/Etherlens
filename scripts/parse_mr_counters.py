#!/usr/bin/env python3
"""Parse MapReduce job counters from log files and write mapreduce_counters.json.

Usage: python scripts/parse_mr_counters.py [output_path]   (default results/mapreduce_counters.json)
"""
import json
import re
import sys
from pathlib import Path


def parse_log(path: str) -> dict:
    text = Path(path).read_text(errors="replace")
    counters = {}
    patterns = {
        "map_output_records": r"Map output records=(\d+)",
        "combine_input_records": r"Combine input records=(\d+)",
        "combine_output_records": r"Combine output records=(\d+)",
        "reduce_input_records": r"Reduce input records=(\d+)",
        "reduce_shuffle_bytes": r"Reduce shuffle bytes=(\d+)",
    }
    for key, pat in patterns.items():
        m = re.search(pat, text)
        counters[key] = int(m.group(1)) if m else None

    elapsed = re.search(r"elapsed time: (\d+)", text)
    counters["elapsed_s"] = int(elapsed.group(1)) if elapsed else None
    return counters


def main():
    logs = sorted(Path("logs").glob("mr_*.log")) if Path("logs").exists() else []
    result = {}
    for log in logs:
        # log names look like mr_no_combiner_<timestamp>.log; later runs overwrite earlier
        variant = "with_combiner" if log.stem.startswith("mr_with_combiner") else "no_combiner"
        result[variant] = parse_log(str(log))

    out = Path(sys.argv[1] if len(sys.argv) > 1 else "results/mapreduce_counters.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=2))
    print(f"Written: {out}")


if __name__ == "__main__":
    main()
