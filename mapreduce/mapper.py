#!/usr/bin/env python3
"""MapReduce mapper — Phase 4.3 (stub)."""
import sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.split(",")
    if len(parts) < 2:
        print("reporter:counter:etherlens,malformed,1", file=sys.stderr)
        continue
    print(f"{parts[0]}\t1")
