#!/usr/bin/env bash
set -euo pipefail

EDGES_DIR="${1:-data/hdfs_sim/edges_csv}"
OUT="results/mapreduce/local_counts.tsv"

mkdir -p results/mapreduce

cat "${EDGES_DIR}"/part-* \
  | python mapreduce/mapper.py \
  | sort -k1,1 \
  | python mapreduce/reducer.py \
  > "$OUT"

echo "MapReduce local run complete: $OUT"
wc -l "$OUT"
