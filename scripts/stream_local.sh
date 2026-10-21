#!/usr/bin/env bash
# Start a poller + the stream job together; Ctrl-C stops both.
# Usage (inside the dev container):
#   bash scripts/stream_local.sh [mock|replay|live]
#     mock   : synthetic blocks (default, works offline)
#     replay : replays one day of local silver at REPLAY_SPEED x real time (REPLAY_DT, default 2025-01-01)
#     live   : real Ethereum mainnet blocks; needs ETH_RPC_URL in .env
# Host usage: docker compose run --rm --service-ports dev bash scripts/stream_local.sh mock
set -euo pipefail

MODE="${1:-mock}"
INTERVAL="${STREAM_INTERVAL:-2}"
MAX_MINUTES="${MAX_MINUTES:-10}"

cleanup() {
    echo "Stopping poller and stream job..."
    kill "$POLLER_PID" 2>/dev/null || true
    kill "$STREAM_PID" 2>/dev/null || true
    wait
    echo "Done."
}
trap cleanup INT TERM

# Start clean: old block files and checkpoints would mix runs (all regenerable)
rm -rf data/stream results/stream

case "$MODE" in
    mock)   POLLER_ARGS=(--mode mock --interval "$INTERVAL" --manifest data/mock/manifest.json) ;;
    replay) POLLER_ARGS=(--mode replay --silver data/silver/tx --dt "${REPLAY_DT:-2025-01-01}" --speed "${REPLAY_SPEED:-60}") ;;
    live)
        if [[ -z "${ETH_RPC_URL:-}" ]]; then echo "live mode needs ETH_RPC_URL (a mainnet RPC URL) in .env"; exit 1; fi
        POLLER_ARGS=(--mode live) ;;
    *) echo "Usage: $0 [mock|replay|live]"; exit 1 ;;
esac

echo "Starting $MODE poller..."
python -m etherlens.stream.poller "${POLLER_ARGS[@]}" --out data/stream/incoming &
POLLER_PID=$!

# The stream job needs the input folder to exist before it starts
for _ in $(seq 1 30); do [[ -d data/stream/incoming ]] && break; sleep 1; done

echo "Starting stream job (max_minutes=${MAX_MINUTES})..."
python jobs/stream_main.py --env local --max-minutes "$MAX_MINUTES" &
STREAM_PID=$!

echo "Spark UI: http://localhost:4040 · results in results/stream/"
echo "Press Ctrl-C to stop."
wait "$STREAM_PID"
kill "$POLLER_PID" 2>/dev/null || true
