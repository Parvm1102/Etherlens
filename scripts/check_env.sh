#!/usr/bin/env bash
set -euo pipefail

FAIL=0

check() {
    local name="$1"
    local cmd="$2"
    if result=$(eval "$cmd" 2>&1); then
        echo "OK  $name: $result"
    else
        echo "FAIL $name: $result"
        FAIL=1
    fi
}

echo "=== EtherLens environment check ==="

check "python"  "python --version 2>&1 | head -1"
check "java"    "java -version 2>&1 | head -1"
check "pyspark" "python -c 'import pyspark; print(pyspark.__version__)'"
check "forge"   "forge --version 2>&1 | head -1"
check "anvil"   "anvil --version 2>&1 | head -1"
check "cast"    "cast --version 2>&1 | head -1"
check "gcloud"  "gcloud --version 2>&1 | head -1"
check "bq"      "bq version 2>&1 | head -1"

echo ""
echo "=== Smoke Spark job ==="
python - <<'PYEOF'
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").appName("smoke").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
n = spark.range(1000).count()
assert n == 1000, f"Expected 1000, got {n}"
print(f"spark.range(1000).count() = {n}  OK")
spark.stop()
PYEOF

echo ""
echo "=== CRLF check ==="
CRLF_FILES=$(find /work -name "*.sh" | xargs grep -rlP "\r" 2>/dev/null || true)
if [ -n "$CRLF_FILES" ]; then
    echo "FAIL: Shell scripts with CRLF:"
    echo "$CRLF_FILES"
    FAIL=1
else
    echo "OK  No CRLF in shell scripts"
fi

echo ""
if [ $FAIL -eq 0 ]; then
    echo "All checks passed."
else
    echo "One or more checks FAILED."
    exit 1
fi
