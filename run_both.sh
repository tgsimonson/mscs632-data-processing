#!/usr/bin/env bash
# runs both versions on the same task file and compares the sorted results
set -u
cd "$(dirname "$0")"
WORKERS="${1:-4}"

echo "========== JAVA =========="
(cd java && ./dataproc run --workers "$WORKERS")
echo
echo "========== GO =========="
(cd go && go run . run -workers "$WORKERS")
echo
echo "========== COMPARE =========="
if diff java/results/java-results.txt go/results/go-results.txt; then
  echo "IDENTICAL: both versions wrote the same $(($(wc -l < go/results/go-results.txt) - 1)) results"
else
  echo "DIFFERENT (diff above)"
  exit 1
fi
