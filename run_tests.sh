#!/usr/bin/env bash
# runs the unit tests for both versions
set -u
cd "$(dirname "$0")"
status=0

echo "========== JAVA TESTS =========="
(
  cd java
  rm -rf out out-test
  javac -d out $(find src -name '*.java') && touch out/.stamp &&
  javac -d out-test -cp out test/dataproc/EngineTest.java &&
  java -cp out:out-test dataproc.EngineTest
) || status=1

echo
echo "========== GO TESTS (race detector on) =========="
(
  cd go
  # the race detector needs cgo; fall back to a plain run where it is missing
  go test -race -count=1 -v ./... 2>/dev/null | grep -E "^(--- |ok|FAIL|PASS)" ||
    go test -count=1 -v ./... | grep -E "^(--- |ok|FAIL|PASS)"
  exit "${PIPESTATUS[0]}"
) || status=1

echo
[ $status -eq 0 ] && echo "ALL TESTS PASSED" || echo "SOME TESTS FAILED"
exit $status
