#!/bin/bash

# Script to run mutable with SortMergeJoin and capture all output
OUTPUT_FILE="sortmerge_debug_output.log"

echo "Running mutable with SortMergeJoin debugging..."
echo "Output will be saved to: $OUTPUT_FILE"

# Run the command and capture both stdout and stderr
./build/debug_shared/bin/shell \
    --backend Interpreter \
    --echo \
    --times \
    --graph \
    --plan \
    --output-partial-plans-file test.json \
    --join-implementations NestedLoops \
    --wasm-opt 0 \
    --no-simd \
    --plan-enumerator GOO \
    - < small_queries.sql 2>&1 | tee "$OUTPUT_FILE"

echo ""
echo "Execution completed. Check $OUTPUT_FILE for debug output."
echo "Looking for 'SortMergeJoin: EXECUTED' in the output..."
grep -n "SortMergeJoin: EXECUTED" "$OUTPUT_FILE" || echo "Debug message not found in output."

exit 0
