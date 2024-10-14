#!/usr/bin/env bash

queries=("chain" "cycle" "tvc")

for query in "${queries[@]}"; do
    python ./benchmark/Benchmark.py --output ./benchmark/result-db-eval/synthetic/recreated_results/${query}_synthetic.csv ./benchmark/result-db-eval/synthetic/experiments_${query}/
done