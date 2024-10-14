#!/usr/bin/env bash

queries=("chain" "star" "cycle" "tvc")

for query in "${queries[@]}"; do
    python ./benchmark/Benchmark.py -n 20 --output ./benchmark/result-db-eval/enumeration_time/recreated_results/${query}.csv ./benchmark/result-db-eval/enumeration_time/${query}.yml
done