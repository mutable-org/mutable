#!/usr/bin/env bash

benchmarks=(
    "star_joins_1"
    "star_joins_2"
    "star_joins_3"
    "star_joins_4"
    "star_proj_2_dim"
    "star_proj_3_dim"
    "star_proj_4_dim"
    "star_proj_fact_1_dim"
    "star_proj_fact_2_dim"
    "star_proj_fact_3_dim"
    "star_proj_fact_4_dim"
    "star_sel_10"
    "star_sel_20"
    "star_sel_30"
    "star_sel_40"
    "star_sel_50"
    "star_sel_60"
    "star_sel_70"
    "star_sel_80"
    "star_sel_90"
    "star_sel_100"
)

for bench in "${benchmarks[@]}"; do
    python3 ./benchmark/Benchmark.py ./benchmark/job/result-db-queries/star-schema/${bench}_benchmark.yml -o ./benchmark/job/result-db-queries/star-schema/${bench}_results.csv
done
