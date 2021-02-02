#!/bin/bash

SCALE_FACTOR=(0 10 20 30 40 50 60 70 80 90 100)
CSV="benchmark/operators/data/Distinct_i32.csv"
NUM_ROWS=$(wc -l "${CSV}" | cut -f 1 -d ' ')
NUM_ROWS=$((NUM_ROWS-1))

# Define path to PostgreSQL CLI
POSTGRESQL=psql

trap 'exit' INT
for sf in ${SCALE_FACTOR[@]};
do
    { ${POSTGRESQL} -U postgres | grep 'Time' | cut -d ' ' -f 2; } << EOF
DROP DATABASE IF EXISTS benchmark_tmp;
CREATE DATABASE benchmark_tmp;
\c benchmark_tmp
set jit=off;
CREATE TABLE Distinct_i32 ( id INT, n1 INT, n10 INT, n100 INT, n1000 INT, n10000 INT, n100000 INT);
\copy Distinct_i32 FROM PROGRAM 'head -n $((NUM_ROWS * sf / 100)) ${CSV}' WITH DELIMITER ',' CSV HEADER;
\timing on
SELECT id FROM Distinct_i32 ORDER BY n100000;
\timing off
EOF
done
