#!/bin/bash

SCALE_FACTORS=(0 10 20 30 40 50 60 70 80 90 100)
CSV="benchmark/operators/data/Relation.csv"
NUM_ROWS=$(wc -l "${CSV}" | cut -f 1 -d ' ')
NUM_ROWS=$((NUM_ROWS-1))

# Define path to PostgreSQL CLI
POSTGRESQL=psql

trap 'exit' INT
for sf in ${SCALE_FACTORS[@]};
do
    { ${POSTGRESQL} -U postgres | grep 'Time' | cut -d ' ' -f 2; } << EOF
set jit=off;
DROP DATABASE IF EXISTS benchmark_tmp;
CREATE DATABASE benchmark_tmp;
\c benchmark_tmp
CREATE TABLE Relation ( id INT PRIMARY KEY, fid INT );
\copy Relation FROM PROGRAM 'head -n $((NUM_ROWS * sf / 100)) ${CSV}' WITH DELIMITER ',' CSV HEADER;
\timing on
SELECT COUNT(*) FROM Relation R, Relation S WHERE R.id = S.fid;
\timing off
EOF
done
