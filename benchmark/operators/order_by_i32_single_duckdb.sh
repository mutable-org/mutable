#!/bin/bash

SCALE_FACTORS=(0 10 20 30 40 50 60 70 80 90 100)
CSV="benchmark/operators/data/Distinct_i32.csv"
NUM_ROWS=$(wc -l "${CSV}" | cut -f 1 -d ' ')
NUM_ROWS=$((NUM_ROWS-1))

# Define path to DuckDB CLI
DUCKDB=duckdb_cli

trap 'exit' INT
for sf in ${SCALE_FACTORS[@]};
do
    { ${DUCKDB} | grep 'Run Time' | cut -d ' ' -f 4 | awk '{print $1 * 1000;}'; } << EOF
CREATE TABLE tmp ( id INT, n1 INT, n10 INT, n100 INT, n1000 INT, n10000 INT, n100000 INT);
COPY tmp FROM '${CSV}' ( HEADER );
CREATE TABLE Distinct_i32 ( id INT, n1 INT, n10 INT, n100 INT, n1000 INT, n10000 INT, n100000 INT);
INSERT INTO Distinct_i32 SELECT * FROM tmp LIMIT $((NUM_ROWS * sf / 100));
.timer on
SELECT id FROM Distinct_i32 ORDER BY n100000;
EOF
done
