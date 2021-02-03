#!/bin/bash

SCALE_FACTORS=(0 10 20 30 40 50 60 70 80 90 100)
CSV="benchmark/operators/data/Relation.csv"
NUM_ROWS=$(wc -l "${CSV}" | cut -f 1 -d ' ')
NUM_ROWS=$((NUM_ROWS-1))

# Define path to DuckDB CLI
DUCKDB=duckdb_cli

trap 'exit' INT
for sf in ${SCALE_FACTORS[@]};
do
    { ${DUCKDB} | grep 'Run Time' | cut -d ' ' -f 4 | awk '{print $1 * 1000;}'; } << EOF
CREATE TABLE tmp ( id INT PRIMARY KEY, fid INT, n2m INT );
COPY tmp FROM '${CSV}' ( HEADER );
CREATE TABLE Relation ( id INT PRIMARY KEY, fid INT, n2m INT );
INSERT INTO Relation SELECT * FROM tmp LIMIT $((NUM_ROWS * sf / 100));
.timer on
SELECT COUNT(*) FROM Relation R, Relation S WHERE R.id = S.fid;
EOF
done
