#!/bin/bash

# Define path to DuckDB CLI
DUCKDB=duckdb_cli

{ ${DUCKDB} | grep 'Run Time' | cut -d ' ' -f 4 | awk '{print $1 * 1000;}'; } << EOF
CREATE TABLE Attributes_i32 ( a0 INT, a1 INT, a2 INT, a3 INT, a4 INT, a5 INT, a6 INT, a7 INT, a8 INT, a9 INT);
COPY Attributes_i32 FROM 'benchmark/operators/data/Attributes_i32.csv' ( HEADER );
.timer on
SELECT COUNT(*) FROM Attributes_i32 WHERE a0 < -2147483647;
SELECT COUNT(*) FROM Attributes_i32 WHERE a0 < -1717986918;
SELECT COUNT(*) FROM Attributes_i32 WHERE a0 < -1288490188;
SELECT COUNT(*) FROM Attributes_i32 WHERE a0 < -858993459;
SELECT COUNT(*) FROM Attributes_i32 WHERE a0 < -429496729;
SELECT COUNT(*) FROM Attributes_i32 WHERE a0 < 0;
SELECT COUNT(*) FROM Attributes_i32 WHERE a0 < 429496729;
SELECT COUNT(*) FROM Attributes_i32 WHERE a0 < 858993459;
SELECT COUNT(*) FROM Attributes_i32 WHERE a0 < 1288490188;
SELECT COUNT(*) FROM Attributes_i32 WHERE a0 < 1717986918;
SELECT COUNT(*) FROM Attributes_i32 WHERE a0 < 2147483647;
EOF
