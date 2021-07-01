#!/bin/bash

# Define path to DuckDB CLI
DUCKDB=duckdb_cli

{ ${DUCKDB} | grep 'Run Time' | cut -d ' ' -f 4 | awk '{print $1 * 1000;}'; } << EOF
CREATE TABLE Attributes_multi_i32 ( id INT, a0 INT, a1 INT, a2 INT, a3 INT );
COPY Attributes_multi_i32 FROM 'benchmark/operators/data/Attributes_multi_i32.csv' ( HEADER );
.timer on
SELECT COUNT(*) FROM Attributes_multi_i32 WHERE a0 < -2104533974 AND a1 < -2104533974;
SELECT COUNT(*) FROM Attributes_multi_i32 WHERE a0 < -1932735282 AND a1 < -2104533974;
SELECT COUNT(*) FROM Attributes_multi_i32 WHERE a0 < -1717986917 AND a1 < -2104533974;
SELECT COUNT(*) FROM Attributes_multi_i32 WHERE a0 < -1288490188 AND a1 < -2104533974;
SELECT COUNT(*) FROM Attributes_multi_i32 WHERE a0 <  -858993458 AND a1 < -2104533974;
SELECT COUNT(*) FROM Attributes_multi_i32 WHERE a0 <  -429496729 AND a1 < -2104533974;
SELECT COUNT(*) FROM Attributes_multi_i32 WHERE a0 <           0 AND a1 < -2104533974;
SELECT COUNT(*) FROM Attributes_multi_i32 WHERE a0 <   429496729 AND a1 < -2104533974;
SELECT COUNT(*) FROM Attributes_multi_i32 WHERE a0 <   858993458 AND a1 < -2104533974;
SELECT COUNT(*) FROM Attributes_multi_i32 WHERE a0 <  1288490188 AND a1 < -2104533974;
SELECT COUNT(*) FROM Attributes_multi_i32 WHERE a0 <  1717986917 AND a1 < -2104533974;
SELECT COUNT(*) FROM Attributes_multi_i32 WHERE a0 <  1932735282 AND a1 < -2104533974;
SELECT COUNT(*) FROM Attributes_multi_i32 WHERE a0 <  2104533974 AND a1 < -2104533974;
EOF
