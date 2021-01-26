#!/bin/bash

# Define path to DuckDB CLI
DUCKDB=duckdb_cli

{ ${DUCKDB} | grep 'Run Time' | cut -d ' ' -f 4 | awk '{print $1 * 1000;}'; } << EOF
CREATE TABLE Attribute_i32 ( id INT, val INT);
COPY Attribute_i32 FROM 'benchmark/operators/data/Attribute_i32.csv' ( HEADER );
.timer on
SELECT COUNT(*) FROM Attribute_i32 WHERE val < -2104533974;
SELECT COUNT(*) FROM Attribute_i32 WHERE val < -1932735282;
SELECT COUNT(*) FROM Attribute_i32 WHERE val < -1717986917;
SELECT COUNT(*) FROM Attribute_i32 WHERE val < -1288490188;
SELECT COUNT(*) FROM Attribute_i32 WHERE val <  -858993458;
SELECT COUNT(*) FROM Attribute_i32 WHERE val <  -429496729;
SELECT COUNT(*) FROM Attribute_i32 WHERE val <           0;
SELECT COUNT(*) FROM Attribute_i32 WHERE val <   429496729;
SELECT COUNT(*) FROM Attribute_i32 WHERE val <   858993458;
SELECT COUNT(*) FROM Attribute_i32 WHERE val <  1288490188;
SELECT COUNT(*) FROM Attribute_i32 WHERE val <  1717986917;
SELECT COUNT(*) FROM Attribute_i32 WHERE val <  1932735282;
SELECT COUNT(*) FROM Attribute_i32 WHERE val <  2104533974;
EOF
