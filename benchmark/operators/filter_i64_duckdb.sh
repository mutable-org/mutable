#!/bin/bash

# Define path to DuckDB CLI
DUCKDB=duckdb_cli

{ ${DUCKDB} | grep 'Run Time' | cut -d ' ' -f 4 | awk '{print $1 * 1000;}'; } << EOF
CREATE TABLE Attribute_i64 ( id INT, val BIGINT );
COPY Attribute_i64 FROM 'benchmark/operators/data/Attribute_i64.csv' ( HEADER );
.timer on
SELECT COUNT(*) FROM Attribute_i64 WHERE val < -9223372036854775807;
SELECT COUNT(*) FROM Attribute_i64 WHERE val < -7378697629483821056;
SELECT COUNT(*) FROM Attribute_i64 WHERE val < -5534023222112865280;
SELECT COUNT(*) FROM Attribute_i64 WHERE val < -3689348814741910528;
SELECT COUNT(*) FROM Attribute_i64 WHERE val < -1844674407370954752;
SELECT COUNT(*) FROM Attribute_i64 WHERE val <                    0;
SELECT COUNT(*) FROM Attribute_i64 WHERE val <  1844674407370954752;
SELECT COUNT(*) FROM Attribute_i64 WHERE val <  3689348814741909504;
SELECT COUNT(*) FROM Attribute_i64 WHERE val <  5534023222112866304;
SELECT COUNT(*) FROM Attribute_i64 WHERE val <  7378697629483821056;
SELECT COUNT(*) FROM Attribute_i64 WHERE val <  9223372036854775807;
EOF
