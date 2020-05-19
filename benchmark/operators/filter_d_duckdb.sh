#!/bin/bash

# Define path to DuckDB CLI
DUCKDB=duckdb_cli

{ ${DUCKDB} | ack 'Run Time' | cut -d ' ' -f 4 | awk '{print $1 * 1000;}'; } << EOF
CREATE TABLE Attributes_d ( a0 DOUBLE, a1 DOUBLE, a2 DOUBLE, a3 DOUBLE, a4 DOUBLE, a5 DOUBLE, a6 DOUBLE, a7 DOUBLE, a8 DOUBLE, a9 DOUBLE);
COPY Attributes_d FROM 'benchmark/operators/data/Attributes_d.csv' ( HEADER );
.timer on
SELECT COUNT(*) FROM Attributes_d WHERE a0 < 0.0;
SELECT COUNT(*) FROM Attributes_d WHERE a0 < 0.1;
SELECT COUNT(*) FROM Attributes_d WHERE a0 < 0.2;
SELECT COUNT(*) FROM Attributes_d WHERE a0 < 0.3;
SELECT COUNT(*) FROM Attributes_d WHERE a0 < 0.4;
SELECT COUNT(*) FROM Attributes_d WHERE a0 < 0.5;
SELECT COUNT(*) FROM Attributes_d WHERE a0 < 0.6;
SELECT COUNT(*) FROM Attributes_d WHERE a0 < 0.7;
SELECT COUNT(*) FROM Attributes_d WHERE a0 < 0.8;
SELECT COUNT(*) FROM Attributes_d WHERE a0 < 0.9;
SELECT COUNT(*) FROM Attributes_d WHERE a0 < 1.0;
EOF
