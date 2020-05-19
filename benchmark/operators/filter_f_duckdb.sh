#!/bin/bash

# Define path to DuckDB CLI
DUCKDB=duckdb_cli

{ ${DUCKDB} | grep 'Run Time' | cut -d ' ' -f 4 | awk '{print $1 * 1000;}'; } << EOF
CREATE TABLE Attributes_f ( a0 REAL, a1 REAL, a2 REAL, a3 REAL, a4 REAL, a5 REAL, a6 REAL, a7 REAL, a8 REAL, a9 REAL);
COPY Attributes_f FROM 'benchmark/operators/data/Attributes_f.csv' ( HEADER );
.timer on
SELECT COUNT(*) FROM Attributes_f WHERE a0 < 0.0;
SELECT COUNT(*) FROM Attributes_f WHERE a0 < 0.1;
SELECT COUNT(*) FROM Attributes_f WHERE a0 < 0.2;
SELECT COUNT(*) FROM Attributes_f WHERE a0 < 0.3;
SELECT COUNT(*) FROM Attributes_f WHERE a0 < 0.4;
SELECT COUNT(*) FROM Attributes_f WHERE a0 < 0.5;
SELECT COUNT(*) FROM Attributes_f WHERE a0 < 0.6;
SELECT COUNT(*) FROM Attributes_f WHERE a0 < 0.7;
SELECT COUNT(*) FROM Attributes_f WHERE a0 < 0.8;
SELECT COUNT(*) FROM Attributes_f WHERE a0 < 0.9;
SELECT COUNT(*) FROM Attributes_f WHERE a0 < 1.0;
EOF
