#!/bin/bash

# Define path to DuckDB CLI
DUCKDB=duckdb_cli

{ ${DUCKDB} | grep 'Run Time' | cut -d ' ' -f 4 | awk '{print $1 * 1000;}'; } << EOF
CREATE TABLE Attribute_f ( id INT, val REAL );
COPY Attribute_f FROM 'benchmark/operators/data/Attribute_f.csv' ( HEADER );
.timer on
SELECT COUNT(*) FROM Attribute_f WHERE val < 0.0;
SELECT COUNT(*) FROM Attribute_f WHERE val < 0.1;
SELECT COUNT(*) FROM Attribute_f WHERE val < 0.2;
SELECT COUNT(*) FROM Attribute_f WHERE val < 0.3;
SELECT COUNT(*) FROM Attribute_f WHERE val < 0.4;
SELECT COUNT(*) FROM Attribute_f WHERE val < 0.5;
SELECT COUNT(*) FROM Attribute_f WHERE val < 0.6;
SELECT COUNT(*) FROM Attribute_f WHERE val < 0.7;
SELECT COUNT(*) FROM Attribute_f WHERE val < 0.8;
SELECT COUNT(*) FROM Attribute_f WHERE val < 0.9;
SELECT COUNT(*) FROM Attribute_f WHERE val < 1.0;
EOF
