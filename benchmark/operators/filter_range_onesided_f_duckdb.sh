#!/bin/bash

# Define path to DuckDB CLI
DUCKDB=duckdb_cli

{ ${DUCKDB} | grep 'Run Time' | cut -d ' ' -f 4 | awk '{print $1 * 1000;}'; } << EOF
CREATE TABLE Attribute_f ( id INT, val REAL );
COPY Attribute_f FROM 'benchmark/operators/data/Attribute_f.csv' ( HEADER );
.timer on
SELECT COUNT(*) FROM Attribute_f WHERE val < 0.01;
SELECT COUNT(*) FROM Attribute_f WHERE val < 0.05;
SELECT COUNT(*) FROM Attribute_f WHERE val < 0.10;
SELECT COUNT(*) FROM Attribute_f WHERE val < 0.20;
SELECT COUNT(*) FROM Attribute_f WHERE val < 0.30;
SELECT COUNT(*) FROM Attribute_f WHERE val < 0.40;
SELECT COUNT(*) FROM Attribute_f WHERE val < 0.50;
SELECT COUNT(*) FROM Attribute_f WHERE val < 0.60;
SELECT COUNT(*) FROM Attribute_f WHERE val < 0.70;
SELECT COUNT(*) FROM Attribute_f WHERE val < 0.80;
SELECT COUNT(*) FROM Attribute_f WHERE val < 0.90;
SELECT COUNT(*) FROM Attribute_f WHERE val < 0.95;
SELECT COUNT(*) FROM Attribute_f WHERE val < 0.99;
EOF
