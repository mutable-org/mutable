#!/bin/bash

# Define path to DuckDB CLI
DUCKDB=duckdb_cli

{ ${DUCKDB} | ack 'Run Time' | cut -d ' ' -f 4 | awk '{print $1 * 1000;}'; } << EOF
CREATE TABLE Attributes_i32 ( a0 INT, a1 INT, a2 INT, a3 INT, a4 INT, a5 INT, a6 INT, a7 INT, a8 INT, a9 INT);
COPY Attributes_i32 FROM 'benchmark/operators/data/Attributes_i32.csv' ( HEADER );
.timer on
SELECT a0 FROM Attributes_i32 ORDER BY a0;
.timer off
DROP TABLE Attributes_i32;

CREATE TABLE Attributes_i32 ( a0 INT, a1 INT, a2 INT, a3 INT, a4 INT, a5 INT, a6 INT, a7 INT, a8 INT, a9 INT);
COPY Attributes_i32 FROM 'benchmark/operators/data/Attributes_i32.csv' ( HEADER );
.timer on
SELECT a0 FROM Attributes_i32 ORDER BY a0, a1;
.timer off
DROP TABLE Attributes_i32;

CREATE TABLE Attributes_i32 ( a0 INT, a1 INT, a2 INT, a3 INT, a4 INT, a5 INT, a6 INT, a7 INT, a8 INT, a9 INT);
COPY Attributes_i32 FROM 'benchmark/operators/data/Attributes_i32.csv' ( HEADER );
.timer on
SELECT a0 FROM Attributes_i32 ORDER BY a0, a1, a2;
.timer off
DROP TABLE Attributes_i32;


CREATE TABLE Attributes_i32 ( a0 INT, a1 INT, a2 INT, a3 INT, a4 INT, a5 INT, a6 INT, a7 INT, a8 INT, a9 INT);
COPY Attributes_i32 FROM 'benchmark/operators/data/Attributes_i32.csv' ( HEADER );
.timer on
SELECT a0 FROM Attributes_i32 ORDER BY a0, a1, a2, a3;
.timer off
DROP TABLE Attributes_i32;
EOF
