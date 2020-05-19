#!/bin/bash

# Define path to PostgreSQL CLI
POSTGRESQL=psql

{ ${POSTGRESQL} -U postgres | grep 'Time' | cut -d ' ' -f 2; } << EOF
DROP DATABASE IF EXISTS benchmark_tmp;
CREATE DATABASE benchmark_tmp;
\c benchmark_tmp

CREATE TABLE Attributes_i32 ( a0 INT, a1 INT, a2 INT, a3 INT, a4 INT, a5 INT, a6 INT, a7 INT, a8 INT, a9 INT);
\copy Attributes_i32 FROM 'benchmark/operators/data/Attributes_i32.csv' WITH DELIMITER ',' CSV HEADER;
\timing on
SELECT a0 FROM Attributes_i32 ORDER BY a0;
\timing off
DROP TABLE Attributes_i32;

CREATE TABLE Attributes_i32 ( a0 INT, a1 INT, a2 INT, a3 INT, a4 INT, a5 INT, a6 INT, a7 INT, a8 INT, a9 INT);
\copy Attributes_i32 FROM 'benchmark/operators/data/Attributes_i32.csv' WITH DELIMITER ',' CSV HEADER;
\timing on
SELECT a0 FROM Attributes_i32 ORDER BY a0, a1;
\timing off
DROP TABLE Attributes_i32;

CREATE TABLE Attributes_i32 ( a0 INT, a1 INT, a2 INT, a3 INT, a4 INT, a5 INT, a6 INT, a7 INT, a8 INT, a9 INT);
\copy Attributes_i32 FROM 'benchmark/operators/data/Attributes_i32.csv' WITH DELIMITER ',' CSV HEADER;
\timing on
SELECT a0 FROM Attributes_i32 ORDER BY a0, a1, a2;
\timing off
DROP TABLE Attributes_i32;


CREATE TABLE Attributes_i32 ( a0 INT, a1 INT, a2 INT, a3 INT, a4 INT, a5 INT, a6 INT, a7 INT, a8 INT, a9 INT);
\copy Attributes_i32 FROM 'benchmark/operators/data/Attributes_i32.csv' WITH DELIMITER ',' CSV HEADER;
\timing on
SELECT a0 FROM Attributes_i32 ORDER BY a0, a1, a2, a3;
\timing off
DROP TABLE Attributes_i32;
EOF
