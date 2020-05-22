#!/bin/bash

# Define path to PostgreSQL CLI
POSTGRESQL=psql

{ ${POSTGRESQL} -U postgres | grep 'Time' | cut -d ' ' -f 2; } << EOF
DROP DATABASE IF EXISTS benchmark_tmp;
CREATE DATABASE benchmark_tmp;
\c benchmark_tmp
CREATE TABLE Relation ( id INT PRIMARY KEY, fid INT );
\copy Relation FROM 'benchmark/operators/data/Relation.csv' WITH DELIMITER ',' CSV HEADER;
\timing on
SELECT COUNT(*) FROM Relation R, Relation S WHERE R.id = S.fid AND R.id <      0 AND S.id <      0;
SELECT COUNT(*) FROM Relation R, Relation S WHERE R.id = S.fid AND R.id <  10000 AND S.id <  10000;
SELECT COUNT(*) FROM Relation R, Relation S WHERE R.id = S.fid AND R.id <  20000 AND S.id <  20000;
SELECT COUNT(*) FROM Relation R, Relation S WHERE R.id = S.fid AND R.id <  30000 AND S.id <  30000;
SELECT COUNT(*) FROM Relation R, Relation S WHERE R.id = S.fid AND R.id <  40000 AND S.id <  40000;
SELECT COUNT(*) FROM Relation R, Relation S WHERE R.id = S.fid AND R.id <  50000 AND S.id <  50000;
SELECT COUNT(*) FROM Relation R, Relation S WHERE R.id = S.fid AND R.id <  60000 AND S.id <  60000;
SELECT COUNT(*) FROM Relation R, Relation S WHERE R.id = S.fid AND R.id <  70000 AND S.id <  70000;
SELECT COUNT(*) FROM Relation R, Relation S WHERE R.id = S.fid AND R.id <  80000 AND S.id <  80000;
SELECT COUNT(*) FROM Relation R, Relation S WHERE R.id = S.fid AND R.id <  90000 AND S.id <  90000;
SELECT COUNT(*) FROM Relation R, Relation S WHERE R.id = S.fid AND R.id < 100000 AND S.id < 100000;
EOF
