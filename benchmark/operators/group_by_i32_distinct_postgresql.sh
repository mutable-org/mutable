#!/bin/bash

# Define path to PostgreSQL CLI
POSTGRESQL=psql

{ ${POSTGRESQL} -U postgres | grep 'Time' | cut -d ' ' -f 2; } << EOF
set jit=off;
DROP DATABASE IF EXISTS benchmark_tmp;
CREATE DATABASE benchmark_tmp;
\c benchmark_tmp
CREATE TABLE Distinct_i32 ( id INT, n1 INT, n10 INT, n100 INT, n1000 INT, n10000 INT, n100000 INT);
\copy Distinct_i32 FROM 'benchmark/operators/data/Distinct_i32.csv' WITH DELIMITER ',' CSV HEADER;
\timing on
SELECT COUNT(*) FROM (SELECT 1 FROM Distinct_i32 GROUP BY     n10) AS T;
SELECT COUNT(*) FROM (SELECT 1 FROM Distinct_i32 GROUP BY    n100) AS T;
SELECT COUNT(*) FROM (SELECT 1 FROM Distinct_i32 GROUP BY   n1000) AS T;
SELECT COUNT(*) FROM (SELECT 1 FROM Distinct_i32 GROUP BY  n10000) AS T;
SELECT COUNT(*) FROM (SELECT 1 FROM Distinct_i32 GROUP BY n100000) AS T;
EOF
