#!/bin/bash

# Define path to PostgreSQL CLI
POSTGRESQL=psql
AGG=MIN

{ ${POSTGRESQL} -U postgres | grep 'Time' | cut -d ' ' -f 2; } << EOF
DROP DATABASE IF EXISTS benchmark_tmp;
CREATE DATABASE benchmark_tmp;
\c benchmark_tmp
set jit=off;
CREATE TABLE Distinct_i32 ( id INT, n1 INT, n10 INT, n100 INT, n1000 INT, n10000 INT, n100000 INT);
\copy Distinct_i32 FROM 'benchmark/operators/data/Distinct_i32.csv' WITH DELIMITER ',' CSV HEADER;
\timing on
SELECT ${AGG}(n100) FROM Distinct_i32 GROUP BY n10;
SELECT ${AGG}(n100), ${AGG}(n1000) FROM Distinct_i32 GROUP BY n10;
SELECT ${AGG}(n100), ${AGG}(n1000), ${AGG}(n10000) FROM Distinct_i32 GROUP BY n10;
SELECT ${AGG}(n100), ${AGG}(n1000), ${AGG}(n10000), ${AGG}(n100000) FROM Distinct_i32 GROUP BY n10;
EOF
