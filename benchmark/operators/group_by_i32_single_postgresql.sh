#!/bin/bash

# Define path to PostgreSQL CLI
POSTGRESQL=psql
CSV="benchmark/operators/data/Distinct_i32.csv"
NUM=$(($(wc -l ${CSV} | cut -f 1 -d ' ') - 1))

{ ${POSTGRESQL} -U postgres | grep 'Time' | cut -d ' ' -f 2; } << EOF
DROP DATABASE IF EXISTS benchmark_tmp;
CREATE DATABASE benchmark_tmp;
\c benchmark_tmp
CREATE TABLE Distinct_i32 ( id INT, n1 INT, n10 INT, n100 INT, n1000 INT, n10000 INT, n100000 INT);

\copy Distinct_i32 FROM PROGRAM 'head -n 0 ${CSV}' WITH DELIMITER ',' CSV HEADER;
\timing on
SELECT COUNT(*) FROM (SELECT 1 FROM Distinct_i32 GROUP BY n1000) AS T;
\timing off
DELETE FROM Distinct_i32;

\copy Distinct_i32 FROM PROGRAM 'head -n $((${NUM} * 1 / 10)) ${CSV}' WITH DELIMITER ',' CSV HEADER;
\timing on
SELECT COUNT(*) FROM (SELECT 1 FROM Distinct_i32 GROUP BY n1000) AS T;
\timing off
DELETE FROM Distinct_i32;

\copy Distinct_i32 FROM PROGRAM 'head -n $((${NUM} * 2 / 10)) ${CSV}' WITH DELIMITER ',' CSV HEADER;
\timing on
SELECT COUNT(*) FROM (SELECT 1 FROM Distinct_i32 GROUP BY n1000) AS T;
\timing off
DELETE FROM Distinct_i32;

\copy Distinct_i32 FROM PROGRAM 'head -n $((${NUM} * 3 / 10)) ${CSV}' WITH DELIMITER ',' CSV HEADER;
\timing on
SELECT COUNT(*) FROM (SELECT 1 FROM Distinct_i32 GROUP BY n1000) AS T;
\timing off
DELETE FROM Distinct_i32;

\copy Distinct_i32 FROM PROGRAM 'head -n $((${NUM} * 4 / 10)) ${CSV}' WITH DELIMITER ',' CSV HEADER;
\timing on
SELECT COUNT(*) FROM (SELECT 1 FROM Distinct_i32 GROUP BY n1000) AS T;
\timing off
DELETE FROM Distinct_i32;

\copy Distinct_i32 FROM PROGRAM 'head -n $((${NUM} * 5 / 10)) ${CSV}' WITH DELIMITER ',' CSV HEADER;
\timing on
SELECT COUNT(*) FROM (SELECT 1 FROM Distinct_i32 GROUP BY n1000) AS T;
\timing off
DELETE FROM Distinct_i32;

\copy Distinct_i32 FROM PROGRAM 'head -n $((${NUM} * 6 / 10)) ${CSV}' WITH DELIMITER ',' CSV HEADER;
\timing on
SELECT COUNT(*) FROM (SELECT 1 FROM Distinct_i32 GROUP BY n1000) AS T;
\timing off
DELETE FROM Distinct_i32;

\copy Distinct_i32 FROM PROGRAM 'head -n $((${NUM} * 7 / 10)) ${CSV}' WITH DELIMITER ',' CSV HEADER;
\timing on
SELECT COUNT(*) FROM (SELECT 1 FROM Distinct_i32 GROUP BY n1000) AS T;
\timing off
DELETE FROM Distinct_i32;

\copy Distinct_i32 FROM PROGRAM 'head -n $((${NUM} * 8 / 10)) ${CSV}' WITH DELIMITER ',' CSV HEADER;
\timing on
SELECT COUNT(*) FROM (SELECT 1 FROM Distinct_i32 GROUP BY n1000) AS T;
\timing off
DELETE FROM Distinct_i32;

\copy Distinct_i32 FROM PROGRAM 'head -n $((${NUM} * 9 / 10)) ${CSV}' WITH DELIMITER ',' CSV HEADER;
\timing on
SELECT COUNT(*) FROM (SELECT 1 FROM Distinct_i32 GROUP BY n1000) AS T;
\timing off
DELETE FROM Distinct_i32;

\copy Distinct_i32 FROM PROGRAM 'head -n ${NUM} ${CSV}' WITH DELIMITER ',' CSV HEADER;
\timing on
SELECT COUNT(*) FROM (SELECT 1 FROM Distinct_i32 GROUP BY n1000) AS T;
\timing off
DELETE FROM Distinct_i32;
EOF
