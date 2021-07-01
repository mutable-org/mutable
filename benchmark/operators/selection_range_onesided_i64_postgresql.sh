#!/bin/bash

# Define path to PostgreSQL CLI
POSTGRESQL=psql

{ ${POSTGRESQL} -U postgres | grep 'Time' | cut -d ' ' -f 2; } << EOF
DROP DATABASE IF EXISTS benchmark_tmp;
CREATE DATABASE benchmark_tmp;
\c benchmark_tmp
set jit=off;
CREATE TABLE Attribute_i64 ( id INT, val BIGINT );
\copy Attribute_i64 FROM 'benchmark/operators/data/Attribute_i64.csv' WITH DELIMITER ',' CSV HEADER;
\timing on
SELECT COUNT(*) FROM Attribute_i64 WHERE val < -9223372036854775807;
SELECT COUNT(*) FROM Attribute_i64 WHERE val < -7378697629483821056;
SELECT COUNT(*) FROM Attribute_i64 WHERE val < -5534023222112865280;
SELECT COUNT(*) FROM Attribute_i64 WHERE val < -3689348814741910528;
SELECT COUNT(*) FROM Attribute_i64 WHERE val < -1844674407370954752;
SELECT COUNT(*) FROM Attribute_i64 WHERE val <                    0;
SELECT COUNT(*) FROM Attribute_i64 WHERE val <  1844674407370954752;
SELECT COUNT(*) FROM Attribute_i64 WHERE val <  3689348814741909504;
SELECT COUNT(*) FROM Attribute_i64 WHERE val <  5534023222112866304;
SELECT COUNT(*) FROM Attribute_i64 WHERE val <  7378697629483821056;
SELECT COUNT(*) FROM Attribute_i64 WHERE val <  9223372036854775807;
EOF
