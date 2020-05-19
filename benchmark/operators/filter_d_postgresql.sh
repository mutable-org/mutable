#!/bin/bash

# Define path to PostgreSQL CLI
POSTGRESQL=psql

{ ${POSTGRESQL} -U postgres | grep 'Time' | cut -d ' ' -f 2; } << EOF
DROP DATABASE IF EXISTS benchmark_tmp;
CREATE DATABASE benchmark_tmp;
\c benchmark_tmp
CREATE TABLE Attributes_d ( a0 DOUBLE PRECISION, a1 DOUBLE PRECISION, a2 DOUBLE PRECISION, a3 DOUBLE PRECISION, a4 DOUBLE PRECISION, a5 DOUBLE PRECISION, a6 DOUBLE PRECISION, a7 DOUBLE PRECISION, a8 DOUBLE PRECISION, a9 DOUBLE PRECISION);
\copy Attributes_d FROM 'benchmark/operators/data/Attributes_d.csv' WITH DELIMITER ',' CSV HEADER;
\timing on
SELECT COUNT(*) FROM Attributes_d WHERE a0 < 0.0;
SELECT COUNT(*) FROM Attributes_d WHERE a0 < 0.1;
SELECT COUNT(*) FROM Attributes_d WHERE a0 < 0.2;
SELECT COUNT(*) FROM Attributes_d WHERE a0 < 0.3;
SELECT COUNT(*) FROM Attributes_d WHERE a0 < 0.4;
SELECT COUNT(*) FROM Attributes_d WHERE a0 < 0.5;
SELECT COUNT(*) FROM Attributes_d WHERE a0 < 0.6;
SELECT COUNT(*) FROM Attributes_d WHERE a0 < 0.7;
SELECT COUNT(*) FROM Attributes_d WHERE a0 < 0.8;
SELECT COUNT(*) FROM Attributes_d WHERE a0 < 0.9;
SELECT COUNT(*) FROM Attributes_d WHERE a0 < 1.0;
EOF
