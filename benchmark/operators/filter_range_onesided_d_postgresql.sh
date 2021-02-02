#!/bin/bash

# Define path to PostgreSQL CLI
POSTGRESQL=psql

{ ${POSTGRESQL} -U postgres | grep 'Time' | cut -d ' ' -f 2; } << EOF
DROP DATABASE IF EXISTS benchmark_tmp;
CREATE DATABASE benchmark_tmp;
\c benchmark_tmp
set jit=off;
CREATE TABLE Attribute_d ( ID INT, val DOUBLE PRECISION );
\copy Attribute_d FROM 'benchmark/operators/data/Attribute_d.csv' WITH DELIMITER ',' CSV HEADER;
\timing on
SELECT COUNT(*) FROM Attribute_d WHERE val < 0.01;
SELECT COUNT(*) FROM Attribute_d WHERE val < 0.05;
SELECT COUNT(*) FROM Attribute_d WHERE val < 0.10;
SELECT COUNT(*) FROM Attribute_d WHERE val < 0.20;
SELECT COUNT(*) FROM Attribute_d WHERE val < 0.30;
SELECT COUNT(*) FROM Attribute_d WHERE val < 0.40;
SELECT COUNT(*) FROM Attribute_d WHERE val < 0.50;
SELECT COUNT(*) FROM Attribute_d WHERE val < 0.60;
SELECT COUNT(*) FROM Attribute_d WHERE val < 0.70;
SELECT COUNT(*) FROM Attribute_d WHERE val < 0.80;
SELECT COUNT(*) FROM Attribute_d WHERE val < 0.90;
SELECT COUNT(*) FROM Attribute_d WHERE val < 0.95;
SELECT COUNT(*) FROM Attribute_d WHERE val < 0.99;
EOF
