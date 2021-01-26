#!/bin/bash

# Define path to PostgreSQL CLI
POSTGRESQL=psql

{ ${POSTGRESQL} -U postgres | grep 'Time' | cut -d ' ' -f 2; } << EOF
DROP DATABASE IF EXISTS benchmark_tmp;
CREATE DATABASE benchmark_tmp;
\c benchmark_tmp
CREATE TABLE Attribute_i32 ( id INT, val INT );
\copy Attribute_i32 FROM 'benchmark/operators/data/Attribute_i32.csv' WITH DELIMITER ',' CSV HEADER;
\timing on
SELECT COUNT(*) FROM Attribute_i32 WHERE val < -2104533974;
SELECT COUNT(*) FROM Attribute_i32 WHERE val < -1932735282;
SELECT COUNT(*) FROM Attribute_i32 WHERE val < -1717986917;
SELECT COUNT(*) FROM Attribute_i32 WHERE val < -1288490188;
SELECT COUNT(*) FROM Attribute_i32 WHERE val <  -858993458;
SELECT COUNT(*) FROM Attribute_i32 WHERE val <  -429496729;
SELECT COUNT(*) FROM Attribute_i32 WHERE val <           0;
SELECT COUNT(*) FROM Attribute_i32 WHERE val <   429496729;
SELECT COUNT(*) FROM Attribute_i32 WHERE val <   858993458;
SELECT COUNT(*) FROM Attribute_i32 WHERE val <  1288490188;
SELECT COUNT(*) FROM Attribute_i32 WHERE val <  1717986917;
SELECT COUNT(*) FROM Attribute_i32 WHERE val <  1932735282;
SELECT COUNT(*) FROM Attribute_i32 WHERE val <  2104533974;
EOF
