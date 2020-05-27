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
SELECT COUNT(*) FROM Attribute_i32 WHERE val < -2147483647;
SELECT COUNT(*) FROM Attribute_i32 WHERE val < -1717986918;
SELECT COUNT(*) FROM Attribute_i32 WHERE val < -1288490188;
SELECT COUNT(*) FROM Attribute_i32 WHERE val <  -858993459;
SELECT COUNT(*) FROM Attribute_i32 WHERE val <  -429496729;
SELECT COUNT(*) FROM Attribute_i32 WHERE val <           0;
SELECT COUNT(*) FROM Attribute_i32 WHERE val <   429496729;
SELECT COUNT(*) FROM Attribute_i32 WHERE val <   858993459;
SELECT COUNT(*) FROM Attribute_i32 WHERE val <  1288490188;
SELECT COUNT(*) FROM Attribute_i32 WHERE val <  1717986918;
SELECT COUNT(*) FROM Attribute_i32 WHERE val <  2147483647;
EOF
