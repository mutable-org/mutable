#!/bin/bash

# Define path to PostgreSQL CLI
POSTGRESQL=psql

{ ${POSTGRESQL} -U postgres | grep 'Time' | cut -d ' ' -f 2; } << EOF
DROP DATABASE IF EXISTS benchmark_tmp;
CREATE DATABASE benchmark_tmp;
\c benchmark_tmp
CREATE TABLE Attributes_multi_i32 ( id INT, a0 INT, a1 INT, a2 INT, a3 INT );
\copy Attributes_multi_i32 FROM 'benchmark/operators/data/Attributes_multi_i32.csv' WITH DELIMITER ',' CSV HEADER;
\timing on
SELECT COUNT(*) FROM Attributes_multi_i32 WHERE a0 < -2147483647 AND a1 < -2147483647;
SELECT COUNT(*) FROM Attributes_multi_i32 WHERE a0 < -1932735283 AND a1 < -1932735283;
SELECT COUNT(*) FROM Attributes_multi_i32 WHERE a0 < -1717986918 AND a1 < -1717986918;
SELECT COUNT(*) FROM Attributes_multi_i32 WHERE a0 < -1288490188 AND a1 < -1288490188;
SELECT COUNT(*) FROM Attributes_multi_i32 WHERE a0 <  -858993459 AND a1 <  -858993459;
SELECT COUNT(*) FROM Attributes_multi_i32 WHERE a0 <  -429496729 AND a1 <  -429496729;
SELECT COUNT(*) FROM Attributes_multi_i32 WHERE a0 <           0 AND a1 <           0;
SELECT COUNT(*) FROM Attributes_multi_i32 WHERE a0 <   429496729 AND a1 <   429496729;
SELECT COUNT(*) FROM Attributes_multi_i32 WHERE a0 <   858993459 AND a1 <   858993459;
SELECT COUNT(*) FROM Attributes_multi_i32 WHERE a0 <  1288490188 AND a1 <  1288490188;
SELECT COUNT(*) FROM Attributes_multi_i32 WHERE a0 <  1717986918 AND a1 <  1717986918;
SELECT COUNT(*) FROM Attributes_multi_i32 WHERE a0 <  1932735283 AND a1 <  1932735283;
SELECT COUNT(*) FROM Attributes_multi_i32 WHERE a0 <  2104533975 AND a1 <  2104533975;
EOF
