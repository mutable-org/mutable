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
SELECT COUNT(*) FROM Attributes_multi_i32 WHERE a0 < -2104533974 AND a1 < -2104533974;
SELECT COUNT(*) FROM Attributes_multi_i32 WHERE a0 < -1932735282 AND a1 < -1932735282;
SELECT COUNT(*) FROM Attributes_multi_i32 WHERE a0 < -1717986917 AND a1 < -1717986917;
SELECT COUNT(*) FROM Attributes_multi_i32 WHERE a0 < -1288490188 AND a1 < -1288490188;
SELECT COUNT(*) FROM Attributes_multi_i32 WHERE a0 <  -858993458 AND a1 <  -858993458;
SELECT COUNT(*) FROM Attributes_multi_i32 WHERE a0 <  -429496729 AND a1 <  -429496729;
SELECT COUNT(*) FROM Attributes_multi_i32 WHERE a0 <           0 AND a1 <           0;
SELECT COUNT(*) FROM Attributes_multi_i32 WHERE a0 <   429496729 AND a1 <   429496729;
SELECT COUNT(*) FROM Attributes_multi_i32 WHERE a0 <   858993458 AND a1 <   858993458;
SELECT COUNT(*) FROM Attributes_multi_i32 WHERE a0 <  1288490188 AND a1 <  1288490188;
SELECT COUNT(*) FROM Attributes_multi_i32 WHERE a0 <  1717986917 AND a1 <  1717986917;
SELECT COUNT(*) FROM Attributes_multi_i32 WHERE a0 <  1932735282 AND a1 <  1932735282;
SELECT COUNT(*) FROM Attributes_multi_i32 WHERE a0 <  2104533974 AND a1 <  2104533974;
EOF
