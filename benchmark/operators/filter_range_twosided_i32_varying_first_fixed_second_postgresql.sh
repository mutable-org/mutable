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
SELECT COUNT(*) FROM Attribute_i32 WHERE -2104533974 < val AND val < -2061584301;
SELECT COUNT(*) FROM Attribute_i32 WHERE -1932735282 < val AND val < -1889785609;
SELECT COUNT(*) FROM Attribute_i32 WHERE -1717986917 < val AND val < -1675037244;
SELECT COUNT(*) FROM Attribute_i32 WHERE -1288490188 < val AND val < -1245540515;
SELECT COUNT(*) FROM Attribute_i32 WHERE  -858993458 < val AND val <  -816043785;
SELECT COUNT(*) FROM Attribute_i32 WHERE  -429496729 < val AND val <  -386547056;
SELECT COUNT(*) FROM Attribute_i32 WHERE           0 < val AND val <    42949672;
SELECT COUNT(*) FROM Attribute_i32 WHERE   429496729 < val AND val <   472446402;
SELECT COUNT(*) FROM Attribute_i32 WHERE   858993458 < val AND val <   901943131;
SELECT COUNT(*) FROM Attribute_i32 WHERE  1288490188 < val AND val <  1331439861;
SELECT COUNT(*) FROM Attribute_i32 WHERE  1717986917 < val AND val <  1760936590;
SELECT COUNT(*) FROM Attribute_i32 WHERE  1932735282 < val AND val <  1975684955;
SELECT COUNT(*) FROM Attribute_i32 WHERE  2104533974 < val AND val <  2147483647;
EOF
