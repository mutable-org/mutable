#!/bin/bash

# Define path to PostgreSQL CLI
POSTGRESQL=psql

{ ${POSTGRESQL} -U postgres | grep 'Time' | cut -d ' ' -f 2; } << EOF
DROP DATABASE IF EXISTS benchmark_tmp;
CREATE DATABASE benchmark_tmp;
\c benchmark_tmp
CREATE TABLE Attribute_f ( id INT, val REAL );
\copy Attribute_f FROM 'benchmark/operators/data/Attribute_f.csv' WITH DELIMITER ',' CSV HEADER;
\timing on
SELECT COUNT(*) FROM Attribute_f WHERE val < 0.0;
SELECT COUNT(*) FROM Attribute_f WHERE val < 0.1;
SELECT COUNT(*) FROM Attribute_f WHERE val < 0.2;
SELECT COUNT(*) FROM Attribute_f WHERE val < 0.3;
SELECT COUNT(*) FROM Attribute_f WHERE val < 0.4;
SELECT COUNT(*) FROM Attribute_f WHERE val < 0.5;
SELECT COUNT(*) FROM Attribute_f WHERE val < 0.6;
SELECT COUNT(*) FROM Attribute_f WHERE val < 0.7;
SELECT COUNT(*) FROM Attribute_f WHERE val < 0.8;
SELECT COUNT(*) FROM Attribute_f WHERE val < 0.9;
SELECT COUNT(*) FROM Attribute_f WHERE val < 1.0;
EOF
