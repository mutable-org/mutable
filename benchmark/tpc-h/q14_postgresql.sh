#!/bin/bash

LINEITEM="benchmark/tpc-h/data/lineitem.tbl"
PART="benchmark/tpc-h/data/part.tbl"

# Define path to PostgreSQL CLI
POSTGRESQL=psql

trap 'exit' INT
{ ${POSTGRESQL} -U postgres | grep 'Time' | cut -d ' ' -f 2; } << EOF
DROP DATABASE IF EXISTS benchmark_tmp;
CREATE DATABASE benchmark_tmp;
\c benchmark_tmp
set jit=off;
CREATE TABLE Lineitem (
    l_orderkey      INT,
    l_partkey       INT,
    l_suppkey       INT,
    l_linenumber    INT,
    l_quantity      DECIMAL(10,2),
    l_extendedprice DECIMAL(10,2),
    l_discount      DECIMAL(10,2),
    l_tax           DECIMAL(10,2),
    l_returnflag    CHAR(1),
    l_linestatus    CHAR(1),
    l_shipdate      DATE,
    l_commitdate    DATE,
    l_receiptdate   DATE,
    l_shipinstruct  CHAR(25),
    l_shipmode      CHAR(10),
    l_comment       CHAR(44)
);
CREATE TABLE Part (
    p_partkey       INT,
    p_name          CHAR(55),
    p_mfgr          CHAR(25),
    p_brand         CHAR(10),
    p_type          CHAR(25),
    p_size          INT,
    p_container     CHAR(10),
    p_retailprice   DECIMAL(10,2),
    p_comment       CHAR(23)
);
\copy Lineitem FROM '${LINEITEM}' WITH DELIMITER '|' CSV;
\copy Part FROM '${PART}' WITH DELIMITER '|' CSV;
\timing on
SELECT
        SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
FROM
        Lineitem,
        Part
WHERE
        l_partkey = p_partkey
        AND l_shipdate >= date '1995-09-01'
        AND l_shipdate < date '1995-10-01';
\timing off
EOF
