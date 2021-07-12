#!/bin/bash

LINEITEM="benchmark/tpc-h/data/lineitem.tbl"
PART="benchmark/tpc-h/data/part.tbl"

# Define path to DuckDB CLI
DUCKDB=duckdb_cli

trap 'exit' INT
{ ${DUCKDB} | grep 'Run Time' | cut -d ' ' -f 4 | awk '{print $1 * 1000;}'; } << EOF
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
COPY Lineitem FROM '${LINEITEM}' ( DELIMITER '|' );
COPY Part FROM '${PART}' ( DELIMITER '|' );
.timer on
SELECT
        SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
FROM
        Lineitem,
        Part
WHERE
        l_partkey = p_partkey
        AND l_shipdate >= date '1995-09-01'
        AND l_shipdate < date '1995-10-01';
EOF
