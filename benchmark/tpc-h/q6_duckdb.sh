#!/bin/bash

LINEITEM="benchmark/tpc-h/data/lineitem.tbl"

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
COPY Lineitem FROM '${LINEITEM}' ( DELIMITER '|' );
.timer on
SELECT
        SUM(l_extendedprice * l_discount) AS revenue
FROM
        Lineitem
WHERE
        l_shipdate >= DATE '1994-01-01'
        AND l_shipdate < DATE '1995-01-01'
        AND l_discount > 0.05 AND l_discount < 0.07
        AND l_quantity < 24;
EOF
