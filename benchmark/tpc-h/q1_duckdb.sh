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
        l_returnflag,
        l_linestatus,
        SUM(l_quantity) AS sum_qty,
        SUM(l_extendedprice) AS sum_base_price,
        SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
        SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
        AVG(l_quantity) AS avg_qty,
        AVG(l_extendedprice) AS avg_price,
        AVG(l_discount) AS avg_disc,
        COUNT(*) AS count_order
FROM
        Lineitem
WHERE
        l_shipdate <= DATE '1998-09-02'
GROUP BY
        l_returnflag,
        l_linestatus
ORDER BY
        l_returnflag,
        l_linestatus;
EOF
