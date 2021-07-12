#!/bin/bash

TBL_LINEITEM="benchmark/tpc-h/data/lineitem.tbl"
TBL_ORDERS="benchmark/tpc-h/data/orders.tbl"

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
CREATE TABLE Orders (
    o_orderkey      INT,
    o_custkey       INT,
    o_orderstatus   CHAR(1),
    o_totalprice    DECIMAL(10,2),
    o_orderdate     DATE,
    o_orderpriority CHAR(15),
    o_clerk         CHAR(15),
    o_shippriority  INT,
    o_comment       CHAR(80)
);
COPY Lineitem FROM '${TBL_LINEITEM}' ( DELIMITER '|' );
COPY Orders FROM '${TBL_ORDERS}' ( DELIMITER '|' );
.timer on
SELECT
    l_shipmode, COUNT(*)
FROM
    Orders,
    Lineitem
WHERE
    o_orderkey = l_orderkey
    AND (l_shipmode = 'MAIL' OR l_shipmode = 'SHIP')
    AND l_commitdate < l_receiptdate
    AND l_shipdate < l_commitdate
    AND l_receiptdate >= date '1994-01-01'
    AND l_receiptdate < date '1995-01-01'
    AND (o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH')
GROUP BY
    l_shipmode
ORDER BY
    l_shipmode;
EOF
