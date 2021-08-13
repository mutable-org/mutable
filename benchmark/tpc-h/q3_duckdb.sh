#!/bin/bash

TBL_LINEITEM="benchmark/tpc-h/data/lineitem.tbl"
TBL_ORDERS="benchmark/tpc-h/data/orders.tbl"
TBL_CUSTOMER="benchmark/tpc-h/data/customer.tbl"

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
CREATE TABLE Customer (
    c_custkey       INT,
    c_name          CHAR(25),
    c_address       CHAR(40),
    c_nationkey     INT,
    c_phone         CHAR(15),
    c_acctbal       DECIMAL(10,2),
    c_mktsegment    CHAR(10),
    c_comment       CHAR(117)
);
COPY Lineitem FROM '${TBL_LINEITEM}' ( DELIMITER '|' );
COPY Orders FROM '${TBL_ORDERS}' ( DELIMITER '|' );
COPY Customer FROM '${TBL_CUSTOMER}' ( DELIMITER '|' );
.timer on
SELECT
        l_orderkey,
        SUM(l_extendedprice * (1 - l_discount)) AS revenue,
        o_orderdate,
        o_shippriority
FROM
        Customer,
        Orders,
        Lineitem
WHERE
        c_mktsegment = 'BUILDING'
        AND c_custkey = o_custkey
        AND l_orderkey = o_orderkey
        AND o_orderdate < date '1995-03-15'
        AND l_shipdate > date '1995-03-15'
GROUP BY
        l_orderkey,
        o_orderdate,
        o_shippriority
ORDER BY
        revenue DESC,
        o_orderdate
LIMIT 10;
EOF
