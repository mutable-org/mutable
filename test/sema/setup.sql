CREATE DATABASE mydb;
USE mydb;

CREATE TABLE LINEITEM (
    orderkey        INT(4),
    partkey         INT(4),
    suppkey         INT(4),
    linenumber      INT(4),
    quantity        INT(8),
    extendedprice   INT(8),
    discount        INT(8),
    tax             INT(8),
    returnflag      CHAR(1),
    linestatus      CHAR(1),
    shipdate        INT(4),
    commitdate      INT(4),
    receiptdate     INT(4),
    shipinstruct    CHAR(25),
    shipmode        CHAR(10),
    comment         VARCHAR(45)
);

CREATE TABLE ORDERS (
    orderkey        INT(4),
    custkey         INT(4),
    orderstatus     CHAR(1),
    totalprice      INT(8),
    orderdate       INT(4),
    orderpriority   CHAR(15),
    clerk           CHAR(15),
    shippriority    INT(4),
    comment         VARCHAR(80)
);
