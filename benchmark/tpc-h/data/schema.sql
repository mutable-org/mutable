CREATE DATABASE tpc_h;
USE tpc_h;

CREATE TABLE Lineitem (
    l_orderkey      INT(4) NOT NULL,
    l_partkey       INT(4) NOT NULL,
    l_suppkey       INT(4) NOT NULL,
    l_linenumber    INT(4) NOT NULL,
    l_quantity      DECIMAL(10,2) NOT NULL,
    l_extendedprice DECIMAL(10,2) NOT NULL,
    l_discount      DECIMAL(10,2) NOT NULL,
    l_tax           DECIMAL(10,2) NOT NULL,
    l_returnflag    CHAR(1) NOT NULL,
    l_linestatus    CHAR(1) NOT NULL,
    l_shipdate      DATE NOT NULL,
    l_commitdate    DATE NOT NULL,
    l_receiptdate   DATE NOT NULL,
    l_shipinstruct  CHAR(25) NOT NULL,
    l_shipmode      CHAR(10) NOT NULL,
    l_comment       CHAR(44) NOT NULL
);

CREATE TABLE Part (
    p_partkey       INT(4) NOT NULL,
    p_name          CHAR(55) NOT NULL,
    p_mfgr          CHAR(25) NOT NULL,
    p_brand         CHAR(10) NOT NULL,
    p_type          CHAR(25) NOT NULL,
    p_size          INT(4) NOT NULL,
    p_container     CHAR(10) NOT NULL,
    p_retailprice   DECIMAL(10,2) NOT NULL,
    p_comment       CHAR(23) NOT NULL
);

CREATE TABLE Supplier (
    s_suppkey       INT(4) NOT NULL,
    s_name          CHAR(25) NOT NULL,
    s_address       VARCHAR(40) NOT NULL,
    s_nationkey     INT(4) NOT NULL,
    s_phone         CHAR(15) NOT NULL,
    s_acctbal       DECIMAL(10,2) NOT NULL,
    s_comment       VARCHAR(101) NOT NULL
);

CREATE TABLE Partsupp (
    ps_partkey      INT(4) NOT NULL,
    ps_suppkey      INT(4) NOT NULL,
    ps_availqty     INT(4) NOT NULL,
    ps_supplycost   DECIMAL(10,2) NOT NULL,
    ps_comment      VARCHAR(199) NOT NULL
);

CREATE TABLE Customer (
    c_custkey       INT(4) NOT NULL,
    c_name          CHAR(25) NOT NULL,
    c_address       CHAR(40) NOT NULL,
    c_nationkey     INT(4) NOT NULL,
    c_phone         CHAR(15) NOT NULL,
    c_acctbal       DECIMAL(10,2) NOT NULL,
    c_mktsegment    CHAR(10) NOT NULL,
    c_comment       CHAR(117) NOT NULL
);

CREATE TABLE Orders (
    o_orderkey      INT(4) NOT NULL,
    o_custkey       INT(4) NOT NULL,
    o_orderstatus   CHAR(1) NOT NULL,
    o_totalprice    DECIMAL(10,2) NOT NULL,
    o_orderdate     DATE NOT NULL,
    o_orderpriority CHAR(15) NOT NULL,
    o_clerk         CHAR(15) NOT NULL,
    o_shippriority  INT(4) NOT NULL,
    o_comment       CHAR(80) NOT NULL
);

CREATE TABLE Nation (
    n_nationkey     INT(4) NOT NULL,
    n_name          CHAR(25) NOT NULL,
    n_regionkey     INT(4) NOT NULL,
    n_comment       VARCHAR(152) NOT NULL
);

CREATE TABLE Region (
    r_regionkey     INT(4) NOT NULL,
    r_name          CHAR(25) NOT NULL,
    r_comment       VARCHAR(152) NOT NULL
);
