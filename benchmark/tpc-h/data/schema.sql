CREATE DATABASE tpc_h;
USE tpc_h;

CREATE TABLE Lineitem (
    l_orderkey      INT(4),
    l_partkey       INT(4),
    l_suppkey       INT(4),
    l_linenumber    INT(4),
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

--  CREATE TABLE PART (
--      p_partkey       INT(4),
--      p_name          VARCHAR(55),
--      p_mfgr          CHAR(25),
--      p_brand         CHAR(10),
--      p_type          VARCHAR(25),
--      p_size          INT(4),
--      p_container     CHAR(10),
--      p_retailprice   DECIMAL(10,2),
--      p_comment       VARCHAR(23)
--  );

--  CREATE TABLE SUPPLIER (
--      s_suppkey       INT(4),
--      s_name          CHAR(25),
--      s_address       VARCHAR(40),
--      s_nationkey     INT(4),
--      s_phone         CHAR(15),
--      s_acctbal       DECIMAL(10,2),
--      s_comment       VARCHAR(101)
--  );

--  CREATE TABLE PARTSUPP (
--      ps_partkey      INT(4),
--      ps_suppkey      INT(4),
--      ps_availqty     INT(4),
--      ps_supplycost   DECIMAL(10,2),
--      ps_comment      VARCHAR(199)
--  );

--  CREATE TABLE CUSTOMER (
--      c_custkey       INT(4),
--      c_name          VARCHAR(25),
--      c_address       VARCHAR(40),
--      c_nationkey     INT(4),
--      c_phone         CHAR(15),
--      c_acctbal       DECIMAL(10,2),
--      c_mktsegment    CHAR(10),
--      c_comment       VARCHAR(117)
--  );

--  CREATE TABLE ORDERS (
--      o_orderkey      INT(4),
--      o_custkey       INT(4),
--      o_orderstatus   CHAR(1),
--      o_totalprice    INT(8),
--      o_orderdate     INT(4),
--      o_orderpriority CHAR(15),
--      o_clerk         CHAR(15),
--      o_shippriority  INT(4),
--      o_comment       VARCHAR(80)
--  );

--  CREATE TABLE NATION (
--      n_nationkey     INT(4),
--      n_name          CHAR(25),
--      n_regionkey     INT(4),
--      n_comment       VARCHAR(152)
--  );

--  CREATE TABLE REGION (
--      r_regionkey     INT(4),
--      r_name          CHAR(25),
--      r_comment       VARCHAR(152)
--  );
