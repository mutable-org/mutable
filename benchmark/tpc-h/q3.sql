IMPORT INTO Lineitem DSV "benchmark/tpc-h/data/lineitem.tbl" DELIMITER "|";
IMPORT INTO Orders DSV "benchmark/tpc-h/data/orders.tbl" DELIMITER "|";
IMPORT INTO Customer DSV "benchmark/tpc-h/data/customer.tbl" DELIMITER "|";

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
        c_mktsegment = "BUILDING"
        AND c_custkey = o_custkey
        AND l_orderkey = o_orderkey
        AND o_orderdate < d'1995-03-15'
        AND l_shipdate > d'1995-03-15'
GROUP BY
        l_orderkey,
        o_orderdate,
        o_shippriority
ORDER BY
        revenue DESC,
        o_orderdate
LIMIT 10;

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
        c_mktsegment = "BUILDING"
        AND c_custkey = o_custkey
        AND l_orderkey = o_orderkey
        AND o_orderdate < d'1995-03-15'
        AND l_shipdate > d'1995-03-15'
GROUP BY
        l_orderkey,
        o_orderdate,
        o_shippriority
ORDER BY
        revenue DESC,
        o_orderdate
LIMIT 10;

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
        c_mktsegment = "BUILDING"
        AND c_custkey = o_custkey
        AND l_orderkey = o_orderkey
        AND o_orderdate < d'1995-03-15'
        AND l_shipdate > d'1995-03-15'
GROUP BY
        l_orderkey,
        o_orderdate,
        o_shippriority
ORDER BY
        revenue DESC,
        o_orderdate
LIMIT 10;

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
        c_mktsegment = "BUILDING"
        AND c_custkey = o_custkey
        AND l_orderkey = o_orderkey
        AND o_orderdate < d'1995-03-15'
        AND l_shipdate > d'1995-03-15'
GROUP BY
        l_orderkey,
        o_orderdate,
        o_shippriority
ORDER BY
        revenue DESC,
        o_orderdate
LIMIT 10;

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
        c_mktsegment = "BUILDING"
        AND c_custkey = o_custkey
        AND l_orderkey = o_orderkey
        AND o_orderdate < d'1995-03-15'
        AND l_shipdate > d'1995-03-15'
GROUP BY
        l_orderkey,
        o_orderdate,
        o_shippriority
ORDER BY
        revenue DESC,
        o_orderdate
LIMIT 10;
