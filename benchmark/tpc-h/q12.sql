IMPORT INTO Lineitem DSV "benchmark/tpc-h/data/lineitem.tbl" DELIMITER "|";
IMPORT INTO Orders DSV "benchmark/tpc-h/data/orders.tbl" DELIMITER "|";

SELECT
    l_shipmode, COUNT(*)
FROM
    Lineitem,
    Orders
WHERE
    o_orderkey = l_orderkey
    AND (l_shipmode = "MAIL" OR l_shipmode = "SHIP")
    AND l_commitdate < l_receiptdate
    AND l_shipdate < l_commitdate
    AND l_receiptdate >= d'1994-01-01'
    AND l_receiptdate < d'1995-01-01'
    AND (o_orderpriority = "1-URGENT" OR o_orderpriority = "2-HIGH")
GROUP BY
    l_shipmode
ORDER BY
    l_shipmode;
SELECT
    l_shipmode, COUNT(*)
FROM
    Lineitem,
    Orders
WHERE
    o_orderkey = l_orderkey
    AND (l_shipmode = "MAIL" OR l_shipmode = "SHIP")
    AND l_commitdate < l_receiptdate
    AND l_shipdate < l_commitdate
    AND l_receiptdate >= d'1994-01-01'
    AND l_receiptdate < d'1995-01-01'
    AND (o_orderpriority = "1-URGENT" OR o_orderpriority = "2-HIGH")
GROUP BY
    l_shipmode
ORDER BY
    l_shipmode;
SELECT
    l_shipmode, COUNT(*)
FROM
    Lineitem,
    Orders
WHERE
    o_orderkey = l_orderkey
    AND (l_shipmode = "MAIL" OR l_shipmode = "SHIP")
    AND l_commitdate < l_receiptdate
    AND l_shipdate < l_commitdate
    AND l_receiptdate >= d'1994-01-01'
    AND l_receiptdate < d'1995-01-01'
    AND (o_orderpriority = "1-URGENT" OR o_orderpriority = "2-HIGH")
GROUP BY
    l_shipmode
ORDER BY
    l_shipmode;
SELECT
    l_shipmode, COUNT(*)
FROM
    Lineitem,
    Orders
WHERE
    o_orderkey = l_orderkey
    AND (l_shipmode = "MAIL" OR l_shipmode = "SHIP")
    AND l_commitdate < l_receiptdate
    AND l_shipdate < l_commitdate
    AND l_receiptdate >= d'1994-01-01'
    AND l_receiptdate < d'1995-01-01'
    AND (o_orderpriority = "1-URGENT" OR o_orderpriority = "2-HIGH")
GROUP BY
    l_shipmode
ORDER BY
    l_shipmode;
SELECT
    l_shipmode, COUNT(*)
FROM
    Lineitem,
    Orders
WHERE
    o_orderkey = l_orderkey
    AND (l_shipmode = "MAIL" OR l_shipmode = "SHIP")
    AND l_commitdate < l_receiptdate
    AND l_shipdate < l_commitdate
    AND l_receiptdate >= d'1994-01-01'
    AND l_receiptdate < d'1995-01-01'
    AND (o_orderpriority = "1-URGENT" OR o_orderpriority = "2-HIGH")
GROUP BY
    l_shipmode
ORDER BY
    l_shipmode;
