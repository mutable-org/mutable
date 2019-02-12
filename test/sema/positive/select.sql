-- Simple SELECT FROM
SELECT *
FROM LINEITEM;

SELECT *, l_discount
FROM LINEITEM;

SELECT o_orderkey, o_totalprice
FROM ORDERS;

-- Simple SELECT with qualified attribute
SELECT l.l_orderkey
FROM LINEITEM AS l;

-- SELECT with WHERE clause
SELECT *
FROM LINEITEM
WHERE l_tax < 0.05 AND l_discount > 0.20;

SELECT o_orderkey
FROM ORDERS
WHERE o_clerk = "#Clerk_0000001";

-- SELECT with GROUP BY
SELECT l_orderkey, l_linenumber
FROM LINEITEM
GROUP BY l_orderkey, l_linenumber;

-- SELECT with HAVING
SELECT "OK"
FROM LINEITEM
HAVING AVG(l_extendedprice) >= 1337.42;

-- SELECT with ORDER BY
SELECT *
FROM LINEITEM
ORDER BY l_orderkey, l_linenumber DESC;

-- SELECT with LIMIT
SELECT *
FROM LINEITEM
LIMIT 42;

SELECT *
FROM LINEITEM
LIMIT 42 OFFSET 1337;
