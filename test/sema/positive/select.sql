-- Simple SELECT FROM
SELECT *
FROM LINEITEM;

SELECT *, discount
FROM LINEITEM;

SELECT orderkey, totalprice
FROM ORDERS;

-- Simple SELECT with qualified attribute
SELECT l.orderkey
FROM LINEITEM AS l;

-- SELECT with WHERE clause
SELECT *
FROM LINEITEM
WHERE tax < 0.05 AND discount > 0.20;

SELECT orderkey
FROM ORDERS
WHERE clerk = "#Clerk_0000001";

-- SELECT with GROUP BY
SELECT orderkey, linenumber
FROM LINEITEM
GROUP BY orderkey, linenumber;

-- SELECT with HAVING
SELECT "OK"
FROM LINEITEM
HAVING AVG(extendedprice) >= 1337.42;

-- SELECT with ORDER BY
SELECT *
FROM LINEITEM
ORDER BY orderkey, linenumber DESC;

-- SELECT with LIMIT
SELECT *
FROM LINEITEM
LIMIT 42;

SELECT *
FROM LINEITEM
LIMIT 42 OFFSET 1337;
