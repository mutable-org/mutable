SELECT orderkey, AVG(tax)
FROM LINEITEM
GROUP BY orderkey
ORDER BY LINEITEM.orderkey;

SELECT orderkey, partkey
FROM LINEITEM
GROUP BY orderkey, partkey
ORDER BY LINEITEM.orderkey;

SELECT orderkey + partkey
FROM LINEITEM
GROUP BY orderkey, partkey
ORDER BY LINEITEM.orderkey;
