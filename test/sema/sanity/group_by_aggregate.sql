SELECT *
FROM LINEITEM
GROUP BY SUM(l_orderkey); -- cannot group by scalar
