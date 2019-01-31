SELECT *
FROM LINEITEM
GROUP BY SUM(orderkey); -- cannot group by scalar
