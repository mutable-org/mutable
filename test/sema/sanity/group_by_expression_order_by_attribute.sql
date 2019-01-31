SELECT orderkey + partkey
FROM LINEITEM
GROUP BY orderkey + partkey
ORDER BY orderkey; -- not scalar
