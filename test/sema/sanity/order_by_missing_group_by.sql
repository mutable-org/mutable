SELECT l_returnflag
FROM LINEITEM
GROUP BY l_returnflag
ORDER BY l_linestatus; -- to order by linestatus one must group by linestatus
