SELECT returnflag
FROM LINEITEM
GROUP BY returnflag
ORDER BY linestatus; -- to order by linestatus one must group by linestatus
