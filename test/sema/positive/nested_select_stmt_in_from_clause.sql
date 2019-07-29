SELECT o_orderkey, o_clerk, avg_price
FROM
    ORDERS,
    (SELECT l_orderkey, AVG(l_extendedprice) AS avg_price
     FROM LINEITEM
     GROUP BY l_orderkey) AS L,
    CUSTOMER AS C
WHERE
    o_orderkey = L.l_orderkey AND
    o_custkey = c_custkey AND
    c_name = "Marco Polo"
;
