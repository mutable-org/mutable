SELECT l_orderkey, l_extendedprice AS l_quantity
FROM LINEITEM
ORDER BY l_quantity * l_extendedprice;
