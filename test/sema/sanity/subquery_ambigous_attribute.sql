SELECT
    l_orderkey, l_suppkey
FROM
    LINEITEM AS LITEM,
    (SELECT ps_suppkey AS l_suppkey
     FROM PARTSUPP) AS PSUPP
WHERE
    LITEM.l_suppkey = PSUPP.l_suppkey;
