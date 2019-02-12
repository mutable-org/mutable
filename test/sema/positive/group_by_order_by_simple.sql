SELECT
    l_orderkey,
    AVG(l_tax)
FROM
    LINEITEM
GROUP BY
    l_orderkey
ORDER BY
    LINEITEM.l_orderkey;

SELECT
    l_orderkey,
    l_partkey
FROM
    LINEITEM
GROUP BY
    l_orderkey,
    l_partkey
ORDER BY
    LINEITEM.l_orderkey;

SELECT
    l_orderkey + l_partkey
FROM
    LINEITEM
GROUP BY
    l_orderkey, l_partkey
ORDER BY
    LINEITEM.l_orderkey;
