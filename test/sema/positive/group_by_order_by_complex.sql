SELECT
    l_orderkey + l_partkey
FROM
    LINEITEM
GROUP BY
    l_orderkey + l_partkey;

SELECT
    l_orderkey + l_partkey
FROM
    LINEITEM
GROUP BY
    l_orderkey + l_partkey
ORDER BY
    l_orderkey + l_partkey;
