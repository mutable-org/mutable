SELECT
    l_suppkey AS l_orderkey
FROM
    LINEITEM
WHERE
    l_orderkey > 42;
