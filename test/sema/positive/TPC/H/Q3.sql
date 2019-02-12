SELECT
    l_orderkey,
    SUM(l_extendedprice*(1-l_discount)) AS revenue,
    o_orderdate,
    o_shippriority
FROM
    CUSTOMER,
    ORDERS,
    LINEITEM
WHERE
    c_mktsegment = "[SEGMENT]"
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate < 42 -- date "[DATE]"
    AND l_shipdate > 13 -- date "[DATE]"
GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority
ORDER BY
    revenue DESC,
    o_orderdate;
