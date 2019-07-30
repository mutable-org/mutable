SELECT
    s_suppkey, ps_partkey
FROM
    SUPPLIER,
    (SELECT ps_partkey, ps_suppkey
     FROM PARTSUPP) AS SUPPLIER
WHERE
    s_suppkey = ps_suppkey;
