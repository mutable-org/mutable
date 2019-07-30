-- Attribute name of innermost subquery should not be visible to outermost query.
SELECT
    ps_partkey, intermediate_ps_suppkey
FROM
    (SELECT ps_partkey, innermost_ps_suppkey AS intermediate_ps_suppkey
     FROM
        (SELECT ps_partkey, ps_suppkey AS innermost_ps_suppkey
         FROM PARTSUPP) AS innermost
    ) AS intermediate;
