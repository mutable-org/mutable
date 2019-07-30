-- Table name of innermost subquery should not be visible to outermost query.
SELECT
    innermost.ps_partkey
FROM
    (SELECT ps_partkey, ps_suppkey
     FROM
        (SELECT ps_partkey, ps_suppkey, ps_comment
         FROM PARTSUPP) AS innermost
    ) AS intermediate;
