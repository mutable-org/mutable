IMPORT INTO Lineitem DSV "benchmark/tpc-h/data/lineitem.tbl" DELIMITER "|";
IMPORT INTO Part DSV "benchmark/tpc-h/data/part.tbl" DELIMITER "|";

SELECT
        SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
FROM
        Lineitem,
        Part
WHERE
        l_partkey = p_partkey
        AND l_shipdate >= d'1995-09-01'
        AND l_shipdate < d'1995-10-01';
SELECT
        SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
FROM
        Lineitem,
        Part
WHERE
        l_partkey = p_partkey
        AND l_shipdate >= d'1995-09-01'
        AND l_shipdate < d'1995-10-01';
SELECT
        SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
FROM
        Lineitem,
        Part
WHERE
        l_partkey = p_partkey
        AND l_shipdate >= d'1995-09-01'
        AND l_shipdate < d'1995-10-01';
SELECT
        SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
FROM
        Lineitem,
        Part
WHERE
        l_partkey = p_partkey
        AND l_shipdate >= d'1995-09-01'
        AND l_shipdate < d'1995-10-01';
SELECT
        SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
FROM
        Lineitem,
        Part
WHERE
        l_partkey = p_partkey
        AND l_shipdate >= d'1995-09-01'
        AND l_shipdate < d'1995-10-01';
