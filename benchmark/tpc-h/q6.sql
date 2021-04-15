IMPORT INTO Lineitem DSV "benchmark/tpc-h/data/lineitem.tbl" DELIMITER "|";

SELECT
        SUM(l_extendedprice * l_discount) AS revenue
FROM
        Lineitem
WHERE
        l_shipdate >= d'1994-01-01'
        AND l_shipdate < d'1995-01-01'
        AND l_quantity < 24;
SELECT
        SUM(l_extendedprice * l_discount) AS revenue
FROM
        Lineitem
WHERE
        l_shipdate >= d'1994-01-01'
        AND l_shipdate < d'1995-01-01'
        AND l_quantity < 24;
SELECT
        SUM(l_extendedprice * l_discount) AS revenue
FROM
        Lineitem
WHERE
        l_shipdate >= d'1994-01-01'
        AND l_shipdate < d'1995-01-01'
        AND l_quantity < 24;
SELECT
        SUM(l_extendedprice * l_discount) AS revenue
FROM
        Lineitem
WHERE
        l_shipdate >= d'1994-01-01'
        AND l_shipdate < d'1995-01-01'
        AND l_quantity < 24;
SELECT
        SUM(l_extendedprice * l_discount) AS revenue
FROM
        Lineitem
WHERE
        l_shipdate >= d'1994-01-01'
        AND l_shipdate < d'1995-01-01'
        AND l_quantity < 24;
