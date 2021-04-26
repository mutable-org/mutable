IMPORT INTO Lineitem DSV "benchmark/tpc-h/data/lineitem.tbl" DELIMITER "|";

SELECT
        l_returnflag,
        l_linestatus,
        SUM(l_quantity) AS sum_qty,
        SUM(l_extendedprice) AS sum_base_price,
        SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
        SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
        SUM(l_quantity) AS avg_qty,
        SUM(l_extendedprice) AS avg_price,
        SUM(l_discount) AS avg_disc,
        COUNT(*) AS count_order
FROM
        Lineitem
WHERE
        l_shipdate <= d'1998-09-02'
GROUP BY
        l_returnflag,
        l_linestatus
ORDER BY
        l_returnflag,
        l_linestatus;
SELECT
        l_returnflag,
        l_linestatus,
        SUM(l_quantity) AS sum_qty,
        SUM(l_extendedprice) AS sum_base_price,
        SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
        SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
        SUM(l_quantity) AS avg_qty,
        SUM(l_extendedprice) AS avg_price,
        SUM(l_discount) AS avg_disc,
        COUNT(*) AS count_order
FROM
        Lineitem
WHERE
        l_shipdate <= d'1998-09-02'
GROUP BY
        l_returnflag,
        l_linestatus
ORDER BY
        l_returnflag,
        l_linestatus;
SELECT
        l_returnflag,
        l_linestatus,
        SUM(l_quantity) AS sum_qty,
        SUM(l_extendedprice) AS sum_base_price,
        SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
        SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
        SUM(l_quantity) AS avg_qty,
        SUM(l_extendedprice) AS avg_price,
        SUM(l_discount) AS avg_disc,
        COUNT(*) AS count_order
FROM
        Lineitem
WHERE
        l_shipdate <= d'1998-09-02'
GROUP BY
        l_returnflag,
        l_linestatus
ORDER BY
        l_returnflag,
        l_linestatus;
SELECT
        l_returnflag,
        l_linestatus,
        SUM(l_quantity) AS sum_qty,
        SUM(l_extendedprice) AS sum_base_price,
        SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
        SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
        SUM(l_quantity) AS avg_qty,
        SUM(l_extendedprice) AS avg_price,
        SUM(l_discount) AS avg_disc,
        COUNT(*) AS count_order
FROM
        Lineitem
WHERE
        l_shipdate <= d'1998-09-02'
GROUP BY
        l_returnflag,
        l_linestatus
ORDER BY
        l_returnflag,
        l_linestatus;
SELECT
        l_returnflag,
        l_linestatus,
        SUM(l_quantity) AS sum_qty,
        SUM(l_extendedprice) AS sum_base_price,
        SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
        SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
        SUM(l_quantity) AS avg_qty,
        SUM(l_extendedprice) AS avg_price,
        SUM(l_discount) AS avg_disc,
        COUNT(*) AS count_order
FROM
        Lineitem
WHERE
        l_shipdate <= d'1998-09-02'
GROUP BY
        l_returnflag,
        l_linestatus
ORDER BY
        l_returnflag,
        l_linestatus;
