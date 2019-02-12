SELECT
    l_returnflag,
    AVG(l_linenumber)
FROM
    LINEITEM
GROUP BY
    l_returnflag
ORDER BY
    MAX(l_quantity);
