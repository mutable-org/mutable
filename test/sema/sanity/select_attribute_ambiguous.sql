SELECT
    T.key
FROM (
    SELECT
        L.l_orderkey AS key,
        O.o_orderkey AS key
    FROM
        LINEITEM AS L,
        ORDERS AS O
    WHERE
        L.l_orderkey = O.o_orderkey
    ) AS T
;
