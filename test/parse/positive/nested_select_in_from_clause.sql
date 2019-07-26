SELECT *
FROM
    Alpha AS A,
    (SELECT x, y FROM Beta ) AS B,
    (SELECT z FROM Gamma AS Y) AS C,
    Delta
;
