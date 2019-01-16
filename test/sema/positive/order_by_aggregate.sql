CREATE DATABASE mydb;
USE mydb;
CREATE TABLE mytable (
    a INT(4),
    b INT(4),
    c INT(4)
);

SELECT a, AVG(b)
FROM mytable
GROUP BY a
ORDER BY MAX(c);
