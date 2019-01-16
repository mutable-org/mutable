CREATE DATABASE mydb;
USE mydb;
CREATE TABLE mytable (
    a INT(4),
    b INT(4)
);

SELECT a, b
FROM mytable
GROUP BY a;
