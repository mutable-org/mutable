CREATE DATABASE mydb;
USE mydb;
CREATE TABLE mytable (
    n INT(4)
);

SELECT *
FROM mytable
WHERE AVG(n) > 42;
