CREATE DATABASE mydb;
USE mydb;
CREATE TABLE mytable (
    n INT(1)
);

SELECT MIN(n), MAX(n), AVG(n), SUM(n)
FROM mytable;

SELECT ISNULL(n)
FROM mytable;
