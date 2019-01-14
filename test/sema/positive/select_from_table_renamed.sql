CREATE DATABASE mydb;
USE mydb;
CREATE TABLE mytable (
    n INT(4)
);

SELECT t.n
FROM mytable AS t;

SELECT n
FROM mytable AS t;
