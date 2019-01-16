CREATE DATABASE mydb;
USE mydb;
CREATE TABLE mytable (
    n INT(4)
);

SELECT nota.fn(n)
FROM mytable;
