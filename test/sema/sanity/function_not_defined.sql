CREATE DATABASE mydb;
USE mydb;
CREATE TABLE mytable (
    n INT(4)
);

SELECT undefined_function(n)
FROM mytable;
