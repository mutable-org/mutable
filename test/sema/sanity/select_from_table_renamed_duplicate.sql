CREATE DATABASE mydb;
USE mydb;
CREATE TABLE mytable (
    n INT(4)
);
CREATE TABLE othertable (
    x FLOAT,
    y FLOAT
);

SELECT n
FROM mytable AS othertable, othertable;
