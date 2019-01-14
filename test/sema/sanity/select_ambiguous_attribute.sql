CREATE DATABASE mydb;
USE mydb;
CREATE TABLE mytable (
    n INT(4)
);
CREATE TABLE othertable (
    n INT(4)
);

SELECT n
FROM mytable, othertable;
