CREATE DATABASE mydb;
USE mydb;
CREATE TABLE mytable (
    n INT(4)
);

SELECT nonexistent_table.attribute
FROM mytable;
