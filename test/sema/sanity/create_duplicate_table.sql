CREATE DATABASE mydb;
USE mydb;
CREATE TABLE mytable (
    n INT(4)
);

CREATE TABLE mytable ( -- error: table already exists
    n INT(4)
);
