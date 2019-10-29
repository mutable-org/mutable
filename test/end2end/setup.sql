CREATE DATABASE simple;

USE simple;

CREATE TABLE R (
    key INT(2),
    fkey INT(2),
    rfloat FLOAT,
    rstring CHAR(15)
);

CREATE TABLE S (
    key INT(2),
    fkey INT(2),
    rfloat FLOAT,
    rstring CHAR(15)
);

CREATE TABLE T (
    key INT(2),
    fkey INT(2),
    rfloat FLOAT,
    rstring CHAR(15)
);

IMPORT INTO R DSV "test/end2end/positive/data/R.csv" SKIP HEADER;
IMPORT INTO S DSV "test/end2end/positive/data/S.csv" SKIP HEADER;
IMPORT INTO T DSV "test/end2end/positive/data/T.csv" SKIP HEADER;
