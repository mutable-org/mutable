CREATE DATABASE ours;

USE ours;

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

IMPORT INTO R DSV "test/ours/data/R.csv" SKIP HEADER;
IMPORT INTO S DSV "test/ours/data/S.csv" SKIP HEADER;
IMPORT INTO T DSV "test/ours/data/T.csv" SKIP HEADER;
