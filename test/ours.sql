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

IMPORT INTO R DSV "test/end2end/data/ours/R.csv" SKIP HEADER;
IMPORT INTO S DSV "test/end2end/data/ours/S.csv" SKIP HEADER;
IMPORT INTO T DSV "test/end2end/data/ours/T.csv" SKIP HEADER;
