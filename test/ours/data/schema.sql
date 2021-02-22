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

CREATE TABLE D (
    key INT(2),
    rdate DATE,
    rdatetime DATETIME
);

IMPORT INTO R DSV "test/ours/data/R.csv" HAS HEADER SKIP HEADER;
IMPORT INTO S DSV "test/ours/data/S.csv" HAS HEADER SKIP HEADER;
IMPORT INTO T DSV "test/ours/data/T.csv" HAS HEADER SKIP HEADER;
IMPORT INTO D DSV "test/ours/data/D.csv" HAS HEADER SKIP HEADER;
