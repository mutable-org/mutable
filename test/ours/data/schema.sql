CREATE DATABASE ours;

USE ours;

CREATE TABLE R (
    key INT(2) NOT NULL,
    fkey INT(2) NOT NULL,
    rfloat FLOAT NOT NULL,
    rstring CHAR(15) NOT NULL
);

CREATE TABLE S (
    key INT(2) NOT NULL,
    fkey INT(2) NOT NULL,
    rfloat FLOAT NOT NULL,
    rstring CHAR(15) NOT NULL
);

CREATE TABLE T (
    key INT(2) NOT NULL,
    fkey INT(2) NOT NULL,
    rfloat FLOAT NOT NULL,
    rstring CHAR(15) NOT NULL
);

CREATE TABLE D (
    key INT(2) NOT NULL,
    rdate DATE NOT NULL,
    rdatetime DATETIME NOT NULL
);

IMPORT INTO R DSV "test/ours/data/R.csv" HAS HEADER SKIP HEADER;
IMPORT INTO S DSV "test/ours/data/S.csv" HAS HEADER SKIP HEADER;
IMPORT INTO T DSV "test/ours/data/T.csv" HAS HEADER SKIP HEADER;
IMPORT INTO D DSV "test/ours/data/D.csv" HAS HEADER SKIP HEADER;
