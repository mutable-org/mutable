CREATE DATABASE result_db;

USE result_db;

CREATE TABLE R (
    id INT(2) NOT NULL,
    s INT(2) NOT NULL,
    t INT(2) NOT NULL,
    u INT(2) NOT NULL,
    v INT(2) NOT NULL
);

CREATE TABLE S (
    id INT(2) NOT NULL,
    r INT(2) NOT NULL,
    t INT(2) NOT NULL,
    u INT(2) NOT NULL,
    v INT(2) NOT NULL
);

CREATE TABLE T (
    id INT(2) NOT NULL,
    r INT(2) NOT NULL,
    s INT(2) NOT NULL,
    u INT(2) NOT NULL,
    v INT(2) NOT NULL
);

CREATE TABLE U (
    id INT(2) NOT NULL,
    r INT(2) NOT NULL,
    s INT(2) NOT NULL,
    t INT(2) NOT NULL,
    v INT(2) NOT NULL
);

CREATE TABLE V (
    id INT(2) NOT NULL,
    r INT(2) NOT NULL,
    s INT(2) NOT NULL,
    t INT(2) NOT NULL,
    u INT(2) NOT NULL
);

IMPORT INTO R DSV "test/result-db/data/R.csv" HAS HEADER SKIP HEADER;
IMPORT INTO S DSV "test/result-db/data/S.csv" HAS HEADER SKIP HEADER;
IMPORT INTO T DSV "test/result-db/data/T.csv" HAS HEADER SKIP HEADER;
IMPORT INTO U DSV "test/result-db/data/U.csv" HAS HEADER SKIP HEADER;
IMPORT INTO V DSV "test/result-db/data/V.csv" HAS HEADER SKIP HEADER;
