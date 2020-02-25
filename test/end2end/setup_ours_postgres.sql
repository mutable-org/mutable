DROP DATABASE IF EXISTS ours;
CREATE DATABASE ours;
\c ours;

CREATE TABLE R (
    key INTEGER,
    fkey INTEGER,
    rfloat REAL, -- 32-bit variable-precision floating point
    rstring CHAR(15)
);

CREATE TABLE S (
    key INTEGER,
    fkey INTEGER,
    rfloat REAL, -- 32-bit variable-precision floating point
    rstring CHAR(15)
);

CREATE TABLE T (
    key INTEGER,
    fkey INTEGER,
    rfloat REAL, -- 32-bit variable-precision floating point
    rstring CHAR(15)
);

\copy R FROM 'test/end2end/data/ours/R.csv' CSV HEADER;
\copy S FROM 'test/end2end/data/ours/S.csv' CSV HEADER;
\copy T FROM 'test/end2end/data/ours/T.csv' CSV HEADER;
