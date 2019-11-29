DROP DATABASE IF EXISTS reference;
CREATE DATABASE reference;
\c reference;

CREATE TABLE R (
    key INTEGER,
    fkey INTEGER,
    rfloat FLOAT,
    rstring CHAR(15)
);

CREATE TABLE S (
    key INTEGER,
    fkey INTEGER,
    rfloat FLOAT,
    rstring CHAR(15)
);

CREATE TABLE T (
    key INTEGER,
    fkey INTEGER,
    rfloat FLOAT,
    rstring CHAR(15)
);

\copy R FROM 'test/end2end/positive/data/R.csv' CSV HEADER;
\copy S FROM 'test/end2end/positive/data/S.csv' CSV HEADER;
\copy T FROM 'test/end2end/positive/data/T.csv' CSV HEADER;
