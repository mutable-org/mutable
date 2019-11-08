CREATE TABLE test (
    a INT(4),
    b INT(4),
    c INT(4)
);

IMPORT INTO test DSV "test/end2end/positive/data/read_dsv.csv";

SELECT * FROM test;
