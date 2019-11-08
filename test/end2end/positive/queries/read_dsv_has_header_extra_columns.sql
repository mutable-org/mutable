CREATE TABLE test (
    a INT(4),
    b INT(4),
    c INT(4)
);

IMPORT INTO test DSV "test/end2end/positive/data/read_dsv_has_header_extra_columns.csv" HAS HEADER;

SELECT * FROM test;
