CREATE TABLE ref_table (
    lineitem_orderkey INT(4) REFERENCES LINEITEM(l_orderkey)
);

CREATE TABLE check_table (
    check_attr INT(4) CHECK (13 < 42)
);

CREATE TABLE unique_not_null_table (
    unn1 INT(4) UNIQUE NOT NULL,
    unn2 INT(4) NOT NULL UNIQUE
);

CREATE TABLE primary_key_check_table (
    key INT(4) PRIMARY KEY CHECK (42 > 0)
);
