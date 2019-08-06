CREATE TABLE incorrect_type_ref_table (
    incorrect_type_ref INT(8) REFERENCES LINEITEM(l_orderkey)
);
