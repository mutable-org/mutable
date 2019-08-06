CREATE TABLE incorrect_type_ref_table (
    incorrect_type_ref INT(8) REFERENCES LINEITEM(l_orderkey)
);

CREATE TABLE multi_ref_table (
    multi_ref INT(4) REFERENCES LINEITEM(l_orderkey) REFERENCES ORDERS(o_orderkey)
);

--CREATE TABLE non_boolean_check_table (
--    non_boolean_check INT(4) CHECK (1+1)
--);
