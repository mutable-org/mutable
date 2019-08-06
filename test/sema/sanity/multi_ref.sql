CREATE TABLE multi_ref_table (
    multi_ref INT(4) REFERENCES LINEITEM(l_orderkey) REFERENCES ORDERS(o_orderkey)
);
