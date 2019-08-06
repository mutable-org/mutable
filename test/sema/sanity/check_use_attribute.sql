CREATE TABLE incorrect_type_check_table (
    a INT(4) CHECK (a + 42)
);
