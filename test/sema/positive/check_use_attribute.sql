CREATE TABLE correct_type_check_table (
    a INT(4) CHECK (a > 42),
    b INT(4) CHECK (b > 2 * a)
);
