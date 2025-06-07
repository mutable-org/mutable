CREATE DATABASE job_light;
USE job_light;

CREATE TABLE table_1 (
    id INT(4) NOT NULL PRIMARY KEY,
    col_1 INT(4) NOT NULL
);
IMPORT INTO table_1 DSV "benchmark/small/data/table1.csv";
CREATE TABLE table_2 (
    id INT(4) NOT NULL PRIMARY KEY,
    col_2 INT(4) NOT NULL
);
IMPORT INTO table_2 DSV "benchmark/small/data/table2.csv";
CREATE TABLE table_3 (
    id INT(4) NOT NULL PRIMARY KEY,
    col_3 INT(4) NOT NULL
);
IMPORT INTO table_3 DSV "benchmark/small/data/table3.csv";
CREATE TABLE table_4 (
    id INT(4) NOT NULL PRIMARY KEY,
    col_4 INT(4) NOT NULL
);
IMPORT INTO table_4 DSV "benchmark/small/data/table4.csv";
-- same but in a single line
SELECT table_1.id, table_1.col_1, table_2.id, table_2.col_2, table_3.id, table_3.col_3, table_4.id, table_4.col_4 FROM table_1, table_2, table_3, table_4 WHERE table_1.id = table_2.id AND table_1.id = table_3.id AND table_1.id = table_4.id AND table_1.col_1 < 500 LIMIT 10;
