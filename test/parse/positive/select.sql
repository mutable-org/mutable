-- Simple select with asterisk.
SELECT * FROM tbl;

-- Simple select without asterisk.
SELECT x FROM tbl;

-- Simple select by qualified attribute
SELECT tbl.x FROM tbl;

-- Select with constants and expressions.
SELECT 42, "string", AVG(x) FROM tbl;

-- Select with renaming.
SELECT a AS x, SUM(b) AS y FROM tbl;

-- Select from multiple sources.
SELECT * FROM tbl_a, tbl_b, tbl_c;

-- Select from table with renaming.
SELECT * FROM tbl_a AS A, tbl_b AS B;

-- Select with where clause.
SELECT * FROM tbl WHERE cond;

-- Select with group by.
SELECT a, b, AVG(x) AS avg_c FROM tbl GROUP BY a, b;

-- Select with having.
SELECT AVG(a) FROM tbl HAVING (MIN(a) > 42);

-- Select with order by.
SELECT * FROM tbl ORDER BY a ASC, b DESC, c ASC;

-- Select with limit.
SELECT * FROM tbl LIMIT 13 OFFSET 42;
