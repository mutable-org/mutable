SELECT
    MIN(l_linenumber),
    MAX(l_linenumber),
    AVG(l_linenumber),
    SUM(l_linenumber)
FROM
    LINEITEM;

SELECT
    ISNULL(l_linenumber)
FROM
    LINEITEM;
