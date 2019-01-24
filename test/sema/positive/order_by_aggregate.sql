SELECT returnflag, AVG(linenumber)
FROM LINEITEM
GROUP BY returnflag
ORDER BY MAX(quantity);
