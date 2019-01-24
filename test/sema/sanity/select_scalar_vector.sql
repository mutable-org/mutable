SELECT linenumber, MAX(quantity) -- must not mix scalar and vectorial expressions
FROM LINEITEM;
