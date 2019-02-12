SELECT l_linenumber, MAX(l_quantity) -- must not mix scalar and vectorial expressions
FROM LINEITEM;
