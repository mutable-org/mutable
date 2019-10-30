SELECT T.key, S.key, R.rstring FROM R, S, T WHERE R.key = S.fkey AND R.key = T.fkey ORDER BY T.key, S.key, R.rstring;
