SELECT T.key, R.rfloat, S.rstring FROM R, S, T WHERE R.key = T.fkey + S.fkey AND R.key < 10 ORDER BY T.key, R.rfloat, S.rstring;
