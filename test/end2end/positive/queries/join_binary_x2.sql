SELECT * FROM R, S, T WHERE R.key = S.fkey AND R.key = T.fkey ORDER BY R.key, S.key, T.key;
