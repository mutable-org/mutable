SELECT R.key, S.key, T.key FROM R, S, T WHERE R.key = S.fkey AND S.key = T.fkey ORDER BY R.key, S.key, T.key;
