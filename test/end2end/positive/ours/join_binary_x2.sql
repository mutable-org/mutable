SELECT R.key, S.key, T.key FROM R, S, T WHERE R.key = S.fkey AND R.key = T.fkey AND R.key < 10 ORDER BY R.key, S.key, T.key;
