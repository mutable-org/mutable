SELECT R.key, S.key FROM R, S WHERE R.key = S.fkey AND R.key < 10 ORDER BY R.key, S.key;
