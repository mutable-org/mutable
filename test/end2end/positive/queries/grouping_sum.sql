SELECT T.key, SUM(S.fkey) FROM T, S WHERE T.key = S.fkey GROUP BY T.key             ORDER BY T.key;
