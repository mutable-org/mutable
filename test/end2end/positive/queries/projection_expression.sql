SELECT (T.key + S.key) * 2 AS sum FROM T, S WHERE T.key = S.fkey             ORDER BY sum;
