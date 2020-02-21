SELECT fkey, cnt FROM (SELECT fkey, COUNT(key) AS cnt FROM R GROUP BY fkey) AS sub ORDER BY fkey;
