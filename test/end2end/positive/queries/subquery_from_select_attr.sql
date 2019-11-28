SELECT fkey FROM (SELECT key, fkey FROM R) AS sub ORDER BY fkey;
