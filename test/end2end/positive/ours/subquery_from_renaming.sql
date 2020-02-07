SELECT k, f, x FROM (SELECT key AS k, 2 * fkey AS f, rfloat AS x FROM R) AS T WHERE T.f < 42 ORDER BY k;
