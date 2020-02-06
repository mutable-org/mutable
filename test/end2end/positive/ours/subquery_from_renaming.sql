SELECT k, f, x FROM (SELECT key AS k, fkey AS f, 2.0 * rfloat AS x FROM R) AS T WHERE T.f < 42 ORDER BY k;
