-- IMPORT DATA --
IMPORT INTO cast_info DSV "benchmark/job/data/cast_info.csv";
IMPORT INTO keyword DSV "benchmark/job/data/keyword.csv";
IMPORT INTO movie_keyword DSV "benchmark/job/data/movie_keyword.csv";
IMPORT INTO name DSV "benchmark/job/data/name.csv";
IMPORT INTO title DSV "benchmark/job/data/title.csv";

-- SQL QUERY --
SELECT k.keyword,
       n.name,
       t.title
FROM cast_info AS ci,
     keyword AS k,
     movie_keyword AS mk,
     name AS n,
     title AS t
WHERE k.keyword = "marvel-cinematic-universe"
  AND n.name LIKE "%Downey%Robert%"
  AND t.production_year > 2010
  AND k.id = mk.keyword_id
  AND t.id = mk.movie_id
  AND t.id = ci.movie_id
  AND ci.movie_id = mk.movie_id
  AND n.id = ci.person_id;
