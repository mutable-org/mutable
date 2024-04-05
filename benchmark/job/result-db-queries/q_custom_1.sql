-- IMPORT DATA --
IMPORT INTO cast_info DSV "benchmark/job/data/cast_info.csv";
IMPORT INTO info_type DSV "benchmark/job/data/info_type.csv";
IMPORT INTO movie_keyword DSV "benchmark/job/data/movie_keyword.csv";
IMPORT INTO keyword DSV "benchmark/job/data/keyword.csv";
IMPORT INTO person_info DSV "benchmark/job/data/person_info.csv";
IMPORT INTO title DSV "benchmark/job/data/title.csv";

-- SQL QUERY --
SELECT
    t.title
FROM
    title AS t,
    cast_info AS ci,
    person_info AS pi,
    info_type AS it,
    movie_keyword AS mk,
    keyword AS k
WHERE it.info = "mini biography"
  AND pi.note = "Volker Boehm"
  AND k.keyword = "marvel-cinematic-universe"
  AND t.production_year >= 1980
  AND t.production_year <= 1984
  AND t.id = mk.movie_id
  AND t.id = ci.movie_id
  AND mk.keyword_id = k.id
  AND ci.person_id = pi.person_id
  AND pi.info_type_id = it.id;
