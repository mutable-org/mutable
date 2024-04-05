-- IMPORT DATA --
IMPORT INTO info_type DSV "benchmark/job/data/info_type.csv";
IMPORT INTO keyword DSV "benchmark/job/data/keyword.csv";
IMPORT INTO movie_info_idx DSV "benchmark/job/data/movie_info_idx.csv";
IMPORT INTO movie_keyword DSV "benchmark/job/data/movie_keyword.csv";
IMPORT INTO title DSV "benchmark/job/data/title.csv";

-- SQL QUERY --
SELECT mi_idx.info,
       t.title
FROM info_type AS it,
     keyword AS k,
     movie_info_idx AS mi_idx,
     movie_keyword AS mk,
     title AS t
WHERE it.info = "rating"
  AND k.keyword LIKE "%sequel%"
  AND mi_idx.info > "5.0"
  AND t.production_year > 2005
  AND t.id = mi_idx.movie_id
  AND t.id = mk.movie_id
  AND mk.movie_id = mi_idx.movie_id
  AND k.id = mk.keyword_id
  AND it.id = mi_idx.info_type_id;
