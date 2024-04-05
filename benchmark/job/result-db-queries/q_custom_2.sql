-- IMPORT DATA --
IMPORT INTO title DSV "benchmark/job/data/title.csv";
IMPORT INTO movie_info DSV "benchmark/job/data/movie_info.csv";
IMPORT INTO movie_info_idx DSV "benchmark/job/data/movie_info_idx.csv";
IMPORT INTO info_type DSV "benchmark/job/data/info_type.csv";

-- SQL QUERY --
SELECT
    t.title
FROM
    title AS t,
    movie_info AS mi,
    movie_info_idx AS mi_idx,
    info_type AS it
WHERE t.production_year >= 1974
    AND mi.info = "English"
    AND NOT (it.info LIKE "LD%")
    AND t.id = mi.movie_id
    AND mi.movie_id = mi_idx.movie_id
    AND mi_idx.info_type_id = it.id;
