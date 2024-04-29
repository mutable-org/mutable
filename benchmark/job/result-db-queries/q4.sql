-- release:
CREATE DATABASE job;
USE job;


-- CREATE TABLE AND IMPORT DATA --
CREATE TABLE info_type (
    id INT(4) NOT NULL PRIMARY KEY,
    info CHAR(32) NOT NULL
);

IMPORT INTO info_type DSV "benchmark/job/data/info_type.csv";

CREATE TABLE keyword (
    id INT(4) NOT NULL PRIMARY KEY,
    keyword CHAR(74) NOT NULL,
    phonetic_code CHAR(5)
);

IMPORT INTO keyword DSV "benchmark/job/data/keyword.csv";

CREATE TABLE movie_info_idx (
    id INT(4) NOT NULL PRIMARY KEY,
    movie_id INT(4) NOT NULL,
    info_type_id INT(4) NOT NULL,
    info CHAR(10) NOT NULL,
    note CHAR(1)
);

IMPORT INTO movie_info_idx DSV "benchmark/job/data/movie_info_idx.csv";

CREATE TABLE movie_keyword (
    id INT(4) NOT NULL PRIMARY KEY,
    movie_id INT(4) NOT NULL,
    keyword_id INT(4) NOT NULL
);

IMPORT INTO movie_keyword DSV "benchmark/job/data/movie_keyword.csv";

CREATE TABLE title (
    id INT(4) NOT NULL PRIMARY KEY,
    title CHAR(334) NOT NULL,
    imdb_index CHAR(5),
    kind_id INT(4) NOT NULL,
    production_year INT(4),
    imdb_id INT(4),
    phonetic_code CHAR(5),
    episode_of_id INT(4),
    season_nr INT(4),
    episode_nr INT(4),
    series_years CHAR(49),
    md5sum CHAR(32)
);

IMPORT INTO title DSV "benchmark/job/data/title.csv";


-- SQL QUERY --
-- SELECT mi_idx.info AS rating,
--        t.title AS movie_title
SELECT mi_idx.info, t.title
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
