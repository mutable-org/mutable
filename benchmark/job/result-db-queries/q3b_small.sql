CREATE DATABASE job;
USE job;

CREATE TABLE keyword (
    id INT(4) NOT NULL PRIMARY KEY,
    keyword CHAR(74) NOT NULL,
    phonetic_code CHAR(5)
);

IMPORT INTO keyword DSV "benchmark/job/data/keyword.csv" ROWS 100;

CREATE TABLE movie_info (
    id INT(4) NOT NULL,
    movie_id INT(4) NOT NULL,
    info_type_id INT(4) NOT NULL,
    info CHAR(43) NOT NULL,
    note CHAR(19)
);

IMPORT INTO movie_info DSV "benchmark/job/data/movie_info.csv" ROWS 100;

CREATE TABLE movie_keyword (
    id INT(4) NOT NULL PRIMARY KEY,
    movie_id INT(4) NOT NULL,
    keyword_id INT(4) NOT NULL
);

IMPORT INTO movie_keyword DSV "benchmark/job/data/movie_keyword.csv" ROWS 100;

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

IMPORT INTO title DSV "benchmark/job/data/title.csv" ROWS 100;

-- SQL QUERY --
SELECT t.title
FROM keyword AS k,
     movie_info AS mi,
     movie_keyword AS mk,
     title AS t
WHERE k.keyword LIKE "%sequel%"
  AND mi.info = "Bulgaria"
  AND t.production_year > 2010
  AND t.id = mi.movie_id
  AND t.id = mk.movie_id
  AND mk.movie_id = mi.movie_id
  AND k.id = mk.keyword_id;
