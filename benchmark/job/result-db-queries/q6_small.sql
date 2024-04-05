-- debug: src/backend/WasmAlgo.hpp:463: Condition 'refs_.count(id) <= 1' failed.  duplicate identifier, lookup ambiguous.
CREATE DATABASE job;
USE job;


-- CREATE TABLE AND IMPORT DATA --
CREATE TABLE cast_info (
    id INT(4) NOT NULL PRIMARY KEY,
    person_id INT(4) NOT NULL,
    movie_id INT(4) NOT NULL,
    person_role_id INT(4),
    note CHAR(18), -- 922
    nr_order INT(4),
    role_id INT(4) NOT NULL
);

IMPORT INTO cast_info DSV "benchmark/job/data/cast_info.csv" ROWS 100;

CREATE TABLE keyword (
    id INT(4) NOT NULL PRIMARY KEY,
    keyword CHAR(74) NOT NULL,
    phonetic_code CHAR(5)
);

IMPORT INTO keyword DSV "benchmark/job/data/keyword.csv" ROWS 100;

CREATE TABLE movie_keyword (
    id INT(4) NOT NULL PRIMARY KEY,
    movie_id INT(4) NOT NULL,
    keyword_id INT(4) NOT NULL
);

IMPORT INTO movie_keyword DSV "benchmark/job/data/movie_keyword.csv" ROWS 100;

CREATE TABLE name (
    id INT(4) NOT NULL PRIMARY KEY,
    name CHAR(106) NOT NULL,
    imdb_index CHAR(9),
    imdb_id INT(4),
    gender CHAR(1),
    name_pcode_cf CHAR(5),
    name_pcode_nf CHAR(5),
    surname_pcode CHAR(5),
    md5sum CHAR(32)
);

IMPORT INTO name DSV "benchmark/job/data/name.csv" ROWS 100;

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
-- SELECT k.keyword AS movie_keyword,
--        n.name AS actor_name,
--        t.title AS marvel_movie
SELECT k.keyword, n.name, t.title
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
