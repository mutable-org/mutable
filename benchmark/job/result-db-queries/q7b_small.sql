-- debug: src/backend/WasmAlgo.hpp:463: Condition 'refs_.count(id) <= 1' failed.  duplicate identifier, lookup ambiguous.
CREATE DATABASE job;
USE job;


-- CREATE TABLE AND IMPORT DATA --
CREATE TABLE aka_name (
    id INT(4) NOT NULL,
    person_id INT(4) NOT NULL,
    name CHAR(218) NOT NULL,
    imdb_index CHAR(12),
    name_pcode_cf CHAR(5),
    name_pcode_nf CHAR(5),
    surname_pcode CHAR(5),
    md5sum CHAR(32)
);
-- 901.343 rows -> ~245 MiB
IMPORT INTO aka_name DSV "benchmark/job/data/aka_name.csv" ROWS 100;

CREATE TABLE cast_info (
    id INT(4) NOT NULL PRIMARY KEY,
    person_id INT(4) NOT NULL,
    movie_id INT(4) NOT NULL,
    person_role_id INT(4),
    note CHAR(18), -- 922
    nr_order INT(4),
    role_id INT(4) NOT NULL
);
-- 36.244.344 rows ->  1452 MiB (with note restricted to its average length, i.e. 18 byte)
IMPORT INTO cast_info DSV "benchmark/job/data/cast_info.csv" ROWS 100;

CREATE TABLE info_type (
    id INT(4) NOT NULL,
    info CHAR(32) NOT NULL
);
-- 113 rows -> ~ 4 KiB
IMPORT INTO info_type DSV "benchmark/job/data/info_type.csv";

CREATE TABLE link_type (
    id INT(4) NOT NULL,
    link CHAR(32) NOT NULL
);
-- 18 rows -> 648 Byte
IMPORT INTO link_type DSV "benchmark/job/data/link_type.csv";

CREATE TABLE movie_link (
    id INT(4) NOT NULL,
    movie_id INT(4) NOT NULL,
    linked_movie_id INT(4) NOT NULL,
    link_type_id INT(4) NOT NULL
);
-- 29.997 rows -> ~469 KiB
IMPORT INTO movie_link DSV "benchmark/job/data/movie_link.csv" ROWS 100;

CREATE TABLE name (
    id INT(4) NOT NULL,
    name CHAR(106) NOT NULL,
    imdb_index CHAR(12),
    imdb_id INT(4),
    gender CHAR(1),
    name_pcode_cf CHAR(5),
    name_pcode_nf CHAR(5),
    surname_pcode CHAR(5),
    md5sum CHAR(32)
);
-- 4.157.491 rows -> ~690 MiB
IMPORT INTO name DSV "benchmark/job/data/name.csv" ROWS 100;

CREATE TABLE person_info (
    id INT(4) NOT NULL,
    person_id INT(4) NOT NULL,
    info_type_id INT(4) NOT NULL,
    info CHAR(112) NOT NULL, -- MAX: 55656
    note CHAR(15) -- MAX: 430
);
-- 2.963.664 rows -> ~155 GiB (original), 393 MiB (with info and note restricted to their average length)
IMPORT INTO person_info DSV "benchmark/job/data/person_info.csv" ROWS 100;

CREATE TABLE title (
    id INT(4) NOT NULL,
    title CHAR(334) NOT NULL,
    imdb_index CHAR(12),
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
-- 2.528.312 rows -> ~1092 MiB
IMPORT INTO title DSV "benchmark/job/data/title.csv" ROWS 100;


-- SQL QUERY --
SELECT n.name,
       t.title
FROM aka_name AS an,
     cast_info AS ci,
     info_type AS it,
     link_type AS lt,
     movie_link AS ml,
     name AS n,
     person_info AS pi,
     title AS t
WHERE an.name LIKE "%a%"
  AND it.info = "mini biography"
  AND lt.link = "features"
  AND n.name_pcode_cf LIKE "D%"
  AND n.gender= "m"
  AND pi.note = "Volker Boehm"
  AND t.production_year >= 1980
  AND t.production_year <= 1984
  AND n.id = an.person_id
  AND n.id = pi.person_id
  AND ci.person_id = n.id
  AND t.id = ci.movie_id
  AND ml.linked_movie_id = t.id
  AND lt.id = ml.link_type_id
  AND it.id = pi.info_type_id
  AND pi.person_id = an.person_id
  AND pi.person_id = ci.person_id
  AND an.person_id = ci.person_id
  AND ci.movie_id = ml.linked_movie_id;
