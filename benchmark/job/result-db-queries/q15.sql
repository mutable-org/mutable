CREATE DATABASE job;
USE job;


-- CREATE TABLE AND IMPORT DATA --
CREATE TABLE aka_title (
    id INT(4) NOT NULL PRIMARY KEY,
    movie_id INT(4) NOT NULL,
    title CHAR(100),
    imdb_index CHAR(4),
    kind_id INT(4) NOT NULL,
    production_year INT(4),
    phonetic_code CHAR(5),
    episode_of_id INT(4),
    season_nr INT(4),
    episode_nr INT(4),
    note CHAR(72),
    md5sum CHAR(32)
);

IMPORT INTO aka_title DSV "benchmark/job/data/aka_title.csv";

CREATE TABLE company_name (
    id INT(4) NOT NULL PRIMARY KEY,
    name CHAR(100) NOT NULL,
    country_code CHAR(6),
    imdb_id INT(4),
    name_pcode_nf CHAR(5),
    name_pcode_sf CHAR(5),
    md5sum CHAR(32)
);

IMPORT INTO company_name DSV "benchmark/job/data/company_name.csv";

CREATE TABLE company_type (
    id INT(4) NOT NULL PRIMARY KEY,
    kind CHAR(32)
);

IMPORT INTO company_type DSV "benchmark/job/data/company_type.csv";

CREATE TABLE info_type (
    id INT(4) NOT NULL PRIMARY KEY,
    info CHAR(32) NOT NULL
);

IMPORT INTO info_type DSV "benchmark/job/data/info_type.csv";

CREATE TABLE keyword (
    id INT(4) NOT NULL PRIMARY KEY,
    keyword CHAR(100) NOT NULL,
    phonetic_code CHAR(5)
);

IMPORT INTO keyword DSV "benchmark/job/data/keyword.csv";

CREATE TABLE movie_companies (
    id INT(4) NOT NULL PRIMARY KEY,
    movie_id INT(4) NOT NULL,
    company_id INT(4) NOT NULL,
    company_type_id INT(4) NOT NULL,
    note CHAR(100)
);

IMPORT INTO movie_companies DSV "benchmark/job/data/movie_companies.csv";

CREATE TABLE movie_info (
    id INT(4) NOT NULL PRIMARY KEY,
    movie_id INT(4) NOT NULL,
    info_type_id INT(4) NOT NULL,
    info CHAR(100) NOT NULL,
    note CHAR(100)
);

IMPORT INTO movie_info DSV "benchmark/job/data/movie_info.csv";

CREATE TABLE movie_keyword (
    id INT(4) NOT NULL PRIMARY KEY,
    movie_id INT(4) NOT NULL,
    keyword_id INT(4) NOT NULL
);

IMPORT INTO movie_keyword DSV "benchmark/job/data/movie_keyword.csv";

CREATE TABLE title (
    id INT(4) NOT NULL PRIMARY KEY,
    title CHAR(100) NOT NULL,
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
SELECT mi.info,
       t.title
FROM aka_title AS at,
     company_name AS cn,
     company_type AS ct,
     info_type AS it1,
     keyword AS k,
     movie_companies AS mc,
     movie_info AS mi,
     movie_keyword AS mk,
     title AS t
WHERE cn.country_code = "[us]"
  AND it1.info = "release dates"
  AND mc.note LIKE "%(200%)%"
  AND mc.note LIKE "%(worldwide)%"
  AND mi.note LIKE "%internet%"
  AND mi.info LIKE "USA:% 200%"
  AND t.production_year > 2000
  AND t.id = at.movie_id
  AND t.id = mi.movie_id
  AND t.id = mk.movie_id
  AND t.id = mc.movie_id
  AND mk.movie_id = mi.movie_id
  AND mk.movie_id = mc.movie_id
  AND mk.movie_id = at.movie_id
  AND mi.movie_id = mc.movie_id
  AND mi.movie_id = at.movie_id
  AND mc.movie_id = at.movie_id
  AND k.id = mk.keyword_id
  AND it1.id = mi.info_type_id
  AND cn.id = mc.company_id
  AND ct.id = mc.company_type_id;
