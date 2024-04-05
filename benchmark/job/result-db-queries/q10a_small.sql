-- debug: src/backend/WasmAlgo.hpp:463: Condition 'refs_.count(id) <= 1' failed.  duplicate identifier, lookup ambiguous.
CREATE DATABASE job;
USE job;


-- CREATE TABLE AND IMPORT DATA --
CREATE TABLE char_name (
    id INT(4) NOT NULL PRIMARY KEY,
    name CHAR(16) NOT NULL, -- 478
    imdb_index CHAR(12),
    imdb_id INT(4),
    name_pcode_nf CHAR(5),
    surname_pcode CHAR(5),
    md5sum CHAR(32)
);

IMPORT INTO char_name DSV "benchmark/job/data/char_name.csv" ROWS 100;

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

CREATE TABLE company_name (
    id INT(4) NOT NULL PRIMARY KEY,
    name CHAR(169) NOT NULL,
    country_code CHAR(6),
    imdb_id INT(4),
    name_pcode_nf CHAR(5),
    name_pcode_sf CHAR(5),
    md5sum CHAR(32)
);

IMPORT INTO company_name DSV "benchmark/job/data/company_name.csv" ROWS 100;

CREATE TABLE company_type (
    id INT(4) NOT NULL PRIMARY KEY,
    kind CHAR(32)
);

IMPORT INTO company_type DSV "benchmark/job/data/company_type.csv";

CREATE TABLE movie_companies (
    id INT(4) NOT NULL PRIMARY KEY,
    movie_id INT(4) NOT NULL,
    company_id INT(4) NOT NULL,
    company_type_id INT(4) NOT NULL,
    note CHAR(208) -- AVG: 24
);

IMPORT INTO movie_companies DSV "benchmark/job/data/movie_companies.csv" ROWS 100;

CREATE TABLE role_type (
    id INT(4) NOT NULL PRIMARY KEY,
    role CHAR(32) NOT NULL
);

IMPORT INTO role_type DSV "benchmark/job/data/role_type.csv";

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
SELECT chn.name,
       t.title
FROM char_name AS chn,
     cast_info AS ci,
     company_name AS cn,
     company_type AS ct,
     movie_companies AS mc,
     role_type AS rt,
     title AS t
WHERE ci.note LIKE "%(voice)%"
  AND ci.note LIKE "%(uncredited)%"
  AND cn.country_code = "[ru]"
  AND rt.role = "actor"
  AND t.production_year > 2005
  AND t.id = mc.movie_id
  AND t.id = ci.movie_id
  AND ci.movie_id = mc.movie_id
  AND chn.id = ci.person_role_id
  AND rt.id = ci.role_id
  AND cn.id = mc.company_id
  AND ct.id = mc.company_type_id;
