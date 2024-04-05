-- debug: src/backend/WasmAlgo.hpp:463: Condition 'refs_.count(id) <= 1' failed.  duplicate identifier, lookup ambiguous.
CREATE DATABASE job;
USE job;


-- CREATE TABLE AND IMPORT DATA --
CREATE TABLE aka_name (
    id INT(4) NOT NULL PRIMARY KEY,
    person_id INT(4) NOT NULL,
    name CHAR(218),
    imdb_index CHAR(3),
    name_pcode_cf CHAR(5),
    name_pcode_nf CHAR(5),
    surname_pcode CHAR(5),
    md5sum CHAR(32)
);

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

CREATE TABLE keyword (
    id INT(4) NOT NULL PRIMARY KEY,
    keyword CHAR(74) NOT NULL,
    phonetic_code CHAR(5)
);

IMPORT INTO keyword DSV "benchmark/job/data/keyword.csv" ROWS 100;

CREATE TABLE movie_companies (
    id INT(4) NOT NULL PRIMARY KEY,
    movie_id INT(4) NOT NULL,
    company_id INT(4) NOT NULL,
    company_type_id INT(4) NOT NULL,
    note CHAR(208)
);

IMPORT INTO movie_companies DSV "benchmark/job/data/movie_companies.csv" ROWS 100;

CREATE TABLE name (
    id INT(4) NOT NULL PRIMARY KEY,
    name CHAR(106) NOT NULL,
    imdb_index CHAR(12),
    imdb_id INT(4),
    gender CHAR(1),
    name_pcode_cf CHAR(5),
    name_pcode_nf CHAR(5),
    surname_pcode CHAR(5),
    md5sum CHAR(32)
);

IMPORT INTO name DSV "benchmark/job/data/name.csv" ROWS 100;

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
SELECT an1.name,
       t.title
FROM aka_name AS an1,
     cast_info AS ci,
     company_name AS cn,
     movie_companies AS mc,
     name AS n1,
     role_type AS rt,
     title AS t
WHERE cn.country_code ="[us]"
  AND rt.role ="costume designer"
  AND an1.person_id = n1.id
  AND n1.id = ci.person_id
  AND ci.movie_id = t.id
  AND t.id = mc.movie_id
  AND mc.company_id = cn.id
  AND ci.role_id = rt.id
  AND an1.person_id = ci.person_id
  AND ci.movie_id = mc.movie_id;
