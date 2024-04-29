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

IMPORT INTO aka_name DSV "benchmark/job/data/aka_name.csv";

CREATE TABLE cast_info (
    id INT(4) NOT NULL PRIMARY KEY,
    person_id INT(4) NOT NULL,
    movie_id INT(4) NOT NULL,
    person_role_id INT(4),
    note CHAR(18), -- 922
    nr_order INT(4),
    role_id INT(4) NOT NULL
);

IMPORT INTO cast_info DSV "benchmark/job/data/cast_info.csv";

CREATE TABLE company_name (
    id INT(4) NOT NULL PRIMARY KEY,
    name CHAR(169) NOT NULL,
    country_code CHAR(6),
    imdb_id INT(4),
    name_pcode_nf CHAR(5),
    name_pcode_sf CHAR(5),
    md5sum CHAR(32)
);

IMPORT INTO company_name DSV "benchmark/job/data/company_name.csv";

CREATE TABLE keyword (
    id INT(4) NOT NULL PRIMARY KEY,
    keyword CHAR(74) NOT NULL,
    phonetic_code CHAR(5)
);

IMPORT INTO keyword DSV "benchmark/job/data/keyword.csv";

CREATE TABLE movie_companies (
    id INT(4) NOT NULL PRIMARY KEY,
    movie_id INT(4) NOT NULL,
    company_id INT(4) NOT NULL,
    company_type_id INT(4) NOT NULL,
    note CHAR(208)
);

IMPORT INTO movie_companies DSV "benchmark/job/data/movie_companies.csv";

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

IMPORT INTO name DSV "benchmark/job/data/name.csv";

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

IMPORT INTO title DSV "benchmark/job-sample/data/title.csv";


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
WHERE ci.note = "(voice: English version)"
  AND cn.country_code = "[jp]"
  AND mc.note LIKE "%(Japan)%"
  AND NOT (mc.note LIKE "%(USA)%")
  AND n1.name LIKE "%Yo%"
  AND NOT (n1.name LIKE "%Yu%")
  AND rt.role = "actress"
  AND an1.person_id = n1.id
  AND n1.id = ci.person_id
  AND ci.movie_id = t.id
  AND t.id = mc.movie_id
  AND mc.company_id = cn.id
  AND ci.role_id = rt.id
  AND an1.person_id = ci.person_id
  AND ci.movie_id = mc.movie_id;
