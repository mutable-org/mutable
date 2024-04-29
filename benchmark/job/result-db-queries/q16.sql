-- debug: src/backend/WasmOperator.cpp:3943:32: runtime error: 1.25e+12 is outside the range of representable values of type 'unsigned int'
-- src/backend/WasmAlgo.cpp:1316: Wasm_insist failed.  entry out-of-bounds.
CREATE DATABASE job;
USE job;


-- CREATE TABLE AND IMPORT DATA --
CREATE TABLE aka_name (
    id INT(4) NOT NULL PRIMARY KEY,
    person_id INT(4) NOT NULL,
    name CHAR(100),
    imdb_index CHAR(3),
    name_pcode_cf CHAR(11),
    name_pcode_nf CHAR(11),
    surname_pcode CHAR(11),
    md5sum CHAR(65)
);

IMPORT INTO aka_name DSV "benchmark/job/data/aka_name.csv";

CREATE TABLE cast_info (
    id INT(4) NOT NULL PRIMARY KEY,
    person_id INT(4) NOT NULL,
    movie_id INT(4) NOT NULL,
    person_role_id INT(4),
    note CHAR(100),
    nr_order INT(4),
    role_id INT(4) NOT NULL
);

IMPORT INTO cast_info DSV "benchmark/job/data/cast_info.csv";

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

CREATE TABLE movie_keyword (
    id INT(4) NOT NULL PRIMARY KEY,
    movie_id INT(4) NOT NULL,
    keyword_id INT(4) NOT NULL
);

IMPORT INTO movie_keyword DSV "benchmark/job/data/movie_keyword.csv";

CREATE TABLE name (
    id INT(4) NOT NULL PRIMARY KEY,
    name CHAR(100) NOT NULL,
    imdb_index CHAR(9),
    imdb_id INT(4),
    gender CHAR(1),
    name_pcode_cf CHAR(5),
    name_pcode_nf CHAR(5),
    surname_pcode CHAR(5),
    md5sum CHAR(32)
);

IMPORT INTO name DSV "benchmark/job/data/name.csv";

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
SELECT an.name AS cool_actor_pseudonym,
       t.title AS series_named_after_char
FROM aka_name AS an,
     cast_info AS ci,
     company_name AS cn,
     keyword AS k,
     movie_companies AS mc,
     movie_keyword AS mk,
     name AS n,
     title AS t
WHERE cn.country_code = "[us]"
  AND k.keyword = "character-name-in-title"
  AND t.episode_nr >= 50
  AND t.episode_nr < 100
  AND an.person_id = n.id
  AND n.id = ci.person_id
  AND ci.movie_id = t.id
  AND t.id = mk.movie_id
  AND mk.keyword_id = k.id
  AND t.id = mc.movie_id
  AND mc.company_id = cn.id
  AND an.person_id = ci.person_id
  AND ci.movie_id = mc.movie_id
  AND ci.movie_id = mk.movie_id
  AND mc.movie_id = mk.movie_id;
