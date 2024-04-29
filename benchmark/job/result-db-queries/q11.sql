-- syntax error: IS NULL
CREATE DATABASE job;
USE job;


-- CREATE TABLE AND IMPORT DATA --
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

CREATE TABLE keyword (
    id INT(4) NOT NULL PRIMARY KEY,
    keyword CHAR(100) NOT NULL,
    phonetic_code CHAR(5)
);

IMPORT INTO keyword DSV "benchmark/job/data/keyword.csv";

CREATE TABLE link_type (
    id INT(4) NOT NULL PRIMARY KEY,
    link CHAR(32) NOT NULL
);

IMPORT INTO link_type DSV "benchmark/job/data/link_type.csv";

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

CREATE TABLE movie_link (
    id INT(4) NOT NULL PRIMARY KEY,
    movie_id INT(4) NOT NULL,
    linked_movie_id INT(4) NOT NULL,
    link_type_id INT(4) NOT NULL
);

IMPORT INTO movie_link DSV "benchmark/job/data/movie_link.csv";

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
SELECT cn.name AS from_company,
       lt.link AS movie_link_type,
       t.title AS non_polish_sequel_movie
FROM company_name AS cn,
     company_type AS ct,
     keyword AS k,
     link_type AS lt,
     movie_companies AS mc,
     movie_keyword AS mk,
     movie_link AS ml,
     title AS t
WHERE cn.country_code != "[pl]"
  AND (cn.name LIKE "%Film%"
       OR cn.name LIKE "%Warner%")
  AND ct.kind = "production companies"
  AND k.keyword = "sequel"
  AND lt.link LIKE "%follow%"
  AND mc.note IS NULL
  AND t.production_year >= 1950
  AND t.production_year <= 2000
  AND lt.id = ml.link_type_id
  AND ml.movie_id = t.id
  AND t.id = mk.movie_id
  AND mk.keyword_id = k.id
  AND t.id = mc.movie_id
  AND mc.company_type_id = ct.id
  AND mc.company_id = cn.id
  AND ml.movie_id = mk.movie_id
  AND ml.movie_id = mc.movie_id
  AND mk.movie_id = mc.movie_id;
