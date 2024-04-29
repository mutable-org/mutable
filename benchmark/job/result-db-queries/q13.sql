CREATE DATABASE job;
USE job;


-- CREATE TABLE AND IMPORT DATA --
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

CREATE TABLE kind_type (
    id INT(4) NOT NULL PRIMARY KEY,
    kind CHAR(15)
);

IMPORT INTO kind_type DSV "benchmark/job/data/kind_type.csv";

CREATE TABLE movie_companies (
    id INT(4) NOT NULL PRIMARY KEY,
    movie_id INT(4) NOT NULL,
    company_id INT(4) NOT NULL,
    company_type_id INT(4) NOT NULL,
    note CHAR(208)
);

IMPORT INTO movie_companies DSV "benchmark/job/data/movie_companies.csv";

CREATE TABLE movie_info (
    id INT(4) NOT NULL PRIMARY KEY,
    movie_id INT(4) NOT NULL,
    info_type_id INT(4) NOT NULL,
    info CHAR(43) NOT NULL,
    note CHAR(19)
);

IMPORT INTO movie_info DSV "benchmark/job/data/movie_info.csv";

CREATE TABLE movie_info_idx (
    id INT(4) NOT NULL PRIMARY KEY,
    movie_id INT(4) NOT NULL,
    info_type_id INT(4) NOT NULL,
    info CHAR(10) NOT NULL,
    note CHAR(1)
);

IMPORT INTO movie_info_idx DSV "benchmark/job/data/movie_info_idx.csv";

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
-- ~size of relations: 2.8 GiB
SELECT mi.info,
       miidx.info,
       t.title
FROM company_name AS cn,
     company_type AS ct,
     info_type AS it,
     info_type AS it2,
     kind_type AS kt,
     movie_companies AS mc,
     movie_info AS mi,
     movie_info_idx AS miidx,
     title AS t
WHERE cn.country_code = "[de]"
  AND ct.kind = "production companies"
  AND it.info = "rating"
  AND it2.info = "release dates"
  AND kt.kind = "movie"
  AND mi.movie_id = t.id
  AND it2.id = mi.info_type_id
  AND kt.id = t.kind_id
  AND mc.movie_id = t.id
  AND cn.id = mc.company_id
  AND ct.id = mc.company_type_id
  AND miidx.movie_id = t.id
  AND it.id = miidx.info_type_id
  AND mi.movie_id = miidx.movie_id
  AND mi.movie_id = mc.movie_id
  AND miidx.movie_id = mc.movie_id;
