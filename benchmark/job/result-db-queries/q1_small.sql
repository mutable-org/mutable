-- debug: src/backend/WasmAlgo.cpp:2342: Wasm_insist failed.
CREATE DATABASE job;
USE job;


-- CREATE TABLE AND IMPORT DATA --
CREATE TABLE company_type (
    id INT(4) NOT NULL PRIMARY KEY,
    kind CHAR(32)
);
-- 4 rows
IMPORT INTO company_type DSV "benchmark/job/data/company_type.csv";

CREATE TABLE info_type (
    id INT(4) NOT NULL PRIMARY KEY,
    info CHAR(32) NOT NULL
);
-- 113 rows
IMPORT INTO info_type DSV "benchmark/job/data/info_type.csv" ROWS 100;

CREATE TABLE movie_companies (
    id INT(4) NOT NULL PRIMARY KEY,
    movie_id INT(4) NOT NULL,
    company_id INT(4) NOT NULL,
    company_type_id INT(4) NOT NULL,
    note CHAR(208)
);
-- 2.609.129 rows -> ~557 MiB
IMPORT INTO movie_companies DSV "benchmark/job/data/movie_companies.csv" ROWS 100;

CREATE TABLE movie_info_idx (
    id INT(4) NOT NULL PRIMARY KEY,
    movie_id INT(4) NOT NULL,
    info_type_id INT(4) NOT NULL,
    info CHAR(10) NOT NULL,
    note CHAR(1)
);
-- 1.380.035 rows -> ~30 MiB
IMPORT INTO movie_info_idx DSV "benchmark/job/data/movie_info_idx.csv" ROWS 100;

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
-- 2.528.312 rows -> ~1092 MiB
IMPORT INTO title DSV "benchmark/job/data/title.csv" ROWS 100;


-- SQL QUERY --
SELECT mc.note, t.title, t.production_year
FROM company_type AS ct,
     info_type AS it,
     movie_companies AS mc,
     movie_info_idx AS mi_idx,
     title AS t
WHERE ct.kind = "production companies"
  AND it.info = "top 250 rank"
  AND NOT (mc.note LIKE "%(as Metro-Goldwyn-Mayer Pictures)%")
  AND (mc.note LIKE "%(co-production)%"
       OR mc.note LIKE "%(presents)%")
  AND ct.id = mc.company_type_id
  AND t.id = mc.movie_id
  AND t.id = mi_idx.movie_id
  AND mc.movie_id = mi_idx.movie_id
  AND it.id = mi_idx.info_type_id;
