CREATE DATABASE job;
USE job;

CREATE TABLE company_type (
    id INT(4) NOT NULL,
    kind CHAR(32) NOT NULL
);
-- 4 rows -> 142 Byte
IMPORT INTO company_type DSV "benchmark/job/data/company_type.csv";

CREATE TABLE info_type (
    id INT(4) NOT NULL,
    info CHAR(32) NOT NULL
);
-- 113 rows -> ~ 4 KiB
IMPORT INTO info_type DSV "benchmark/job/data/info_type.csv";

CREATE TABLE movie_companies (
    id INT(4) NOT NULL,
    movie_id INT(4) NOT NULL,
    company_id INT(4) NOT NULL,
    company_type_id INT(4) NOT NULL,
    note CHAR(208)
);
-- 2.609.129 rows -> ~557 MiB
IMPORT INTO movie_companies DSV "benchmark/job/data/movie_companies.csv" ROWS 100;

CREATE TABLE movie_info (
    id INT(4) NOT NULL,
    movie_id INT(4) NOT NULL,
    info_type_id INT(4) NOT NULL,
    info CHAR(43) NOT NULL,
    note CHAR(19)
);
-- 1047 MiB
IMPORT INTO movie_info DSV "benchmark/job/data/movie_info.csv" ROWS 100;

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
-- 1092 MiB
IMPORT INTO title DSV "benchmark/job/data/title.csv" ROWS 100;

-- SQL QUERY --
-- Note: The query was slightly modified, we use `mi.info = "USA" OR mi.info = "America"` instead of `IN`
SELECT t.title
FROM company_type AS ct,
     info_type AS it,
     movie_companies AS mc,
     movie_info AS mi,
     title AS t
WHERE ct.kind = "production companies"
  AND mc.note LIKE "%(VHS)%"
  AND mc.note LIKE "%(USA)%"
  AND mc.note LIKE "%(1994)%"
  AND (mi.info = "USA" OR
       mi.info = "America")
  AND t.production_year > 2010
  AND t.id = mi.movie_id
  AND t.id = mc.movie_id
  AND mc.movie_id = mi.movie_id
  AND ct.id = mc.company_type_id
  AND it.id = mi.info_type_id;
