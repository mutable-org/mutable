CREATE DATABASE job;
USE job;

CREATE TABLE company_name (
    id INT(4) NOT NULL,
    name CHAR(169) NOT NULL,
    country_code CHAR(6),
    imdb_id INT(4),
    name_pcode_nf CHAR(5),
    name_pcode_sf CHAR(5),
    md5sum CHAR(32)
);
-- 234.997 rows -> 50 MiB
IMPORT INTO company_name DSV "benchmark/job/data/company_name.csv";

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
IMPORT INTO movie_companies DSV "benchmark/job/data/movie_companies.csv";

CREATE TABLE movie_info (
    id INT(4) NOT NULL,
    movie_id INT(4) NOT NULL,
    info_type_id INT(4) NOT NULL,
    info CHAR(43) NOT NULL,
    note CHAR(19)
);
-- 1047 MiB
IMPORT INTO movie_info DSV "benchmark/job/data/movie_info.csv";

CREATE TABLE movie_info_idx (
    id INT(4) NOT NULL PRIMARY KEY,
    movie_id INT(4) NOT NULL,
    info_type_id INT(4) NOT NULL,
    info CHAR(10) NOT NULL,
    note CHAR(1)
);
-- 1.380.035 rows -> ~30 MiB
IMPORT INTO movie_info_idx DSV "benchmark/job/data/movie_info_idx.csv";

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
IMPORT INTO title DSV "benchmark/job/data/title.csv";

-- SQL QUERY --
SELECT cn.name, mi_idx.info, t.title
FROM company_name AS cn,
     company_type AS ct,
     info_type AS it1,
     info_type AS it2,
     movie_companies AS mc,
     movie_info AS mi,
     movie_info_idx AS mi_idx,
     title AS t
WHERE cn.country_code = "[us]"
  AND ct.kind = "production companies"
  AND it1.info = "genres"
  AND it2.info = "rating"
  AND (mi.info = "Drama" OR
       mi.info = "Horror")
  AND mi_idx.info > "8.0"
  AND t.production_year >= 2005
  AND t.production_year <= 2008
  AND t.id = mi.movie_id
  AND t.id = mi_idx.movie_id
  AND mi.info_type_id = it1.id
  AND mi_idx.info_type_id = it2.id
  AND t.id = mc.movie_id
  AND ct.id = mc.company_type_id
  AND cn.id = mc.company_id
  AND mc.movie_id = mi.movie_id
  AND mc.movie_id = mi_idx.movie_id
  AND mi.movie_id = mi_idx.movie_id;
