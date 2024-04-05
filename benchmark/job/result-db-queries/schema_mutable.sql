CREATE DATABASE result_db;
USE result_db;

CREATE TABLE aka_name (
    id INT(4) NOT NULL,
    person_id INT(4) NOT NULL,
    name CHAR(218) NOT NULL,
    imdb_index CHAR(12),
    name_pcode_cf CHAR(5),
    name_pcode_nf CHAR(5),
    surname_pcode CHAR(5),
    md5sum CHAR(32)
);
-- 901.343 rows -> ~245 MiB

CREATE TABLE aka_title (
    id INT(4) NOT NULL,
    movie_id INT(4) NOT NULL,
    title CHAR(553) NOT NULL,
    imdb_index CHAR(12),
    kind_id INT(4) NOT NULL,
    production_year INT(4),
    phonetic_code CHAR(5),
    episode_of_id INT(4),
    season_nr INT(4),
    episode_nr INT(4),
    note CHAR(72),
    md5sum CHAR(32)
);
-- 361.472 rows -> ~242 MiB

CREATE TABLE cast_info (
    id INT(4) NOT NULL,
    person_id INT(4) NOT NULL,
    movie_id INT(4) NOT NULL,
    person_role_id INT(4),
    note CHAR(18), -- MAX: 922
    nr_order INT(4),
    role_id INT(4) NOT NULL
);
-- 36.244.344 rows -> 32 GiB (with MAX char size), ~1452 MiB (with note restricted to its average length, i.e. 18 byte)

CREATE TABLE char_name (
    id INT(4) NOT NULL,
    name CHAR(478) NOT NULL,
    imdb_index CHAR(12),
    imdb_id INT(4),
    name_pcode_nf CHAR(5),
    surname_pcode CHAR(5),
    md5sum CHAR(32)
);
-- 3.140.339 rows -> ~1617 MiB

CREATE TABLE comp_cast_type (
    id INT(4) NOT NULL,
    kind CHAR(32) NOT NULL
);
-- 4 rows -> 142 Byte

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

CREATE TABLE company_type (
    id INT(4) NOT NULL,
    kind CHAR(32) NOT NULL
);
-- 4 rows -> 142 Byte

CREATE TABLE complete_cast (
    id INT(4) NOT NULL,
    movie_id INT(4),
    subject_id INT(4) NOT NULL,
    status_id INT(4) NOT NULL
);
-- 135.086 rows -> ~ 2 MiB

CREATE TABLE info_type (
    id INT(4) NOT NULL,
    info CHAR(32) NOT NULL
);
-- 113 rows -> ~ 4 KiB

CREATE TABLE keyword (
    id INT(4) NOT NULL,
    keyword CHAR(74) NOT NULL,
    phonetic_code CHAR(5)
);
-- 135.086 rows -> ~11 MiB

CREATE TABLE kind_type (
    id INT(4) NOT NULL,
    kind CHAR(15) NOT NULL
);
-- 7 rows -> 133 byte

CREATE TABLE link_type (
    id INT(4) NOT NULL,
    link CHAR(32) NOT NULL
);
-- 18 rows -> 648 Byte

CREATE TABLE movie_companies (
    id INT(4) NOT NULL,
    movie_id INT(4) NOT NULL,
    company_id INT(4) NOT NULL,
    company_type_id INT(4) NOT NULL,
    note CHAR(208)
);
-- 2.609.129 rows -> ~557 MiB

CREATE TABLE movie_info (
    id INT(4) NOT NULL,
    movie_id INT(4) NOT NULL,
    info_type_id INT(4) NOT NULL,
    info CHAR(43) NOT NULL, -- MAX: 19793
    note CHAR(19) -- MAX: 387
);
-- 14.835.720 rows -> 280 GiB (with MAX char sizes), 1047 MiB (with info and note restricted to their average size, i.e. 43 and 19 byte)
-- note: queries with filter condidtions on `info` or `note` must still be valid, e.g. an equality check for `note`s
-- with more than 19 characters may yield different results in mutable (with the restricted size) compared to postgres

CREATE TABLE movie_info_idx (
    id INT(4) NOT NULL,
    movie_id INT(4) NOT NULL,
    info_type_id INT(4) NOT NULL,
    info CHAR(10) NOT NULL,
    note CHAR(1)
);
-- 1.380.035 rows -> ~30 MiB

CREATE TABLE movie_keyword (
    id INT(4) NOT NULL,
    movie_id INT(4) NOT NULL,
    keyword_id INT(4) NOT NULL
);
-- 4.523.930 rows -> 52 MiB

CREATE TABLE movie_link (
    id INT(4) NOT NULL,
    movie_id INT(4) NOT NULL,
    linked_movie_id INT(4) NOT NULL,
    link_type_id INT(4) NOT NULL
);
-- 29.997 rows -> ~469 KiB

CREATE TABLE name (
    id INT(4) NOT NULL,
    name CHAR(106) NOT NULL,
    imdb_index CHAR(12),
    imdb_id INT(4),
    gender CHAR(1),
    name_pcode_cf CHAR(5),
    name_pcode_nf CHAR(5),
    surname_pcode CHAR(5),
    md5sum CHAR(32)
);
-- 4.157.491 rows -> ~690 MiB

CREATE TABLE person_info (
    id INT(4) NOT NULL,
    person_id INT(4) NOT NULL,
    info_type_id INT(4) NOT NULL,
    info CHAR(112) NOT NULL, -- MAX: 55656
    note CHAR(15) -- MAX: 430
);
-- 2.963.664 rows -> ~155 GiB (with MAX char size), ~393 MiB (with info and note restricted to their average)

CREATE TABLE role_type (
    id INT(4) NOT NULL,
    role CHAR(32) NOT NULL
);
-- 12 rows -> ~ 432 byte

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
