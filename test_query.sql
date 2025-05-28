CREATE DATABASE job_light;
USE job_light;

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
IMPORT INTO title DSV "benchmark/job-light/data/title.csv" ROWS 10;


CREATE TABLE cast_info (
    id INT(4) NOT NULL PRIMARY KEY,
    person_id INT(4) NOT NULL,
    movie_id INT(4) NOT NULL,
    person_role_id INT(4),
    note CHAR(100),
    nr_order INT(4),
    role_id INT(4) NOT NULL
);

IMPORT INTO cast_info DSV "benchmark/job-light/data/cast_info.csv" ROWS 2;

SELECT COUNT(*) FROM cast_info,title  WHERE title.id=cast_info.movie_id AND title.production_year < 2000 AND title.production_year>1984;
