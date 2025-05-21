#!/bin/bash

# Define flags in an array for easy activation/deactivation
FLAGS=(
    # --help
    # --statistics
    # --echo
    # --graph
    # --benchmark
    # --times
    # --backend WasmV8
    # --data-layout PAX4M
)

# Build the command with the active flags
CMD="build/debug_shared/bin/shell ${FLAGS[*]} -"

# Define the query in a separate variable
QUERY=$(cat <<EOF
CREATE DATABASE job_light;
USE job_light;

CREATE TABLE cast_info (
    id INT(4) NOT NULL PRIMARY KEY,
    person_id INT(4) NOT NULL,
    movie_id INT(4) NOT NULL,
    person_role_id INT(4),
    note CHAR(100),
    nr_order INT(4),
    role_id INT(4) NOT NULL
);

IMPORT INTO cast_info DSV "benchmark/job-light/data/cast_info.csv" ROWS 100;
SELECT id, person_id, movie_id, person_role_id, note, nr_order, role_id FROM cast_info LIMIT 5;

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
IMPORT INTO title DSV "benchmark/job-light/data/title.csv" ROWS 100;
SELECT id, title, kind_id, production_year, imdb_id, phonetic_code, episode_of_id, season_nr, episode_nr, series_years, md5sum FROM title LIMIT 5;

SELECT * FROM title LIMIT 5; -- works (limit)

SELECT * FROM title WHERE production_year > 1900; -- works (all rows)

SELECT * FROM title WHERE production_year > 2100; -- works (no rows)

SELECT * FROM title WHERE production_year > 2000; -- works (some rows)

SELECT COUNT(*) FROM title WHERE production_year > 2000; -- works (count)

SELECT * FROM title WHERE title.production_year > 2000; -- works (adress column more specific)

SELECT * FROM title WHERE production_year < 2000 AND production_year>1984; -- works (multiple conditions)

SELECT * FROM cast_info,title  WHERE title.id=cast_info.movie_id; -- works (full inner join)

SELECT COUNT(*) FROM cast_info,title  WHERE title.id=cast_info.movie_id AND title.production_year < 2000 AND title.production_year>1984; -- works (full JOB query)

EOF
)
# SELECT COUNT(*) FROM cast_info ci,title t WHERE t.id=ci.movie_id; AND t.production_year>1980 AND t.production_year<1984

echo "$CMD"
echo
echo "$QUERY"
echo


# Execute the command with the query
echo "$QUERY" | $CMD