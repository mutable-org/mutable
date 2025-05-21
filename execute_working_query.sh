#!/bin/bash

# Define flags in an array for easy activation/deactivation
FLAGS=(
    # --help
    # --statistics
    --echo
    # --graph
    # --benchmark
    # --times
    # --backend WasmV8
    # --data-layout PAX4M
)

# Build the command with the active flags
CMD="build/debug_shared/bin/shell ${FLAGS[*]} -"

# Define the query in a separate variable
# Define queries in separate variables
QUERY1=$(cat <<EOF
CREATE DATABASE job_light;
USE job_light;
EOF
)

QUERY2=$(cat <<EOF
CREATE TABLE movie_companies (
    id INT(4) NOT NULL PRIMARY KEY,
    movie_id INT(4) NOT NULL,
    company_id INT(4) NOT NULL,
    company_type_id INT(4) NOT NULL,
    note CHAR(100)
);
IMPORT INTO movie_companies DSV "benchmark/job-light/data/movie_companies.csv" ROWS 1000;
SELECT COUNT(*) FROM movie_companies;
EOF
)

QUERY3=$(cat <<EOF
CREATE TABLE cast_info (
    id INT(4) NOT NULL PRIMARY KEY,
    person_id INT(4) NOT NULL,
    movie_id INT(4) NOT NULL,
    person_role_id INT(4),
    note CHAR(100),
    nr_order INT(4),
    role_id INT(4) NOT NULL
);
IMPORT INTO cast_info DSV "benchmark/job-light/data/cast_info.csv" ROWS 1000;
SELECT COUNT(*) FROM cast_info LIMIT 5;
EOF
)

QUERY4=$(cat <<EOF
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
IMPORT INTO title DSV "benchmark/job-light/data/title.csv" ROWS 1000;
SELECT COUNT(*) FROM title;
EOF
)

TEST_QUERIES=$(cat <<EOF
SELECT * FROM title LIMIT 5;
SELECT * FROM title WHERE production_year > 1900;
SELECT * FROM title WHERE production_year > 2100;
SELECT * FROM title WHERE production_year > 2000;
SELECT COUNT(*) FROM title WHERE production_year > 2000;
SELECT * FROM title WHERE title.production_year > 2000;
SELECT * FROM title WHERE production_year < 2000 AND production_year>1984;
SELECT * FROM cast_info,title  WHERE title.id=cast_info.movie_id;
SELECT COUNT(*) FROM cast_info,title  WHERE title.id=cast_info.movie_id AND title.production_year < 2000 AND title.production_year>1984;
EOF
)


QUERY6=$(cat <<EOF
SELECT COUNT(*) FROM cast_info, title, movie_companies WHERE title.id=cast_info.movie_id AND title.id=movie_companies.movie_id;
EOF
)


ALL_QUERIES="$QUERY1
$QUERY2
$QUERY3
$QUERY4
$QUERY6
"

echo "$ALL_QUERIES" > /tmp/all_queries.sql
$CMD < /tmp/all_queries.sql

# Execute the command with the query
# echo "$ALL_QUERIES" | $CMD