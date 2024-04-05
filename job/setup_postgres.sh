#!/usr/bin/env bash

#psql -d imdb -f imdb-queries/schema.sql

psql -d imdb -c "COPY aka_name FROM '$(pwd)/imdb-datasets/aka_name.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
psql -d imdb -c "COPY aka_title FROM '$(pwd)/imdb-datasets/aka_title.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
psql -d imdb -c "COPY cast_info FROM '$(pwd)/imdb-datasets/cast_info.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
psql -d imdb -c "COPY char_name FROM '$(pwd)/imdb-datasets/char_name.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
psql -d imdb -c "COPY comp_cast_type FROM '$(pwd)/imdb-datasets/comp_cast_type.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
psql -d imdb -c "COPY company_name FROM '$(pwd)/imdb-datasets/company_name.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
psql -d imdb -c "COPY company_type FROM '$(pwd)/imdb-datasets/company_type.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
psql -d imdb -c "COPY complete_cast FROM '$(pwd)/imdb-datasets/complete_cast.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
psql -d imdb -c "COPY info_type FROM '$(pwd)/imdb-datasets/info_type.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
psql -d imdb -c "COPY keyword FROM '$(pwd)/imdb-datasets/keyword.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
psql -d imdb -c "COPY kind_type FROM '$(pwd)/imdb-datasets/kind_type.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
psql -d imdb -c "COPY link_type FROM '$(pwd)/imdb-datasets/link_type.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
psql -d imdb -c "COPY movie_companies FROM '$(pwd)/imdb-datasets/movie_companies.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
psql -d imdb -c "COPY movie_info FROM '$(pwd)/imdb-datasets/movie_info.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
psql -d imdb -c "COPY movie_info_idx FROM '$(pwd)/imdb-datasets/movie_info_idx.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
psql -d imdb -c "COPY movie_keyword FROM '$(pwd)/imdb-datasets/movie_keyword.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
psql -d imdb -c "COPY movie_link FROM '$(pwd)/imdb-datasets/movie_link.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
psql -d imdb -c "COPY name FROM '$(pwd)/imdb-datasets/name.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
psql -d imdb -c "COPY person_info FROM '$(pwd)/imdb-datasets/person_info.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
psql -d imdb -c "COPY role_type FROM '$(pwd)/imdb-datasets/role_type.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
psql -d imdb -c "COPY title FROM '$(pwd)/imdb-datasets/title.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
