#!/usr/bin/env bash

if [ $# -eq 0 ]
then
    echo "Supply PostgreSQL username."
    exit 1
fi

USER=$1
DB_NAME="imdb"

# Drop and create database
psql -U ${USER} -c "DROP DATABASE IF EXISTS "${DB_NAME}
psql -U ${USER} -c "CREATE DATABASE "${DB_NAME}

# Create tables
psql -U ${USER} -d ${DB_NAME} -f "$(pwd)/benchmark/job/result-db-queries/postgres/schema_postgres.sql"

# Import data
psql -U ${USER} -d ${DB_NAME} -c "\copy aka_name FROM '$(pwd)/benchmark/job/data/aka_name.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
psql -U ${USER} -d ${DB_NAME} -c "\copy aka_title FROM '$(pwd)/benchmark/job/data/aka_title.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
psql -U ${USER} -d ${DB_NAME} -c "\copy cast_info FROM '$(pwd)/benchmark/job/data/cast_info.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
psql -U ${USER} -d ${DB_NAME} -c "\copy char_name FROM '$(pwd)/benchmark/job/data/char_name.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
psql -U ${USER} -d ${DB_NAME} -c "\copy comp_cast_type FROM '$(pwd)/benchmark/job/data/comp_cast_type.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
psql -U ${USER} -d ${DB_NAME} -c "\copy company_name FROM '$(pwd)/benchmark/job/data/company_name.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
psql -U ${USER} -d ${DB_NAME} -c "\copy company_type FROM '$(pwd)/benchmark/job/data/company_type.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
psql -U ${USER} -d ${DB_NAME} -c "\copy complete_cast FROM '$(pwd)/benchmark/job/data/complete_cast.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
psql -U ${USER} -d ${DB_NAME} -c "\copy info_type FROM '$(pwd)/benchmark/job/data/info_type.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
psql -U ${USER} -d ${DB_NAME} -c "\copy keyword FROM '$(pwd)/benchmark/job/data/keyword.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
psql -U ${USER} -d ${DB_NAME} -c "\copy kind_type FROM '$(pwd)/benchmark/job/data/kind_type.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
psql -U ${USER} -d ${DB_NAME} -c "\copy link_type FROM '$(pwd)/benchmark/job/data/link_type.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
psql -U ${USER} -d ${DB_NAME} -c "\copy movie_companies FROM '$(pwd)/benchmark/job/data/movie_companies.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
psql -U ${USER} -d ${DB_NAME} -c "\copy movie_info FROM '$(pwd)/benchmark/job/data/movie_info.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
psql -U ${USER} -d ${DB_NAME} -c "\copy movie_info_idx FROM '$(pwd)/benchmark/job/data/movie_info_idx.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
psql -U ${USER} -d ${DB_NAME} -c "\copy movie_keyword FROM '$(pwd)/benchmark/job/data/movie_keyword.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
psql -U ${USER} -d ${DB_NAME} -c "\copy movie_link FROM '$(pwd)/benchmark/job/data/movie_link.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
psql -U ${USER} -d ${DB_NAME} -c "\copy name FROM '$(pwd)/benchmark/job/data/name.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
psql -U ${USER} -d ${DB_NAME} -c "\copy person_info FROM '$(pwd)/benchmark/job/data/person_info.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
psql -U ${USER} -d ${DB_NAME} -c "\copy role_type FROM '$(pwd)/benchmark/job/data/role_type.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
psql -U ${USER} -d ${DB_NAME} -c "\copy title FROM '$(pwd)/benchmark/job/data/title.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";

# Create indexes
psql -U ${USER} -d ${DB_NAME} -f "$(pwd)/benchmark/job/result-db-queries/postgres/fkindexes.sql"
