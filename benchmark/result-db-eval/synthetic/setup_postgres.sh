#!/usr/bin/env bash

if [ $# -eq 0 ]
then
    echo "Supply PostgreSQL username."
    exit 1
fi

USER=$1
DB_NAME="synthetic"

# Drop and create database
psql -U ${USER} -d postgres -c "DROP DATABASE IF EXISTS "${DB_NAME}
psql -U ${USER} -d postgres -c "CREATE DATABASE "${DB_NAME}

# Create tables
psql -U ${USER} -d ${DB_NAME} -f "$(pwd)/benchmark/result-db-eval/synthetic/schema_postgres.sql"

# Import data
psql -U ${USER} -d ${DB_NAME} -c "\copy rel_0 FROM '$(pwd)/benchmark/result-db-eval/synthetic/data/fact.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
psql -U ${USER} -d ${DB_NAME} -c "\copy rel_1 FROM '$(pwd)/benchmark/result-db-eval/synthetic/data/dim.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
psql -U ${USER} -d ${DB_NAME} -c "\copy rel_2 FROM '$(pwd)/benchmark/result-db-eval/synthetic/data/add.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
psql -U ${USER} -d ${DB_NAME} -c "\copy rel_3 FROM '$(pwd)/benchmark/result-db-eval/synthetic/data/add.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
psql -U ${USER} -d ${DB_NAME} -c "\copy rel_4 FROM '$(pwd)/benchmark/result-db-eval/synthetic/data/add.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
psql -U ${USER} -d ${DB_NAME} -c "\copy rel_5 FROM '$(pwd)/benchmark/result-db-eval/synthetic/data/add.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
psql -U ${USER} -d ${DB_NAME} -c "\copy rel_6 FROM '$(pwd)/benchmark/result-db-eval/synthetic/data/add.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
psql -U ${USER} -d ${DB_NAME} -c "\copy rel_7 FROM '$(pwd)/benchmark/result-db-eval/synthetic/data/dim.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";