#!/usr/bin/env bash

if [ $# -eq 0 ]
then
    echo "Supply PostgreSQL username."
    exit 1
fi

USER=$1
DB_NAME="star"

# Drop and create database
psql -U ${USER} -d postgres -c "DROP DATABASE IF EXISTS "${DB_NAME}
psql -U ${USER} -d postgres -c "CREATE DATABASE "${DB_NAME}

# Create tables
psql -U ${USER} -d ${DB_NAME} -f "$(pwd)/benchmark/job/result-db-queries/star-schema/schema_postgres.sql"

# Import data
psql -U ${USER} -d ${DB_NAME} -c "\copy dim1 FROM '$(pwd)/benchmark/job/result-db-queries/star-schema/dim1.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
psql -U ${USER} -d ${DB_NAME} -c "\copy dim2 FROM '$(pwd)/benchmark/job/result-db-queries/star-schema/dim2.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
psql -U ${USER} -d ${DB_NAME} -c "\copy dim3 FROM '$(pwd)/benchmark/job/result-db-queries/star-schema/dim3.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
psql -U ${USER} -d ${DB_NAME} -c "\copy dim4 FROM '$(pwd)/benchmark/job/result-db-queries/star-schema/dim4.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
psql -U ${USER} -d ${DB_NAME} -c "\copy fact FROM '$(pwd)/benchmark/job/result-db-queries/star-schema/fact.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\\')";
