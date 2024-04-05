-- SQL queries to compute the length of `text` attributes in the join-order-benchmark schema. We use this information to
-- specify the VARCHAR length when declaring the mutable schema since it does not support an unlimited variable length
-- data type like `TEXT` in PostgresQL for example.

SELECT MAX(length(name))
FROM aka_name;

SELECT MAX(length(title))
FROM aka_title;

SELECT MAX(length(note))
FROM aka_title;

SELECT MAX(length(note))
FROM cast_info;

SELECT MAX(length(name))
FROM char_name;

SELECT MAX(length(name))
FROM company_name;

SELECT MAX(length(keyword))
FROM keyword;

SELECT MAX(length(note))
FROM movie_companies;

SELECT MAX(length(info))
FROM movie_info;

SELECT MAX(length(note))
FROM movie_info;

SELECT MAX(length(info))
FROM movie_info_idx;

SELECT MAX(length(note))
FROM movie_info_idx;

SELECT MAX(length(name))
FROM name;

SELECT MAX(length(info))
FROM person_info;

SELECT MAX(length(note))
FROM person_info;

SELECT MAX(length(title))
FROM title;
