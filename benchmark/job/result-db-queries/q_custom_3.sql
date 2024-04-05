-- IMPORT DATA --
IMPORT INTO title DSV "benchmark/job/data/title.csv";
IMPORT INTO movie_companies DSV "benchmark/job/data/movie_companies.csv";
IMPORT INTO company_name DSV "benchmark/job/data/company_name.csv";
IMPORT INTO company_type DSV "benchmark/job/data/company_type.csv";
IMPORT INTO movie_keyword DSV "benchmark/job/data/movie_keyword.csv";
IMPORT INTO keyword DSV "benchmark/job/data/keyword.csv";

-- SQL QUERY --
SELECT
    t.title,
    mc.note
FROM
    title AS t,
    movie_companies AS mc,
    company_name AS cn,
    company_type as ct,
    movie_keyword as mk,
    keyword as k
WHERE t.production_year <= 2005
    AND k.keyword LIKE "%sequel%"
    AND cn.country_code = "[us]"
    AND ct.kind != "miscellaneous companies"
    AND t.id = mc.movie_id
    AND mc.company_id = cn.id
    AND mc.company_type_id = ct.id
    AND t.id = mk.movie_id
    AND mk.keyword_id = k.id;
