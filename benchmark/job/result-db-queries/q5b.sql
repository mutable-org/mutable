-- IMPORT DATA --
IMPORT INTO company_type DSV "benchmark/job/data/company_type.csv";
IMPORT INTO info_type DSV "benchmark/job/data/info_type.csv";
IMPORT INTO movie_companies DSV "benchmark/job/data/movie_companies.csv";
IMPORT INTO movie_info DSV "benchmark/job/data/movie_info.csv";
IMPORT INTO title DSV "benchmark/job/data/title.csv";

-- SQL QUERY --
SELECT t.title
FROM company_type AS ct,
     info_type AS it,
     movie_companies AS mc,
     movie_info AS mi,
     title AS t
WHERE ct.kind = "production companies"
  AND mc.note LIKE "%(VHS)%"
  AND mc.note LIKE "%(USA)%"
  AND mc.note LIKE "%(1994)%"
  AND (mi.info = "USA" OR
       mi.info = "America")
  AND t.production_year > 2010
  AND t.id = mi.movie_id
  AND t.id = mc.movie_id
  AND mc.movie_id = mi.movie_id
  AND ct.id = mc.company_type_id
  AND it.id = mi.info_type_id;
