-- IMPORT DATA --
IMPORT INTO company_type DSV "benchmark/job/data/company_type.csv";
IMPORT INTO info_type DSV "benchmark/job/data/info_type.csv";
IMPORT INTO movie_companies DSV "benchmark/job/data/movie_companies.csv";
IMPORT INTO movie_info_idx DSV "benchmark/job/data/movie_info_idx.csv";
IMPORT INTO title DSV "benchmark/job/data/title.csv";

-- SQL QUERY --
SELECT mc.note,
       t.title,
       t.production_year
FROM company_type AS ct,
     info_type AS it,
     movie_companies AS mc,
     movie_info_idx AS mi_idx,
     title AS t
WHERE ct.kind = "production companies"
  AND it.info = "top 250 rank"
  AND NOT (mc.note LIKE "%(as Metro-Goldwyn-Mayer Pictures)%")
  AND (mc.note LIKE "%(co-production)%"
       OR mc.note LIKE "%(presents)%")
  AND ct.id = mc.company_type_id
  AND t.id = mc.movie_id
  AND t.id = mi_idx.movie_id
  AND mc.movie_id = mi_idx.movie_id
  AND it.id = mi_idx.info_type_id;
