-- IMPORT DATA --
IMPORT INTO char_name DSV "benchmark/job/data/char_name.csv";
IMPORT INTO cast_info DSV "benchmark/job/data/cast_info.csv";
IMPORT INTO company_name DSV "benchmark/job/data/company_name.csv";
IMPORT INTO company_type DSV "benchmark/job/data/company_type.csv";
IMPORT INTO movie_companies DSV "benchmark/job/data/movie_companies.csv";
IMPORT INTO role_type DSV "benchmark/job/data/role_type.csv";
IMPORT INTO title DSV "benchmark/job/data/title.csv";

-- SQL QUERY --
SELECT chn.name,
       t.title
FROM char_name AS chn,
     cast_info AS ci,
     company_name AS cn,
     company_type AS ct,
     movie_companies AS mc,
     role_type AS rt,
     title AS t
WHERE ci.note LIKE "%(voice)%"
  AND ci.note LIKE "%(uncredited)%"
  AND cn.country_code = "[ru]"
  AND rt.role = "actor"
  AND t.production_year > 2005
  AND t.id = mc.movie_id
  AND t.id = ci.movie_id
  AND ci.movie_id = mc.movie_id
  AND chn.id = ci.person_role_id
  AND rt.id = ci.role_id
  AND cn.id = mc.company_id
  AND ct.id = mc.company_type_id;
