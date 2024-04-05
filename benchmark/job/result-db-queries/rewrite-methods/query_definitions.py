from query_utility import Relation, Join, JoinGraph


########################################################################################################################
# JOB Queries
########################################################################################################################
def create_q1a():
    ### Relations
    company_type = Relation("company_type", "ct", ["kind", "id"], ["ct.kind = 'production companies'"])
    info_type = Relation("info_type", "it", ["info", "id"], ["it.info = 'top 250 rank'"])
    movie_companies = Relation("movie_companies", "mc", ["company_type_id", "movie_id", "note"],
                               ["NOT (mc.note LIKE '%(as Metro-Goldwyn-Mayer Pictures)%')",
                                "(mc.note like '%(co-production)%' OR mc.note LIKE '%(presents)%')"], ["note"])
    movie_info_idx = Relation("movie_info_idx", "mi_idx", ["movie_id", "info_type_id"])
    title = Relation("title", "t", ["id", "title", "production_year"], projections=["title", "production_year"])
    relations = [company_type, info_type, movie_companies, movie_info_idx, title]

    ### Joins
    j01 = Join(company_type, movie_companies, ["id"], ["company_type_id"])
    j02 = Join(title, movie_companies, ["id"], ["movie_id"])
    j03 = Join(title, movie_info_idx, ["id"], ["movie_id"])
    j04 = Join(movie_companies, movie_info_idx, ["movie_id"], ["movie_id"])
    j05 = Join(info_type, movie_info_idx, ["id"], ["info_type_id"])
    joins = [j01, j02, j03, j04, j05]

    return JoinGraph(relations, joins)

def create_q2a():
    ### Relations
    company_name = Relation("company_name", "cn", ["country_code", "id"], ["cn.country_code = '[de]'"])
    keyword = Relation("keyword", "k", ["keyword", "id"], ["k.keyword = 'character-name-in-title'"])
    movie_companies = Relation("movie_companies", "mc", ["company_id", "movie_id"])
    movie_keyword = Relation("movie_keyword", "mk", ["movie_id", "keyword_id"])
    title = Relation("title", "t", ["id", "title"], projections=["title"])
    relations = [company_name, keyword, movie_companies, movie_keyword, title]

    ### Joins
    j01 = Join(company_name, movie_companies, ["id"], ["company_id"])
    j02 = Join(movie_companies, title, ["movie_id"], ["id"])
    j03 = Join(title, movie_keyword, ["id"], ["movie_id"])
    j04 = Join(movie_keyword, keyword, ["keyword_id"], ["id"])
    j05 = Join(movie_companies, movie_keyword, ["movie_id"], ["movie_id"])
    joins = [j01, j02, j03, j04, j05]

    return JoinGraph(relations, joins)

def create_q2d():
    ### Relations
    company_name = Relation("company_name", "cn", ["country_code", "id"], ["cn.country_code = '[us]'"])
    keyword = Relation("keyword", "k", ["keyword", "id"], ["k.keyword = 'character-name-in-title'"])
    movie_companies = Relation("movie_companies", "mc", ["company_id", "movie_id"])
    movie_keyword = Relation("movie_keyword", "mk", ["movie_id", "keyword_id"])
    title = Relation("title", "t", ["id", "title"], projections=["title"])
    relations = [company_name, keyword, movie_companies, movie_keyword, title]

    ### Joins
    j01 = Join(company_name, movie_companies, ["id"], ["company_id"])
    j02 = Join(movie_companies, title, ["movie_id"], ["id"])
    j03 = Join(title, movie_keyword, ["id"], ["movie_id"])
    j04 = Join(movie_keyword, keyword, ["keyword_id"], ["id"])
    j05 = Join(movie_companies, movie_keyword, ["movie_id"], ["movie_id"])
    joins = [j01, j02, j03, j04, j05]

    return JoinGraph(relations, joins)

def create_q3b():
    ### Relations
    keyword = Relation("keyword", "k", ["keyword", "id"], ["k.keyword LIKE '%sequel%'"])
    movie_info = Relation("movie_info", "mi", ["info", "movie_id"], ["mi.info = 'Bulgaria'"])
    movie_keyword = Relation("movie_keyword", "mk", ["movie_id", "keyword_id"])
    title = Relation("title", "t", ["id", "title", "production_year"], ["t.production_year > 2010"], ["title"])
    relations = [keyword, movie_info, movie_keyword, title]

    ### Joins
    j01 = Join(title, movie_info, ["id"], ["movie_id"])
    j02 = Join(title, movie_keyword, ["id"], ["movie_id"])
    j03 = Join(movie_keyword, movie_info, ["movie_id"], ["movie_id"])
    j04 = Join(keyword, movie_keyword, ["id"], ["keyword_id"])
    joins = [j01, j02, j03, j04]

    return JoinGraph(relations, joins)

def create_q4a():
    ### Relations
    info_type = Relation("info_type", "it", ["info", "id"], ["it.info = 'rating'"])
    keyword = Relation("keyword", "k", ["keyword", "id"], ["k.keyword LIKE '%sequel%'"])
    movie_info_idx = Relation("movie_info_idx", "mi_idx", ["info", "movie_id", "info_type_id"], ["mi_idx.info > '5.0'"],
                              ["info"])
    movie_keyword = Relation("movie_keyword", "mk", ["movie_id", "keyword_id"])
    title = Relation("title", "t", ["id", "title", "production_year"], ["t.production_year > 2005"], ["title"])
    relations = [info_type, keyword, movie_info_idx, movie_keyword, title]

    ### Joins
    j01 = Join(title, movie_info_idx, ["id"], ["movie_id"])
    j02 = Join(title, movie_keyword, ["id"], ["movie_id"])
    j03 = Join(movie_keyword, movie_info_idx, ["movie_id"], ["movie_id"])
    j04 = Join(keyword, movie_keyword, ["id"], ["keyword_id"])
    j05 = Join(info_type, movie_info_idx, ["id"], ["info_type_id"])
    joins = [j01, j02, j03, j04, j05]

    return JoinGraph(relations, joins)

def create_q5b():
    ### Relations
    company_type = Relation("company_type", "ct", ["kind", "id"], ["ct.kind = 'production companies'"])
    info_type = Relation("info_type", "it", ["id"])
    movie_companies = Relation("movie_companies", "mc", ["company_type_id", "movie_id", "note"],
                               filters=["mc.note LIKE '%(VSHS)%'", "mc.note LIKE '%(USA)%'", "mc.note LIKE '%(1994)%'"])
    movie_info = Relation("movie_info", "mi", ["info", "movie_id", "info_type_id"],
                          filters=["(mi.info = 'USA' OR mi.info = 'America')"])
    title = Relation("title", "t", ["id", "title", "production_year"], ["t.production_year > 2010"], ["title"])
    relations = [company_type, info_type, movie_companies, movie_info, title]

    ### Joins
    j01 = Join(title, movie_info, ["id"], ["movie_id"])
    j02 = Join(title, movie_companies, ["id"], ["movie_id"])
    j03 = Join(movie_companies, movie_info, ["movie_id"], ["movie_id"])
    j04 = Join(company_type, movie_companies, ["id"], ["company_type_id"])
    j05 = Join(info_type, movie_info, ["id"], ["info_type_id"])
    joins = [j01, j02, j03, j04, j05]

    return JoinGraph(relations, joins)

def create_q6a():
    ### Relations
    cast_info = Relation("cast_info", "ci", ["person_id", "movie_id"])
    keyword = Relation("keyword", "k", ["keyword", "id"], filters=["k.keyword = 'marvel-cinematic-universe'"],
                       projections=["keyword"])
    movie_keyword = Relation("movie_keyword", "mk", ["movie_id", "keyword_id"])
    name = Relation("name", "n", ["id", "name"], filters=["n.name LIKE '%Downey%Robert%'"], projections=["name"])
    title = Relation("title", "t", ["id", "title", "production_year"], filters=["t.production_year > 2010"],
                     projections=["title"])
    relations = [cast_info, keyword, movie_keyword, name, title]

    ### Joins
    j01 = Join(keyword, movie_keyword, ["id"], ["keyword_id"])
    j02 = Join(title, movie_keyword, ["id"], ["movie_id"])
    j03 = Join(title, cast_info, ["id"], ["movie_id"])
    j04 = Join(cast_info, movie_keyword, ["movie_id"], ["movie_id"])
    j05 = Join(name, cast_info, ["id"], ["person_id"])
    joins = [j01, j02, j03, j04, j05]

    return JoinGraph(relations, joins)

def create_q7b():
    ### Relations
    aka_name = Relation("aka_name", "an", ["name", "person_id"], filters=["an.name LIKE '%a%'"])
    cast_info = Relation("cast_info", "ci", ["person_id", "movie_id"])
    info_type = Relation("info_type", "it", ["info", "id"], filters=["it.info = 'mini biography'"])
    link_type = Relation("link_type", "lt", ["id", "link"], filters=["lt.link = 'features'"])
    movie_link = Relation("movie_link", "ml", ["linked_movie_id", "link_type_id"])
    name = Relation("name", "n", ["id", "gender", "name_pcode_cf"],
                    filters=["n.name_pcode_cf LIKE 'D%'", "n.gender = 'm'"],
                    projections=["name"])
    person_info = Relation("person_info", "pi", ["person_id", "note", "info_type_id"],
                           filters=["pi.note = 'Volker Boehm'"])
    title = Relation("title", "t", ["id", "title"], filters=["t.production_year >= 1980", "t.production_year <= 1984"],
                     projections=["title"])

    relations = [aka_name, cast_info, info_type, link_type, movie_link, name, person_info, title]

    ### Joins
    j01 = Join(name, aka_name, ["id"], ["person_id"])
    j02 = Join(name, person_info, ["id"], ["person_id"])
    j03 = Join(cast_info, name, ["person_id"], ["id"])
    j04 = Join(title, cast_info, ["id"], ["movie_id"])
    j05 = Join(movie_link, title, ["linked_movie_id"], ["id"])
    j06 = Join(link_type, movie_link, ["id"], ["link_type_id"])
    j07 = Join(info_type, person_info, ["id"], ["info_type_id"])
    j08 = Join(person_info, aka_name, ["person_id"], ["person_id"])
    j09 = Join(person_info, cast_info, ["person_id"], ["person_id"])
    j10 = Join(aka_name, cast_info, ["person_id"], ["person_id"])
    j11 = Join(cast_info, movie_link, ["movie_id"], ["linked_movie_id"])
    joins = [j01, j02, j03, j04, j05, j06, j07, j08, j09, j10, j11]

    return JoinGraph(relations, joins)

def create_q8d():
    ### Relations
    aka_name = Relation("aka_name", "an1", ["name", "person_id"], projections=["name"])
    cast_info = Relation("cast_info", "ci", ["person_id", "movie_id", "role_id"])
    company_name = Relation("company_name", "cn", ["country_code", "id"], filters=["cn.country_code = '[us]'"])
    movie_companies = Relation("movie_companies", "mc", ["company_id", "movie_id"])
    name = Relation("name", "n1", ["id"])
    role_type = Relation("role_type", "rt", ["role", "id"], filters=["rt.role = 'costume designer'"])
    title = Relation("title", "t", ["id", "title"], projections=["title"])
    relations = [aka_name, cast_info, company_name, movie_companies, name, role_type, title]

    ### Joins
    j01 = Join(aka_name, name, ["person_id"], ["id"])
    j02 = Join(name, cast_info, ["id"], ["person_id"])
    j03 = Join(cast_info, title, ["movie_id"], ["id"])
    j04 = Join(title, movie_companies, ["id"], ["movie_id"])
    j05 = Join(movie_companies, company_name, ["company_id"], ["id"])
    j06 = Join(cast_info, role_type, ["role_id"], ["id"])
    j07 = Join(aka_name, cast_info, ["person_id"], ["person_id"])
    j08 = Join(cast_info, movie_companies, ["movie_id"], ["movie_id"])
    joins = [j01, j02, j03, j04, j05, j06, j07, j08]

    return JoinGraph(relations, joins)

def create_q9b():
    ### Relations
    aka_name = Relation("aka_name", "an", ["name", "person_id"], projections=["name"])
    char_name = Relation("char_name", "chn", ["id", "name"], projections=["name"])
    cast_info = Relation("cast_info", "ci", ["person_id", "movie_id", "role_id", "person_role_id", "note"],
                         filters=["ci.note = '(voice)'"])
    company_name = Relation("company_name", "cn", ["country_code", "id"], filters=["cn.country_code = '[us]'"])
    movie_companies = Relation("movie_companies", "mc", ["company_id", "movie_id", "note"],
                               filters=["mc.note LIKE '%(200%)%'", "(mc.note LIKE '%(USA)%' OR mc.note LIKE '%(worldwide)%')"])
    name = Relation("name", "n", ["id", "name", "gender"], filters=["n.gender = 'f'", "n.name LIKE '%Angel%'"],
                    projections=["name"])
    role_type = Relation("role_type", "rt", ["role", "id"], filters=["rt.role = 'actress'"])
    title = Relation("title", "t", ["id", "title", "production_year"],
                     filters=["t.production_year >= 2007", "t.production_year <= 2010"], projections=["title"])
    relations = [aka_name, char_name, cast_info, company_name, movie_companies, name, role_type, title]

    ### Joins
    j01 = Join(cast_info, title, ["movie_id"], ["id"])
    j02 = Join(title, movie_companies, ["id"], ["movie_id"])
    j03 = Join(cast_info, movie_companies, ["movie_id"], ["movie_id"])
    j04 = Join(movie_companies, company_name, ["company_id"], ["id"])
    j05 = Join(cast_info, role_type, ["role_id"], ["id"])
    j06 = Join(name, cast_info, ["id"], ["person_id"])
    j07 = Join(char_name, cast_info, ["id"], ["person_role_id"])
    j08 = Join(aka_name, name, ["person_id"], ["id"])
    j09 = Join(aka_name, cast_info, ["person_id"], ["person_id"])
    joins = [j01, j02, j03, j04, j05, j06, j07, j08, j09]

    return JoinGraph(relations, joins)

def create_q10a():
    ### Relations
    char_name = Relation("char_name", "chn", ["id", "name"], projections=["name"])
    cast_info = Relation("cast_info", "ci", ["movie_id", "role_id", "person_role_id", "note"],
                         filters=["ci.note LIKE '%(voice)%'", "ci.note LIKE '%(uncredited)%'"])
    company_name = Relation("company_name", "cn", ["country_code", "id"], filters=["cn.country_code = '[ru]'"])
    company_type = Relation("company_type", "ct", ["id"])
    movie_companies = Relation("movie_companies", "mc", ["company_id", "movie_id", "company_type_id"])
    role_type = Relation("role_type", "rt", ["role", "id"], filters=["rt.role = 'actor'"])
    title = Relation("title", "t", ["id", "title", "production_year"],
                     filters=["t.production_year > 2005"], projections=["title"])
    relations = [char_name, cast_info, company_name, company_type, movie_companies, role_type, title]

    ### Joins
    j01 = Join(title, movie_companies, ["id"], ["movie_id"])
    j02 = Join(title, cast_info, ["id"], ["movie_id"])
    j03 = Join(cast_info, movie_companies, ["movie_id"], ["movie_id"])
    j04 = Join(char_name, cast_info, ["id"], ["person_role_id"])
    j05 = Join(role_type, cast_info, ["id"], ["role_id"])
    j06 = Join(company_name, movie_companies, ["id"], ["company_id"])
    j07 = Join(company_type, movie_companies, ["id"], ["company_type_id"])
    joins = [j01, j02, j03, j04, j05, j06, j07]

    return JoinGraph(relations, joins)

def create_q13a():
    ### Relations
    company_name = Relation("company_name", "cn", ["country_code", "id"], filters=["cn.country_code = '[de]'"])
    company_type = Relation("company_type", "ct", ["id", "kind"], filters=["ct.kind = 'production companies'"])
    info_type = Relation("info_type", "it", ["info", "id"], filters=["it.info = 'rating'"])
    info_type_2 = Relation("info_type", "it2", ["info", "id"], filters=["it2.info = 'release dates'"])
    kind_type = Relation("kind_type", "kt", ["id", "kind"], filters=["kt.kind = 'movie'"])
    movie_companies = Relation("movie_companies", "mc", ["company_id", "movie_id", "company_type_id"])
    title = Relation("title", "t", ["id", "title", "kind_id"], projections=["title"])
    relations = [company_name, company_type, info_type, info_type_2, kind_type, movie_companies, movie_info, movie_info_idx, title]

    ### Joins
    j01 = Join(movie_info, title, ["movie_id"], ["id"])
    j02 = Join(info_type_2, movie_info, ["id"], ["info_type_id"])
    j03 = Join(kind_type, title, ["id"], ["kind_id"])
    j04 = Join(movie_companies, title, ["movie_id"], ["id"])
    j05 = Join(company_name, movie_companies, ["id"], ["company_id"])
    j06 = Join(company_type, movie_companies, ["id"], ["company_type_id"])
    j07 = Join(movie_info_idx, title, ["movie_id"], ["id"])
    j08 = Join(info_type, movie_info_idx, ["id"], ["info_type_id"])
    j09 = Join(movie_info, movie_info_idx, ["movie_id"], ["movie_id"])
    j10 = Join(movie_info, movie_companies, ["movie_id"], ["movie_id"])
    j11 = Join(movie_info_idx, movie_companies, ["movie_id"], ["movie_id"])
    joins = [j01, j02, j03, j04, j05, j06, j07, j08, j09, j10, j11]

    return JoinGraph(relations, joins)

def create_q17a():
    ### Relations
    cast_info = Relation("cast_info", "ci", ["person_id", "movie_id"])
    company_name = Relation("company_name", "cn", ["country_code", "id"], filters=["cn.country_code = '[us]'"])
    keyword = Relation("keyword", "k", ["keyword", "id"], ["k.keyword = 'character-name-in-title'"])
    movie_companies = Relation("movie_companies", "mc", ["company_id", "movie_id"])
    movie_keyword = Relation("movie_keyword", "mk", ["movie_id", "keyword_id"])
    name = Relation("name", "n", ["id", "name"], filters=["n.name LIKE 'B%'"], projections=["name", "name"])
    title = Relation("title", "t", ["id"])
    relations = [cast_info, company_name, keyword, movie_companies, movie_keyword, name, title]

    ### Joins
    j01 = Join(name, cast_info, ["id"], ["person_id"])
    j02 = Join(cast_info, title, ["movie_id"], ["id"])
    j03 = Join(title, movie_keyword, ["id"], ["movie_id"])
    j04 = Join(movie_keyword, keyword, ["keyword_id"], ["id"])
    j05 = Join(title, movie_companies, ["id"], ["movie_id"])
    j06 = Join(movie_companies, company_name, ["company_id"], ["id"])
    j07 = Join(cast_info, movie_companies, ["movie_id"], ["movie_id"])
    j08 = Join(cast_info, movie_keyword, ["movie_id"], ["movie_id"])
    j09 = Join(movie_companies, movie_keyword, ["movie_id"], ["movie_id"])
    joins = [j01, j02, j03, j04, j05, j06, j07, j08, j09]

    return JoinGraph(relations, joins)


########################################################################################################################
# Custom JOB Queries
########################################################################################################################
def create_q_custom_1():
    ### Relations
    cast_info = Relation("cast_info", "ci", ["person_id", "movie_id"])
    info_type = Relation("info_type", "it", ["info", "id"], filters=["it.info = 'mini biography'"])
    person_info = Relation("person_info", "pi", ["person_id", "note", "info_type_id"],
                           filters=["pi.note = 'Volker Boehm'"])
    keyword = Relation("keyword", "k", ["keyword", "id"], ["k.keyword = 'marvel-cinematic-universe'"])
    movie_keyword = Relation("movie_keyword", "mk", ["movie_id", "keyword_id"])
    title = Relation("title", "t", ["id", "title, production_year"], filters=["t.production_year >= 1980", "t.production_year <= 1984"],
                     projections=["title"])

    relations = [cast_info, info_type, person_info, movie_keyword, keyword, title]

    ### Joins
    j01 = Join(title, movie_keyword, ["id"], ["movie_id"])
    j02 = Join(title, cast_info, ["id"], ["movie_id"])
    j03 = Join(movie_keyword, keyword, ["keyword_id"], ["id"])
    j04 = Join(cast_info, person_info, ["person_id"], ["person_id"])
    j05 = Join(person_info, info_type, ["info_type_id"], ["id"])
    joins = [j01, j02, j03, j04, j05]

    return JoinGraph(relations, joins)

def create_q_custom_2():
    ### Relations
    title = Relation("title", "t", ["id", "title, production_year"], filters=["t.production_year >= 1974"],
                     projections=["title"])
    movie_info = Relation("movie_info", "mi", ["info", "movie_id"], filters=["mi.info = 'English'"])
    movie_info_idx = Relation("movie_info_idx", "mi_idx", ["movie_id", "info_type_id"])
    info_type = Relation("info_type", "it", ["info", "id"], filters=["NOT (it.info LIKE 'LD%')"])

    relations = [title, movie_info, movie_info_idx, info_type]

    ### Joins
    j01 = Join(title, movie_info, ["id"], ["movie_id"])
    j02 = Join(movie_info, movie_info_idx, ["movie_id"], ["movie_id"])
    j03 = Join(movie_info_idx, info_type, ["info_type_id"], ["id"])
    joins = [j01, j02, j03]

    return JoinGraph(relations, joins)

def create_q_custom_3():
    ### Relations
    title = Relation("title", "t", ["id", "title, production_year"], filters=["t.production_year >= 1974"],
                     projections=["title"])
    movie_companies = Relation("movie_companies", "mc", ["movie_id", "company_id", "company_type_id", "note"],
                               projections=["note"])
    company_name = Relation("company_name", "cn", ["id", "country_code"], filters=["cn.country_code = '[us]'"])
    company_type = Relation("company_type", "ct", ["id", "kind"], filters=["ct.kind != 'miscellaneous companies'"])
    movie_keyword = Relation("movie_keyword", "mk", ["movie_id", "keyword_id"])
    keyword = Relation("keyword", "k", ["id", "keyword"], filters=["k.keyword LIKE '%sequel%'"])

    relations = [title, movie_companies, company_name, company_type, movie_keyword, keyword]

    ### Joins
    j01 = Join(title, movie_companies, ["id"], ["movie_id"])
    j02 = Join(movie_companies, company_name, ["company_type"], ["id"])
    j03 = Join(movie_companies, company_type, ["company_type_id"], ["id"])
    j04 = Join(title, movie_keyword, ["id"], ["movie_id"])
    j05 = Join(movie_keyword, keyword, ["keyword_id"], ["id"])
    joins = [j01, j02, j03, j04, j05]

    return JoinGraph(relations, joins)

########################################################################################################################
# Star Schema Queries
########################################################################################################################
##### Varying number of joins ##########################################################################################
def create_star_joins_1():
    ### Relations
    fact = Relation("fact", "f", ["id", "fkd1", "fkd2", "fkd3", "fkd4", "a", "b"], projections=["id", "fkd1", "fkd2", "fkd3", "fkd4", "a", "b"])
    dim1 = Relation("dim1", "d1", ["id", "a", "b"], projections=["id", "a", "b"])
    relations = [fact, dim1]

    ### Joins
    j01 = Join(fact, dim1, ["fkd1"], ["id"])
    joins = [j01]

    return JoinGraph(relations, joins)

def create_star_joins_2():
    ### Relations
    fact = Relation("fact", "f", ["id", "fkd1", "fkd2", "fkd3", "fkd4", "a", "b"],
                    projections=["id", "fkd1", "fkd2", "fkd3", "fkd4", "a", "b"])
    dim1 = Relation("dim1", "d1", ["id", "a", "b"], projections=["id", "a", "b"])
    dim2 = Relation("dim2", "d2", ["id", "a", "b"], projections=["id", "a", "b"])
    relations = [fact, dim1, dim2]

    ### Joins
    j01 = Join(fact, dim1, ["fkd1"], ["id"])
    j02 = Join(fact, dim2, ["fkd2"], ["id"])
    joins = [j01, j02]

    return JoinGraph(relations, joins)

def create_star_joins_3():
    ### Relations
    fact = Relation("fact", "f", ["id", "fkd1", "fkd2", "fkd3", "fkd4", "a", "b"],
                    projections=["id", "fkd1", "fkd2", "fkd3", "fkd4", "a", "b"])
    dim1 = Relation("dim1", "d1", ["id", "a", "b"], projections=["id", "a", "b"])
    dim2 = Relation("dim2", "d2", ["id", "a", "b"], projections=["id", "a", "b"])
    dim3 = Relation("dim3", "d3", ["id", "a", "b"], projections=["id", "a", "b"])
    relations = [fact, dim1, dim2, dim3]

    ### Joins
    j01 = Join(fact, dim1, ["fkd1"], ["id"])
    j02 = Join(fact, dim2, ["fkd2"], ["id"])
    j03 = Join(fact, dim3, ["fkd3"], ["id"])
    joins = [j01, j02, j03]

    return JoinGraph(relations, joins)

def create_star_joins_4():
    ### Relations
    fact = Relation("fact", "f", ["id", "fkd1", "fkd2", "fkd3", "fkd4", "a", "b"],
                    projections=["id", "fkd1", "fkd2", "fkd3", "fkd4", "a", "b"])
    dim1 = Relation("dim1", "d1", ["id", "a", "b"], projections=["id", "a", "b"])
    dim2 = Relation("dim2", "d2", ["id", "a", "b"], projections=["id", "a", "b"])
    dim3 = Relation("dim3", "d3", ["id", "a", "b"], projections=["id", "a", "b"])
    dim4 = Relation("dim4", "d4", ["id", "a", "b"], projections=["id", "a", "b"])
    relations = [fact, dim1, dim2, dim3, dim4]

    ### Joins
    j01 = Join(fact, dim1, ["fkd1"], ["id"])
    j02 = Join(fact, dim2, ["fkd2"], ["id"])
    j03 = Join(fact, dim3, ["fkd3"], ["id"])
    j04 = Join(fact, dim4, ["fkd4"], ["id"])
    joins = [j01, j02, j03, j04]

    return JoinGraph(relations, joins)

##### Varying number of selected relations #############################################################################
### Two relations
def create_star_proj_2_dim():
    ### Relations
    fact = Relation("fact", "f", ["fkd1", "fkd2", "fkd3", "fkd4"])
    dim1 = Relation("dim1", "d1", ["id", "a", "b"], projections=["id", "a", "b"])
    dim2 = Relation("dim2", "d2", ["id", "a", "b"], projections=["id", "a", "b"])
    dim3 = Relation("dim3", "d3", ["id"])
    dim4 = Relation("dim4", "d4", ["id"])
    relations = [fact, dim1, dim2, dim3, dim4]

    ### Joins
    j01 = Join(fact, dim1, ["fkd1"], ["id"])
    j02 = Join(fact, dim2, ["fkd2"], ["id"])
    j03 = Join(fact, dim3, ["fkd3"], ["id"])
    j04 = Join(fact, dim4, ["fkd4"], ["id"])
    joins = [j01, j02, j03, j04]

    return JoinGraph(relations, joins)

def create_star_proj_fact_1_dim():
    ### Relations
    fact = Relation("fact", "f", ["id", "fkd1", "fkd2", "fkd3", "fkd4", "a", "b"],
                    projections=["id", "fkd1", "fkd2", "fkd3", "fkd4", "a", "b"])
    dim1 = Relation("dim1", "d1", ["id", "a", "b"], projections=["id", "a", "b"])
    dim2 = Relation("dim2", "d2", ["id"])
    dim3 = Relation("dim3", "d3", ["id"])
    dim4 = Relation("dim4", "d4", ["id"])
    relations = [fact, dim1, dim2, dim3, dim4]

    ### Joins
    j01 = Join(fact, dim1, ["fkd1"], ["id"])
    j02 = Join(fact, dim2, ["fkd2"], ["id"])
    j03 = Join(fact, dim3, ["fkd3"], ["id"])
    j04 = Join(fact, dim4, ["fkd4"], ["id"])
    joins = [j01, j02, j03, j04]

    return JoinGraph(relations, joins)

### Thre relations
def create_star_proj_3_dim():
    ### Relations
    fact = Relation("fact", "f", ["fkd1", "fkd2", "fkd3", "fkd4"])
    dim1 = Relation("dim1", "d1", ["id", "a", "b"], projections=["id", "a", "b"])
    dim2 = Relation("dim2", "d2", ["id", "a", "b"], projections=["id", "a", "b"])
    dim3 = Relation("dim3", "d3", ["id", "a", "b"], projections=["id", "a", "b"])
    dim4 = Relation("dim4", "d4", ["id"])
    relations = [fact, dim1, dim2, dim3, dim4]

    ### Joins
    j01 = Join(fact, dim1, ["fkd1"], ["id"])
    j02 = Join(fact, dim2, ["fkd2"], ["id"])
    j03 = Join(fact, dim3, ["fkd3"], ["id"])
    j04 = Join(fact, dim4, ["fkd4"], ["id"])
    joins = [j01, j02, j03, j04]

    return JoinGraph(relations, joins)

def create_star_proj_fact_2_dim():
    ### Relations
    fact = Relation("fact", "f", ["id", "fkd1", "fkd2", "fkd3", "fkd4", "a", "b"],
                    projections=["id", "fkd1", "fkd2", "fkd3", "fkd4", "a", "b"])
    dim1 = Relation("dim1", "d1", ["id", "a", "b"], projections=["id", "a", "b"])
    dim2 = Relation("dim2", "d2", ["id", "a", "b"], projections=["id", "a", "b"])
    dim3 = Relation("dim3", "d3", ["id"])
    dim4 = Relation("dim4", "d4", ["id"])
    relations = [fact, dim1, dim2, dim3, dim4]

    ### Joins
    j01 = Join(fact, dim1, ["fkd1"], ["id"])
    j02 = Join(fact, dim2, ["fkd2"], ["id"])
    j03 = Join(fact, dim3, ["fkd3"], ["id"])
    j04 = Join(fact, dim4, ["fkd4"], ["id"])
    joins = [j01, j02, j03, j04]

    return JoinGraph(relations, joins)

### Four relations
def create_star_proj_4_dim():
    ### Relations
    fact = Relation("fact", "f", ["fkd1", "fkd2", "fkd3", "fkd4"])
    dim1 = Relation("dim1", "d1", ["id", "a", "b"], projections=["id", "a", "b"])
    dim2 = Relation("dim2", "d2", ["id", "a", "b"], projections=["id", "a", "b"])
    dim3 = Relation("dim3", "d3", ["id", "a", "b"], projections=["id", "a", "b"])
    dim4 = Relation("dim4", "d4", ["id", "a", "b"], projections=["id", "a", "b"])
    relations = [fact, dim1, dim2, dim3, dim4]

    ### Joins
    j01 = Join(fact, dim1, ["fkd1"], ["id"])
    j02 = Join(fact, dim2, ["fkd2"], ["id"])
    j03 = Join(fact, dim3, ["fkd3"], ["id"])
    j04 = Join(fact, dim4, ["fkd4"], ["id"])
    joins = [j01, j02, j03, j04]

    return JoinGraph(relations, joins)

def create_star_proj_fact_3_dim():
    ### Relations
    fact = Relation("fact", "f", ["id", "fkd1", "fkd2", "fkd3", "fkd4", "a", "b"],
                    projections=["id", "fkd1", "fkd2", "fkd3", "fkd4", "a", "b"])
    dim1 = Relation("dim1", "d1", ["id", "a", "b"], projections=["id", "a", "b"])
    dim2 = Relation("dim2", "d2", ["id", "a", "b"], projections=["id", "a", "b"])
    dim3 = Relation("dim3", "d3", ["id", "a", "b"], projections=["id", "a", "b"])
    dim4 = Relation("dim4", "d4", ["id"])
    relations = [fact, dim1, dim2, dim3, dim4]

    ### Joins
    j01 = Join(fact, dim1, ["fkd1"], ["id"])
    j02 = Join(fact, dim2, ["fkd2"], ["id"])
    j03 = Join(fact, dim3, ["fkd3"], ["id"])
    j04 = Join(fact, dim4, ["fkd4"], ["id"])
    joins = [j01, j02, j03, j04]

    return JoinGraph(relations, joins)

### Five relations
def create_star_proj_fact_4_dim():
    ### Relations
    fact = Relation("fact", "f", ["id", "fkd1", "fkd2", "fkd3", "fkd4", "a", "b"],
                    projections=["id", "fkd1", "fkd2", "fkd3", "fkd4", "a", "b"])
    dim1 = Relation("dim1", "d1", ["id", "a", "b"], projections=["id", "a", "b"])
    dim2 = Relation("dim2", "d2", ["id", "a", "b"], projections=["id", "a", "b"])
    dim3 = Relation("dim3", "d3", ["id", "a", "b"], projections=["id", "a", "b"])
    dim4 = Relation("dim4", "d4", ["id", "a", "b"], projections=["id", "a", "b"])
    relations = [fact, dim1, dim2, dim3, dim4]

    ### Joins
    j01 = Join(fact, dim1, ["fkd1"], ["id"])
    j02 = Join(fact, dim2, ["fkd2"], ["id"])
    j03 = Join(fact, dim3, ["fkd3"], ["id"])
    j04 = Join(fact, dim4, ["fkd4"], ["id"])
    joins = [j01, j02, j03, j04]

    return JoinGraph(relations, joins)

##### Varying selectivity of dimension tables ##########################################################################
def create_star_sel_10():
    ### Relations
    fact = Relation("fact", "f", ["id", "fkd1", "fkd2", "fkd3", "fkd4", "a", "b"],
                    projections=["id", "fkd1", "fkd2", "fkd3", "fkd4", "a", "b"])
    dim1 = Relation("dim1", "d1", ["id", "a", "b"], filters=["d1.id < 6"], projections=["id", "a", "b"])
    dim2 = Relation("dim2", "d2", ["id", "a", "b"], filters=["d2.id < 6"], projections=["id", "a", "b"])
    dim3 = Relation("dim3", "d3", ["id", "a", "b"], filters=["d3.id < 6"], projections=["id", "a", "b"])
    dim4 = Relation("dim4", "d4", ["id", "a", "b"], filters=["d4.id < 6"], projections=["id", "a", "b"])
    relations = [fact, dim1, dim2, dim3, dim4]

    ### Joins
    j01 = Join(fact, dim1, ["fkd1"], ["id"])
    j02 = Join(fact, dim2, ["fkd2"], ["id"])
    j03 = Join(fact, dim3, ["fkd3"], ["id"])
    j04 = Join(fact, dim4, ["fkd4"], ["id"])
    joins = [j01, j02, j03, j04]

    return JoinGraph(relations, joins)

def create_star_sel_20():
    ### Relations
    fact = Relation("fact", "f", ["id", "fkd1", "fkd2", "fkd3", "fkd4", "a", "b"],
                    projections=["id", "fkd1", "fkd2", "fkd3", "fkd4", "a", "b"])
    dim1 = Relation("dim1", "d1", ["id", "a", "b"], filters=["d1.id < 12"], projections=["id", "a", "b"])
    dim2 = Relation("dim2", "d2", ["id", "a", "b"], filters=["d2.id < 12"], projections=["id", "a", "b"])
    dim3 = Relation("dim3", "d3", ["id", "a", "b"], filters=["d3.id < 12"], projections=["id", "a", "b"])
    dim4 = Relation("dim4", "d4", ["id", "a", "b"], filters=["d4.id < 12"], projections=["id", "a", "b"])
    relations = [fact, dim1, dim2, dim3, dim4]

    ### Joins
    j01 = Join(fact, dim1, ["fkd1"], ["id"])
    j02 = Join(fact, dim2, ["fkd2"], ["id"])
    j03 = Join(fact, dim3, ["fkd3"], ["id"])
    j04 = Join(fact, dim4, ["fkd4"], ["id"])
    joins = [j01, j02, j03, j04]

    return JoinGraph(relations, joins)

def create_star_sel_30():
    ### Relations
    fact = Relation("fact", "f", ["id", "fkd1", "fkd2", "fkd3", "fkd4", "a", "b"],
                    projections=["id", "fkd1", "fkd2", "fkd3", "fkd4", "a", "b"])
    dim1 = Relation("dim1", "d1", ["id", "a", "b"], filters=["d1.id < 18"], projections=["id", "a", "b"])
    dim2 = Relation("dim2", "d2", ["id", "a", "b"], filters=["d2.id < 18"], projections=["id", "a", "b"])
    dim3 = Relation("dim3", "d3", ["id", "a", "b"], filters=["d3.id < 18"], projections=["id", "a", "b"])
    dim4 = Relation("dim4", "d4", ["id", "a", "b"], filters=["d4.id < 18"], projections=["id", "a", "b"])
    relations = [fact, dim1, dim2, dim3, dim4]

    ### Joins
    j01 = Join(fact, dim1, ["fkd1"], ["id"])
    j02 = Join(fact, dim2, ["fkd2"], ["id"])
    j03 = Join(fact, dim3, ["fkd3"], ["id"])
    j04 = Join(fact, dim4, ["fkd4"], ["id"])
    joins = [j01, j02, j03, j04]

    return JoinGraph(relations, joins)

def create_star_sel_40():
    ### Relations
    fact = Relation("fact", "f", ["id", "fkd1", "fkd2", "fkd3", "fkd4", "a", "b"],
                    projections=["id", "fkd1", "fkd2", "fkd3", "fkd4", "a", "b"])
    dim1 = Relation("dim1", "d1", ["id", "a", "b"], filters=["d1.id < 24"], projections=["id", "a", "b"])
    dim2 = Relation("dim2", "d2", ["id", "a", "b"], filters=["d2.id < 24"], projections=["id", "a", "b"])
    dim3 = Relation("dim3", "d3", ["id", "a", "b"], filters=["d3.id < 24"], projections=["id", "a", "b"])
    dim4 = Relation("dim4", "d4", ["id", "a", "b"], filters=["d4.id < 24"], projections=["id", "a", "b"])
    relations = [fact, dim1, dim2, dim3, dim4]

    ### Joins
    j01 = Join(fact, dim1, ["fkd1"], ["id"])
    j02 = Join(fact, dim2, ["fkd2"], ["id"])
    j03 = Join(fact, dim3, ["fkd3"], ["id"])
    j04 = Join(fact, dim4, ["fkd4"], ["id"])
    joins = [j01, j02, j03, j04]

    return JoinGraph(relations, joins)

def create_star_sel_50():
    ### Relations
    fact = Relation("fact", "f", ["id", "fkd1", "fkd2", "fkd3", "fkd4", "a", "b"],
                    projections=["id", "fkd1", "fkd2", "fkd3", "fkd4", "a", "b"])
    dim1 = Relation("dim1", "d1", ["id", "a", "b"], filters=["d1.id < 30"], projections=["id", "a", "b"])
    dim2 = Relation("dim2", "d2", ["id", "a", "b"], filters=["d2.id < 30"], projections=["id", "a", "b"])
    dim3 = Relation("dim3", "d3", ["id", "a", "b"], filters=["d3.id < 30"], projections=["id", "a", "b"])
    dim4 = Relation("dim4", "d4", ["id", "a", "b"], filters=["d4.id < 30"], projections=["id", "a", "b"])
    relations = [fact, dim1, dim2, dim3, dim4]

    ### Joins
    j01 = Join(fact, dim1, ["fkd1"], ["id"])
    j02 = Join(fact, dim2, ["fkd2"], ["id"])
    j03 = Join(fact, dim3, ["fkd3"], ["id"])
    j04 = Join(fact, dim4, ["fkd4"], ["id"])
    joins = [j01, j02, j03, j04]

    return JoinGraph(relations, joins)

def create_star_sel_60():
    ### Relations
    fact = Relation("fact", "f", ["id", "fkd1", "fkd2", "fkd3", "fkd4", "a", "b"],
                    projections=["id", "fkd1", "fkd2", "fkd3", "fkd4", "a", "b"])
    dim1 = Relation("dim1", "d1", ["id", "a", "b"], filters=["d1.id < 36"], projections=["id", "a", "b"])
    dim2 = Relation("dim2", "d2", ["id", "a", "b"], filters=["d2.id < 36"], projections=["id", "a", "b"])
    dim3 = Relation("dim3", "d3", ["id", "a", "b"], filters=["d3.id < 36"], projections=["id", "a", "b"])
    dim4 = Relation("dim4", "d4", ["id", "a", "b"], filters=["d4.id < 36"], projections=["id", "a", "b"])
    relations = [fact, dim1, dim2, dim3, dim4]

    ### Joins
    j01 = Join(fact, dim1, ["fkd1"], ["id"])
    j02 = Join(fact, dim2, ["fkd2"], ["id"])
    j03 = Join(fact, dim3, ["fkd3"], ["id"])
    j04 = Join(fact, dim4, ["fkd4"], ["id"])
    joins = [j01, j02, j03, j04]

    return JoinGraph(relations, joins)

def create_star_sel_70():
    ### Relations
    fact = Relation("fact", "f", ["id", "fkd1", "fkd2", "fkd3", "fkd4", "a", "b"],
                    projections=["id", "fkd1", "fkd2", "fkd3", "fkd4", "a", "b"])
    dim1 = Relation("dim1", "d1", ["id", "a", "b"], filters=["d1.id < 42"], projections=["id", "a", "b"])
    dim2 = Relation("dim2", "d2", ["id", "a", "b"], filters=["d2.id < 42"], projections=["id", "a", "b"])
    dim3 = Relation("dim3", "d3", ["id", "a", "b"], filters=["d3.id < 42"], projections=["id", "a", "b"])
    dim4 = Relation("dim4", "d4", ["id", "a", "b"], filters=["d4.id < 42"], projections=["id", "a", "b"])
    relations = [fact, dim1, dim2, dim3, dim4]

    ### Joins
    j01 = Join(fact, dim1, ["fkd1"], ["id"])
    j02 = Join(fact, dim2, ["fkd2"], ["id"])
    j03 = Join(fact, dim3, ["fkd3"], ["id"])
    j04 = Join(fact, dim4, ["fkd4"], ["id"])
    joins = [j01, j02, j03, j04]

    return JoinGraph(relations, joins)

def create_star_sel_80():
    ### Relations
    fact = Relation("fact", "f", ["id", "fkd1", "fkd2", "fkd3", "fkd4", "a", "b"],
                    projections=["id", "fkd1", "fkd2", "fkd3", "fkd4", "a", "b"])
    dim1 = Relation("dim1", "d1", ["id", "a", "b"], filters=["d1.id < 48"], projections=["id", "a", "b"])
    dim2 = Relation("dim2", "d2", ["id", "a", "b"], filters=["d2.id < 48"], projections=["id", "a", "b"])
    dim3 = Relation("dim3", "d3", ["id", "a", "b"], filters=["d3.id < 48"], projections=["id", "a", "b"])
    dim4 = Relation("dim4", "d4", ["id", "a", "b"], filters=["d4.id < 48"], projections=["id", "a", "b"])
    relations = [fact, dim1, dim2, dim3, dim4]

    ### Joins
    j01 = Join(fact, dim1, ["fkd1"], ["id"])
    j02 = Join(fact, dim2, ["fkd2"], ["id"])
    j03 = Join(fact, dim3, ["fkd3"], ["id"])
    j04 = Join(fact, dim4, ["fkd4"], ["id"])
    joins = [j01, j02, j03, j04]

    return JoinGraph(relations, joins)

def create_star_sel_90():
    ### Relations
    fact = Relation("fact", "f", ["id", "fkd1", "fkd2", "fkd3", "fkd4", "a", "b"],
                    projections=["id", "fkd1", "fkd2", "fkd3", "fkd4", "a", "b"])
    dim1 = Relation("dim1", "d1", ["id", "a", "b"], filters=["d1.id < 54"], projections=["id", "a", "b"])
    dim2 = Relation("dim2", "d2", ["id", "a", "b"], filters=["d2.id < 54"], projections=["id", "a", "b"])
    dim3 = Relation("dim3", "d3", ["id", "a", "b"], filters=["d3.id < 54"], projections=["id", "a", "b"])
    dim4 = Relation("dim4", "d4", ["id", "a", "b"], filters=["d4.id < 54"], projections=["id", "a", "b"])
    relations = [fact, dim1, dim2, dim3, dim4]

    ### Joins
    j01 = Join(fact, dim1, ["fkd1"], ["id"])
    j02 = Join(fact, dim2, ["fkd2"], ["id"])
    j03 = Join(fact, dim3, ["fkd3"], ["id"])
    j04 = Join(fact, dim4, ["fkd4"], ["id"])
    joins = [j01, j02, j03, j04]

    return JoinGraph(relations, joins)

def create_star_sel_100():
    ### Relations
    fact = Relation("fact", "f", ["id", "fkd1", "fkd2", "fkd3", "fkd4", "a", "b"],
                    projections=["id", "fkd1", "fkd2", "fkd3", "fkd4", "a", "b"])
    dim1 = Relation("dim1", "d1", ["id", "a", "b"], filters=["d1.id < 60"], projections=["id", "a", "b"])
    dim2 = Relation("dim2", "d2", ["id", "a", "b"], filters=["d2.id < 60"], projections=["id", "a", "b"])
    dim3 = Relation("dim3", "d3", ["id", "a", "b"], filters=["d3.id < 60"], projections=["id", "a", "b"])
    dim4 = Relation("dim4", "d4", ["id", "a", "b"], filters=["d4.id < 60"], projections=["id", "a", "b"])
    relations = [fact, dim1, dim2, dim3, dim4]

    ### Joins
    j01 = Join(fact, dim1, ["fkd1"], ["id"])
    j02 = Join(fact, dim2, ["fkd2"], ["id"])
    j03 = Join(fact, dim3, ["fkd3"], ["id"])
    j04 = Join(fact, dim4, ["fkd4"], ["id"])
    joins = [j01, j02, j03, j04]

    return JoinGraph(relations, joins)
