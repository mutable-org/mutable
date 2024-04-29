from query_utility import Relation, Join, JoinGraph

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
    # TODO: we might want to define q4 such that it includes the post-join information as well
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

def create_q8d():
    # TODO: we might want to define q8d such that it includes the post-join information as well
    ### Relations
    aka_name = Relation("aka_name", "an1", ["name", "person_id"], projections=["name"])
    cast_info = Relation("cast_info", "ci", ["person_id", "movie_id", "role_id"])
    company_name = Relation("company_name", "cn", ["country_code", "id"], ["cn.country_code = '[us]'"])
    movie_companies = Relation("movie_companies", "mc", ["company_id", "movie_id"])
    name = Relation("name", "n1", ["id"])
    role_type = Relation("role_type", "rt", ["role", "id"], ["rt.role = 'costume designer'"])
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
