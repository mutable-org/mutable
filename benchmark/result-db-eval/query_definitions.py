from query_utility import Relation, Join, JoinGraph
import math


def create_q1a(acyclic=False):
    ### Relations
    company_type = Relation(
        "company_type", "ct", ["kind", "id"], ["ct.kind = 'production companies'"]
    )
    info_type = Relation(
        "info_type", "it", ["info", "id"], ["it.info = 'top 250 rank'"]
    )
    movie_companies = Relation(
        "movie_companies",
        "mc",
        ["company_type_id", "movie_id", "note"],
        projections=["note"],
    )
    movie_info_idx = Relation("movie_info_idx", "mi_idx", ["movie_id", "info_type_id"])
    title = Relation(
        "title",
        "t",
        ["id", "title", "production_year"],
        projections=["title", "production_year"],
    )
    relations = [company_type, info_type, movie_companies, movie_info_idx, title]

    ### Joins
    j01 = Join(company_type, movie_companies, ["id"], ["company_type_id"])
    j02 = Join(title, movie_companies, ["id"], ["movie_id"])
    j03 = Join(title, movie_info_idx, ["id"], ["movie_id"])
    j04 = Join(info_type, movie_info_idx, ["id"], ["info_type_id"])
    joins = [j01, j02, j03, j04]
    if not acyclic:
        joins.append(Join(movie_companies, movie_info_idx, ["movie_id"], ["movie_id"]))

    return JoinGraph(relations, joins)


def create_q2a(acyclic=False):
    ### Relations
    company_name = Relation(
        "company_name", "cn", ["country_code", "id"], ["cn.country_code = '[de]'"]
    )
    keyword = Relation(
        "keyword", "k", ["keyword", "id"], ["k.keyword = 'character-name-in-title'"]
    )
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
    joins = [j01, j02, j03, j04]
    if not acyclic:
        joins.append(j05)

    return JoinGraph(relations, joins)


def create_q3b(acyclic=False):
    ### Relations
    keyword = Relation("keyword", "k", ["keyword", "id"])
    movie_info = Relation(
        "movie_info", "mi", ["info", "movie_id"], ["mi.info = 'Bulgaria'"]
    )
    movie_keyword = Relation("movie_keyword", "mk", ["movie_id", "keyword_id"])
    title = Relation(
        "title",
        "t",
        ["id", "title", "production_year"],
        ["t.production_year > 2010"],
        ["title"],
    )
    relations = [keyword, movie_info, movie_keyword, title]

    ### Joins
    j01 = Join(title, movie_info, ["id"], ["movie_id"])
    j02 = Join(title, movie_keyword, ["id"], ["movie_id"])
    j03 = Join(movie_keyword, movie_info, ["movie_id"], ["movie_id"])
    j04 = Join(keyword, movie_keyword, ["id"], ["keyword_id"])
    joins = [j01, j02, j04]
    if not acyclic:
        joins.append(j03)

    return JoinGraph(relations, joins)


def create_q4a(acyclic=False):
    ### Relations
    info_type = Relation("info_type", "it", ["info", "id"], ["it.info = 'rating'"])
    keyword = Relation("keyword", "k", ["keyword", "id"])
    movie_info_idx = Relation(
        "movie_info_idx",
        "mi_idx",
        ["info", "movie_id", "info_type_id"],
        ["mi_idx.info > '5.0'"],
        ["info"],
    )
    movie_keyword = Relation("movie_keyword", "mk", ["movie_id", "keyword_id"])
    title = Relation(
        "title",
        "t",
        ["id", "title", "production_year"],
        ["t.production_year > 2005"],
        ["title"],
    )
    relations = [info_type, keyword, movie_info_idx, movie_keyword, title]

    ### Joins
    j01 = Join(title, movie_info_idx, ["id"], ["movie_id"])
    j02 = Join(title, movie_keyword, ["id"], ["movie_id"])
    j03 = Join(movie_keyword, movie_info_idx, ["movie_id"], ["movie_id"])
    j04 = Join(keyword, movie_keyword, ["id"], ["keyword_id"])
    j05 = Join(info_type, movie_info_idx, ["id"], ["info_type_id"])
    joins = [j01, j02, j04, j05]
    if not acyclic:
        joins.append(j03)

    return JoinGraph(relations, joins)


def create_q5b(acyclic=False):
    ### Relations
    company_type = Relation(
        "company_type", "ct", ["kind", "id"], ["ct.kind = 'production companies'"]
    )
    info_type = Relation("info_type", "it", ["id"])
    movie_companies = Relation(
        "movie_companies", "mc", ["company_type_id", "movie_id", "note"]
    )
    movie_info = Relation("movie_info", "mi", ["info", "movie_id", "info_type_id"])
    title = Relation(
        "title",
        "t",
        ["id", "title", "production_year"],
        ["t.production_year > 2010"],
        ["title"],
    )
    relations = [company_type, info_type, movie_companies, movie_info, title]

    ### Joins
    j01 = Join(title, movie_info, ["id"], ["movie_id"])
    j02 = Join(title, movie_companies, ["id"], ["movie_id"])
    j03 = Join(movie_companies, movie_info, ["movie_id"], ["movie_id"])
    j04 = Join(company_type, movie_companies, ["id"], ["company_type_id"])
    j05 = Join(info_type, movie_info, ["id"], ["info_type_id"])
    joins = [j01, j02, j04, j05]
    if not acyclic:
        joins.append(j03)

    return JoinGraph(relations, joins)


def create_q7b(acyclic=False):
    ### Relations
    aka_name = Relation("aka_name", "an", ["name", "person_id"])
    cast_info = Relation("cast_info", "ci", ["person_id", "movie_id"])
    info_type = Relation(
        "info_type", "it", ["info", "id"], filters=["it.info = 'mini biography'"]
    )
    link_type = Relation(
        "link_type", "lt", ["id", "link"], filters=["lt.link = 'features'"]
    )
    movie_link = Relation("movie_link", "ml", ["linked_movie_id", "link_type_id"])
    name = Relation("name", "n", ["id", "gender", "name_pcode_cf"])
    person_info = Relation(
        "person_info",
        "pi",
        ["person_id", "note", "info_type_id"],
        filters=["pi.note = 'Volker Boehm'"],
    )
    title = Relation(
        "title",
        "t",
        ["id", "title"],
        filters=["t.production_year >= 1980", "t.production_year <= 1984"],
        projections=["title"],
    )

    relations = [
        aka_name,
        cast_info,
        info_type,
        link_type,
        movie_link,
        name,
        person_info,
        title,
    ]

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
    joins = [j01, j02, j03, j04, j05, j06, j07, j08]
    if not acyclic:
        joins.extend([j09, j10, j11])

    return JoinGraph(relations, joins)


def create_q9b(acyclic=False):
    ### Relations
    aka_name = Relation("aka_name", "an", ["name", "person_id"], projections=["name"])
    char_name = Relation("char_name", "chn", ["id", "name"], projections=["name"])
    cast_info = Relation(
        "cast_info",
        "ci",
        ["person_id", "movie_id", "role_id", "person_role_id", "note"],
        filters=["ci.note = '(voice)'"],
    )
    company_name = Relation(
        "company_name",
        "cn",
        ["country_code", "id"],
        filters=["cn.country_code = '[us]'"],
    )
    movie_companies = Relation(
        "movie_companies", "mc", ["company_id", "movie_id", "note"]
    )
    name = Relation("name", "n", ["id", "name", "gender"], projections=["name"])
    role_type = Relation(
        "role_type", "rt", ["role", "id"], filters=["rt.role = 'actress'"]
    )
    title = Relation(
        "title",
        "t",
        ["id", "title", "production_year"],
        filters=["t.production_year >= 2007", "t.production_year <= 2010"],
        projections=["title"],
    )
    relations = [
        aka_name,
        char_name,
        cast_info,
        company_name,
        movie_companies,
        name,
        role_type,
        title,
    ]

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
    joins = [j01, j02, j03, j04, j05, j06, j07, j08]

    if not acyclic:
        joins.append(j09)

    return JoinGraph(relations, joins)


def create_q10a(acyclic=False):
    ### Relations
    char_name = Relation("char_name", "chn", ["id", "name"], projections=["name"])
    cast_info = Relation(
        "cast_info", "ci", ["movie_id", "role_id", "person_role_id", "note"]
    )
    company_name = Relation(
        "company_name",
        "cn",
        ["country_code", "id"],
        filters=["cn.country_code = '[ru]'"],
    )
    company_type = Relation("company_type", "ct", ["id"])
    movie_companies = Relation(
        "movie_companies", "mc", ["company_id", "movie_id", "company_type_id"]
    )
    role_type = Relation(
        "role_type", "rt", ["role", "id"], filters=["rt.role = 'actor'"]
    )
    title = Relation(
        "title",
        "t",
        ["id", "title", "production_year"],
        filters=["t.production_year > 2005"],
        projections=["title"],
    )
    relations = [
        char_name,
        cast_info,
        company_name,
        company_type,
        movie_companies,
        role_type,
        title,
    ]

    ### Joins
    j01 = Join(title, movie_companies, ["id"], ["movie_id"])
    j02 = Join(title, cast_info, ["id"], ["movie_id"])
    j03 = Join(cast_info, movie_companies, ["movie_id"], ["movie_id"])
    j04 = Join(char_name, cast_info, ["id"], ["person_role_id"])
    j05 = Join(role_type, cast_info, ["id"], ["role_id"])
    j06 = Join(company_name, movie_companies, ["id"], ["company_id"])
    j07 = Join(company_type, movie_companies, ["id"], ["company_type_id"])
    joins = [j01, j02, j04, j05, j06, j07]

    if not acyclic:
        joins.append(j03)

    return JoinGraph(relations, joins)

def create_synthetic_chain_join(selectivity: float):
    relations: list[Relation] = []
    joins: list[Join] = []

    # Fact 1
    relations.append(Relation(f"rel_{0}", f"r_{0}", ["id_0", "id_1", "w"], [f"r_0.id_1 < {selectivity}"]))
    # Dimension
    relations.append(Relation(f"rel_{1}", f"r_{1}", ["id", "fk", "w"]))
    for i in range(2, 7):
        relations.append(Relation(f"rel_{i}", f"r_{i}", ["id", "w"]))
    # Fact 2
    relations.append(Relation(f"rel_{7}", f"r_{7}", ["id", "fk", "w"]))


    joins.append(Join(relations[0], relations[1], ["id_0"], ["id"]))
    joins.append(Join(relations[7], relations[0], ["id"], ["id_0"]))
    joins.append(Join(relations[1], relations[2], ["fk"], ["id"]))
    joins.append(Join(relations[2], relations[3], ["id"], ["id"]))
    joins.append(Join(relations[4], relations[5], ["id"], ["id"]))
    joins.append(Join(relations[5], relations[6], ["id"], ["id"]))
    joins.append(Join(relations[6], relations[7], ["id"], ["fk"]))

    return JoinGraph(relations, joins)

def create_synthetic_cycle_join(selectivity: int):
    relations: list[Relation] = []
    joins: list[Join] = []

    # Fact 1
    relations.append(Relation(f"rel_{0}", f"r_{0}", ["id_0", "id_1", "w"], [f"r_0.id_1 < {selectivity}"]))
    # Dimension
    relations.append(Relation(f"rel_{1}", f"r_{1}", ["id", "fk", "w"]))
    for i in range(2, 7):
        relations.append(Relation(f"rel_{i}", f"r_{i}", ["id", "w"]))
    # Fact 2
    relations.append(Relation(f"rel_{7}", f"r_{7}", ["id", "fk", "w"]))


    joins.append(Join(relations[0], relations[1], ["id_0"], ["id"]))
    joins.append(Join(relations[1], relations[2], ["fk"], ["id"]))
    joins.append(Join(relations[2], relations[3], ["id"], ["id"]))
    joins.append(Join(relations[3], relations[4], ["id"], ["id"]))
    joins.append(Join(relations[4], relations[5], ["id"], ["id"]))
    joins.append(Join(relations[5], relations[6], ["id"], ["id"]))
    joins.append(Join(relations[6], relations[7], ["id"], ["fk"]))
    joins.append(Join(relations[7], relations[0], ["id"], ["id_0"]))

    return JoinGraph(relations, joins)

def create_synthetic_tvc_join(selectivity: int):
    relations: list[Relation] = []
    joins: list[Join] = []

    # Fact 1
    relations.append(Relation(f"rel_{0}", f"r_{0}", ["id_0", "id_1", "w"], [f"r_0.id_1 < {selectivity}"]))
    # Dimension
    relations.append(Relation(f"rel_{1}", f"r_{1}", ["id", "fk", "w"]))
    for i in range(2, 7):
        relations.append(Relation(f"rel_{i}", f"r_{i}", ["id", "w"]))
    # Fact 2
    relations.append(Relation(f"rel_{7}", f"r_{7}", ["id", "fk", "w"]))


    joins.append(Join(relations[0], relations[1], ["id_0"], ["id"]))
    joins.append(Join(relations[1], relations[2], ["fk"], ["id"]))
    joins.append(Join(relations[1], relations[6], ["fk"], ["id"]))
    joins.append(Join(relations[2], relations[5], ["id"], ["id"]))
    joins.append(Join(relations[2], relations[3], ["id"], ["id"]))
    joins.append(Join(relations[3], relations[4], ["id"], ["id"]))
    joins.append(Join(relations[4], relations[5], ["id"], ["id"]))
    joins.append(Join(relations[5], relations[6], ["id"], ["id"]))
    joins.append(Join(relations[6], relations[7], ["id"], ["fk"]))
    joins.append(Join(relations[7], relations[0], ["id"], ["id_0"]))

    return JoinGraph(relations, joins)


