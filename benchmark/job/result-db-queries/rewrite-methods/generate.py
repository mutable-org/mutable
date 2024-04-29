from typing import DefaultDict
from collections import defaultdict
from copy import deepcopy
from pathlib import Path

from query_utility import Relation, Join, JoinGraph, compute_join_keys
import query_definitions as q_def

OUTPUT_DIR='benchmark/job/result-db-queries/rewrite-methods/'

def write_to_file(output_file: str, text: str, mode: str):
    with open(f'{OUTPUT_DIR}{output_file}', mode) as file:
        file.write(text)

def generate_q3b(directory: str):
    query_graph_q3b = q_def.create_q3b()
    Path(OUTPUT_DIR + directory).mkdir(exist_ok=True)

    generate_default(query_graph_q3b, directory + "/q3b_default.sql")
    generate_RM1(query_graph_q3b, directory + "/q3b_RM1.sql")
    generate_RM2(query_graph_q3b, directory + "/q3b_RM2.sql")
    generate_RM3(query_graph_q3b, directory + "/q3b_RM3.sql")
    generate_RM4(query_graph_q3b, directory + "/q3b_RM4.sql")

def generate_q4a(directory: str):
    query_graph_q4a = q_def.create_q4a()
    Path(OUTPUT_DIR + directory).mkdir(exist_ok=True)

    generate_default(query_graph_q4a, directory + "/q4a_default.sql")
    generate_RM1(query_graph_q4a, directory + "/q4a_RM1.sql")
    generate_RM2(query_graph_q4a, directory + "/q4a_RM2.sql")
    generate_RM3(query_graph_q4a, directory + "/q4a_RM3.sql")
    generate_RM4(query_graph_q4a, directory + "/q4a_RM4.sql")

def generate_q8d(directory: str):
    query_graph_q8d = q_def.create_q8d()
    Path(OUTPUT_DIR + directory).mkdir(exist_ok=True)

    generate_default(query_graph_q8d, directory + "/q8d_default.sql")
    generate_RM1(query_graph_q8d, directory + "/q8d_RM1.sql")
    generate_RM2(query_graph_q8d, directory + "/q8d_RM2.sql")
    generate_RM3(query_graph_q8d, directory + "/q8d_RM3.sql")
    generate_RM4(query_graph_q8d, directory + "/q8d_RM4.sql")

def generate_default(join_graph: JoinGraph, output_file: str):
    query = '\\timing on\n\n'
    # collect projections, relations and filters
    relations: list[Relation] = join_graph.relations
    joins: list[Join] = join_graph.joins
    projections = defaultdict(list) # map relation alias to a list of projection attributes
    filters: list[str] = []
    for r in relations:
        for p in r.projections:
            projections[r.alias].append(p)
        for f in r.filters:
            filters.append(f)

    # generate SELECT clause
    query += 'SELECT'
    num_proj = 0
    for r_alias, projs in projections.items():
        for proj_attr in projs:
            if num_proj != 0:
                query += ','
            query += f'\n\t{r_alias}.{proj_attr}'
            num_proj += 1

    # generate FROM clause
    query += '\nFROM'
    for idx, r in enumerate(relations):
        if idx != 0:
            query += ','
        query += f'\n\t{r.name} AS {r.alias}'

    # generate WHERE clause
    query += '\n WHERE'
    ## filters
    for idx, f in enumerate(filters):
        if idx != 0:
            query += ' AND'
        query += f'\n\t{f}'
    ## joins
    if filters:
        query += ' AND'
    for idx, join in enumerate(joins):
        for lhs_attr, rhs_attr in zip(join.left_attributes, join.right_attributes):
            join_predicate = f'{join.left_relation.alias}.{lhs_attr} = {join.right_relation.alias}.{rhs_attr}'
            query += f'\n\t{join_predicate}'
            if idx != len(joins) - 1: # do NOT emit `AND` for last join predicate
                query += ' AND'

    query += ';'
    query += '\n\n\\timing off'

    # write to file
    write_to_file(output_file, query, 'w')

def generate_RM1(join_graph: JoinGraph, output_file: str):
    # collect projections, relations and filters
    relations = join_graph.relations
    joins = join_graph.joins
    projections = defaultdict(list)
    filters = []
    for r in relations:
        for p in r.projections:
            projections[r.alias].append(p)
        for f in r.filters:
            filters.append(f)

    write_to_file(output_file, f'BEGIN;\n\\timing on', 'w')
    ##### Generate query body, i.e. FROM and WHERE clause, because the query body does not change.
    # generate FROM clause
    from_clause = 'FROM'
    for idx, r in enumerate(relations):
        if idx != 0:
            from_clause += ','
        from_clause += f'\n\t{r.name} AS {r.alias}'

    # generate WHERE clause
    where_clause = 'WHERE'
    ## filters
    for idx, f in enumerate(filters):
        if idx != 0:
            where_clause += ' AND'
        where_clause += f'\n\t{f}'
    ## joins
    if filters:
        where_clause += ' AND'
    for idx, join in enumerate(joins):
        for lhs_attr, rhs_attr in zip(join.left_attributes, join.right_attributes):
            join_predicate = f'{join.left_relation.alias}.{lhs_attr} = {join.right_relation.alias}.{rhs_attr}'
            where_clause += f'\n\t{join_predicate}'
            if idx != len(joins) - 1: # do NOT emit `AND` for last join predicate
                where_clause += ' AND'

    for r_alias, projs in projections.items(): # iterate over relations and their projections
        # generate SELECT clause
        select_clause = 'SELECT DISTINCT'
        for idx, proj_attr in enumerate(projs):
            if idx != 0:
                select_clause += ','
            select_clause += f'\n\t{r_alias}.{proj_attr}'

        query = f'{select_clause}\n{from_clause}\n{where_clause};'

        # write to file
        write_to_file(output_file, f'\n\n{query}', 'a')

    write_to_file(output_file, f'\n\n\\timing off\nCOMMIT;', 'a')

def generate_RM2(join_graph: JoinGraph, output_file: str):
    ### Materialized View
    # collect projections, relations, attributes, and filters
    relations = join_graph.relations
    joins = join_graph.joins
    projections = defaultdict(list)
    attributes = defaultdict(list)
    filters = []
    for r in relations:
        for p in r.projections:
            projections[r.alias].append(p)
        for a in r.attributes:
            attributes[r.alias].append(a)
        for f in r.filters:
            filters.append(f)

    write_to_file(output_file, f'\\timing on\n\n', 'w')

    # generate SELECT clause for mat. view
    mat_view_select = 'SELECT'
    c = 0
    for r_alias, attrs in attributes.items():
        for a in attrs:
            if c != 0:
                mat_view_select += ','
            mat_view_select += f'\n\t{r_alias}.{a} AS {r_alias}_{a}'
            c += 1
    # generate FROM clause for mat. view
    mat_view_from = 'FROM'
    for idx, r in enumerate(relations):
        if idx != 0:
            mat_view_from += ','
        mat_view_from += f'\n\t{r.name} AS {r.alias}'
    # generate WHERE clause for mat. view
    mat_view_where = 'WHERE'
    ## filters
    for idx, f in enumerate(filters):
        if idx != 0:
            mat_view_where += ' AND'
        mat_view_where += f'\n\t{f}'
    ## joins
    if filters:
        mat_view_where += ' AND'
    for idx, join in enumerate(joins):
        for lhs_attr, rhs_attr in zip(join.left_attributes, join.right_attributes):
            join_predicate = f'{join.left_relation.alias}.{lhs_attr} = {join.right_relation.alias}.{rhs_attr}'
            mat_view_where += f'\n\t{join_predicate}'
            if idx != len(joins) - 1: # do NOT emit `AND` for last join predicate
                mat_view_where += ' AND'

    mat_view = f'CREATE MATERIALIZED VIEW q AS\n{mat_view_select}\n{mat_view_from}\n{mat_view_where};'

    write_to_file(output_file, mat_view, 'a')

    ### SELECT DISTINCT
    from_clause = 'FROM q'
    for r_alias, projs in projections.items(): # iterate over relations and their projections
        # generate SELECT clause
        select_clause = 'SELECT DISTINCT'
        for idx, proj_attr in enumerate(projs):
            if idx != 0:
                select_clause += ','
            select_clause += f'\n\t{r_alias}_{proj_attr}'

        query = f'{select_clause}\n{from_clause};'

        # write to file
        write_to_file(output_file, f'\n\n{query}', 'a')

    # Drop MATERIALIZED VIEW
    write_to_file(output_file, f'\n\n\\timing off', 'a')
    write_to_file(output_file, "\n\nDROP MATERIALIZED VIEW q;", 'a')


### Algorithm
# For each relation R that occurs in the projections do:
#   compute all neighbors
#   for each neighbor do: (i.e. create one subquery)
#       if neighbor already processed in one of the previous iterations -> skip
#       compute the reachable set of relations
#       compute join between reachable set
#       include all filters on those relations
#       project on join keys with R
#
### Example Q4
# We have two relations that occur in the projections: {movie_info_idx, title}
# movie_info_idx:
#   neighbors: {title, movie_keyword, info_type}
#   title:
#       reachable(title, {movie_info_idx}): {title, movie_keyword, keyword}
#   movie_keyword:
#       reachable(movie_keyword, {movie_info_idx}): {title, movie_keyword, keyword}
#   info_type:
#       reachable(info_type, {movie_info_idx}): {info_type}
# ==> subquery_relations_to_joins:
#       {title, movie_keyword, keyword} : [movie_info_idx ⋈ title, movie_info_idx ⋈ movie_keyword]
#       {info_type} : [movie_info_idx ⋈ info_type]
#
# title:
#   neighbors: {movie_info_idx, movie_keyword}
#   movie_info_idx:
#       reachable(movie_info_idx, {title}): {keyword, movie_keyword, movie_info_idx, info_type}
#   movie_keyword:
#       reachable(movie_keyword, {title}): {keyword, movie_keyword, movie_info_idx, info_type}
# ==> subquery_relations_to_joins:
#       {keyword, movie_keyword, movie_info_idx, info_type} : [title ⋈ movie_inf_idx, title ⋈ movie_keyword]
def generate_RM3(join_graph: JoinGraph, output_file: str):
    # collect projections, relations, attributes, and filters
    relations = join_graph.relations
    joins = join_graph.joins
    projections = defaultdict(list)
    attributes = defaultdict(list)
    for r in relations:
        for p in r.projections:
            projections[r].append(p)
        for a in r.attributes:
            attributes[r].append(a)
    write_to_file(output_file, 'BEGIN;\n\\timing on\n\n', 'w')

    for r, projs in projections.items():
        select_clause = 'SELECT DISTINCT'
        for idx, proj_attr in enumerate(projs):
            if idx != 0:
                select_clause += ','
            select_clause += f'\n\t{r.alias}.{proj_attr}'

        from_clause = f'FROM {r.name} AS {r.alias}'

        # compute subqueries
        neighbors = join_graph.neighbors(r)
        subquery_relations_to_joins = defaultdict(list) # [frozenset[Relation], list[Join]]
        excluded_relations = set([r])
        for neighbor_relation in neighbors:
            reachable_relations = join_graph.reachable(neighbor_relation, deepcopy(excluded_relations))
            join = join_graph.get_join(r, neighbor_relation)
            assert(join is not None)
            subquery_relations_to_joins[frozenset(reachable_relations)].append(join)

        subqueries = []
        num_subquery = 0
        for subquery_relations, joins in subquery_relations_to_joins.items():
            # create header, d.h. IN statement that simulates the "join predicate"
            in_clause = '\t'
            if num_subquery != 0:
                in_clause += ') AND '
            in_clause += '(' #) to prevent idendation
            outer_join_keys, inner_join_keys = compute_join_keys(r, joins)
            assert(len(outer_join_keys.keys()) == 1)
            for idx, join_attribute in enumerate(outer_join_keys[r]):
                if idx != 0:
                    in_clause += ','
                in_clause += f'{r.alias}.{join_attribute}'
            in_clause += ') IN (' #)

            # create subquery body
            subquery_select = '\t\tSELECT'
            num_inner_join_keys = 0
            for inner_relation, join_attributes in inner_join_keys.items():
                for join_attr in join_attributes:
                    if num_inner_join_keys != 0:
                        subquery_select += ','
                    subquery_select += f'\n\t\t\t{inner_relation.alias}.{join_attr}'
                    num_inner_join_keys += 1

            subquery_from = '\t\tFROM'

            for idx, inner_relation in enumerate(subquery_relations):
                if idx != 0:
                    subquery_from += ','
                subquery_from += f'\n\t\t\t{inner_relation.name} AS {inner_relation.alias}'

            subquery_where = '\t\tWHERE'
            num_predicates = 0
            for inner_relation in subquery_relations:
                for filter in inner_relation.filters:
                    if num_predicates != 0:
                        subquery_where += ' AND'
                    subquery_where += f'\n\t\t\t{filter}'
                    num_predicates += 1
            subquery_joins = join_graph.get_joins(subquery_relations)
            for inner_join in subquery_joins:
                if num_predicates != 0:
                    subquery_where += ' AND'
                for lhs_attr, rhs_attr in zip(inner_join.left_attributes, inner_join.right_attributes):
                    join_predicate = f'{inner_join.left_relation.alias}.{lhs_attr} = {inner_join.right_relation.alias}.{rhs_attr}'
                    subquery_where += f'\n\t\t\t{join_predicate}'
                    num_predicates += 1

            subquery = f'{in_clause}\n{subquery_select}\n{subquery_from}\n{subquery_where}'
            subqueries.append(subquery)
            num_subquery += 1
        subqueries[-1] += '\n\t);'

        where_clause = 'WHERE'

        # add filters before subqueries
        for idx, f in enumerate(r.filters):
            if idx != 0:
                where_clause += ' AND'
            where_clause += f'\n\t{f}'

        # add subqueries
        if (len(r.filters) > 0):
            where_clause += ' AND'
        for idx, subq in enumerate(subqueries):
            where_clause += f'\n\t{subq}'

        query = f'{select_clause}\n{from_clause}\n{where_clause}\n\n'
        write_to_file(output_file, query, 'a')

    write_to_file(output_file, f'\\timing off\nCOMMIT;', 'a')

def generate_RM4(join_graph: JoinGraph, output_file: str):
    relations = join_graph.relations
    joins = join_graph.joins
    projections: DefaultDict[Relation, list[str]] = defaultdict(list)
    attributes = defaultdict(list)
    filters = []
    for r in relations:
        for p in r.projections:
            projections[r].append(p)
        for a in r.attributes:
            attributes[r.alias].append(a)
        for f in r.filters:
            filters.append(f)

    write_to_file(output_file, '\\timing on\n\n', 'w')

    mat_view = 'CREATE MATERIALIZED VIEW q AS'

    # MV select
    mat_view_select = 'SELECT' # for each relation that occurs in the projection -> project to primary key
    num_projections = 0
    for r, _ in projections.items():
        if num_projections != 0:
            mat_view_select += ','
        mat_view_select += f'\n\t{r.alias}.id AS {r.alias}_id'
        num_projections += 1

    # MV from
    mat_view_from = 'FROM'
    for idx, r in enumerate(relations):
        if idx != 0:
            mat_view_from += ','
        mat_view_from += f'\n\t{r.name} AS {r.alias}'

    # MV where
    mat_view_where = 'WHERE'
    for idx, f in enumerate(filters):
        if idx != 0:
            mat_view_where += ' AND'
        mat_view_where += f'\n\t{f}'

    # MV joins
    if filters:
        mat_view_where += ' AND'
    for idx, join in enumerate(joins):
        for lhs_attr, rhs_attr in zip(join.left_attributes, join.right_attributes):
            join_predicate = f'{join.left_relation.alias}.{lhs_attr} = {join.right_relation.alias}.{rhs_attr}'
            mat_view_where += f'\n\t{join_predicate}'
            if idx != len(joins) - 1: # do NOT emit `AND` for last join predicate
                mat_view_where += ' AND'

    mat_view += f'\n{mat_view_select}\n{mat_view_from}\n{mat_view_where};'

    if filters:
        mat_view_where += ' AND'
    for idx, join in enumerate(joins):
        for lhs_attr, rhs_attr in zip(join.left_attributes, join.right_attributes):
            join_predicate = f'{join.left_relation.alias}.{lhs_attr} = {join.right_relation.alias}.{rhs_attr}'
            mat_view_where += f'\n\t{join_predicate}'
            if idx != len(joins) - 1: # do NOT emit `AND` for last join predicate
                mat_view_where += ' AND'

    write_to_file(output_file, mat_view, 'a')

    # SELECT DISTINCT Queries
    for r, projs in projections.items():
        select_clause = 'SELECT DISTINCT'
        for idx, proj_attr in enumerate(projs):
            if idx != 0:
                select_clause += ','
            select_clause += f'\n\t{r.alias}.{proj_attr}'

        from_clause = f'FROM {r.name} AS {r.alias}'

        where_clause = f'WHERE {r.alias}.id IN (\n' #)
        subquery = f'\tSELECT\n\t\t{r.alias}_id\n\tFROM q\n'
        where_clause += f'{subquery})'

        query = f'\n\n{select_clause}\n{from_clause}\n{where_clause};'
        write_to_file(output_file, query, 'a')

    write_to_file(output_file, '\n\n\\timing off\n\nDROP MATERIALIZED VIEW q;', 'a')


if __name__ == '__main__':
    generate_q3b('q3b')
    generate_q4a('q4a')
    generate_q8d('q8d')
