from collections import defaultdict
from pathlib import Path
from typing import DefaultDict, Any
import argparse
import glob
import itertools
import os
import re
import subprocess

from query_utility import Relation, Join, JoinGraph, shortest_path
import query_definitions as q_def

numeric_attributes = [
    'id', 'person_id', 'movie_id', 'kind_id', 'production_year', 'season_nr', 'episode_nr', 'person_role_id',
    'nr_order', 'role_id', 'imdb_id', 'company_id', 'company_type_id', 'info_type_id', 'keyword_id', 'linked_movie_id',
    'link_type_id', 'person_type_id', 'episode_of_id'
]

def write_to_file(output_dir: str, output_file: str, text: str, mode: str = 'w') -> None:
    with open(f'{output_dir}/{output_file}', mode) as file:
        file.write(text)

def compute_result_set_size(query: str, config):
    output_dir = config['output_dir'] + query
    Path(output_dir).mkdir(exist_ok=True)

    query_graph: JoinGraph = getattr(q_def, f"create_{query}")() # execute the function `create_<query>` of module `q_def`

    projections = defaultdict(set)
    filters = []

    # relations
    from_clause = f'FROM'
    for i, r in enumerate(query_graph.relations):
        for p in r.projections:
            projections[r].add(p)
        for f in r.filters:
            filters.append(f)
        if i != 0:
            from_clause += ','
        from_clause += f'\n\t{r} AS {r.alias}'

    # filter
    where_clause = f'\nWHERE'
    for i, f in enumerate(filters):
        if i != 0:
            where_clause += ' AND'
        where_clause += f'\n\t{f}'

    # joins
    if filters:
        where_clause += ' AND'
    for idx, join in enumerate(query_graph.joins):
        for lhs_attr, rhs_attr in zip(join.left_attributes, join.right_attributes):
            join_predicate = f'{join.left_relation.alias}.{lhs_attr} = {join.right_relation.alias}.{rhs_attr}'
            where_clause += f'\n\t{join_predicate}'
            if idx != len(query_graph.joins) - 1: # do NOT emit `AND` for last join predicate
                where_clause += ' AND'

    query_body = f'{from_clause}{where_clause}'

    ########## Single Table ##########
    select_clause = 'SELECT'
    select_clause += '\n\tCOUNT(*)'
    for r, attributes in projections.items():
        for attr in attributes:
            select_clause += ','
            if attr in numeric_attributes:
                select_clause += f'\n\tCOUNT({r.alias}.{attr}) * 4'
            else:
                select_clause += f'\n\tSUM(LENGTH({r.alias}.{attr}))'
    single_table = f'{select_clause}\n{query_body};'
    write_to_file(output_dir, 'single_table.sql', single_table)

    ########## Result DB w/o post-join information ##########
    def create_result_db_query(projections, filename: str):
        for r, projs in projections.items():
            select_subquery = f'SELECT DISTINCT'
            select_outer = f'SELECT\n\tCOUNT(*),'
            for i, attr in enumerate(projs):
                if i != 0:
                    select_subquery += ','
                    select_outer += ','
                select_subquery += f'\n\t{r.alias}.{attr}'
                if attr in numeric_attributes:
                    select_outer += f'\n\tCOUNT(nested.{attr}) * 4'
                else:
                    select_outer += f'\n\tSUM(LENGTH(nested.{attr}))'
            query_text = f'{select_outer}\nFROM (\n{select_subquery}\n{query_body}) AS nested'
            write_to_file(output_dir, f'{filename}_{r}.sql', query_text)

    create_result_db_query(projections, 'rdb_wo_post_join_info')

    # compute the extended set of relations and attributes required for post-join
    for start, end in itertools.combinations(projections.keys(), 2):
        # TODO: we might have to compute the minimal connected subgraph for projections that are not directly connected, for
        # now just compute the shortest path for all combinations of relations
        relations_on_shortest_path = shortest_path(query_graph, start, end)
        # iterate over shortest_path
        for i in range(len(relations_on_shortest_path) - 1):
            # take join  of shortest_path[i] and shortest_path[i+1] and add join attributes to projections
            join = query_graph.get_join(relations_on_shortest_path[i], relations_on_shortest_path[i+1])
            projections[join.left_relation].update(join.left_attributes)
            projections[join.right_relation].update(join.right_attributes)

    create_result_db_query(projections, 'rdb_w_post_join_info')

    # execute queries and write result set sizes to output file
    for f in sorted(glob.glob(f'{output_dir}/*.sql')): # iterate over all files in rewrite-methods/<query>/*.sql
        # compute method name
        basename = os.path.basename(f).rsplit('.', 1)[0] # take filename and remove extension
        method = ''
        relation = ''
        if 'single_table' in basename:
            method = 'Single Table'
        else:
            symbol = '_'
            parts = basename.split(symbol)
            position = 5
            method = symbol.join(parts[:position])
            relation = symbol.join(parts[position:])

        command = f"psql -U {config['user']} -d {config['database']} -f {f} | grep -E '^\s+[0-9]+'"
        completed_proc = subprocess.run(command, capture_output=True, text=True, shell=True)
        output = completed_proc.stdout
        output = re.sub(r'\s+', '', output) # remove all whitespaces
        output = output.split('|')
        count = output[0] # first entry is always the count(*) result
        size = sum([ int(x) for x in output[1:]])

        measurements = ','.join([config['database'], query, method, relation, str(count), str(size)])
        write_to_file(config['output_dir'], config['output_file'], '\n' + measurements, 'a')


if __name__ == '__main__':
    # Command Line Arguments
    parser = argparse.ArgumentParser(prog = 'Compute result set sizes',
                                     description = '''Script to compute the result set sizes for a set of queries.''')

    parser.add_argument("-u", "--user", default='joris')
    parser.add_argument("-d", "--database", default='imdb')
    parser.add_argument("-q", "--queries", default=["imdb"], nargs='+', type=str)
    parser.add_argument("-o", "--output_dir", default="benchmark/job/result-db-queries/result-set-sizes/")
    parser.add_argument("-of", "--output_file", default="result-set-sizes.csv")

    args = parser.parse_args()
    config: dict[str, Any] = vars(args)

    config['queries'] = ["q1a", "q2a", "q3b", "q4a", "q6a", "q7b", "q8d", "q9b", "q10a", "q13a", "q17a"]

    if __debug__: print('Configuration:', config)

    header = 'database,query,method,relation,count,size'
    write_to_file(config['output_dir'], config['output_file'], header)

    for query in config['queries']:
        compute_result_set_size(query, config)
