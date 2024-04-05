import argparse
import subprocess
import json
from itertools import combinations
from pathlib import Path
from typing import Any
from query_utility import Relation, JoinGraph
import query_definitions as q_def

DATABASE='result_db'

def write_to_file(output_file: str, text: str, mode: str):
    with open(output_file, mode) as file:
        file.write(text)

def create_injected_cardinalities(query_name: str, config) -> None:
    # enumerate each subproblem (of sizes 1, 2, 3, ...)
    # for each subproblem, check if connected
    # create the following query
    #  SELECT COUNT(*)
    #  FROM relations
    #  WHERE  filter_predicates AND join_predicates;
    join_graph: JoinGraph = getattr(q_def, f"create_{query_name}")() # execute the function `create_<query_name>` of module `q_def`
    n = len(join_graph.relations)
    query_file_dir = f'{config["output_dir"]}{query_name}_cardinalities/'
    Path(query_file_dir).mkdir(exist_ok=True)

    cardinality_file = f'{config["output_dir"]}/{query_name}_injected_cardinalities.json'
    cardinality_text = '{\n'
    cardinality_text += f'\t"{DATABASE}": [\n'
    for i in range(n + 1):
        for subproblem in combinations(join_graph.relations,  i):
            S: set[Relation] = set(subproblem)
            # check connectedness
            if not join_graph.connected(S):
                continue

            total_num_filters = 0
            select_clause = 'SELECT COUNT(*)'
            from_clause = 'FROM'
            for idx, r in enumerate(S):
                total_num_filters += len(r.filters)
                if idx != 0:
                    from_clause += ','
                from_clause += f'\n\t{r.name} AS {r.alias}'

            joins = join_graph.get_joins(S)
            if (total_num_filters == 0 and len(joins) == 0):
                # edge case: we do not produce an empty where clause if no filters and no joins are present
                where_clause = ''
            else:
                where_clause = 'WHERE'
                num_filter = 0
                for r in S:
                    for f in r.filters: # add filters
                        if num_filter != 0:
                            where_clause += ' AND'
                        where_clause += f'\n\t{f}'
                        num_filter += 1

                # compute join predicates
                if total_num_filters != 0 and len(joins) !=0:
                    where_clause += ' AND'
                for idx, join in enumerate(join_graph.get_joins(S)):
                    for lhs_attr, rhs_attr in zip(join.left_attributes, join.right_attributes):
                        join_predicate = f'{join.left_relation.alias}.{lhs_attr} = {join.right_relation.alias}.{rhs_attr}'
                        where_clause += f'\n\t{join_predicate}'
                        if idx != len(joins) - 1:
                            where_clause += ' AND'

            query = f'{select_clause}\n{from_clause}\n{where_clause};'

            query_file = f'{query_file_dir}{query_name}'
            for r in S:
                query_file += f'-{r}'
            query_file += '.sql'
            write_to_file(query_file, query, 'w')

            # use postgres to execute each file and compute count(*)
            command = f"psql -U {config['user']} -d {config['database']} -f {query_file} | sed '3!d'"

            completed_proc = subprocess.run(command, capture_output=True, text=True, shell=True)
            return_code = completed_proc.returncode
            # TODO: the exit code of a pipeline is the exit code of the last command, i.e. not the psql command
            if return_code: # something went wrong during execution
                print(f'Failure during execution of `{command}` with return code {return_code}.')
            cardinality = completed_proc.stdout.strip() # remove whitespaces

            # write cardinality file
            relation_aliases = [ r.alias for r in S]
            line = f'"relations": {json.dumps(relation_aliases)}, "size": {cardinality}'
            cardinality_text += '\t\t{' + line + '}'
            if (len(S) != join_graph.num_relations()):
                cardinality_text += ','
            cardinality_text += '\n'

    cardinality_text += '\t]\n'
    cardinality_text += '}'
    write_to_file(cardinality_file, cardinality_text, 'w')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog = 'Generate Injected Cardinalities',
                                     description = '''Compute the real cardinality for each subproblem of a given query
                                     using PostgreSQL.''')

    parser.add_argument("-u", "--user", default="joris")
    parser.add_argument("-d", "--database", default="imdb")
    parser.add_argument("-q", "--queries", default=["imdb"], nargs='+', type=str)
    parser.add_argument("-o", "--output_dir", default="benchmark/job/result-db-queries/")

    args = parser.parse_args()
    config: dict[str, Any] = vars(args)

    if "imdb" in config['queries']:
        assert len(config['queries']) == 1, "list of queries may only contain 'imdb' or actual queries"
        config['queries'] = ["q1a", "q2a", "q3b", "q4a", "q5b", "q6a", "q7b", "q8d", "q9b", "q10a", "q13a", "q17a"]
    elif "star" in config['queries']:
        assert len(config['queries']) == 1, "list of queries may only contain 'star' or actual queries"
        config['queries'] = [
                             'star_joins_1',
                             'star_joins_2',
                             'star_joins_3',
                             'star_joins_4',
                             'star_proj_2_dim',
                             'star_proj_3_dim',
                             'star_proj_4_dim',
                             'star_proj_fact_1_dim',
                             'star_proj_fact_2_dim',
                             'star_proj_fact_3_dim',
                             'star_proj_fact_4_dim',
                             'star_sel_10',
                             'star_sel_20',
                             'star_sel_30',
                             'star_sel_40',
                             'star_sel_50',
                             'star_sel_60',
                             'star_sel_70',
                             'star_sel_80',
                             'star_sel_90',
                             'star_sel_100',
                            ]

    if __debug__: print('Configuration:', config)

    for query in config['queries']:
        create_injected_cardinalities(query, config)
