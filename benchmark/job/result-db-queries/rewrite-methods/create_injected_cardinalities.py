import argparse
import subprocess
import json
from itertools import combinations
from pathlib import Path
from typing import Any
from query_utility import Relation, JoinGraph
import query_definitions as q_def

OUTPUT_DIR='benchmark/job/result-db-queries/'

def write_to_file(output_file: str, text: str, mode: str):
    with open(output_file, mode) as file:
        file.write(text)

def create_injected_cardinalities(join_graph: JoinGraph, query_name: str, config) -> None:
    # enumerate each subproblem (of sizes 1, 2, 3, ...)
    # for each subproblem, check if connected
    # create the following query
    #  SELECT COUNT(*)
    #  FROM relations
    #  WHERE  filter_predicates AND join_predicates;
    n = len(join_graph.relations)
    query_file_dir = f'{OUTPUT_DIR}{query_name}_cardinalities/'
    Path(query_file_dir).mkdir(exist_ok=True)

    cardinality_file = f'{OUTPUT_DIR}/{query_name}_injected_cardinalities.json' 
    cardinality_text = '{\n'
    cardinality_text += '\t"job": [\n'
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
                                     description = '''Compute the real cardinality for each subproblem of (multiple)
                                     given JOB queries.''')

    parser.add_argument("-u", "--user", default="joris", help="")
    parser.add_argument("-d", "--database", default="imdb", help="")

    args = parser.parse_args()
    config: dict[str, Any] = vars(args)
    if __debug__: print('Configuration:', config)

    # q1a = q_def.create_q1a()
    # create_injected_cardinalities(q1a, "q1a", config)

    # q2a = q_def.create_q2a()
    # create_injected_cardinalities(q2a, "q2a", config)

    # q3b = q_def.create_q3b()
    # create_injected_cardinalities(q3b, "q3b", config)

    # q4a = q_def.create_q4a()
    # create_injected_cardinalities(q4a, "q4a", config)

    # q5b = q_def.create_q5b()
    # create_injected_cardinalities(q5b, "q5b", config)

    # q6a = q_def.create_q6a()
    # create_injected_cardinalities(q6a, "q6a", config)

    # q7b = q_def.create_q7b()
    # create_injected_cardinalities(q7b, "q7b", config)

    # q8d = q_def.create_q8d()
    # create_injected_cardinalities(q8d, "q8d", config)

    # q9b = q_def.create_q9b()
    # create_injected_cardinalities(q9b, "q9b", config)

    # q10a = q_def.create_q10a()
    # create_injected_cardinalities(q10a, "q10a", config)

    # q13a = q_def.create_q13a()
    # create_injected_cardinalities(q13a, "q13a", config)

    q17a = q_def.create_q17a()
    create_injected_cardinalities(q17a, "q17a", config)
