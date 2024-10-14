#!/usr/bin/env python3

import argparse
import subprocess
import json
from itertools import combinations
from functools import reduce
from pathlib import Path
from typing import Any
from query_utility import Relation, JoinGraph
import query_definitions as q_def
import shutil

OUTPUT_DIR = "benchmark/result-db-eval"


def write_to_file(output_file: str, text: str):
    with open(output_file, "w") as file:
        file.write(text)


def create_injected_cardinalities(
    join_graph: JoinGraph, query_name: str, config, cardinality_file_name: str,
) -> None:
    # enumerate each subproblem (of sizes 1, 2, 3, ...)
    # for each subproblem, check if connected
    # create the following query
    #  SELECT COUNT(*)
    #  FROM relations
    #  WHERE  filter_predicates AND join_predicates;

    def generate_from_and_where_clause(relation_set: set[Relation]) -> tuple[str, str]:
        from_clause = "FROM"
        total_num_filters = 0
        for idx, r in enumerate(relation_set):
            total_num_filters += len(r.filters)
            if idx != 0:
                from_clause += ","
            from_clause += f"\n\t{r.name} AS {r.alias}"

        joins = join_graph.get_joins(relation_set)
        if total_num_filters == 0 and len(joins) == 0:
            # edge case: we do not produce an empty where clause if no filters and no joins are present
            where_clause = ""
        else:
            where_clause = "WHERE"
            num_filter = 0
            for r in relation_set:
                for f in r.filters:  # add filters
                    if num_filter != 0:
                        where_clause += " AND"
                    where_clause += f"\n\t{f}"
                    num_filter += 1

            # compute join predicates
            if total_num_filters != 0 and len(joins) != 0:
                where_clause += " AND"
            for idx, join in enumerate(join_graph.get_joins(relation_set)):
                for lhs_attr, rhs_attr in zip(
                    join.left_attributes, join.right_attributes
                ):
                    join_predicate = f"{join.left_relation.alias}.{lhs_attr} = {join.right_relation.alias}.{rhs_attr}"
                    where_clause += f"\n\t{join_predicate}"
                    if idx != len(joins) - 1:
                        where_clause += " AND"
        return from_clause, where_clause

    def create_select_for_view(relation_set: set[Relation]) -> str:
        # Make each returned attribute unique by prefixing it with its relation name
        adapted_select_clause = f"SELECT "
        for idx_S, r in enumerate(relation_set):
            for idx_attr, attr in enumerate(r.attributes):
                if idx_S == len(relation_set) - 1 and idx_attr == len(r.attributes) - 1:
                    adapted_select_clause += f"{r.alias}.{attr} AS {r.name}_{attr}"
                else:
                    adapted_select_clause += f"{r.alias}.{attr} AS {r.name}_{attr}, "
        return adapted_select_clause

    def create_reduction(
        main_set: set[Relation], reducer_sets: list[set[Relation]]
    ) -> str:
        reduction_str = ""
        reducer_views = ""
        for idx, reducer_set in enumerate(reducer_sets):
            reducer_select_clause = create_select_for_view(reducer_set)
            (
                reducer_from_clause,
                reducer_where_clause,
            ) = generate_from_and_where_clause(reducer_set)
            reducer_query = f"{reducer_select_clause}\n{reducer_from_clause}\n{reducer_where_clause};"
            reducer_view = f"DROP VIEW IF EXISTS reducer_{idx};\nCREATE VIEW reducer_{idx} AS\n{reducer_query}\n\n"
            reducer_views += reducer_view

        final_select = "SELECT COUNT(*)"
        final_from = "FROM to_be_reduced AS tbr"
        final_where = "WHERE "

        for set_idx, reducer_set in enumerate(reducer_sets):
            if set_idx == 0:
                final_where += (
                    f"EXISTS (SELECT 1 FROM reducer_{set_idx} AS r_{set_idx}_t WHERE "
                )
            else:
                final_where += f"AND EXISTS (SELECT 1 FROM reducer_{set_idx} AS r_{set_idx}_t WHERE "
            l_r_joins = join_graph.get_joins_between_left_right(reducer_set, main_set)
            for idx, join in enumerate(l_r_joins):
                for lhs_attr, rhs_attr in zip(
                    join.left_attributes, join.right_attributes
                ):
                    if join.left_relation in reducer_set:
                        left, right = f"r_{set_idx}_t", "tbr"
                    else:
                        right, left = f"r_{set_idx}_t", "tbr"
                    join_predicate = f"{left}.{join.left_relation}_{lhs_attr} = {right}.{join.right_relation}_{rhs_attr}"
                    final_where += f"\n\t{join_predicate}"
                    if idx != len(l_r_joins) - 1:
                        final_where += " AND"
                    else:
                        final_where += ")\n"

        final_query = f"{view_query}\n\n{reducer_views}\n\n{final_select}\n{final_from}\n{final_where};"

        query_file = f"{query_file_dir}{query_name}"
        for r in main_set:
            query_file += f"-{r}"
        query_file += f"--"
        for r_set in reducer_sets:
            for r in r_set:
                query_file += f"-{r}"
        query_file += ".sql"
        write_to_file(query_file, final_query)

        # use postgres to execute each file and compute count(*)
        command = f"psql -U {config['user']} -d {config['database']} -f {query_file} | sed '{5 + 2*len(reducer_sets)}!d'"

        completed_proc = subprocess.run(
            command, capture_output=True, text=True, shell=True
        )
        return_code = completed_proc.returncode
        # TODO: the exit code of a pipeline is the exit code of the last command, i.e. not the psql command
        if return_code:  # something went wrong during execution
            print(
                f"Failure during execution of `{command}` with return code {return_code}."
            )
        reduced_cardinality = completed_proc.stdout.strip()  # remove whitespaces
        print(
            f"{main_set}:{reduce(lambda x, y: x | y, reducer_sets)}:{reduced_cardinality}"
        )

        reducer_relation_aliases = [
            r.alias for r in reduce(lambda x, y: x | y, reducer_sets)
        ]
        reduction_str += f'"right_relations": {json.dumps(reducer_relation_aliases)}, "size": {reduced_cardinality}'
        reduction_str += "}"
        return reduction_str

    n = len(join_graph.relations)
    query_file_dir = f"{OUTPUT_DIR}{query_name}_cardinalities/"
    Path(query_file_dir).mkdir(exist_ok=True)

    database_name: str = config['card_entry']
    cardinality_text = "{\n"
    cardinality_text += f'\t"{database_name}": [\n'
    for i in range(n + 1):
        for subproblem in combinations(join_graph.relations, i):
            S: set[Relation] = set(subproblem)
            # check connectedness
            if not join_graph.connected(S):
                continue

            select_clause = "SELECT COUNT(*)"
            from_clause, where_clause = generate_from_and_where_clause(S)
            query = f"{select_clause}\n{from_clause}\n{where_clause};"

            query_file = f"{query_file_dir}{query_name}"
            for r in S:
                query_file += f"-{r}"
            query_file += ".sql"
            write_to_file(query_file, query)

            # use postgres to execute each file and compute count(*)
            command = f"psql -U {config['user']} -d {config['database']} -f {query_file} | sed '3!d'"

            completed_proc = subprocess.run(
                command, capture_output=True, text=True, shell=True
            )
            return_code = completed_proc.returncode
            # TODO: the exit code of a pipeline is the exit code of the last command, i.e. not the psql command
            if return_code:  # something went wrong during execution
                print(
                    f"Failure during execution of `{command}` with return code {return_code}."
                )
            cardinality = completed_proc.stdout.strip()  # remove whitespaces
            print(S, cardinality)

            adapted_select_clause = create_select_for_view(S)

            view_query = f"DROP VIEW IF EXISTS to_be_reduced;\nCREATE VIEW to_be_reduced AS\n{adapted_select_clause}\n{from_clause}\n{where_clause};"

            # Compute (relevant) semi-join reductions for Yannakakis optimizations
            reductions = f"["
            S_neighbors = join_graph.neighbors_of_set(S)
            checked = set()
            for idx, r in enumerate(S_neighbors):
                reducer_relations = join_graph.reachable(r, S)
                red_tuple = tuple(sorted(reducer_relations))
                if red_tuple in checked:
                    continue
                checked.add(red_tuple)
                if idx == 0:
                    reductions += "{"
                else:
                    reductions += ", {"
                reductions += create_reduction(S, [reducer_relations])
            checked_combs = set()
            for j in range(2, len(checked) + 1):
                for red_combination in combinations(checked, j):
                    reducer_relations = set()
                    for s in red_combination:
                        reducer_relations |= set(s)
                    red_comb_tuple = tuple(sorted(reducer_relations))
                    if red_comb_tuple in checked_combs | checked:
                        continue
                    checked_combs.add(red_comb_tuple)
                    reductions += ", {"
                    reductions += create_reduction(
                        S, [set(tup) for tup in red_combination]
                    )

            reductions += "]"

            # write cardinality file
            relation_aliases = [r.alias for r in S]
            line = f'"relations": {json.dumps(relation_aliases)}, "size": {cardinality}, "reductions": {reductions}'
            cardinality_text += "\t\t{" + line + "}"
            if len(S) != join_graph.num_relations():
                cardinality_text += ","
            cardinality_text += "\n"

    cardinality_text += "\t]\n"
    cardinality_text += "}"
    write_to_file(cardinality_file_name, cardinality_text)
    shutil.rmtree(query_file_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="Generate Injected Cardinalities",
        description="""Compute the real cardinality for each subproblem of given queries.""",
    )

    parser.add_argument("-u", "--user", default="postgres", help="")

    args = parser.parse_args()
    config: dict[str, Any] = vars(args)
    if __debug__:
        print("Configuration:", config)

    config["database"] = "imdb"
    config["card_entry"] = "job"

    q1a = q_def.create_q1a()
    create_injected_cardinalities(q1a, "q1a", config, f"{OUTPUT_DIR}/job/q1a_injected_cardinalities.json")

    q2a = q_def.create_q2a()
    create_injected_cardinalities(q2a, "q2a", config, f"{OUTPUT_DIR}/job/q2a_injected_cardinalities.json")

    q3b = q_def.create_q3b()
    create_injected_cardinalities(q3b, "q3b", config, f"{OUTPUT_DIR}/job/q3b_injected_cardinalities.json")

    q4a = q_def.create_q4a()
    create_injected_cardinalities(q4a, "q4a", config, f"{OUTPUT_DIR}/job/q4a_injected_cardinalities.json")

    q5b = q_def.create_q5b()
    create_injected_cardinalities(q5b, "q5b", config, f"{OUTPUT_DIR}/job/q5b_injected_cardinalities.json")

    q7b = q_def.create_q7b()
    create_injected_cardinalities(q7b, "q7b", config, f"{OUTPUT_DIR}/job/q7b_injected_cardinalities.json")

    q9b = q_def.create_q9b()
    create_injected_cardinalities(q9b, "q9b", config, f"{OUTPUT_DIR}/job/q9b_injected_cardinalities.json")

    q10a = q_def.create_q10a()
    create_injected_cardinalities(q10a, "q10a", config, f"{OUTPUT_DIR}/job/q10a_injected_cardinalities.json")

    config["database"] = "synthetic"
    config["card_entry"] = "synthetic"

    selectivities = [1800, 1400, 1000, 600, 200]
    for index, selectivity in enumerate(reversed(selectivities)):
        redundant_graph = q_def.create_synthetic_chain_join(selectivity)
        create_injected_cardinalities(redundant_graph, f"{(2*index) + 1}", config, f'{OUTPUT_DIR}/synthetic/experiments_chain/{(2*index) + 1}_injected_cardinalities.json')

    for index, selectivity in enumerate(reversed(selectivities)):
        redundant_graph = q_def.create_synthetic_cycle_join(selectivity)
        create_injected_cardinalities(redundant_graph, f"{(2*index) + 1}", config, f'{OUTPUT_DIR}/synthetic/experiments_cycle/{(2*index) + 1}_injected_cardinalities.json')

    for index, selectivity in enumerate(reversed(selectivities)):
        redundant_graph = q_def.create_synthetic_tvc_join(selectivity)
        create_injected_cardinalities(redundant_graph, f"{(2*index) + 1}", config, f'{OUTPUT_DIR}/synthetic/experiments_tvc/{(2*index) + 1}_injected_cardinalities.json')




