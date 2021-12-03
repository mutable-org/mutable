#!env python3

import argparse
import io
import itertools
import sys


DATABASE = 'db'
RELATION_SIZE_MIN = 10
RELATION_SIZE_MAX = 100
JOIN_SELECTIVITY_MIN = .01
JOIN_SELECTIVITY_MAX = .1


#=======================================================================================================================
# Helper functions
#=======================================================================================================================

def powerset(iterable):
    s = list(iterable)
    return itertools.chain.from_iterable(itertools.combinations(s, r) for r in range(len(s)+1))


#=======================================================================================================================
# Generator functions
#=======================================================================================================================

def write_header(file: io.TextIOBase):
    print(f'CREATE DATABASE {DATABASE};\nUSE {DATABASE};', file=file)

def write_query_head(file: io.TextIOBase, relations: list):
    print('SELECT ', file=file, end='')
    print(', '.join(map(lambda R: f'{R}.id', relations)), file=file)
    print('FROM ', file=file, end='')
    print(', '.join(relations), file=file)
    print('WHERE', file=file)

#===== Chain ===========================================================================================================

def write_chain_schema(file: io.TextIOBase, relations: list):
    write_header(file)
    for i in range(len(relations)):
        print(f'\nCREATE TABLE {relations[i]} (', file=file, end='')
        print(f'\n    id INT(4)', file=file, end='')
        if i > 0:
            print(f',\n    fid_{relations[i-1]} INT(4)', file=file, end='')
        print(f'\n);', file=file)

def write_chain_query(file: io.TextIOBase, relations: list):
    write_query_head(file, relations)
    for i in range(len(relations)-1):
        if i != 0:
            print(' AND', file=file)
        print(f'  {relations[i]}.id = {relations[i+1]}.fid_{relations[i]}', file=file, end='')
    print(';', file=file)

#===== Cycle ===========================================================================================================

def write_cycle_schema(file: io.TextIOBase, relations: list):
    write_header(file)
    for i in range(len(relations)):
        print(f'\nCREATE TABLE {relations[i]} (', file=file, end='')
        print(f'\n    id INT(4)', file=file, end='')
        if i == 0:
            print(f',\n    fid_{relations[-1]} INT(4)', file=file, end='')
        else:
            print(f',\n    fid_{relations[i-1]} INT(4)', file=file, end='')
        print(f'\n);', file=file)

def write_cycle_query(file: io.TextIOBase, relations: list):
    write_query_head(file, relations)
    for i in range(len(relations)-1):
        if i != 0:
            print(' AND', file=file)
        print(f'  {relations[i]}.id = {relations[i+1]}.fid_{relations[i]}', file=file, end='')
    print(' AND', file=file)
    print(f'  {relations[-1]}.id = {relations[0]}.fid_{relations[-1]}', file=file, end='')
    print(';', file=file)

#===== Star ============================================================================================================

def write_star_schema(file: io.TextIOBase, relations: list):
    write_header(file)
    print(f'\nCREATE TABLE {relations[0]} (', file=file, end='')
    print(f'\n    id INT(4)', file=file, end='')
    for i in range(1, len(relations)):
        print(f',\n    fid_{relations[i]} INT(4)', file=file, end='')
    print(f'\n);', file=file)

    for i in range(1, len(relations)):
        print(f'\nCREATE TABLE {relations[i]} (', file=file, end='')
        print(f'\n    id INT(4)', file=file, end='')
        print(f'\n);', file=file)

def write_star_query(file: io.TextIOBase, relations: list):
    write_query_head(file, relations)
    for i in range(1, len(relations)):
        if i != 1:
            print(' AND', file=file)
        print(f'  {relations[0]}.fid_{relations[i]} = {relations[i]}.id', file=file, end='')
    print(';', file=file)

#===== Clique ==========================================================================================================

def write_clique_schema(file: io.TextIOBase, relations: list):
    write_header(file)

    for i in range(0, len(relations)):
        print(f'\nCREATE TABLE {relations[i]} (', file=file, end='')
        print(f'\n    id INT(4)', file=file, end='')
        for j in range(i + 1, len(relations)):
            print(f',\n    fid_{relations[j]} INT(4)', file=file, end='')
        print(f'\n);', file=file)

def write_clique_query(file: io.TextIOBase, relations: list):
    write_query_head(file, relations)
    for i in range(len(relations)-1):
        for j in range(i+1, len(relations)):
            if not (i == 0 and j == 1):
                print(' AND', file=file)
            print(f'  {relations[i]}.fid_{relations[j]} = {relations[j]}.id', file=file, end='')

    print(';', file=file)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate problem statements in form of a SQL query and a JSON '
                                                 'specification of cardinalities of subproblems.')
    parser.add_argument('-n', help='Number of relations in the problem', dest='num_relations', type=int, default=3,
                        metavar='N')
    parser.add_argument('-t', help='Type of problem (chain, cycle, star, clique)', dest='query_type', default='chain',
                        action='store', metavar='TYPE')
    parser.add_argument('--count', help='Repeat query multiple times', dest='count', type=int, default=1,
                        metavar='COUNT')
    args = parser.parse_args()

    try:
        write_schema = globals()[f'write_{args.query_type}_schema']
        write_query  = globals()[f'write_{args.query_type}_query']
    except KeyError:
        print(f'Unsupported query type "{args.query_type}".', file=sys.stderr)
        sys.exit(1)

    filename_base = f'{args.query_type}-{args.num_relations}'
    schema_filename = f'{filename_base}.schema.sql'
    query_filename  = f'{filename_base}.query.sql'

    print(f'Generating problem statement {filename_base}.{{schema,query}}.sql for {args.query_type} query of '
           '{args.num_relations} relations.')

    # Generate relation names
    relations = list(map(lambda n: f'T{n}', range(args.num_relations)))

    with open(schema_filename, 'w') as F:
        write_schema(F, relations)
    with open(query_filename, 'w') as F:
        write_query(F, relations)
