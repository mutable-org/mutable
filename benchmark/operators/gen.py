#!env python3

import itertools
import math
import multiprocessing
import numpy
import os
import random
import string


NUM_TUPLES = 10_000_000
STRLEN = 10
OUTPUT_DIR = os.path.join('benchmark', 'operators', 'data')
NUM_DISTINCT_VALUES = NUM_TUPLES // 10
FKEY_JOIN_SELECTIVITY = 1e-8
N_M_JOIN_SELECTIVITY = 1e-6

TYPE_TO_STR = {
        'b':    'BOOL',
        'i8':   'INT(1)',
        'i16':  'INT(2)',
        'i32':  'INT(4)',
        'i64':  'INT(8)',
        'f':    'FLOAT',
        'd':    'DOUBLE',
}

SCHEMA = {
    "Attribute_b": [
        ( 'id', 'i32' ),
        ( 'val', 'b' ),
    ],

    "Attribute_i8": [
        ( 'id', 'i32' ),
        ( 'val', 'i8' ),
    ],

    "Attribute_i16": [
        ( 'id', 'i32' ),
        ( 'val', 'i16' ),
    ],

    "Attribute_i32": [
        ( 'id', 'i32' ),
        ( 'val', 'i32' ),
    ],

    "Attribute_i64": [
        ( 'id', 'i32' ),
        ( 'val', 'i64' ),
    ],

    "Attribute_f": [
        ( 'id', 'i32' ),
        ( 'val', 'f' ),
    ],

    "Attribute_d": [
        ( 'id', 'i32' ),
        ( 'val', 'd' ),
    ],

    "Distinct_i32": [
        ( 'id',      'i32' ),
        ( 'n1',      'i32', 1 ),
        ( 'n10',     'i32', 10 ),
        ( 'n100',    'i32', 100 ),
        ( 'n1000',   'i32', 1000 ),
        ( 'n10000',  'i32', 10000 ),
        ( 'n100000', 'i32', 100000 ),
    ],

    "Relation": [
        ( 'id',     'i32' ),
        ( 'fid',    'i32' ),
        ( 'n2m',    'i32' ),
    ],

    "Attributes_multi_b": [
        ( 'id', 'i32' ),
        ( 'a0', 'b' ),
        ( 'a1', 'b' ),
        ( 'a2', 'b' ),
        ( 'a3', 'b' ),
    ],

    "Attributes_multi_i32": [
        ( 'id', 'i32' ),
        ( 'a0', 'i32' ),
        ( 'a1', 'i32' ),
        ( 'a2', 'i32' ),
        ( 'a3', 'i32' ),
    ],

    "Attributes_multi_i64": [
        ( 'id', 'i32' ),
        ( 'a0', 'i64' ),
        ( 'a1', 'i64' ),
        ( 'a2', 'i64' ),
        ( 'a3', 'i64' ),
    ],

    "Attributes_multi_f": [
        ( 'id', 'i32' ),
        ( 'a0', 'f' ),
        ( 'a1', 'f' ),
        ( 'a2', 'f' ),
        ( 'a3', 'f' ),
    ],

    "Attributes_multi_d": [
        ( 'id', 'i32' ),
        ( 'a0', 'd' ),
        ( 'a1', 'd' ),
        ( 'a2', 'd' ),
        ( 'a3', 'd' ),
    ],
}


#=======================================================================================================================
# Helper Functions
#=======================================================================================================================

# Process an `iterable` in groups of size `n`
def grouper(iterable, n):
    it = iter(iterable)
    while True:
        chunk_it = itertools.islice(it, n)
        try:
            first_el = next(chunk_it)
        except StopIteration:
            return
        yield itertools.chain((first_el,), chunk_it)

# Generate `num` distinct integer values, drawn uniformly at random from the range [ `smallest`, `largest` ).
def gen_random_int_values(smallest :int, largest :int, num :int):
    assert largest - smallest >= num

    if largest - smallest == num:
        return list(range(smallest, largest))

    taken = set()
    counter = largest - num
    values = list()

    for i in range(0, num):
        val = random.randrange(smallest, largest - num)
        if val in taken:
            values.append(counter)
            counter += 1
        else:
            taken.add(val)
            values.append(val)

    assert len(values) == len(set(values))
    return values


#=======================================================================================================================
# Data Generation
#=======================================================================================================================

def gen_database(name, schema, path_to_dir):
    with open(os.path.join(path_to_dir, 'schema.sql'), 'w') as sql:
        sql.write(f'''\
CREATE DATABASE {name};
USE {name};
''')

        for table_name, attributes in schema.items():
            sql.write(f'''
CREATE TABLE {table_name}
(
''')
            attrs = list()
            for attr in attributes:
                attrs.append(f'    {attr[0]} {TYPE_TO_STR[attr[1]]} NOT NULL')
            sql.write(',\n'.join(attrs))

            path_to_csv = os.path.join(path_to_dir, f'{table_name}.csv')
            sql.write(f'''
);
'''
)

def gen_column(attr, num_tuples):
    name = attr[0]
    ty = attr[1]
    num_distinct_values = attr[2] if len(attr) >= 3 else NUM_DISTINCT_VALUES
    random.seed(hash(name))

    if 'fid' in name:
        num_fids_joining = min(int(FKEY_JOIN_SELECTIVITY * num_tuples * num_tuples), num_tuples)
        foreign_keys = [ random.randrange(0, num_tuples) for i in range(num_fids_joining) ]
        foreign_keys.extend([num_tuples] * (num_tuples - num_fids_joining))
        assert len(foreign_keys) == num_tuples
        random.shuffle(foreign_keys)
        print(f'  + Generated column {name} of {num_tuples:,} rows with {num_fids_joining:,} foreign keys with a join partner.')
        return map(str, foreign_keys)
    elif 'id' in name:
        print(f'  + Generated column {name} of {num_tuples:,} rows with keys from 0 to {num_tuples-1:,}.')
        return map(str, range(num_tuples))
    elif 'n2m' in name: # n to m join
        num_distinct_values = int(round(1 / N_M_JOIN_SELECTIVITY))
        values = gen_random_int_values(-2**31 + 1, 2**31, num_distinct_values)
    elif ty == 'b':
        values = [ 'TRUE', 'FALSE' ]
    elif ty == 'f' or ty == 'd':
        values = [ random.random() for i in range(num_distinct_values) ]
    elif ty == 'i8':
        values = gen_random_int_values( -2**7 + 1,  2**7, min( 2**8 - 1, num_distinct_values))
    elif ty == 'i16':
        values = gen_random_int_values(-2**15 + 1, 2**15, min(2**16 - 1, num_distinct_values))
    elif ty == 'i32':
        values = gen_random_int_values(-2**31 + 1, 2**31, min(2**32 - 1, num_distinct_values))
    elif ty == 'i64':
        values = gen_random_int_values(-2**63 + 1, 2**63, min(2**64 - 1, num_distinct_values))
    else:
        raise Exception('unsupported type')

    data = list(itertools.chain.from_iterable(itertools.repeat(values, math.ceil(num_tuples / len(values)))))[0:num_tuples]
    print(f'  + Generated column {name} of {len(data):,} rows with {len(set(data)):,} distinct values.')
    random.shuffle(data)
    return map(str, data)

def gen_table(table_name, attributes, path_to_dir):
    print(f'Generating data for table {table_name}')
    with multiprocessing.Pool(multiprocessing.cpu_count()) as pool:
        path = os.path.join(path_to_dir, table_name + '.csv')
        columns = pool.starmap(gen_column, zip(attributes, [NUM_TUPLES] * len(attributes)))

        with open(path, 'w') as csv:
            # write header
            csv.write(','.join(map(lambda attr: attr[0], attributes)) + '\n')
            rows = map(','.join, zip(*columns))
            for g in grouper(rows, 1000):
                csv.write('\n'.join(g))
                csv.write('\n')

def gen_tables(schema, path_to_dir):
    for table_name, attributes in schema.items():
        gen_table(table_name, attributes, path_to_dir)

if __name__ == '__main__':
    print(f'Generating data in {OUTPUT_DIR}')
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    gen_database('operators', SCHEMA, OUTPUT_DIR)
    gen_tables(SCHEMA, OUTPUT_DIR)
