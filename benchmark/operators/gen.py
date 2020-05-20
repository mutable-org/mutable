#!env python3

import itertools
import math
import os
import random
import string


NUM_TUPLES = 1_000_000
STRLEN = 10
OUTPUT_DIR = os.path.join('benchmark', 'operators', 'data')
NUM_DISTINCT_VALUES = NUM_TUPLES // 10

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
    "Attributes_b": [
        ( 'a0', 'b' ),
        ( 'a1', 'b' ),
        ( 'a2', 'b' ),
        ( 'a3', 'b' ),
        ( 'a4', 'b' ),
        ( 'a5', 'b' ),
        ( 'a6', 'b' ),
        ( 'a7', 'b' ),
        ( 'a8', 'b' ),
        ( 'a9', 'b' ),
    ],

    "Attributes_i8": [
        ( 'a0', 'i8' ),
        ( 'a1', 'i8' ),
        ( 'a2', 'i8' ),
        ( 'a3', 'i8' ),
        ( 'a4', 'i8' ),
        ( 'a5', 'i8' ),
        ( 'a6', 'i8' ),
        ( 'a7', 'i8' ),
        ( 'a8', 'i8' ),
        ( 'a9', 'i8' ),
    ],

    "Attributes_i16": [
        ( 'a0', 'i16' ),
        ( 'a1', 'i16' ),
        ( 'a2', 'i16' ),
        ( 'a3', 'i16' ),
        ( 'a4', 'i16' ),
        ( 'a5', 'i16' ),
        ( 'a6', 'i16' ),
        ( 'a7', 'i16' ),
        ( 'a8', 'i16' ),
        ( 'a9', 'i16' ),
    ],

    "Attributes_i32": [
        ( 'a0', 'i32' ),
        ( 'a1', 'i32' ),
        ( 'a2', 'i32' ),
        ( 'a3', 'i32' ),
        ( 'a4', 'i32' ),
        ( 'a5', 'i32' ),
        ( 'a6', 'i32' ),
        ( 'a7', 'i32' ),
        ( 'a8', 'i32' ),
        ( 'a9', 'i32' ),
    ],

    "Attributes_i64": [
        ( 'a0', 'i64' ),
        ( 'a1', 'i64' ),
        ( 'a2', 'i64' ),
        ( 'a3', 'i64' ),
        ( 'a4', 'i64' ),
        ( 'a5', 'i64' ),
        ( 'a6', 'i64' ),
        ( 'a7', 'i64' ),
        ( 'a8', 'i64' ),
        ( 'a9', 'i64' ),
    ],

    "Attributes_f": [
        ( 'a0', 'f' ),
        ( 'a1', 'f' ),
        ( 'a2', 'f' ),
        ( 'a3', 'f' ),
        ( 'a4', 'f' ),
        ( 'a5', 'f' ),
        ( 'a6', 'f' ),
        ( 'a7', 'f' ),
        ( 'a8', 'f' ),
        ( 'a9', 'f' ),
    ],

    "Attributes_d": [
        ( 'a0', 'd' ),
        ( 'a1', 'd' ),
        ( 'a2', 'd' ),
        ( 'a3', 'd' ),
        ( 'a4', 'd' ),
        ( 'a5', 'd' ),
        ( 'a6', 'd' ),
        ( 'a7', 'd' ),
        ( 'a8', 'd' ),
        ( 'a9', 'd' ),
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
}


#=======================================================================================================================
# Helper Functions
#=======================================================================================================================

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
                attrs.append(f'    {attr[0]} {TYPE_TO_STR[attr[1]]}')
            sql.write(',\n'.join(attrs))

            path_to_csv = os.path.join(path_to_dir, f'{table_name}.csv')
            sql.write(f'''
);
IMPORT INTO {table_name} DSV "{path_to_csv}" SKIP HEADER;
'''
)

def gen_column(attr, num_tuples):
    name = attr[0]
    ty = attr[1]
    num_distinct_values = attr[2] if len(attr) >= 3 else NUM_DISTINCT_VALUES
    random.seed(hash(name))

    if 'fid' in name:
        pass # TODO repeat some values
    elif 'id' in name:
        return range(num_tuples)

    if ty == 'b':
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
    return data

def gen_table(table_name, attributes, path_to_dir):
    print(f'Generating data for table {table_name}')
    path = os.path.join(path_to_dir, table_name + '.csv')
    columns = [ gen_column(attr, NUM_TUPLES) for attr in attributes ]

    with open(path, 'w') as csv:
        # write header
        csv.write(','.join(map(lambda attr: attr[0], attributes)) + '\n')
        for i in range(NUM_TUPLES):
            row = list(map(lambda col: col[i], columns))
            csv.write(','.join(map(str, row)) + '\n')

def gen_tables(schema, path_to_dir):
    for table_name, attributes in schema.items():
        gen_table(table_name, attributes, path_to_dir)

if __name__ == '__main__':
    print(f'Generating data in {OUTPUT_DIR}')
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    gen_database('operators', SCHEMA, OUTPUT_DIR)
    gen_tables(SCHEMA, OUTPUT_DIR)
