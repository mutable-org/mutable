#!env python3

import os
import random
import string


NUM_TUPLES = 10000
STRLEN = 10
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data')


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
    "T0": [
        ( 'key', 'i32' ),
        ( 'fkey', 'i32' ),
    ],
    "T1": [
        ( 'key', 'i32' ),
        ( 'fkey', 'i32' ),
    ],
    "T2": [
        ( 'key', 'i32' ),
        ( 'fkey', 'i32' ),
    ],
    "T3": [
        ( 'key', 'i32' ),
        ( 'fkey', 'i32' ),
    ],
    "T4": [
        ( 'key', 'i32' ),
        ( 'fkey', 'i32' ),
    ],
    "T5": [
        ( 'key', 'i32' ),
        ( 'fkey', 'i32' ),
    ],
    "T6": [
        ( 'key', 'i32' ),
        ( 'fkey', 'i32' ),
    ],
    "T7": [
        ( 'key', 'i32' ),
        ( 'fkey', 'i32' ),
    ],
    "T8": [
        ( 'key', 'i32' ),
        ( 'fkey', 'i32' ),
    ],
    "T9": [
        ( 'key', 'i32' ),
        ( 'fkey', 'i32' ),
    ],
    "T10": [
        ( 'key', 'i32' ),
        ( 'fkey', 'i32' ),
    ],
    "T11": [
        ( 'key', 'i32' ),
        ( 'fkey', 'i32' ),
    ],
    "T12": [
        ( 'key', 'i32' ),
        ( 'fkey', 'i32' ),
    ],
    "T13": [
        ( 'key', 'i32' ),
        ( 'fkey', 'i32' ),
    ],
    "T14": [
        ( 'key', 'i32' ),
        ( 'fkey', 'i32' ),
    ],
    "T15": [
        ( 'key', 'i32' ),
        ( 'fkey', 'i32' ),
    ],
    "T16": [
        ( 'key', 'i32' ),
        ( 'fkey', 'i32' ),
    ],
    "T17": [
        ( 'key', 'i32' ),
        ( 'fkey', 'i32' ),
    ],
    "T18": [
        ( 'key', 'i32' ),
        ( 'fkey', 'i32' ),
    ],
    "T19": [
        ( 'key', 'i32' ),
        ( 'fkey', 'i32' ),
    ]
}

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
            for name, ty in attributes:
                attrs.append(f'    {name} {TYPE_TO_STR[ty]}')
            sql.write(',\n'.join(attrs))

            path_to_csv = os.path.join(path_to_dir, f'{table_name}.csv')
            sql.write(f'''
);
'''
)

def gen_column(name, ty, num_tuples):
    random.seed(42)
    if ('id' in name):
        return range(num_tuples)
    if ty == 'b':
        return [ 'TRUE' if random.getrandbits(1) else 'FALSE' for i in range(num_tuples) ]
    elif ty == 'i8':
        return [ random.randrange(-2**8 + 1, 2**8-1) for i in range(num_tuples) ]
    elif ty == 'i16':
        return [ random.randrange(-2**16 + 1, 2**16-1) for i in range(num_tuples) ]
    elif ty == 'i32':
        return [ random.randrange(-2**32 + 1, 2**32-1) for i in range(num_tuples) ]
    elif ty == 'i64':
        return [ random.randrange(-2**64 + 1, 2**64-1) for i in range(num_tuples) ]
    elif ty == 'f' or ty == 'd':
        return [ random.random() for i in range(num_tuples) ]
    else:
        raise Exception('unsupported type')

def gen_table(table_name, attributes, path_to_dir):
    print(f'Generating data for table {table_name}')
    path = os.path.join(path_to_dir, table_name + '.csv')
    columns = [ gen_column(attr[0], attr[1], NUM_TUPLES) for attr in attributes ]

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
    gen_database('optimizer', SCHEMA, OUTPUT_DIR)
