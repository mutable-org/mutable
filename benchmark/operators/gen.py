#!env python3

import os
import random
import string


NUM_TUPLES = 10000
STRLEN = 10
OUTPUT_DIR = os.path.join(os.getcwd(), 'benchmark', 'operators', 'data')

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
IMPORT INTO {table_name} DSV "{path_to_csv}" SKIP HEADER;
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
    print(f'Generating data for table {table_name}.')
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
    gen_database('operators', SCHEMA, OUTPUT_DIR)
    gen_tables(SCHEMA, OUTPUT_DIR)
