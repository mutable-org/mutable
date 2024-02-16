#!/usr/bin/env python3

import datetime
import itertools
import math
import multiprocessing
import os
import random
import re
import string
import sys

NUM_TUPLES = 1_000_000
CHARS = ['0', '1']
OUTPUT_DIR = os.path.join('benchmark', 'phys-cost-models', 'data')
NUM_DISTINCT_VALUES = NUM_TUPLES // 10
FKEY_JOIN_SELECTIVITY = 1e-8
N_M_JOIN_SELECTIVITY = 1e-6

TYPE_TO_STR = {
    'b':            'BOOL',
    'i8':           'INT(1)',
    'i16':          'INT(2)',
    'i32':          'INT(4)',
    'i64':          'INT(8)',
    'f':            'FLOAT',
    'd':            'DOUBLE',
    'c1':           'CHAR(1)',
    'c2':           'CHAR(2)',
    'c3':           'CHAR(3)',
    'c4':           'CHAR(4)',
    'c5':           'CHAR(5)',
    'c6':           'CHAR(6)',
    'c56_dummy':    'CHAR(56)',
    'c59_dummy':    'CHAR(59)',
    'c60_dummy':    'CHAR(60)',
    'c124_dummy':   'CHAR(124)',
    'date':         'DATE',
    'datetime':     'DATETIME',
    'dec10:2':      'DECIMAL(10,2)',
}
SCHEMA = {
    'Relation_parent': [
        ( 'id',  'i32', ['NOT NULL', 'PRIMARY KEY'] ),
        ( 'n2m', 'i32', ['NOT NULL'] ),
        ( 'a0',  'i32', ['NOT NULL'] ),
        ( 'a1',  'i32', ['NOT NULL'] ),
        ( 'a2',  'i32', ['NOT NULL'] ),
        ( 'a3',  'i32', ['NOT NULL'] ),
    ],

    'Relation_child': [
        ( 'id',       'i32', ['NOT NULL'] ),
        ( 'fid_1e_6', 'i32', ['NOT NULL', 'REFERENCES Relation_parent(id)'], {'fkey_join_selectivity': 1e-6} ),
        ( 'fid_9e_7', 'i32', ['NOT NULL', 'REFERENCES Relation_parent(id)'], {'fkey_join_selectivity': 9e-7} ),
        ( 'fid_8e_7', 'i32', ['NOT NULL', 'REFERENCES Relation_parent(id)'], {'fkey_join_selectivity': 8e-7} ),
        ( 'fid_7e_7', 'i32', ['NOT NULL', 'REFERENCES Relation_parent(id)'], {'fkey_join_selectivity': 7e-7} ),
        ( 'fid_6e_7', 'i32', ['NOT NULL', 'REFERENCES Relation_parent(id)'], {'fkey_join_selectivity': 6e-7} ),
        ( 'fid_5e_7', 'i32', ['NOT NULL', 'REFERENCES Relation_parent(id)'], {'fkey_join_selectivity': 5e-7} ),
        ( 'fid_4e_7', 'i32', ['NOT NULL', 'REFERENCES Relation_parent(id)'], {'fkey_join_selectivity': 4e-7} ),
        ( 'fid_3e_7', 'i32', ['NOT NULL', 'REFERENCES Relation_parent(id)'], {'fkey_join_selectivity': 3e-7} ),
        ( 'fid_2e_7', 'i32', ['NOT NULL', 'REFERENCES Relation_parent(id)'], {'fkey_join_selectivity': 2e-7} ),
        ( 'fid_1e_7', 'i32', ['NOT NULL', 'REFERENCES Relation_parent(id)'], {'fkey_join_selectivity': 1e-7} ),
        ( 'fid_1e_8', 'i32', ['NOT NULL', 'REFERENCES Relation_parent(id)'], {'fkey_join_selectivity': 1e-8} ),
        ( 'fid_1e_9', 'i32', ['NOT NULL', 'REFERENCES Relation_parent(id)'], {'fkey_join_selectivity': 1e-9} ),
        ( 'n2m',       'i32', ['NOT NULL'] ),
        ( 'a0',        'i32', ['NOT NULL'] ),
        ( 'a1',        'i32', ['NOT NULL'] ),
        ( 'a2',        'i32', ['NOT NULL'] ),
        ( 'a3',        'i32', ['NOT NULL'] ),
    ],

    'Selectivity_i32': [
        ( 'id',         'i32', ['NOT NULL'] ),
        ( 'i32',        'i32', ['NOT NULL'] ),
        ( 'i32_sorted', 'i32', ['NOT NULL'] ),
        ( 'payload',    'i32', ['NOT NULL'] ),
    ],

    'Selectivity_i64': [
        ( 'id',         'i32', ['NOT NULL'] ),
        ( 'i64',        'i64', ['NOT NULL'] ),
        ( 'i64_sorted', 'i64', ['NOT NULL'] ),
        ( 'payload',    'i32', ['NOT NULL'] ),
    ],

    'Selectivity_f': [
        ( 'id',       'i32', ['NOT NULL'] ),
        ( 'f',        'f',   ['NOT NULL'] ),
        ( 'f_sorted', 'f',   ['NOT NULL'] ),
        ( 'payload',  'i32', ['NOT NULL'] ),
    ],

    'Selectivity_d': [
        ( 'id',       'i32', ['NOT NULL'] ),
        ( 'd',        'd',   ['NOT NULL'] ),
        ( 'd_sorted', 'd',   ['NOT NULL'] ),
        ( 'payload',  'i32', ['NOT NULL'] ),
    ],

    'Distinct_multi_i32': [
        ( 'id',           'i32', ['NOT NULL'] ),
        ( 'n10',          'i32', ['NOT NULL'], {'num_distinct_values': 10} ),
        ( 'n100',         'i32', ['NOT NULL'], {'num_distinct_values': 100} ),
        ( 'n1000',        'i32', ['NOT NULL'], {'num_distinct_values': 1000} ),
        ( 'n1000_sorted', 'i32', ['NOT NULL'], {'num_distinct_values': 1000} ),
        ( 'n10000',       'i32', ['NOT NULL'], {'num_distinct_values': 10000} ),
        ( 'n100000',      'i32', ['NOT NULL'], {'num_distinct_values': 100000} ),
        ( 'n1000000',     'i32', ['NOT NULL'], {'num_distinct_values': 1000000} ),
        ( '_n1000000',    'i32', ['NOT NULL'], {'num_distinct_values': 1000000} ),
        ( '__n1000000',   'i32', ['NOT NULL'], {'num_distinct_values': 1000000} ),
        ( '___n1000000',  'i32', ['NOT NULL'], {'num_distinct_values': 1000000} ),
    ],

    'Distinct_multi_c1': [
        ( 'id', 'i32', ['NOT NULL'] ),
        ( 'a',  'c1',  ['NOT NULL'] ),
        ( 'b',  'c1',  ['NOT NULL'] ),
        ( 'c',  'c1',  ['NOT NULL'] ),
        ( 'd',  'c1',  ['NOT NULL'] ),
        ( 'e',  'c1',  ['NOT NULL'] ),
        ( 'f',  'c1',  ['NOT NULL'] ),
        ( 'g',  'c1',  ['NOT NULL'] ),
        ( 'h',  'c1',  ['NOT NULL'] ),
        ( 'i',  'c1',  ['NOT NULL'] ),
    ],

    'Distinct_multi_c2': [
        ( 'id', 'i32', ['NOT NULL'] ),
        ( 'a',  'c2',  ['NOT NULL'] ),
        ( 'b',  'c2',  ['NOT NULL'] ),
        ( 'c',  'c2',  ['NOT NULL'] ),
        ( 'd',  'c2',  ['NOT NULL'] ),
        ( 'e',  'c2',  ['NOT NULL'] ),
        ( 'f',  'c2',  ['NOT NULL'] ),
        ( 'g',  'c2',  ['NOT NULL'] ),
        ( 'h',  'c2',  ['NOT NULL'] ),
        ( 'i',  'c2',  ['NOT NULL'] ),
    ],

    'Distinct_multi_c3': [
        ( 'id', 'i32', ['NOT NULL'] ),
        ( 'a',  'c3',  ['NOT NULL'] ),
        ( 'b',  'c3',  ['NOT NULL'] ),
        ( 'c',  'c3',  ['NOT NULL'] ),
        ( 'd',  'c3',  ['NOT NULL'] ),
        ( 'e',  'c3',  ['NOT NULL'] ),
        ( 'f',  'c3',  ['NOT NULL'] ),
        ( 'g',  'c3',  ['NOT NULL'] ),
        ( 'h',  'c3',  ['NOT NULL'] ),
        ( 'i',  'c3',  ['NOT NULL'] ),
    ],

    'Distinct_multi_c4': [
        ( 'id', 'i32', ['NOT NULL'] ),
        ( 'a',  'c4',  ['NOT NULL'] ),
        ( 'b',  'c4',  ['NOT NULL'] ),
        ( 'c',  'c4',  ['NOT NULL'] ),
        ( 'd',  'c4',  ['NOT NULL'] ),
        ( 'e',  'c4',  ['NOT NULL'] ),
        ( 'f',  'c4',  ['NOT NULL'] ),
        ( 'g',  'c4',  ['NOT NULL'] ),
        ( 'h',  'c4',  ['NOT NULL'] ),
        ( 'i',  'c4',  ['NOT NULL'] ),
    ],

    'Distinct_multi_c5': [
        ( 'id', 'i32', ['NOT NULL'] ),
        ( 'a',  'c5',  ['NOT NULL'] ),
        ( 'b',  'c5',  ['NOT NULL'] ),
        ( 'c',  'c5',  ['NOT NULL'] ),
        ( 'd',  'c5',  ['NOT NULL'] ),
        ( 'e',  'c5',  ['NOT NULL'] ),
        ( 'f',  'c5',  ['NOT NULL'] ),
        ( 'g',  'c5',  ['NOT NULL'] ),
        ( 'h',  'c5',  ['NOT NULL'] ),
        ( 'i',  'c5',  ['NOT NULL'] ),
    ],

    'Distinct_multi_c6': [
        ( 'id', 'i32', ['NOT NULL'] ),
        ( 'a',  'c6',  ['NOT NULL'] ),
        ( 'b',  'c6',  ['NOT NULL'] ),
        ( 'c',  'c6',  ['NOT NULL'] ),
        ( 'd',  'c6',  ['NOT NULL'] ),
        ( 'e',  'c6',  ['NOT NULL'] ),
        ( 'f',  'c6',  ['NOT NULL'] ),
        ( 'g',  'c6',  ['NOT NULL'] ),
        ( 'h',  'c6',  ['NOT NULL'] ),
        ( 'i',  'c6',  ['NOT NULL'] ),
    ],

    'Load_factors_multi_i32': [
        ( 'id',   'i32', ['NOT NULL'] ),
        ( 'lf05', 'i32', ['NOT NULL'], {'num_distinct_values': 1023} ),
        ( 'lf06', 'i32', ['NOT NULL'], {'num_distinct_values': 1228} ),
        ( 'lf07', 'i32', ['NOT NULL'], {'num_distinct_values': 1433} ),
        ( 'lf08', 'i32', ['NOT NULL'], {'num_distinct_values': 1637} ),
        ( 'lf09', 'i32', ['NOT NULL'], {'num_distinct_values': 1842} ),
        ( 'lf10', 'i32', ['NOT NULL'], {'num_distinct_values': 2046} ), # s.t. they will always fit in 2048 slots
        ( 'lf11', 'i32', ['NOT NULL'], {'num_distinct_values': 2251} ),
        ( 'lf12', 'i32', ['NOT NULL'], {'num_distinct_values': 2456} ),
        ( 'lf13', 'i32', ['NOT NULL'], {'num_distinct_values': 2660} ),
        ( 'lf14', 'i32', ['NOT NULL'], {'num_distinct_values': 2865} ),
        ( 'lf15', 'i32', ['NOT NULL'], {'num_distinct_values': 3069} ),
        ( 'lf16', 'i32', ['NOT NULL'], {'num_distinct_values': 3274} ),
        ( 'lf17', 'i32', ['NOT NULL'], {'num_distinct_values': 3479} ),
        ( 'lf18', 'i32', ['NOT NULL'], {'num_distinct_values': 3683} ),
        ( 'lf19', 'i32', ['NOT NULL'], {'num_distinct_values': 3888} ),
        ( 'lf20', 'i32', ['NOT NULL'], {'num_distinct_values': 4092} ),
        ( 'lf22', 'i32', ['NOT NULL'], {'num_distinct_values': 4502} ),
        ( 'lf24', 'i32', ['NOT NULL'], {'num_distinct_values': 4911} ),
        ( 'lf26', 'i32', ['NOT NULL'], {'num_distinct_values': 5320} ),
        ( 'lf28', 'i32', ['NOT NULL'], {'num_distinct_values': 5729} ),
        ( 'lf30', 'i32', ['NOT NULL'], {'num_distinct_values': 6138} ),
    ],

    'Load_factors_huge_multi_i32': [
        ( 'id',   'i32', ['NOT NULL'] ),
        ( 'lf05', 'i32', ['NOT NULL'], {'num_distinct_values': 131070} ),
        ( 'lf06', 'i32', ['NOT NULL'], {'num_distinct_values': 157284} ),
        ( 'lf07', 'i32', ['NOT NULL'], {'num_distinct_values': 183498} ),
        ( 'lf08', 'i32', ['NOT NULL'], {'num_distinct_values': 209712} ),
        ( 'lf09', 'i32', ['NOT NULL'], {'num_distinct_values': 235926} ),
        ( 'lf10', 'i32', ['NOT NULL'], {'num_distinct_values': 262140} ), # s.t. they will always fit in 262144 slots
        ( 'lf12', 'i32', ['NOT NULL'], {'num_distinct_values': 314568} ),
        ( 'lf14', 'i32', ['NOT NULL'], {'num_distinct_values': 366996} ),
        ( 'lf16', 'i32', ['NOT NULL'], {'num_distinct_values': 419424} ),
        ( 'lf18', 'i32', ['NOT NULL'], {'num_distinct_values': 417852} ),
        ( 'lf20', 'i32', ['NOT NULL'], {'num_distinct_values': 524280} ),
        ( 'lf22', 'i32', ['NOT NULL'], {'num_distinct_values': 576708} ),
        ( 'lf24', 'i32', ['NOT NULL'], {'num_distinct_values': 629136} ),
        ( 'lf26', 'i32', ['NOT NULL'], {'num_distinct_values': 681564} ),
        ( 'lf28', 'i32', ['NOT NULL'], {'num_distinct_values': 733992} ),
        ( 'lf30', 'i32', ['NOT NULL'], {'num_distinct_values': 786420} ),
    ],

    'Attributes_multi_i32': [
        ( 'a0', 'i32', ['NOT NULL'] ),
        ( 'a1', 'i32', ['NOT NULL'] ),
        ( 'a2', 'i32', ['NOT NULL'] ),
        ( 'a3', 'i32', ['NOT NULL'] ),
    ],

    'Attributes_multi': [
        ( 'id', 'i32', ['NOT NULL'] ),
        ( 'a0', 'i8',  ['NOT NULL'] ),
        ( 'a1', 'i64', ['NOT NULL'] ),
        ( 'a2', 'i16', ['NOT NULL'] ),
        ( 'a3', 'i32', ['NOT NULL'] ),
    ],

    'Attributes_huge_i32': [
        ( 'a0', 'i32',       ['NOT NULL'] ),
        ( 'c0', 'c60_dummy', ['NOT NULL'] ),
        ( 'a1', 'i32',       ['NOT NULL'] ),
        ( 'a2', 'i32',       ['NOT NULL'] ),
        ( 'c1', 'c56_dummy', ['NOT NULL'] ),
        ( 'a3', 'i32',       ['NOT NULL'] ),
        ( 'c2', 'c124_dummy', ['NOT NULL'] ),
    ],

    'Attributes_huge': [
        ( 'id', 'i32',       ['NOT NULL'] ),
        ( 'a0', 'i8',        ['NOT NULL'] ),
        ( 'c0', 'c59_dummy', ['NOT NULL'] ),
        ( 'a1', 'i64',       ['NOT NULL'] ),
        ( 'a2', 'i16',       ['NOT NULL'] ),
        ( 'c1', 'c56_dummy', ['NOT NULL'] ),
        ( 'a3', 'i32',       ['NOT NULL'] ),
        ( 'c2', 'c60_dummy', ['NOT NULL'] ),
    ],

    'Attributes_simd': [
        ( 'id',   'i32', ['NOT NULL'] ),
        ( 'i8',   'i8',  ['NOT NULL'] ),
        ( 'i8_2', 'i8',  ['NOT NULL'] ),
        ( 'i16',  'i16', ['NOT NULL'] ),
        ( 'i32',  'i32', ['NOT NULL'] ),
        ( 'i64',  'i64', ['NOT NULL'] ),
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

# Generate `num` distinct strings of `length` characters, each character drawn at random from `CHARS`.
def gen_random_string_values(length :int, num :int):
    assert len(CHARS) ** length >= num

    if len(CHARS) ** length == num:
        return ['"' + ''.join(t) + '"' for t in itertools.product(CHARS, repeat=length)]

    values = set()

    i = 0
    while i < num:
        str = '"' + ''.join(random.choice(CHARS) for _ in range(0, length)) + '"'
        if str in values:
            continue
        values.add(str)
        i += 1

    assert len(values) == len(list(values))
    return values

# Generate `num` distinct decimal values of `scale` and `precision`.
def gen_random_decimal_values(scale :int, precision :int, num :int):
    assert scale + precision >= math.log10(num)

    values = set()

    while len(values) < num:
        val = round(random.uniform(-10**scale + sys.float_info.epsilon, 10**scale), precision)
        values.add(val)

    assert len(values) == len(list(values))
    return values

# Generate `num` distinct date values.
def gen_random_date_values(num :int):
    lo = datetime.date.min.toordinal()
    hi = datetime.date.max.toordinal()
    assert num < hi - lo

    values = set()

    while len(values) < num:
        val = datetime.date.fromordinal(random.randint(lo, hi)).strftime("%Y-%m-%d")
        values.add(val)

    assert len(values) == len(list(values))
    return values

# Generate `num` distinct datetime values.
def gen_random_datetime_values(num :int):
    lo = int(datetime.datetime.min.replace(tzinfo=datetime.timezone.utc).timestamp())
    hi = int(datetime.datetime.max.replace(tzinfo=datetime.timezone.utc).timestamp())

    values = set()

    while len(values) < num:
        val = datetime.date.fromtimestamp(random.randint(lo, hi)).strftime("%Y-%m-%d %I:%M:%S")
        values.add(val)

    assert len(values) == len(list(values))
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
                attrs.append(f'    {attr[0]} {TYPE_TO_STR[attr[1]]} {" ".join(attr[2]) if len(attr) >= 3 else ""}')
            sql.write(',\n'.join(attrs))
            sql.write(f'''
);
'''
)

def gen_cardinalities(name, schema, path_to_dir):
    with open(os.path.join(path_to_dir, 'cardinalities.json'), 'w') as json:
        json.write(f'''{{
    "{name}": [
''')

        join_added = False
        cardinalities = list()
        for table_name, attributes in schema.items():
            cardinalities.append(f'        {{ "relations": ["{table_name}"], "size": {NUM_TUPLES} }}')
            for attr in attributes:
                attr_name = attr[0]
                constraints = attr[2]
                args = attr[3] if len(attr) >= 4 else dict()
                num_distinct_values = args.get('num_distinct_values', NUM_DISTINCT_VALUES)
                fkey_join_selectivity = args.get('fkey_join_selectivity', FKEY_JOIN_SELECTIVITY)
                nm_join_selectivity = args.get('nm_join_selectivity', N_M_JOIN_SELECTIVITY)

                if 'fid' in attr_name or 'n2m' in attr_name:
                    matcher = re.compile('^REFERENCES (\w+)\((\w+)\)')
                    for constraint in constraints:
                        match = matcher.match(constraint)
                        if match and not join_added: #TODO: add multiple joins on different attributes once supported by `InjectionCardinalityEstimator`
                            join_added = True
                            ref_table = match.group(1)
                            size = (min(int(fkey_join_selectivity * NUM_TUPLES * NUM_TUPLES), NUM_TUPLES)
                                   if 'fid' in attr_name else nm_join_selectivity * NUM_TUPLES * NUM_TUPLES)
                            cardinalities.append(f'        {{ "relations": ["{table_name}", "{ref_table}"], "size": {size} }}')

                cardinalities.append(f'        {{ "relations": ["g#{table_name}.{attr_name}"], "size": {num_distinct_values} }}')

        json.write(',\n'.join(cardinalities))
        json.write('''
    ]
}''')

def gen_column(attr, num_tuples):
    name = attr[0]
    ty = attr[1]
    args = attr[3] if len(attr) >= 4 else dict()
    num_distinct_values = args.get('num_distinct_values', NUM_DISTINCT_VALUES)
    fkey_join_selectivity = args.get('fkey_join_selectivity', FKEY_JOIN_SELECTIVITY)
    nm_join_selectivity = args.get('nm_join_selectivity', N_M_JOIN_SELECTIVITY)
    random.seed(hash(name))

    if 'fid' in name:
        num_fids_joining = min(int(fkey_join_selectivity * num_tuples * num_tuples), num_tuples)
        foreign_keys = [ random.randrange(0, num_tuples) for _ in range(num_fids_joining) ]
        foreign_keys.extend([num_tuples] * (num_tuples - num_fids_joining))
        assert len(foreign_keys) == num_tuples
        random.shuffle(foreign_keys)
        print(f'  + Generated column {name} of {num_tuples:,} rows with {num_fids_joining:,} foreign keys with a join partner.')
        return map(str, foreign_keys)
    elif 'id' in name:
        print(f'  + Generated column {name} of {num_tuples:,} rows with keys from 0 to {num_tuples-1:,}.')
        return map(str, range(num_tuples))
    elif 'n2m' in name: # n to m join
        num_distinct_values = int(round(1 / nm_join_selectivity))
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
    elif ty == 'c1':
        values = gen_random_string_values(1,  min(len(CHARS) ** 1,  num_distinct_values))
    elif ty == 'c2':
        values = gen_random_string_values(2,  min(len(CHARS) ** 2,  num_distinct_values))
    elif ty == 'c3':
        values = gen_random_string_values(3,  min(len(CHARS) ** 3,  num_distinct_values))
    elif ty == 'c4':
        values = gen_random_string_values(4,  min(len(CHARS) ** 4,  num_distinct_values))
    elif ty == 'c5':
        values = gen_random_string_values(5,  min(len(CHARS) ** 5,  num_distinct_values))
    elif ty == 'c6':
        values = gen_random_string_values(6,  min(len(CHARS) ** 6,  num_distinct_values))
    elif ty == 'c56_dummy' or ty == 'c59_dummy' or ty == 'c60_dummy' or ty == 'c124_dummy':
        values = [ '"ThisIsALongDummyString"' ]
    elif ty == 'dec10:2':
        values = gen_random_decimal_values(10, 2, num_distinct_values)
    elif ty == 'date':
        values = gen_random_date_values(num_distinct_values)
    elif ty == 'datetime':
        values = gen_random_datetime_values(num_distinct_values)
    else:
        raise Exception('unsupported type')

    data = list(itertools.chain.from_iterable(itertools.repeat(values, math.ceil(num_tuples / len(values)))))[0:num_tuples]
    print(f'  + Generated column {name} of {len(data):,} rows with {len(set(data)):,} distinct values.')
    if 'sorted' in name:
        data.sort()
    else:
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
    gen_database('phys_cost_models', SCHEMA, OUTPUT_DIR)
    gen_cardinalities('phys_cost_models', SCHEMA, OUTPUT_DIR)
    gen_tables(SCHEMA, OUTPUT_DIR)
