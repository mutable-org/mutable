#!/usr/bin/env python3

import argparse
import glob
import hashlib
import math
import numpy as np
import os
import pandas as pd
import pathlib
import random


TYPE_MAP: dict[str, type] = {
    'f':        np.float32,
    'd':        np.float64,
    'i8':       np.int8,
    'i16':      np.int16,
    'i32':      np.int32,
    'i64':      np.int64,
    'date':     object,
    'datetime': object,
}
SQLTYPE_MAP: dict[str, str] = {
    'id':       'INT',
    'f':        'FLOAT',
    'd':        'DOUBLE',
    'i8':       'TINYINT',
    'i16':      'SMALLINT',
    'i32':      'INT',
    'i64':      'BIGINT',
    'date':     'DATE',
    'datetime': 'DATETIME',
}
PATTERNS: list[tuple[str, str, str]] = [
    ('exec_query',                   '\'^Execute query:.*\'',                          'Execution time'),
    ('exec_machinecode',             '\'^Execute machine code:.*\'',                   'Query runtime'),
    ('comp_webassembly+machinecode', '\'^Compile SQL to machine code:.*\'',            'Code generation and compilation time'),
    ('comp_webassembly',             '\'^\|- Compile SQL to WebAssembly:.*\'',         'Code generation time'),
    ('comp_machinecode',             '\'^ ` Compile WebAssembly to machine code:.*\'', 'Compile time'),
    #  ('opt_querygraph',               '\'^Construct the query graph:.*\'',              'Query graph construction time'),
    #  ('opt_logicalplan',              '\'^Compute the logical query plan:.*\'',         'Logical plan computation time'),
    #  ('opt_planenum',                 '\'^Plan enumeration:.*\'',                       'Plan enumeration time'),
    #  ('opt_physicalplan',             '\'^Compute the physical plan:.*\'',              'Physical plan computation time'),
    ('size_webassamly',              '\'^Wasm code size:/*\'',                         'WebAssembly code size'),
    ('size_machindecode',            '\'^Machine code size:/*\'',                      'Machine code size'),
]

MATERIALIZATIONS: list[tuple[str, str, str]] = [
    #  ('wo_materialization', '',                                                   'without materialization'),
    ('w_materialization',  '--soft-pipeline-breaker AfterFilter,AfterIndexScan', 'with materialization'),
]
CACHING_CONFIGURATIONS: list[tuple[str, str, str]] = [
    ('wo_caching', '--no-wasm-compilation-cache', 'without code caching'),
    ('w_caching',  ''                           , 'with code caching'),
]
SCAN_CONFIGURATIONS: list[tuple[str, str]] = [
    ('table scan + filter, branching',             '--scan-implementations Scan --filter-selection-strategy Branching'),
    ('table scan + filter, predicated',            '--scan-implementations Scan --filter-selection-strategy Predicated'),
]
INDEX_CONFIGURATIONS: list[tuple[str, str]] = [
    ('index scan (compiled), inline/hostcall',         '--scan-implementations IndexScan --index-scan-strategy Compilation --index-scan-materialization-strategy Inline --index-scan-compilation-strategy Callback'),
    ('index scan (compiled), memory/hostcall',         '--scan-implementations IndexScan --index-scan-strategy Compilation --index-scan-materialization-strategy Memory --index-scan-compilation-strategy Callback'),
    #  ('index scan (compiled), inline/exposed memory',   '--scan-implementations IndexScan --index-scan-strategy Compilation --index-scan-materialization-strategy Inline --index-scan-compilation-strategy ExposedMemory'),
    #  ('index scan (compiled), memory/exposed memory',   '--scan-implementations IndexScan --index-scan-strategy Compilation --index-scan-materialization-strategy Memory --index-scan-compilation-strategy ExposedMemory'),
    ('index scan (hybrid), inline/hostcall',       '--scan-implementations IndexScan --index-scan-strategy Hybrid --index-scan-materialization-strategy Inline --index-scan-compilation-strategy Callback'),
    ('index scan (hybrid), memory/hostcall',       '--scan-implementations IndexScan --index-scan-strategy Hybrid --index-scan-materialization-strategy Memory --index-scan-compilation-strategy Callback'),
    #  ('index scan (hybrid), inline/exposed memory', '--scan-implementations IndexScan --index-scan-strategy Hybrid --index-scan-materialization-strategy Inline --index-scan-compilation-strategy ExposedMemory'),
    #  ('index scan (hybrid), memory/exposed memory', '--scan-implementations IndexScan --index-scan-strategy Hybrid --index-scan-materialization-strategy Memory --index-scan-compilation-strategy ExposedMemory'),
    ('index scan (interpreted), inline',        '--scan-implementations IndexScan --index-scan-strategy Interpretation --index-scan-materialization-strategy Inline'),
    ('index scan (interpreted), memory',        '--scan-implementations IndexScan --index-scan-strategy Interpretation --index-scan-materialization-strategy Memory'),
]
METHODS: list[tuple[str, str]] = [
    ('array', '--index-implementations Array'),
    ('rmi',   '--index-implementations Rmi'),
]
LAYOUTS: list[tuple[str, str, str]] = [
    ('row', '--data-layout Row', 'Row layout'),
    #  ('pax16tup', '--data-layout PAX16Tup', 'PAX16Tup layout'),
    #  ('pax128tup', '--data-layout PAX128Tup', 'PAX128Tup layout'),
    #  ('pax1014tup', '--data-layout PAX1024Tup', 'PAX1024Tup layout'),
    #  ('pax4k', '--data-layout PAX4K', 'PAX4K layout'),
    #  ('pax64k', '--data-layout PAX64K', 'PAX64K layout'),
    #  ('pax512k', '--data-layout PAX512K', 'PAX512K layout'),
    #  ('pax4m', '--data-layout PAX4M', 'PAX4M layout'),
    #  ('pax64m', '--data-layout PAX64M', 'PAX64M layout'),
]

SELECTIVITIES: list[float] = [
    10**0,  0.5 * 10**0,
    10**-1, 0.5 * 10**-1,
    10**-2, 0.5 * 10**-2,
    10**-3, 0.5 * 10**-3,
    10**-4, 0.5 * 10**-4,
    10**-5, 0.5 * 10**-5,
    10**-6
]

BATCH_SIZE_SELECTIVITY: float = 0.1
BATCH_SIZE_ARG: str = '--index-sequential-scan-batch-size'
BATCH_SIZES: list[int] = [10**0, 10**1, 10**2, 10**3, 10**4, 10**5, 10**6]

MUTABLE_ARGS = '--backend WasmV8 --no-simd --statistics'

OUTPUT_DIR: str = os.path.join('benchmark', 'phys-cost-models', 'index-scan')
DATA_DIR: str = os.path.join('benchmark', 'phys-cost-models', 'data')

#=======================================================================================================================
# Data Handling Helper Functions
#=======================================================================================================================


# Returns all relevant csv files in \p path.
def get_csv_files(path: str) -> list[str]:
    return sorted(glob.glob(os.path.join(path, 'Selectivity-*-simple.csv')))


# Returns a mapping from column name to type.
def get_dtypes(csv_file) -> dict[str, type]:
    column: str = get_column_name(csv_file)
    hm: dict[str, type] = {
        'id': np.int32,
        column: get_type_from_column(column),
    }
    return hm


# Returns dataframe containing date in \p csv_file
def import_csv_to_df(csv_file: str) -> pd.core.frame.DataFrame:
    column: str = get_column_name(csv_file)
    if 'date' in column:
        parse_dates: list[int] = [1, 2]
        date_format: str = '%Y-%m-%d %H:%M:%S' if 'datetime' in column else '%Y-%m-%d'
        return pd.read_csv(csv_file, sep=',', header=0, parse_dates=parse_dates, date_format=date_format, dtype=get_dtypes(csv_file))
    else:
        return pd.read_csv(csv_file, sep=',', header=0, dtype=get_dtypes(csv_file))


# Extracts table name from csv filename.
def get_table_name(csv_file: str) -> str:
    return pathlib.Path(csv_file).stem.replace('-', '_')


# Exracts column name from csv filename.
def get_column_name(csv_file: str) -> str:
    return pathlib.Path(csv_file).stem.split('-')[1]


# Returns type of the given column name.
def get_type_from_column(column: str) -> type:
    return TYPE_MAP[column.split('_')[0]]


# Returns a string represenation of the SQL type of the given column name.
def get_sqltype_from_column(column: str) -> str:
    return SQLTYPE_MAP[column.split('_')[0]]


# Returns a string representation of the type for a given column name.
def get_type_str_from_column(column: str) -> str:
    return column.split('_')[0]


# Formats float to decimal representation.
def format_float(f: float) -> str:
    return np.format_float_positional(f, trim='-')


# Returns a query string to query a pandas dataframe.
def get_pandas_range_query_str(lo, hi, column) -> str:
    if isinstance(lo, str) or isinstance(lo, pd._libs.tslibs.timestamps.Timestamp):
        return f'{column} >= \'{lo}\' & {column} <= \'{hi}\''
    else:
        return f'{column} >= {lo} & {column} <= {hi}'


# Returns a query string to query a pandas dataframe.
def get_pandas_point_query_str(val, column) -> str:
    if isinstance(val, str) or isinstance(val, pd._libs.tslibs.timestamps.Timestamp):
        return f'{column} == \'{val}\''
    else:
        return f'{column} == {val}'


# Returns a string hash that is consistent across Python invocations.
def get_string_hash(x: str) -> int:
    return int(hashlib.sha256(x.encode('utf-8')).hexdigest(), 16) % 10**8


#=======================================================================================================================
# Generate Benchmarks Helper Functions
#=======================================================================================================================


# Generate a range query string for a given table, column, and hi and lo values.
def generate_range_query_str(table: str, column: str, lo, hi) -> str:
    if isinstance(lo, pd._libs.tslibs.timestamps.Timestamp):
        format: str = '%Y-%m-%d %H:%M:%S' if 'datetime' in column else '%Y-%m-%d'
        return f'SELECT id, {column} FROM {table} WHERE {column} >= d\'{lo.strftime(format)}\' AND {column} <= d\'{hi.strftime(format)}\';'
    if isinstance(lo, str):
        return f'SELECT id, {column} FROM {table} WHERE {column} >= "{lo}" AND {column} <= "{hi}";'
    else:
        return f'SELECT id, {column} FROM {table} WHERE {column} >= {lo} AND {column} <= {hi};'


# Generate a point query string for a given table, column, and value.
def generate_point_query_str(table: str, column: str, val) -> str:
    if isinstance(val, pd._libs.tslibs.timestamps.Timestamp):
        format: str = '%Y-%m-%d %H:%M:%S' if 'datetime' in column else '%Y-%m-%d'
        return f'SELECT id, {column} FROM {table} WHERE {column} = d\'{val.strftime(format)}\';'
    if isinstance(val, str):
        return f'SELECT id, {column} FROM {table} WHERE {column} = "{val}";'
    else:
        return f'SELECT id, {column} FROM {table} WHERE {column} = {val};'


# Generates the indexes section of the benchmark file.
def generate_indexes_str(column: str) -> str:
    indexes_str: str = '        indexes:\n'
    for method, _ in METHODS:
        indexes_str += (
f'            \'{column}_{method}_idx\':\n'
f'                attributes: \'{column}\'\n'
f'                method: \'{method}\'\n'
        )
    return indexes_str


# Generates the data section of the benchmark file.
def generate_data_str(table :str, columns :list[str], datafile :str, indexed_column :str | None) -> str:
    data_str = (
f'data:\n'
f'    \'{table}\':\n'
f'        file: \'{datafile}\'\n'
f'        format: \'csv\'\n'
f'        delimiter: \',\'\n'
f'        header: 1\n'
f'        attributes:\n'
    )
    for column in columns:
        data_str += (
f'            \'{column}\': \'{get_sqltype_from_column(column)} NOT NULL\'\n'
        )
    if indexed_column is not None:
        data_str += (
f'{generate_indexes_str(indexed_column)}\n'
        )
    return data_str

# Generates the pattern section of the configurations of the benchmark file.
def generate_pattern_str(pattern: tuple[str, str, str] | None = None) -> str:
    pattern_str: str = '                pattern:\n'
    if pattern is None:
        for _, pattern, pattern_description in PATTERNS:
            pattern_str += (
f'                    {pattern_description}: {pattern}\n'
        )
    else:
        pattern_str += (
f'                    {pattern[2]}: {pattern[1]}\n'
        )
    return pattern_str


# Generates the configurations section concerned with scans of the benchmark file.
def generate_scan_configurations_str(pattern: tuple[str, str, str] | None = None) -> str:
    config_str: str = ''
    for config, config_args in SCAN_CONFIGURATIONS:
        config_str += (
f'            \'{config}, 0, no index\':\n'  # scan config, index, batch size, pattern
f'                args: {config_args}\n'
f'{generate_pattern_str(pattern)}\n'
        )
    return config_str


# Generates the configurations section concerned with indexes of the benchmark file.
def generate_index_configurations_str(pattern: tuple[str, str, str] | None = None) -> str:
    config_str: str = ''
    for config, config_args in INDEX_CONFIGURATIONS:
        for method, method_args in METHODS:
            if 'hostcall' in config:
                for batch_size in BATCH_SIZES:
                    config_str += (
f'            \'{config}, {batch_size}, {method}\':\n'  # scan config, index, batch size, pattern
f'                args: {config_args} {method_args} {BATCH_SIZE_ARG} {batch_size}\n'
f'{generate_pattern_str(pattern)}\n'
                    )
            else:
                config_str += (
f'            \'{config}, 0, {method}\':\n'  # scan config, index, batch size, pattern
f'                args: {config_args} {method_args}\n'
f'{generate_pattern_str(pattern)}\n'
                    )
    return config_str


# Generates the cases section of the benchmark file.
def generate_cases_str(cases: list[tuple[str, str]]) -> str:
    cases_str: str = '        cases:\n'
    max_case_len = max([len(case) for case, _ in cases])
    for case, query in cases:
        cases_str += f'            {case}:{" " * (max_case_len - len(case))} {query}\n'
    return cases_str


#=======================================================================================================================
# Generate Benchmarks
#=======================================================================================================================


# Generates benchmark with varying selectivity for the column of the provided data.
def generate_performance_benchmark(data: pd.core.frame.DataFrame, table: str, column: str, selectivities: list[float], datafile: str, out_dir: str, is_ordered:bool=False, verbose: bool=False):
    random.seed(get_string_hash(table+column))
    series: pd.core.series.Series = data[column] if is_ordered else data[column].sort_values()

    n_rows: int = len(series)
    n_unique: int = len(pd.unique(series))
    n_reps: int = int(n_rows/n_unique)  # How often is each value repeated

    cases: list[tuple[str, str]] = list()  # list of tuples consisting of selectivity and query
    selectivity: float
    for selectivity in selectivities:
        if n_reps/n_rows > selectivity:
            print(f'  ! WARNING: Selectivity {selectivity} cannot be accomplished with {n_unique} unique values in {n_rows} rows! Skipping.')
            continue

        target: int = int(n_rows * selectivity)  # targetted number of qualifying rows
        lo: int = round(random.randint(0, n_rows - target), int(-math.log10(n_reps)))  # position of low bound in sorted list
        hi: int = lo + target - 1  # position of high bound in sorted list
        actual: int = len(data.query(get_pandas_range_query_str(series.iloc[lo], series.iloc[hi], column)))  # actual number of qualifying rows

        if 1 - min([actual, target])/max([actual, target]) > 0.01:
            print(f'  ! WARNING: Selectivity {selectivity} deviates by more than 1%! Skipping.')
            continue

        query: str = generate_range_query_str(table, column, series.iloc[lo], series.iloc[hi])
        cases.append((format_float(selectivity), query))
        if verbose:
            print(f'  + Generated case with selectivity {format_float(actual/n_rows)}.')

    for materialization_abbrev, materialization_args, materialization_description in MATERIALIZATIONS: # Materialization
        for layout_abbrev, layout_args, layout_description in LAYOUTS:
            blueprint: str = (
    f'description: Comparing table scan and index scan on {"ordered" if is_ordered else "unordered"} {get_type_str_from_column(column)} data in {layout_description} {materialization_description} for varying selectivities\n'
    f'suite: phys-cost-models\n'
    f'benchmark: index-scan\n'
    f'name: {get_type_str_from_column(column)},{"ordered" if is_ordered else "unordered"},{layout_abbrev},{materialization_abbrev}\n'
    f'readonly: true\n'
    f'{generate_data_str(table, data.columns, datafile, column)}'
    f'systems:\n'
    f'    mutable:\n'
    f'        args: {MUTABLE_ARGS} --no-wasm-compilation-cache {layout_args} {materialization_args}\n'
    f'        configurations:\n'
    f'{generate_scan_configurations_str()}'
    f'{generate_index_configurations_str()}'
    f'{generate_cases_str(cases)}'
            )

            filename: str = f'table_vs_index-{get_type_str_from_column(column)}-{"ordered" if is_ordered else "unordered"}-{layout_abbrev}-{materialization_abbrev}.yml'
            filepath: str = os.path.join(out_dir, filename)
            pathlib.Path(filepath).parent.mkdir(parents=True, exist_ok=True)
            with open(filepath, 'w') as file:
                file.write(blueprint)
                file.close()
            print(f'  ✓ Generated performance benchmarks for {table}.{column} in {layout_description} {materialization_description} containing {len(cases)} cases.')
    return


# Generates benchmark to evaluate caching of compiled queries.
def generate_caching_benchmark(data: pd.core.frame.DataFrame, table: str, column: str, n_queries: int, datafile: str, out_dir: str, is_ordered:bool=False, verbose: bool=False):
    random.seed(get_string_hash(table+column))
    series: pd.core.series.Series = data[column] if is_ordered else data[column].sort_values()

    n_rows: int = len(series)
    n_unique: int = len(pd.unique(series))
    n_reps: int = int(n_rows/n_unique)  # How often is each value repeated

    pattern: tuple[str, str, str] = ('comp_machinecode', '\'^ ` Compile WebAssembly to machine code:.*\'', 'Compile time')
    cases: list[tuple[str, str]] = [('Dummy', 'SELECT 1;')]  # Add dummy query in the beginning
    for i in range(n_queries):
        selectivity: float = random.uniform(0.0001, 0.01)
        if n_reps/n_rows > selectivity:
            print(f'  ! WARNING: Selectivity {selectivity} cannot be accomplished with {n_unique} unique values in {n_rows} rows! Skipping.')
            continue

        target: int = int(n_rows * selectivity)  # targetted number of qualifying rows
        lo: int = round(random.randint(0, n_rows - target), int(-math.log10(n_reps)))  # position of low bound in sorted list
        hi: int = lo + target - 1  # position of high bound in sorted list
        actual: int = len(data.query(get_pandas_range_query_str(series.iloc[lo], series.iloc[hi], column)))  # actual number of qualifying rows

        if 1 - min([actual, target])/max([actual, target]) > 0.01:
            print(f'  ! WARNING: Selectivity {selectivity} deviates by more than 1%! Skipping.')
            continue

        query: str = generate_range_query_str(table, column, series.iloc[lo], series.iloc[hi])
        cases.append((f'Q{i}', query))
        if verbose:
            print(f'  + Generated case with selectivity {format_float(actual/n_rows)}.')

    for materialization_abbrev, materialization_args, materialization_description in MATERIALIZATIONS: # Materialization
        for layout_abbrev, layout_args, layout_description in LAYOUTS:
            for caching_abbrev, caching_args, caching_description in CACHING_CONFIGURATIONS:
                blueprint: str = (
        f'description: Comparing table scan and index scan on {"ordered" if is_ordered else "unordered"} {get_type_str_from_column(column)} data in {layout_description} {materialization_description} and {caching_description} for varying selectivities\n'
        f'suite: phys-cost-models\n'
        f'benchmark: index-scan-caching\n'
        f'name: {get_type_str_from_column(column)},{"ordered" if is_ordered else "unordered"},{layout_abbrev},{materialization_abbrev},{caching_abbrev}\n'
        f'readonly: true\n'
        f'{generate_data_str(table, data.columns, datafile, column)}'
        f'systems:\n'
        f'    mutable:\n'
        f'        args: {MUTABLE_ARGS} {caching_args} {layout_args} {materialization_args}\n'
        f'        configurations:\n'
        f'{generate_scan_configurations_str(pattern)}'
        f'{generate_index_configurations_str(pattern)}'
        f'{generate_cases_str(cases)}'
                )

                filename: str = f'index_caching-{get_type_str_from_column(column)}-{"ordered" if is_ordered else "unordered"}-{layout_abbrev}-{materialization_abbrev}-{caching_abbrev}.yml'
                filepath: str = os.path.join(out_dir, filename)
                pathlib.Path(filepath).parent.mkdir(parents=True, exist_ok=True)
                with open(filepath, 'w') as file:
                    file.write(blueprint)
                    file.close()
            print(f'  ✓ Generated caching benchmark for {table}.{column} in {layout_description} {materialization_description} containing {len(cases)} queries.')
    return

if __name__ == '__main__':
    parser: argparse.ArgumentParser = argparse.ArgumentParser(prog="Benchmark-Gen", description="Generate benchmarks for the index-scan operator in mutable")
    parser.add_argument('-v', '--verbose', action='store_true')
    args: argparse.Namespace = parser.parse_args()

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    if args.verbose:
        print(f'Generating benchmark files in {OUTPUT_DIR}')

    csv_file: str
    for csv_file in get_csv_files(DATA_DIR):
        df: pd.core.frame.DataFrame = import_csv_to_df(csv_file)
        # We assume the following table schema
        # id: int | <type>: <type> or id: int | <type>_ordered: <type>
        assert df.columns.size == 2

        table_name: str = get_table_name(csv_file)
        column_name: str = get_column_name(csv_file)
        is_ordered = "ordered" in column_name

        print(f'Generating benchmarks for {table_name}.')
        outdir: str = os.path.join(OUTPUT_DIR, 'performance')
        generate_performance_benchmark(df, table_name, column_name, SELECTIVITIES, csv_file, outdir, is_ordered=is_ordered, verbose=args.verbose)

        outdir = os.path.join(OUTPUT_DIR, 'caching')
        generate_caching_benchmark(df, table_name, column_name, 5, csv_file, outdir, is_ordered=is_ordered, verbose=args.verbose)