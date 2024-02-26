from .connector import *
from benchmark_utils import *

from typeguard import typechecked
from typing import Any
import os
import psycopg2
import psycopg2.extensions


db_options: dict[str, str] = {
    'dbname': 'benchmark_tmp',
    'user': 'postgres'
}
TMP_SQL_FILE: str            = 'tmp.sql'
COMPLETE_TABLE_SUFFIX: str   = '_complete'


# The connector for PostgreSQL
@typechecked
class PostgreSQL(Connector):

    def __init__(self, args: dict[str, Any]) -> None:
        self.verbose = args.get('verbose', False)   # optional

    POSTGRESQL_TYPE_PARSER: dict[str, Callable[[list[str]], str]] = {
        'TINYINT':     lambda ty: 'SMALLINT', # TINYINT not supported by PostgreSQL, fallback to SMALLINT
        'SMALLINT':    lambda ty: 'SMALLINT',
        'INT':         lambda ty: 'INT',
        'BIGINT':      lambda ty: 'BIGINT',
        'FLOAT':       lambda ty: 'REAL',
        'DOUBLE':      lambda ty: 'DOUBLE PRECISION',
        'DECIMAL':     lambda ty: f'DECIMAL({ty[1]}, {ty[2]})',
        'CHAR':        lambda ty: f'CHAR({ty[1]})',
        'DATE':        lambda ty: 'DATE',
        'DATETIME':    lambda ty: 'TIMESTAMP',
        'NOT NULL':    lambda ty: 'NOT NULL',
        'PRIMARY KEY': lambda ty: 'PRIMARY KEY',
        'UNIQUE':      lambda ty: 'UNIQUE',
        'REFERENCES':  lambda ty: f'REFERENCES {ty[ty.index("REFERENCES")+1]}',
    }

    # Runs an experiment n_runs time, all parameters are in 'params'
    def execute(self, n_runs: int, params: dict[str, Any]) -> ConnectorResult:
        suite: str = params['suite']
        benchmark: str = params['benchmark']
        experiment: str = params['name']
        cases: dict[Case, Any] = params['cases']
        tqdm_print(f'` Perform experiment {suite}/{benchmark}/{experiment} with configuration PostgreSQL.')

        config_result: ConfigResult = dict()      # map that is returned with the measured times
        for case in cases.keys():
            config_result[case] = list()

        # Variables
        connection: psycopg2.extensions.connection
        cursor: psycopg2.extensions.cursor
        timeout: int
        benchmark_info: str
        durations: list[float]

        verbose_printed = False

        # For query execution
        command: str = f"psql -U {db_options['user']} -d {db_options['dbname']} -f {TMP_SQL_FILE} | grep 'Time' | cut -d ' ' -f 2"
        popen_args: dict[str, Any] = {'shell': True}
        benchmark_info = f"{suite}/{benchmark}/{experiment} [PostgreSQL]"

        if self.check_execute_single_cases(params):
            # If tables contain scale factors, they have to be loaded separately for every case
            for _ in range(n_runs):
                # Prepare db
                self.prepare_db(params)

                for case, query_stmt in cases.items():
                    connection = psycopg2.connect(**db_options)
                    try:
                        connection.autocommit = True
                        cursor = connection.cursor()
                        # Create tables from tmp tables with scale factor
                        for table_name, table in params['data'].items():
                            sf: float | int
                            if table.get('scale_factors') is not None:
                                scale_factors = table['scale_factors']
                                if isinstance(scale_factors, float) or isinstance(scale_factors, int):
                                    sf = scale_factors
                                elif case is not None:
                                    sf = scale_factors[case]
                                else:
                                    sf = 1
                            else:
                                sf = 1
                            header: int = int(table.get('header', 0))
                            num_rows: int = round((table['lines_in_file'] - header) * sf)
                            cursor.execute(f'DELETE FROM "{table_name}";')     # empty existing table
                            if 'indexes' in table:
                                drop_indexes: list[str] = self.generate_drop_index_stmts(table['indexes'])
                                cursor.execute(''.join(drop_indexes))
                            cursor.execute(f'INSERT INTO "{table_name}" SELECT * FROM "{table_name}{COMPLETE_TABLE_SUFFIX}" LIMIT {num_rows};')    # copy data with scale factor
                            if 'indexes' in table:
                                create_indexes: list[str] = self.generate_create_index_stmts(table_name, table['indexes'])
                                cursor.execute(''.join(create_indexes))
                    finally:
                        connection.close()
                        del connection

                    # Write case/query to a file that will be passed to the command to execute
                    with open(TMP_SQL_FILE, "w") as tmp:
                        tmp.write(f'set statement_timeout = {TIMEOUT_PER_CASE * 1000:.0f};\n')
                        tmp.write("\\timing on\n")
                        tmp.write(query_stmt + '\n')
                        tmp.write("\\timing off\n")
                        tmp.write(f'set statement_timeout = 0;\n')

                    # Execute query as benchmark and get measurement time
                    if self.verbose:
                        self.print_command(command, '')

                    timeout = TIMEOUT_PER_CASE
                    try:
                        out: str = self.benchmark_query(command=command, query='', timeout=timeout,
                                                        popen_args=popen_args, benchmark_info=benchmark_info,
                                                        verbose=self.verbose)
                        durations = self.parse_results(out)
                    except ExperimentTimeoutExpired:
                        config_result[case].append(float(TIMEOUT_PER_CASE * 1000))
                    else:
                        if len(durations) != 1:
                            raise ConnectorException(f"Expected 1 measurement but got {len(durations)}.")
                        config_result[case].append(durations[0])

                self.clean_up()

        else:
            # Prepare db
            actual_tables: list[str] = self.prepare_db(params)

            # Dropping and recreating tables and indexes in between runs removes any cache influences
            refill_stmts: list[str] = list()
            for name, table in params['data'].items():
                refill_stmts.append(f'DROP TABLE "{name}";')
                # Dropping a table also drops its indexes
            refill_stmts.extend(actual_tables)
            for name, table in params['data'].items():
                refill_stmts.append(f'INSERT INTO "{name}" (SELECT * FROM "{name}{COMPLETE_TABLE_SUFFIX}");')
                create_indexes: list[str] = self.generate_create_index_stmts(name, table.get('indexes', dict()))
                refill_stmts.extend(create_indexes)

            # Write cases/queries to a file that will be passed to the command to execute
            with open(TMP_SQL_FILE, "w") as tmp:
                tmp.write(f'set statement_timeout = {TIMEOUT_PER_CASE * 1000:.0f};\n')
                for _ in range(n_runs):
                    for stmt in refill_stmts:
                        tmp.write(stmt + '\n')
                    tmp.write("\\timing on\n")
                    for case_query in cases.values():
                        tmp.write(case_query + '\n')
                    tmp.write("\\timing off\n")
                tmp.write(f'set statement_timeout = 0;\n')

            # Execute query file and collect measurement data
            if self.verbose:
                self.print_command(command, '')

            timeout = DEFAULT_TIMEOUT + TIMEOUT_PER_CASE * len(cases) * n_runs
            try:
                out: str = self.benchmark_query(command=command, query='', timeout=timeout,
                                                popen_args=popen_args, benchmark_info=benchmark_info,
                                                verbose=self.verbose)
                durations = self.parse_results(out)
            except ExperimentTimeoutExpired:
                for _ in range(n_runs):
                    for case in cases.keys():
                        config_result[case].append(float(TIMEOUT_PER_CASE * 1000))
            else:
                if len(durations) != n_runs * len(cases):
                    raise ConnectorException(f"Expected {n_runs * len(cases)} measurements but got {len(durations)}.")
                for i in range(n_runs):
                    run_durations: list[float] = durations[i * len(cases) : (i + 1) * len(cases)]
                    for case, dur in zip(list(cases.keys()), run_durations):
                        config_result[case].append(float(dur))

        self.clean_up()

        return {'PostgreSQL': config_result}


    # Sets up the database
    def setup(self) -> None:
        # Delete existing 'benchmark_tmp' database and create a new empty one
        connection = psycopg2.connect(user=db_options['user'])
        try:
            connection.autocommit = True
            cursor = connection.cursor()
            cursor.execute(f"DROP DATABASE IF EXISTS {db_options['dbname']};")
            cursor.execute(f"CREATE DATABASE {db_options['dbname']};")
        finally:
            connection.close()


    # Deletes the used temporary database and file
    def clean_up(self) -> None:
        connection = psycopg2.connect(user=db_options['user'])
        try:
            connection.autocommit = True
            cursor = connection.cursor()
            cursor.execute(f"DROP DATABASE IF EXISTS {db_options['dbname']};")
        finally:
            connection.close()
            if os.path.exists(TMP_SQL_FILE):
                os.remove(TMP_SQL_FILE)


    def prepare_db(self, params: dict[str, Any]) -> list[str]:
        # Set up database
        self.setup()

        # Connect to database and set up tables
        connection = psycopg2.connect(**db_options)
        try:
            connection.autocommit = True
            cursor = connection.cursor()
            cursor.execute("set jit=off;")
            actual_tables: list[str] = self.create_tables(cursor, params['data'])
        finally:
            connection.close()
            del connection
        return actual_tables

    # Creates tables in the database and copies contents of given files into them.
    # The complete data is in the 'T_complete' tables. For the individual cases the actual table T
    # can be filled using 'INSERT INTO T (SELECT * FROM T_complete LIMIT x)'
    # Returns the list of actual table statements
    def create_tables(self, cursor: psycopg2.extensions.cursor, data: dict[str, dict[str, Any]]) -> list[str]:
        actual_tables: list[str] = list()
        for table_name, table in data.items():
            columns: str = Connector.parse_attributes(self.POSTGRESQL_TYPE_PARSER, table['attributes'])

            # Use an additional table with the *complete* data set to quickly recreate the table with the benchmark
            # data, in case of varying scale factor.
            complete_table_name: str = table_name + COMPLETE_TABLE_SUFFIX
            quoted_table_name: str = f'"{complete_table_name}"'

            create: str = f"CREATE UNLOGGED TABLE {quoted_table_name} {columns};"
            copy: str = f"COPY {quoted_table_name} FROM STDIN"
            if 'delimiter' in table:
                delim: str = table['delimiter'].replace("'", "")
                copy += f" WITH DELIMITER \'{delim}\'"
            if 'format' in table:
                copy += f" {table['format'].upper()}"
            if 'header' in table:
                copy += ' HEADER' if table['header'] == 1 else ''

            copy += ";"

            cursor.execute(create)
            with open(f"{os.path.abspath(os.getcwd())}/{table['file']}", 'r') as datafile:
                try:
                    cursor.copy_expert(sql=copy, file=datafile)
                except psycopg2.errors.BadCopyFileFormat as ex:
                    raise ConnectorException(str(ex))

            # Create actual table that will be used for experiment
            actual: str = f'CREATE UNLOGGED TABLE "{table_name}" {columns};'
            cursor.execute(actual)
            actual_tables.append(actual)

        return actual_tables


    # Parse `results` for timings
    @staticmethod
    def parse_results(results: str) -> list[float]:
        durations: list[str] = results.split('\n')
        durations.remove('')
        timings: list[float] = [float(dur.replace("\n", "").replace(",", ".")) for dur in durations]
        return timings


    # Overrides `print_command` from Connector ABC
    def print_command(self, command: str | bytes | Sequence[str | bytes], query: str, indent: str = '') -> None:
        # postgres connector only uses str as command
        if command is not str:
            pass
        indent = '    '
        tqdm_print(f'{indent}$ {command}')
        tqdm_print(f'{indent}Content of `tmp.sql:`')
        with open(TMP_SQL_FILE) as tmp:
            tqdm_print(indent + indent.join(tmp.readlines()))
