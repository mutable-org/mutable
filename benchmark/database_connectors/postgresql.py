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
    }

    # Runs an experiment one time, all parameters are in 'params'
    def execute(self, n_runs: int, params: dict[str, Any]) -> ConnectorResult:
        suite: str = params['suite']
        benchmark: str = params['benchmark']
        experiment: str = params['name']
        tqdm_print(f'` Perform experiment {suite}/{benchmark}/{experiment} with configuration PostgreSQL.')

        config_result: ConfigResult = dict()      # map that is returned with the measured times

        # Variables
        connection: psycopg2.extensions.connection
        cursor: psycopg2.extensions.cursor
        command: str
        timeout: int
        benchmark_info: str
        durations: list[float]

        verbose_printed = False

        # For query execution
        command = f"psql -U {db_options['user']} -d {db_options['dbname']} -f {TMP_SQL_FILE} | grep 'Time' | cut -d ' ' -f 2"
        popen_args: dict[str, Any] = {'shell': True}
        benchmark_info = f"{suite}/{benchmark}/{experiment} [PostgreSQL]"
        for _ in range(n_runs):
            # Set up database
            self.setup()

            # Connect to database and set up tables
            connection = psycopg2.connect(**db_options)
            try:
                connection.autocommit = True
                cursor = connection.cursor()
                cursor.execute("set jit=off;")
                self.create_tables(cursor, params['data'], self.check_with_scale_factors(params))
            finally:
                connection.close()
                del connection

            if self.check_execute_single_cases(params):
                # If tables contain scale factors, they have to be loaded separately for every case
                for case, query_stmt in params['cases'].items():
                    connection = psycopg2.connect(**db_options)
                    try:
                        connection.autocommit = True
                        cursor = connection.cursor()
                        # Create tables from tmp tables with scale factor
                        for table_name, table in params['data'].items():
                            sf: float | int
                            if table.get('scale_factors'):
                                sf = table['scale_factors'][case]
                            else:
                                sf = 1
                            header: int = int(table.get('header', 0))
                            num_rows: int = round((table['lines_in_file'] - header) * sf)
                            cursor.execute(f'DELETE FROM "{table_name}";')     # empty existing table
                            cursor.execute(f'INSERT INTO "{table_name}" SELECT * FROM "{table_name}{COMPLETE_TABLE_SUFFIX}" LIMIT {num_rows};')    # copy data with scale factor
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
                        tqdm_print(f"    $ {command}")
                        if not verbose_printed:
                            verbose_printed = True
                            with open(TMP_SQL_FILE) as tmp:
                                tqdm_print("    " + "    ".join(tmp.readlines()))

                    timeout = TIMEOUT_PER_CASE
                    try:
                        out: str = self.benchmark_query(command=command, query='', timeout=timeout,
                                                        popen_args=popen_args, benchmark_info=benchmark_info,
                                                        verbose=self.verbose)
                        durations = self.parse_results(out)
                    except ExperimentTimeoutExpired:
                        if case not in config_result.keys():
                            config_result[case] = list()
                        config_result[case].append(float(TIMEOUT_PER_CASE * 1000))
                    else:
                        for idx, time in enumerate(durations):
                            if case not in config_result.keys():
                                config_result[case] = list()
                            config_result[case].append(float(time))

            else:
                # Otherwise, tables have to be created just once before the measurements (done above)
                # Write cases/queries to a file that will be passed to the command to execute
                with open(TMP_SQL_FILE, "w") as tmp:
                    tmp.write(f'set statement_timeout = {TIMEOUT_PER_CASE * 1000:.0f};\n')
                    tmp.write("\\timing on\n")
                    for case_query in params['cases'].values():
                        tmp.write(case_query + '\n')
                    tmp.write("\\timing off\n")
                    tmp.write(f'set statement_timeout = 0;\n')

                # Execute query file and collect measurement data
                if self.verbose:
                    tqdm_print(f"    $ {command}")
                    if not verbose_printed:
                        verbose_printed = True
                        with open(TMP_SQL_FILE) as tmp:
                            tqdm_print("    " + "    ".join(tmp.readlines()))

                timeout = DEFAULT_TIMEOUT + TIMEOUT_PER_CASE * len(params['cases'])
                try:
                    out: str = self.benchmark_query(command=command, query='', timeout=timeout,
                                                    popen_args=popen_args, benchmark_info=benchmark_info,
                                                    verbose=self.verbose)
                    durations = self.parse_results(out)
                except ExperimentTimeoutExpired:
                    for case in params['cases'].keys():
                        if case not in config_result.keys():
                            config_result[case] = list()
                        config_result[case].append(float(TIMEOUT_PER_CASE * 1000))
                else:
                    for idx, time in enumerate(durations):
                        case = list(params['cases'].keys())[idx]
                        if case not in config_result.keys():
                            config_result[case] = list()
                        config_result[case].append(float(time))

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


    # Creates tables in the database and copies contents of given files into them
    # Call with 'with_scale_factors'=False if data should be loaded as a whole
    # Call with 'with_scale_factors'=True if data should be placed in tmp tables
    # and copied for each case with different scale factor
    def create_tables(self, cursor: psycopg2.extensions.cursor, data: dict[str, dict[str, Any]], with_scale_factors: bool) -> None:
        for table_name, table in data.items():
            columns: str = Connector.parse_attributes(self.POSTGRESQL_TYPE_PARSER, table['attributes'])

            # Use an additional table with the *complete* data set to quickly recreate the table with the benchmark
            # data, in case of varying scale factor.
            complete_table_name: str = table_name + COMPLETE_TABLE_SUFFIX if with_scale_factors else table_name
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

            if with_scale_factors:
                cursor.execute(f'CREATE UNLOGGED TABLE "{table_name}" {columns};')     # Create actual table that will be used for experiment


    # Parse `results` for timings
    @staticmethod
    def parse_results(results: str) -> list[float]:
        durations: list[str] = results.split('\n')
        durations.remove('')
        timings: list[float] = [float(dur.replace("\n", "").replace(",", ".")) for dur in durations]
        return timings
