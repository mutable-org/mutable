from .connector import *

from tqdm import tqdm
from typeguard import typechecked
from typing import Any
import os
import psycopg2
import psycopg2.extensions
import subprocess
import sys

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

    # Runs an experiment one time, all parameters are in 'params'
    def execute(self, n_runs: int, params: dict[str, Any]) -> ConnectorResult:
        suite: str = params['suite']
        benchmark: str = params['benchmark']
        experiment: str = params['name']
        tqdm.write(f'` Perform experiment {suite}/{benchmark}/{experiment} with configuration PostgreSQL.')
        sys.stdout.flush()

        config_result: ConfigResult = dict()      # map that is returned with the measured times

        # Check whether tables contain scale factors
        with_scale_factors: bool = False
        for table in params['data'].values():
            if table.get('scale_factors'):
                with_scale_factors = True
                break

        # Variables
        connection: psycopg2.extensions.connection
        cursor: psycopg2.extensions.cursor
        command: str
        timeout: int
        benchmark_info: str
        durations: list[float]

        verbose_printed = False
        for _ in range(n_runs):
            # Set up database
            self.setup()

            # Connect to database and set up tables
            connection = psycopg2.connect(**db_options)
            try:
                connection.autocommit = True
                cursor = connection.cursor()
                cursor.execute("set jit=off;")
                self.create_tables(cursor, params['data'], with_scale_factors)
            finally:
                connection.close()
                del connection

            # If tables contain scale factors, they have to be loaded separately for every case
            if with_scale_factors or not params.get('readonly', False):
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
                    command = f"psql -U {db_options['user']} -d {db_options['dbname']} -f {TMP_SQL_FILE} | grep 'Time' | cut -d ' ' -f 2"
                    if self.verbose:
                        tqdm.write(f"    $ {command}")
                        if not verbose_printed:
                            verbose_printed = True
                            with open(TMP_SQL_FILE) as tmp:
                                tqdm.write("    " + "    ".join(tmp.readlines()))
                        sys.stdout.flush()

                    timeout = TIMEOUT_PER_CASE
                    benchmark_info = f"{suite}/{benchmark}/{experiment} [PostgreSQL]"
                    try:
                        durations = self.run_command(command, timeout, benchmark_info)
                    except ExperimentTimeoutExpired:
                        if case not in config_result.keys():
                            config_result[case] = list()
                        config_result[case].append(TIMEOUT_PER_CASE * 1000)
                    else:
                        for idx, time in enumerate(durations):
                            if case not in config_result.keys():
                                config_result[case] = list()
                            config_result[case].append(time)

            # Otherwise, tables have to be created just once before the measurements (done above)
            else:
                # Write cases/queries to a file that will be passed to the command to execute
                with open(TMP_SQL_FILE, "w") as tmp:
                    tmp.write(f'set statement_timeout = {TIMEOUT_PER_CASE * 1000:.0f};\n')
                    tmp.write("\\timing on\n")
                    for case_query in params['cases'].values():
                        tmp.write(case_query + '\n')
                    tmp.write("\\timing off\n")
                    tmp.write(f'set statement_timeout = 0;\n')

                # Execute query file and collect measurement data
                command = f"psql -U {db_options['user']} -d {db_options['dbname']} -f {TMP_SQL_FILE} | grep 'Time' | cut -d ' ' -f 2"
                if self.verbose:
                    tqdm.write(f"    $ {command}")
                    if not verbose_printed:
                        verbose_printed = True
                        with open(TMP_SQL_FILE) as tmp:
                            tqdm.write("    " + "    ".join(tmp.readlines()))
                    sys.stdout.flush()

                timeout = DEFAULT_TIMEOUT + TIMEOUT_PER_CASE * len(params['cases'])
                benchmark_info = f"{suite}/{benchmark}/{experiment} [PostgreSQL]"
                try:
                    durations = self.run_command(command, timeout, benchmark_info)
                except ExperimentTimeoutExpired:
                    for case in params['cases'].keys():
                        if case not in config_result.keys():
                            config_result[case] = list()
                        config_result[case].append(TIMEOUT_PER_CASE * 1000)
                else:
                    for idx, time in enumerate(durations):
                        case = list(params['cases'].keys())[idx]
                        if case not in config_result.keys():
                            config_result[case] = list()
                        config_result[case].append(time)

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


    # Parse attributes of one table, return as string ready for a CREATE TABLE query
    def parse_attributes(self, attributes: dict[str, str]) -> str:
        columns: list[str] = list()
        for column_name, type_info in attributes.items():
            ty_list: list[str] = [column_name]

            ty = type_info.split(' ')
            match ty[0]:
                case 'INT':
                    ty_list.append('INT')
                case 'BIGINT':
                    ty_list.append('BIGINT')
                case 'FLOAT':
                    ty_list.append('REAL')
                case 'DOUBLE':
                    ty_list.append('DOUBLE PRECISION')
                case 'DECIMAL':
                    ty_list.append(f'DECIMAL({ty[1]}, {ty[2]})')
                case 'CHAR':
                    ty_list.append(f'CHAR({ty[1]})')
                case 'DATE':
                    ty_list.append('DATE')
                case 'DATETIME':
                    ty_list.append('TIMESTAMP')
                case _:
                    raise Exception(f"Unknown type given for '{column_name}'")

            if 'NOT NULL' in type_info:
                ty_list.append('NOT NULL')
            if 'PRIMARY KEY' in type_info:
                ty_list.append('PRIMARY KEY')
            if 'UNIQUE' in type_info:
                ty_list.append('UNIQUE')

            columns.append(' '.join(ty_list))
        return '(' + ',\n'.join(columns) + ')'


    # Creates tables in the database and copies contents of given files into them
    # Call with 'with_scale_factors'=False if data should be loaded as a whole
    # Call with 'with_scale_factors'=True if data should be placed in tmp tables
    # and copied for each case with different scale factor
    def create_tables(self, cursor: psycopg2.extensions.cursor, data: dict[str, dict[str, Any]], with_scale_factors: bool) -> None:
        for table_name, table in data.items():
            columns: str = self.parse_attributes(table['attributes'])

            delimiter: str = table.get('delimiter')
            header: int = table.get('header')
            format: str = table.get('format')

            # Use an additional table with the *complete* data set to quickly recreate the table with the benchmark
            # data, in case of varying scale factor.
            complete_table_name: str = table_name + COMPLETE_TABLE_SUFFIX if with_scale_factors else table_name
            quoted_table_name: str = f'"{complete_table_name}"'

            create: str = f"CREATE UNLOGGED TABLE {quoted_table_name} {columns};"
            copy: str = f"COPY {quoted_table_name} FROM STDIN"
            if delimiter:
                delim: str = delimiter.replace("'", "")
                copy += f" WITH DELIMITER \'{delim}\'"
            if format:
                copy += f" {format.upper()}"
            if header:
                copy += ' HEADER' if header == 1 else ''

            copy += ";"

            cursor.execute(create)
            with open(f"{os.path.abspath(os.getcwd())}/{table['file']}", 'r') as datafile:
                try:
                    cursor.copy_expert(sql=copy, file=datafile)
                except psycopg2.errors.BadCopyFileFormat as ex:
                    raise ConnectorException(str(ex))

            if with_scale_factors:
                cursor.execute(f'CREATE UNLOGGED TABLE "{table_name}" {columns};')     # Create actual table that will be used for experiment


    def run_command(self, command, timeout: int, benchmark_info: str) -> list[float]:
        process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   cwd=os.getcwd(), shell=True)
        try:
            proc_out, proc_err = process.communicate("".encode('latin-1'), timeout=timeout)
        except subprocess.TimeoutExpired:
            raise ExperimentTimeoutExpired(f'Query timed out after {timeout} seconds')
        finally:
            if process.poll() is None: # if process is still alive
                process.terminate() # try to shut down gracefully
                try:
                    process.wait(timeout=1) # give process 1 second to terminate
                except subprocess.TimeoutExpired:
                    process.kill() # kill if process did not terminate in time

        out: str = proc_out.decode('latin-1')
        err: str = proc_err.decode('latin-1')

        assert process.returncode is not None
        if process.returncode or len(err):
            outstr = '\n'.join(out.split('\n')[-20:])
            tqdm.write(f'''\
    Unexpected failure during execution of benchmark "{benchmark_info}" with return code {process.returncode}:''')
            tqdm.write(command)
            tqdm.write(f'''\
    ===== stdout =====
    {outstr}
    ===== stderr =====
    {err}
    ==================
    ''')
            sys.stdout.flush()
            if process.returncode:
                raise ConnectorException(f'Benchmark failed with return code {process.returncode}.')

        # Parse `out` for timings
        durations: list[str] = out.split('\n')
        durations.remove('')
        timings: list[float] = [float(dur.replace("\n", "").replace(",", ".")) for dur in durations]

        return timings
