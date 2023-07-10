from .connector import *

from tqdm import tqdm
import json
import os
import subprocess
import sys


TMP_DB = 'tmp.duckdb'

class DuckDB(Connector):

    def __init__(self, args = dict()):
        self.duckdb_cli = args.get('path_to_binary') # required
        self.verbose = args.get('verbose', False) # optional
        self.multithreaded = args.get('multithreaded', False)

    # Runs an experiment 'n_runs' times, all parameters are in 'params'
    def execute(self, n_runs, params: dict):
        suite = params['suite']
        benchmark = params['benchmark']
        experiment = params['name']
        configname = f'DuckDB ({get_num_cores()} cores)' if self.multithreaded else 'DuckDB (single core)'
        tqdm.write(f'` Perform experiment {suite}/{benchmark}/{experiment} with configuration {configname}.')
        sys.stdout.flush()

        self.clean_up()

        measurement_times = dict()      # map that is returned with the measured times

        # Check wether tables contain scale factors
        with_scale_factors = False
        for table in params['data'].values():
            if (table.get('scale_factors')):
                with_scale_factors = True
                break

        verbose_printed = False
        for _ in range(n_runs):
            try:
                # Set up database
                create_tbl_stmts = self.generate_create_table_stmts(params['data'], with_scale_factors)


                # If tables contain scale factors, they have to be loaded separately for every case
                if (with_scale_factors or not bool(params.get('readonly'))):
                    timeout = DEFAULT_TIMEOUT + TIMEOUT_PER_CASE
                    # Write cases/queries to a file that will be passed to the command to execute
                    for i in range(len(params['cases'])):
                        case = list(params['cases'].keys())[i]
                        query_stmt = list(params['cases'].values())[i]

                        statements = list()
                        if i==0:
                            # Also use the create table stmts in this case
                            statements = create_tbl_stmts

                        # Create tables from tmp tables with scale factor
                        for table_name, table in params['data'].items():
                            statements.append(f'DELETE FROM "{table_name}";')     # empty existing table
                            if table.get('scale_factors'):
                                sf = table['scale_factors'][case]
                            else:
                                sf = 1
                            header = int(table.get('header', 0))
                            num_rows = round((table['lines_in_file'] - header) * sf)
                            statements.append(f'INSERT INTO "{table_name}" SELECT * FROM "{table_name}_tmp" LIMIT {num_rows};')

                        statements.append(".timer on")
                        statements.append(query_stmt)   # Actual query from this case
                        statements.append(".timer off")

                        combined_query = "\n".join(statements)

                        if self.verbose and not verbose_printed:
                            verbose_printed = True
                            tqdm.write(combined_query)
                            sys.stdout.flush()

                        benchmark_info = f"{suite}/{benchmark}/{experiment} [{configname}]"
                        try:
                            time = self.run_query(combined_query, timeout, benchmark_info)[0]
                        except ExperimentTimeoutExpired as ex:
                            time = timeout

                        if case not in measurement_times.keys():
                            measurement_times[case] = list()
                        measurement_times[case].append(time)



                # Otherwise, tables have to be created just once before the measurements (done above)
                else:
                    timeout = DEFAULT_TIMEOUT + TIMEOUT_PER_CASE * len(params['cases'])

                    statements = create_tbl_stmts
                    statements.append(".timer on")
                    for case_query in params['cases'].values():
                        statements.append(case_query)
                    statements.append(".timer off")

                    combined_query = "\n".join(statements)

                    if self.verbose and not verbose_printed:
                        verbose_printed = True
                        tqdm.write(combined_query)
                        sys.stdout.flush()

                    benchmark_info = f"{suite}/{benchmark}/{experiment} [{configname}]"
                    try:
                        durations = self.run_query(combined_query, timeout, benchmark_info)
                    except ExperimentTimeoutExpired as ex:
                        for case in params['cases'].keys():
                            if case not in measurement_times.keys():
                                measurement_times[case] = list()
                            measurement_times[case].append(timeout * 1000)
                    else:
                        for idx, time in enumerate(durations):
                            case = list(params['cases'].keys())[idx]
                            if case not in measurement_times.keys():
                                measurement_times[case] = list()
                            measurement_times[case].append(time)


            finally:
                self.clean_up()

        return { configname: measurement_times }


    # Deletes the used temporary database
    def clean_up(self):
        if os.path.exists(TMP_DB):
            os.remove(TMP_DB)


    # Parse attributes of one table, return as string
    def parse_attributes(self, attributes: dict):
        columns = list()
        for column_name, ty in attributes.items():
            not_null = 'NOT NULL' if 'NOT NULL' in ty else ''
            ty = ty.split(' ')
            match (ty[0]):
                case 'INT':
                    type = 'INT'
                case 'CHAR':
                    type = f'CHAR({ty[1]})'
                case 'DECIMAL':
                    type = f'DECIMAL({ty[1]},{ty[2]})'
                case 'DATE':
                    type = 'DATE'
                case 'DOUBLE':
                    type = 'DOUBLE'
                case 'FLOAT':
                    type = 'REAL'
                case 'BIGINT':
                    type = 'BIGINT'
                case _:
                    raise AttributeTypeUnknown(f"Unknown type given for '{column_name}'")
            columns.append(f'"{column_name}" {type} {not_null}')
        return '(' + ',\n'.join(columns) + ')'


    # Creates tables in the database and copies contents of given files into them
    # Call with 'with_scale_factors'=False if data should be loaded as a whole
    # Call with 'with_scale_factors'=True if data should be placed in tmp tables
    # and copied for each case with different scale factor
    def generate_create_table_stmts(self, data: dict, with_scale_factors):
        statements = list()
        for table_name, table in data.items():
            columns = self.parse_attributes(table['attributes'])

            delimiter = table.get('delimiter')
            header = table.get('header')
            format = table.get('format')

            if with_scale_factors:
                table_name += "_tmp"

            create = f'CREATE TABLE "{table_name}" {columns};'
            copy = f'COPY "{table_name}" FROM \'{table["file"]}\' ( '
            if delimiter:
                delim = delimiter.replace("'", "")
                copy += f" DELIMITER \'{delim}\',"
            if format:
                copy += f" FORMAT {format.upper()},"
            if header:
                copy += f" HEADER," if (header==1) else ""

            copy = copy[:-1] + " );"

            statements.append(create)
            statements.append(copy)

            if with_scale_factors:
                # Create actual table that will be used for experiment
                statements.append(f'CREATE TABLE "{table_name[:-4]}" {columns};')

        return statements


    def run_query(self, query, timeout, benchmark_info):
        command = f"./{self.duckdb_cli} {TMP_DB}"
        if not self.multithreaded:
            command = 'taskset -c 2 ' + command

        process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   cwd=os.getcwd(), shell=True, text=True)
        try:
            out, err = process.communicate(query, timeout=timeout)
        except subprocess.TimeoutExpired:
            process.kill()
            tqdm.write(f"    ! Query \n'{query}'\n' timed out after {timeout} seconds")
            sys.stdout.flush()
            raise ExperimentTimeoutExpired(f'Query timed out after {timeout} seconds')
        finally:
            if process.poll() is None: # if process is still alive
                process.terminate() # try to shut down gracefully
                try:
                    process.wait(timeout=1) # wait for process to terminate
                except subprocess.TimeoutExpired:
                    process.kill() # kill if process did not terminate in time


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
        durations = os.popen(f"echo '{out}'" + " | grep 'Run Time' | cut -d ' ' -f 5 | awk '{print $1 * 1000;}'").read()
        durations = durations.split('\n')
        durations.remove('')
        durations = [float(i.replace("\n", "").replace(",", ".")) for i in durations]

        return durations
