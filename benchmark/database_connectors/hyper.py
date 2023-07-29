from database_connectors.connector import *
from database_connectors import hyperconf

from tableauhyperapi import HyperProcess, Telemetry, Connection, CreateMode, NOT_NULLABLE, NULLABLE, SqlType, \
        TableDefinition, Inserter, escape_name, escape_string_literal, HyperException, TableName
from tqdm import tqdm
import os
import subprocess
import sys
import sys
import time


# Used as function for new process to track time / timeout
def run_multiple_queries(connection, queries, data, queue):
    times = hyperconf.benchmark_execution_times(connection, queries, data)
    queue.put(times)

def run_single_query(connection, query):
    with connection.execute_query(query) as result:
        for row in result:
            pass


class HyPer(Connector):

    def __init__(self, args = dict()):
        self.multithreaded = args.get('multithreaded', False)
        self.verbose = args.get('verbose', False) # optional

    def execute(self, n_runs, params: dict()):
        suite = params['suite']
        benchmark = params['benchmark']
        experiment = params['name']
        suffix = f' ({get_num_cores()} cores)' if self.multithreaded else ' (single core)'
        tqdm.write(f'` Perform experiment {suite}/{benchmark}/{experiment} with configuration HyPer{suffix}.')
        sys.stdout.flush()

        path = os.getcwd()
        script = f'''
import sys
sys.path.insert(0, '{path}/benchmark')
import database_connectors.hyper
print(repr(database_connectors.hyper.HyPer._execute({n_runs}, {repr(params)})))
sys.stdout.flush()
'''

        args = list(['python3', '-c', script])

        # If not multithreaded, limit to a single core
        if not self.multithreaded:
            args = ['taskset', '-c', '2'] + args

        if self.verbose:
            tqdm.write(f"    $ {' '.join(args)}")
            sys.stdout.flush()

        timeout = n_runs * (DEFAULT_TIMEOUT + TIMEOUT_PER_CASE * len(params['cases']))
        process = subprocess.Popen(
            args=args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=path
        )

        try:
            process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            tqdm.write(f'Benchmark timed out after {timeout} seconds')
            # Set execution time of every case of every run to timeout
            times = [TIMEOUT_PER_CASE*1000 for _ in range(n_runs)]
            result = {case: times for case in params['cases'].keys()}
            result = {f'HyPer{suffix}': result}
            return result
        finally:
            if process.poll() is None: # if process is still alive
                process.terminate() # try to shut down gracefully
                try:
                    process.wait(timeout=15) #give process 15 seconds to terminate
                except subprocess.TimeoutExpired:
                    process.kill() # kill if process did not terminate in time

        # Check returncode
        result = None
        if process.returncode == 0:
            result = eval(process.stdout.read().decode('latin-1'))
        else:
            raise ConnectorException(f"Process failed with return code {process.returncode}")

        patched_result = dict()
        for key, val in result.items():
            patched_result[f'{key}{suffix}'] = val
        return patched_result

    @staticmethod
    def _execute(n_runs, params: dict()):
        measurement_times = dict()

        # Check wether tables contain scale factors
        with_scale_factors = False
        for table in params['data'].values():
            if (table.get('scale_factors')):
                with_scale_factors = True
                break

        hyperconf.init() # prepare for measurements

        for run_id in range(n_runs):
            # If tables contain scale factors, they have to be loaded separately for every case
            if with_scale_factors or not bool(params.get('readonly')):
                with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
                    with Connection(endpoint=hyper.endpoint, database='benchmark.hyper', create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
                        # Create tmp tables used for copying
                        for table_name, table in params['data'].items():
                            columns = HyPer.parse_attributes(table['attributes'])
                            table_tmp = TableDefinition(
                                table_name=f"{table_name}_tmp",
                                columns=columns
                            )
                            hyperconf.load_table(connection, table_tmp, table['file'], FORMAT=table['format'], DELIMITER=f"\'{table['delimiter']}\'", HEADER=table['header'])

                            table_def = TableDefinition(
                                table_name=table_name,
                                columns=columns
                            )
                            connection.catalog.create_table(table_def)

                        # Execute cases
                        for case, query in params['cases'].items():
                            # Set up tables
                            for table_name, table in params['data'].items():
                                connection.execute_command(f'DELETE FROM "{table_name}";')  # Empty table first

                                if table.get('scale_factors'):
                                    sf = table['scale_factors'][case]
                                else:
                                    sf = 1
                                header = int(table.get('header', 0))
                                num_rows = round((table['lines_in_file'] - header) * sf)
                                connection.execute_command(f'INSERT INTO "{table_name}" SELECT * FROM "{table_name}_tmp" LIMIT {num_rows};')

                            # Execute query
                            with connection.execute_query(query) as result:
                                for row in result:
                                    pass

                        # Extract results
                        matches = hyperconf.filter_results(
                            hyperconf.extract_results(),
                            [
                                lambda x: 'k' in x and x['k'] == 'query-end' and 'v' in x and 'statement' in x['v'] and x['v']['statement'] == 'SELECT'
                            ]
                        )
                        times = map(lambda m: m['v']['execution-time'] * 1000, matches)
                        times = list(map(lambda t: f'{t:.3f}', times))
                        times = times[run_id * len(list(params['cases'].keys())) : ] # get only times of this run, ignore previous runs
                        times = list(zip(params['cases'].keys(), times))

                        for case, time in times:
                            if case not in measurement_times.keys():
                                measurement_times[case] = list()
                            measurement_times[case].append(time)

            else:
                # Otherwise, tables have to be created just once before the measurements
                with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
                    with Connection(endpoint=hyper.endpoint, database='benchmark.hyper', create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
                        table_defs = HyPer.get_tables(params)
                        queries = HyPer.get_cases_queries(params, table_defs)
                        data = HyPer.get_data(params, table_defs)

                        times = hyperconf.benchmark_execution_times(connection, queries.values(), data)
                        times = times[run_id * len(queries.keys()):]    # get only times of this run, ignore previous runs
                        times = list(zip(queries.keys(), list(map(lambda t: float(f'{t:.3f}'), times))))

                        for case, time in times:
                            if case not in measurement_times.keys():
                                measurement_times[case] = list()
                            measurement_times[case].append(time)

                        connection.close()
                        hyper.close()

        return {'HyPer': measurement_times}


    # returns dict of {table_name: table_def} for each table_def
    @staticmethod
    def get_tables(params: dict):
        table_defs = dict()
        for table_name, table in params['data'].items():
            table_defs[table_name] = HyPer.get_table(table_name, table)
        return table_defs


    @staticmethod
    def get_table(table_name :str, table: dict):
        # If table exists, return it
        table_def = hyperconf.table_defs.get(table_name)
        if table_def:
            return table_def

        # Otherwise create a new table_def
        columns = HyPer.parse_attributes(table['attributes'])
        return TableDefinition(table_name=table_name, columns=columns)


    # Parse attributes of one table
    @staticmethod
    def parse_attributes(attributes: dict):
        columns = []
        for column_name, ty in attributes.items():
            not_null = NOT_NULLABLE if 'NOT NULL' in ty else NULLABLE
            ty = ty.split(' ')
            match (ty[0]):
                case 'INT':
                    typ = SqlType.int()
                case 'CHAR':
                    typ = SqlType.char(int(ty[1]))
                case 'DECIMAL':
                    typ = SqlType.numeric(int(ty[1]), int(ty[2]))
                case 'DATE':
                    typ = SqlType.date()
                case 'DOUBLE':
                    typ = SqlType.double()
                case 'FLOAT':
                    typ = SqlType.double()
                case 'BIGINT':
                    typ = SqlType.big_int()
                case _:
                    raise Exception(f"Unknown type given for '{column_name}'")
            columns.append(TableDefinition.Column(column_name, typ, NOT_NULLABLE))
        return columns


    # Returns cases/queries
    @staticmethod
    def get_cases_queries(params: dict, table_defs: dict):
        cases_queries = dict()
        for case, query in params['cases'].items():
            for table_name, table_def in table_defs.items():
                query.replace(table_name, f"{table_def.table_name}")
            cases_queries[case] = query

        return cases_queries


    @staticmethod
    def get_data(params: dict, table_defs: dict):
        result = []
        for table_name, table in params['data'].items():
            file = table['file']
            delimiter = table.get('delimiter', ',')
            if delimiter:
                delimiter = delimiter.replace("'", "")
            form = table.get('format', 'csv')
            header = int(table.get('header', 0))
            par = {
                'delimiter': "'" + delimiter + "'",
                'format': form,
                'header': header
            }
            result.append((table_defs[table_name], file, par))
        return result
