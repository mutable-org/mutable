from database_connectors.connector import *
from database_connectors import hyperconf
from benchmark_utils import *

from tableauhyperapi import HyperProcess, Telemetry, Connection, CreateMode, NOT_NULLABLE, NULLABLE, SqlType, \
        TableDefinition, Inserter, escape_name, escape_string_literal, HyperException, TableName
from typeguard import typechecked
from typing import Any, Sequence
import os


@typechecked
class HyPer(Connector):

    def __init__(self, args: dict[str, Any]) -> None:
        self.multithreaded = args.get('multithreaded', False)
        self.verbose = args.get('verbose', False)   # optional

    def execute(self, n_runs: int, params: dict[str, Any]) -> ConnectorResult:
        suite: str = params['suite']
        benchmark: str = params['benchmark']
        experiment: str = params['name']
        suffix: str = f' ({get_num_cores()} cores)' if self.multithreaded else ' (single core)'
        tqdm_print(f'` Perform experiment {suite}/{benchmark}/{experiment} with configuration HyPer{suffix}.')

        script = f'''
import sys
sys.path.insert(0, '{os.getcwd()}/benchmark')
import database_connectors.hyper
print(repr(database_connectors.hyper.HyPer._execute({n_runs}, {repr(params)})))
sys.stdout.flush()
'''

        args: list[str] = ['python3', '-c', script]

        # If not multithreaded, limit to a single core
        if not self.multithreaded:
            args = ['taskset', '-c', '2'] + args

        if self.verbose:
            tqdm_print(f"    $ {' '.join(args)}")

        timeout: int = n_runs * (DEFAULT_TIMEOUT + TIMEOUT_PER_CASE * len(params['cases']))

        try:
            out: str = self.benchmark_query(command=args, query='', timeout=timeout,
                benchmark_info=f'{suite}/{benchmark}/{experiment} [HyPer{suffix}]',
                verbose=self.verbose)
        except ExperimentTimeoutExpired:
            times: list[float] = [float(TIMEOUT_PER_CASE * 1000) for _ in range(n_runs)]
            config_result: ConfigResult = {case: times for case in params['cases'].keys()}
            result: ConnectorResult = {f'HyPer{suffix}': config_result}
            return result

        result: ConnectorResult = eval(out)
        patched_result: ConnectorResult = dict()
        for key, val in result.items():
            patched_result[f'{key}{suffix}'] = val
        return patched_result

    @staticmethod
    def _execute(n_runs: int, params: dict[str, Any]) -> ConnectorResult:
        config_result: ConfigResult = dict()

        cases: dict[Case, Any] = params['cases']
        for case in cases.keys():
            config_result[case] = list()

        hyperconf.init()    # prepare for measurements

        # If tables contain scale factors, they have to be loaded separately for every case
        if Connector.check_execute_single_cases(params):
            for _ in range(n_runs):
                with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
                    with Connection(endpoint=hyper.endpoint, database='benchmark.hyper', create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
                        # Create tmp tables used for copying
                        for table_name, table in params['data'].items():
                            columns: list[TableDefinition.Column] = HyPer.parse_attributes(table['attributes'])
                            table_tmp = TableDefinition(
                                table_name=f"{table_name}_tmp",
                                columns=columns
                            )
                            hyperconf.load_table(connection, table_tmp, table['file'], FORMAT=table.get('format', 'csv'), DELIMITER=f"\'{table['delimiter']}\'", HEADER=table.get('header', 0))

                            table_def: TableDefinition = TableDefinition(
                                table_name=table_name,
                                columns=columns
                            )
                            connection.catalog.create_table(table_def)
                        # Execute cases
                        for case, query in cases.items():
                            # Set up tables
                            for table_name, table in params['data'].items():
                                connection.execute_command(f'DELETE FROM "{table_name}";')  # Empty table first
                                # Index support is currently disabled in tableauhyperapi
                                # drop_indexes: list[str] = HyPer.generate_drop_index_stmts(table.get('indexes', dict()))
                                # for stmt in drop_indexes:
                                #     connection.execute_command(stmt)  # Drop indexes

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
                                connection.execute_command(f'INSERT INTO "{table_name}" SELECT * FROM "{table_name}_tmp" LIMIT {num_rows};')
                                # Index support is currently disabled in tableauhyperapi
                                # create_indexes: list[str] = HyPer.generate_create_index_stmts(table_name, table.get('indexes', dict()))
                                # for stmt in create_indexes:
                                #     connection.execute_command(stmt)  # Create indexes

                            # Execute query
                            with connection.execute_query(query) as result:
                                for row in result:
                                    pass
                        connection.close()
                        hyper.close()

                # Extract results
                matches: list[Any] = hyperconf.filter_results(
                    hyperconf.extract_results(),
                    [
                        lambda x: 'k' in x and x['k'] == 'query-end' and 'v' in x and 'statement' in x['v'] and x['v']['statement'] == 'SELECT'
                    ]
                )
                times: list[float] = list(map(lambda m: float(m['v']['execution-time'] * 1000), matches))
                times = list(map(lambda t: float(f'{t:.3f}'), times))
                times = times[ - len(cases) : ]    # get only times of this run, ignore previous runs
                for case, time in zip(cases.keys(), times):
                    config_result[case].append(time)

        else:
            # Otherwise, tables have to be created just once before the measurements
            with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
                with Connection(endpoint=hyper.endpoint, database='benchmark.hyper', create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
                    table_defs: dict[str, TableDefinition] = HyPer.get_tables(params)
                    queries: dict[Case, str] = HyPer.get_cases_queries(params, table_defs)
                    data: list[tuple[TableDefinition, str, dict]] = HyPer.get_data(params, table_defs)

                    all_queries: list[str] = list()
                    for _ in range(n_runs):
                        all_queries.extend(queries.values())
                    times: list = hyperconf.benchmark_execution_times(connection, all_queries, data)

                    for run_id in range(n_runs):
                        # get only times of this run, ignore previous runs
                        this_run: list = times[run_id * len(queries) : (run_id + 1) * len(queries)]
                        this_run = list(zip(queries, list(map(lambda t: f'{t:.3f}', this_run))))

                        for case, time in this_run:
                            config_result[case].append(float(time))

                    connection.close()
                    hyper.close()

        return {'HyPer': config_result}


    # returns dict of {table_name: table_def} for each table_def
    @staticmethod
    def get_tables(params: dict[str, Any]) -> dict[str, TableDefinition]:
        table_defs: dict[str, TableDefinition] = dict()

        for table_name, table in params['data'].items():
            table_defs[table_name] = HyPer.get_table(table_name, table)

        return table_defs


    @staticmethod
    def get_table(table_name: str, table: dict[str, Any]) -> TableDefinition:
        # If table exists, return it
        table_def: TableDefinition | None = hyperconf.table_defs.get(table_name)
        if table_def:
            return table_def

        # Otherwise create a new table_def
        columns: list[TableDefinition.Column] = HyPer.parse_attributes(table['attributes'])
        return TableDefinition(table_name=table_name, columns=columns)


    # Parse attributes of one table
    @staticmethod
    def parse_attributes(attributes: dict[str, str]) -> list[TableDefinition.Column]:
        columns: list[TableDefinition.Column] = list()
        for column_name, type_info in attributes.items():
            ty = type_info.split(' ')
            match ty[0]:
                case 'TINYINT':
                    typ = SqlType.small_int() # TINYINT not supported by HyPer, fallback to SMALLINT
                case 'SMALLINT':
                    typ = SqlType.small_int()
                case 'INT':
                    typ = SqlType.int()
                case 'BIGINT':
                    typ = SqlType.big_int()
                case 'FLOAT':
                    typ = SqlType.double()
                case 'DOUBLE':
                    typ = SqlType.double()
                case 'DECIMAL':
                    typ = SqlType.numeric(int(ty[1]), int(ty[2]))
                case 'CHAR':
                    typ = SqlType.char(int(ty[1]))
                case 'DATE':
                    typ = SqlType.date()
                case 'DATETIME':
                    typ = SqlType.timestamp()
                case _:
                    raise Exception(f"Unknown type given for '{column_name}'")

            # The documentation at https://tableau.github.io/hyper-db/lang_docs/py/index.html lists no means to declare
            # PRIMARY KEY, UNIQUE, or REFERENCES constraints.
            col = TableDefinition.Column(column_name, typ, NOT_NULLABLE if 'NOT NULL' in type_info else NULLABLE)
            columns.append(col)
        return columns


    # Returns cases/queries and replaces table names by the corresponding name in its table_def
    @staticmethod
    def get_cases_queries(params: dict[str, Any], table_defs: dict[str, TableDefinition]) -> dict[Case, str]:
        cases_queries: dict[Case, str] = dict()
        for case, query in params['cases'].items():
            for table_name, table_def in table_defs.items():
                query.replace(table_name, f"{table_def.table_name}")
            cases_queries[case] = query

        return cases_queries


    @staticmethod
    def get_data(params: dict[str, Any], table_defs: dict[str, TableDefinition]) -> list[tuple[TableDefinition, str, dict[str, Any]]]:
        result = []
        for table_name, table in params['data'].items():
            file: str = table['file']
            delimiter: str = table.get('delimiter', ',')
            if delimiter:
                delimiter = delimiter.replace("'", "")
            form: str = table.get('format', 'csv')
            header: int = int(table.get('header', 0))
            par: dict[str, Any] = {
                'delimiter': "'" + delimiter + "'",
                'format': form,
                'header': header
            }
            result.append((table_defs[table_name], file, par))
        return result

    def print_command(self, command: str | bytes | Sequence[str | bytes], query: str, indent: str = '') -> None:
        # hyper connector only uses list[str] as command
        if command is not list[str]:
            pass
        tqdm_print(f"    $ {' '.join(command)}")
