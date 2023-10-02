from .connector import *
from benchmark_utils import *

from typeguard import typechecked
from typing import Any
import os


TMP_DB = 'tmp.duckdb'


@typechecked
class DuckDB(Connector):

    def __init__(self, args: dict[str, Any]) -> None:
        self.duckdb_cli = args.get('path_to_binary')            # required
        self.verbose = args.get('verbose', False)               # optional
        self.multithreaded = args.get('multithreaded', False)   # optional

    DUCKDB_TYPE_PARSER: dict[str, Callable[[list[str]], str]] = {
        'INT':         lambda ty: 'INT',
        'BIGINT':      lambda ty: 'BIGINT',
        'FLOAT':       lambda ty: 'FLOAT',
        'DOUBLE':      lambda ty: 'DOUBLE',
        'DECIMAL':     lambda ty: f'DECIMAL({ty[1]}, {ty[2]})',
        'CHAR':        lambda ty: f'CHAR({ty[1]})',
        'DATE':        lambda ty: 'DATE',
        'DATETIME':    lambda ty: 'DATETIME',
        'NOT NULL':    lambda ty: 'NOT NULL',
        'PRIMARY KEY': lambda ty: 'PRIMARY KEY',
        'UNIQUE':      lambda ty: 'UNIQUE',
    }

    # Runs an experiment 'n_runs' times, all parameters are in 'params'
    def execute(self, n_runs: int, params: dict[str, Any]) -> ConnectorResult:
        suite: str = params['suite']
        benchmark: str = params['benchmark']
        experiment: str = params['name']
        configname: str = f'DuckDB ({get_num_cores()} cores)' if self.multithreaded else 'DuckDB (single core)'
        tqdm_print(f'` Perform experiment {suite}/{benchmark}/{experiment} with configuration {configname}.')

        self.clean_up()

        config_result: ConfigResult = dict()
        verbose_printed: bool = False

        # For query execution
        command: str = f"./{self.duckdb_cli} {TMP_DB}"
        popen_args: dict[str, Any] = {'shell': True, 'text': True}
        benchmark_info: str = f"{suite}/{benchmark}/{experiment} [{configname}]"
        for _ in range(n_runs):
            try:
                # Used variables
                statements: list[str]
                combined_query: str
                benchmark_info: str

                # Set up database
                create_tbl_stmts: list[str] = self.generate_create_table_stmts(params['data'], self.check_with_scale_factors(params))

                if self.check_execute_single_cases(params):
                    # Execute cases singly
                    timeout = DEFAULT_TIMEOUT + TIMEOUT_PER_CASE
                    # Write cases/queries to a file that will be passed to the command to execute
                    for i in range(len(params['cases'])):
                        case: Case = list(params['cases'].keys())[i]
                        query_stmt: str = list(params['cases'].values())[i]

                        statements: list[str] = list()
                        if i == 0:
                            # Also use the create_tbl_stmts in this case
                            statements = create_tbl_stmts

                        # Create tables from tmp tables with scale factor
                        for table_name, table in params['data'].items():
                            statements.append(f'DELETE FROM "{table_name}";')     # empty existing table
                            sf: float | int
                            if table.get('scale_factors'):
                                sf = table['scale_factors'][case]
                            else:
                                sf = 1
                            header: int = int(table.get('header', 0))
                            num_rows: int = round((table['lines_in_file'] - header) * sf)
                            statements.append(f'INSERT INTO "{table_name}" SELECT * FROM "{table_name}_tmp" LIMIT {num_rows};')

                        statements.append(".timer on")
                        statements.append(query_stmt)   # Actual query from this case
                        statements.append(".timer off")

                        combined_query = "\n".join(statements)

                        if self.verbose and not verbose_printed:
                            verbose_printed = True
                            tqdm_print(combined_query)

                        time: float
                        try:
                            out: str = self.benchmark_query(command=command, query=combined_query, timeout=timeout,
                                                            popen_args=popen_args, benchmark_info=benchmark_info,
                                                            verbose=self.verbose, encode_query=False)
                            time = self.parse_results(out)[0]
                        except ExperimentTimeoutExpired:
                            time = float(timeout)
                        if case not in config_result.keys():
                            config_result[case] = list()
                        config_result[case].append(time)

                else:
                    # Otherwise, tables have to be created just once before the measurements (done above)
                    timeout = DEFAULT_TIMEOUT + TIMEOUT_PER_CASE * len(params['cases'])

                    statements = create_tbl_stmts
                    statements.append(".timer on")
                    for case_query in params['cases'].values():
                        statements.append(case_query)
                    statements.append(".timer off")

                    combined_query = "\n".join(statements)

                    if self.verbose and not verbose_printed:
                        verbose_printed = True
                        tqdm_print(combined_query)

                    try:
                        out: str = self.benchmark_query(command=command, query=combined_query, timeout=timeout,
                                                        popen_args=popen_args, benchmark_info=benchmark_info,
                                                        verbose=self.verbose, encode_query=False)
                        durations: list[float] = self.parse_results(out)
                    except ExperimentTimeoutExpired:
                        for case in params['cases'].keys():
                            if case not in config_result.keys():
                                config_result[case] = list()
                            config_result[case].append(float(timeout * 1000))
                    else:
                        for idx, time in enumerate(durations):
                            case: Case = list(params['cases'].keys())[idx]
                            if case not in config_result.keys():
                                config_result[case] = list()
                            config_result[case].append(float(time))

            finally:
                self.clean_up()

        return { configname: config_result }


    # Deletes the used temporary database
    def clean_up(self) -> None:
        if os.path.exists(TMP_DB):
            os.remove(TMP_DB)


    # Creates tables in the database and copies contents of given files into them
    # Call with 'with_scale_factors'=False if data should be loaded as a whole
    # Call with 'with_scale_factors'=True if data should be placed in tmp tables
    # and copied for each case with different scale factor
    def generate_create_table_stmts(self, data: dict[str, dict[str, Any]], with_scale_factors: bool) -> list[str]:
        statements: list[str] = list()
        for table_name, table in data.items():
            columns: str = Connector.parse_attributes(self.DUCKDB_TYPE_PARSER, table['attributes'])

            if with_scale_factors:
                table_name += "_tmp"

            create: str = f'CREATE TABLE "{table_name}" {columns};'
            copy: str = f'COPY "{table_name}" FROM \'{table["file"]}\' ( '
            if 'delimiter' in table:
                delim = table['delimiter'].replace("'", "")
                copy += f" DELIMITER \'{delim}\',"
            if 'format' in table:
                copy += f" FORMAT {table['format'].upper()},"
            if 'header' in table:
                copy += f" HEADER," if table['header'] == 1 else ""

            copy = copy[:-1] + " );"

            statements.append(create)
            statements.append(copy)

            if with_scale_factors:
                # Create actual table that will be used for experiment
                statements.append(f'CREATE TABLE "{table_name[:-4]}" {columns};')

        return statements


    # Parse `results` for timings
    @staticmethod
    def parse_results(results: str) -> list[float]:
        durations_str: str = os.popen(
            f"echo '{results}'" + " | grep 'Run Time' | cut -d ' ' -f 5 | awk '{print $1 * 1000;}'").read()
        durations: list[str] = durations_str.split('\n')
        durations.remove('')
        timings: list[float] = [float(dur.replace("\n", "").replace(",", ".")) for dur in durations]
        return timings
