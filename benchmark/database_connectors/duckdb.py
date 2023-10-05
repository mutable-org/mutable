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
        cases: dict[Case, Any] = params['cases']
        configname: str = f'DuckDB ({get_num_cores()} cores)' if self.multithreaded else 'DuckDB (single core)'
        tqdm_print(f'` Perform experiment {suite}/{benchmark}/{experiment} with configuration {configname}.')

        self.clean_up()

        config_result: ConfigResult = dict()
        for case in cases.keys():
            config_result[case] = list()

        # For query execution
        command: str = f"./{self.duckdb_cli} {TMP_DB}"
        popen_args: dict[str, Any] = {'shell': True, 'text': True}
        benchmark_info: str = f"{suite}/{benchmark}/{experiment} [{configname}]"

        try:
            # Set up database
            create_tbl_stmts: list[str] = self.generate_create_table_stmts(params['data'], self.check_with_scale_factors(params))

            if self.check_execute_single_cases(params):
                # Execute cases singly
                timeout: int = DEFAULT_TIMEOUT + TIMEOUT_PER_CASE

                for run in range(n_runs):
                    for i in range(len(cases)):
                        case: Case = list(cases.keys())[i]
                        query_stmt: str = list(cases.values())[i]

                        statements: list[str] = list()
                        if i == 0:
                            # Also use the create_tbl_stmts in this case
                            statements.extend(create_tbl_stmts)

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

                        combined_query: str = "\n".join(statements)

                        if self.verbose and run == 0:
                            tqdm_print(combined_query)

                        time: float
                        try:
                            out: str = self.benchmark_query(command=command, query=combined_query, timeout=timeout,
                                                            popen_args=popen_args, benchmark_info=benchmark_info,
                                                            verbose=self.verbose, encode_query=False)
                            durations: list[float] = self.parse_results(out)
                            if len(durations) != 1:
                                raise ConnectorException(f"Expected 1 measurement but got {len(durations)}.")
                            time = durations[0]
                        except ExperimentTimeoutExpired:
                            time = float(timeout)
                        config_result[case].append(time)

                    self.clean_up()


            else:
                # Otherwise, tables have to be created just once before the measurements (done above)
                timeout: int = DEFAULT_TIMEOUT + TIMEOUT_PER_CASE * len(cases) * n_runs

                statements: list[str] = list()
                statements.extend(create_tbl_stmts)

                statements.append(".timer on")
                for _ in range(n_runs):
                    for case_query in cases.values():
                        statements.append(case_query)
                statements.append(".timer off")

                combined_query: str = "\n".join(statements)

                if self.verbose:
                    tqdm_print(combined_query)

                try:
                    out: str = self.benchmark_query(command=command, query=combined_query, timeout=timeout,
                                                    popen_args=popen_args, benchmark_info=benchmark_info,
                                                    verbose=self.verbose, encode_query=False)
                    durations: list[float] = self.parse_results(out)
                except ExperimentTimeoutExpired:
                    for _ in range(n_runs):
                        for case in cases.keys():
                            config_result[case].append(float(TIMEOUT_PER_CASE * 1000))
                else:
                    if len(durations) != n_runs * len(cases):
                        raise ConnectorException(
                            f"Expected {n_runs * len(cases)} measurements but got {len(durations)}.")
                    for i in range(n_runs):
                        run_durations: list[float] = durations[i * len(cases): (i + 1) * len(cases)]
                        for case, dur in zip(list(cases.keys()), run_durations):
                            config_result[case].append(float(dur))

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
