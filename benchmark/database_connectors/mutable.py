from .connector import *
from benchmark_utils import *

from colorama import Fore, Style
from typeguard import typechecked
from typing import Any
import os
import random
import re


class BenchmarkError(Exception):
    pass

class BenchmarkTimeoutException(Exception):
    pass


@typechecked
class Mutable(Connector):

    def __init__(self, args: dict[str, Any]) -> None:
        self.mutable_binary: str = args['path_to_binary']     # required
        self.verbose: bool = args.get('verbose', False)       # optional

    MUTABLE_TYPE_PARSER: dict[str, Callable[[list[str]], str]] = {
        'INT':         lambda ty: 'INT(4)',
        'BIGINT':      lambda ty: 'INT(8)',
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


    def execute(self, n_runs: int, params: dict[str, Any]) -> ConnectorResult:
        result: ConnectorResult = dict()
        for run_id in range(n_runs):
            exp: dict[str, dict[Case, float]] = self.perform_experiment(run_id, params)
            for config, cases in exp.items():
                if config not in result.keys():
                    result[config] = dict()
                for case, time in cases.items():
                    if case not in result[config].keys():
                        result[config][case] = list()
                    result[config][case].append(time)

        return result


#=======================================================================================================================
# Perform the experiment specified in a YAML file with all provided configurations
#
# @param params             the parameters for the experiment
# @return                   a map from configuration name to {map from case name to measurement}
#=======================================================================================================================
    def perform_experiment(self, run_id: int, params: dict[str, Any]) -> dict[str, dict[Case, float]]:
        configs: dict[str, Any] = params.get('configurations', dict())
        experiment_name: str = params['name']
        suite: str = params['suite']
        benchmark: str = params['benchmark']
        experiment: dict[str, dict[Case, float]] = dict()

        # Run benchmark under different configurations
        for config_name, config in configs.items():
            config_name = f"mutable (single core, {config_name})"
            if run_id == 0:
                tqdm_print(f'` Perform experiment {suite}/{benchmark}/{experiment_name} with configuration {config_name}.')

            measurements: dict[Case, float] = self.run_configuration(experiment_name, config_name, config, params)
            experiment[config_name] = measurements

        return experiment


#=======================================================================================================================
# Perform an experiment under a particular configuration.
#
# @param experiment     the name of the experiment (YAML file)
# @param config_name    the name of this configuration
# @param yml            the loaded YAML file
# @param config         the configuration of this experiment
# @return               a map from case name to measurement
#=======================================================================================================================
    def run_configuration(self, experiment: str, config_name: str, config: dict[str, Any], yml: dict[str, Any]) -> dict[Case, float]:
        # Extract YAML settings
        suite: str = yml['suite']
        benchmark: str = yml['benchmark']
        is_readonly: bool = yml['readonly']
        assert type(is_readonly) == bool
        path_to_file: str = yml['path_to_file']
        path_to_data: str = os.path.join('benchmark', suite, 'data')
        cases: dict[Case, Any] = yml['cases']
        if 'script' in cases:
            return self.run_repeated_case(cases, config, path_to_file)

        supplementary_args: str | None = yml.get('args', None)
        binargs: str | None = yml.get('binargs', None)

        # Assemble command
        command: list[str] = [ self.mutable_binary, '--benchmark', '--times']
        if binargs:
            command.extend(binargs.split(' '))
        if supplementary_args:
            command.extend(supplementary_args.split(' '))
        if config:
            command.extend(config['args'].split(' '))
        command = command + ['--quiet', '-']

        if is_readonly:
            tables: dict[str, dict[str, Any]] | None = yml.get('data')
            if tables:
                for table in tables.values():
                    is_readonly = is_readonly and (table.get('scale_factors') is None)

        # Collect results in dict
        execution_times: dict[Case, float] = dict()
        timeout: int
        import_str: str
        durations: list[float]
        try:
            if is_readonly:
                # Produce code to load data into tables
                imports: list[str] = self.get_setup_statements(suite, path_to_data, yml['data'], None)

                import_str = '\n'.join(imports)
                timeout = DEFAULT_TIMEOUT + TIMEOUT_PER_CASE * len(cases)
                combined_query: list[str] = list()
                combined_query.append(import_str)
                for case in cases.values():
                    if self.verbose:
                        self.print_command(command, import_str + '\n' + case, '    ')
                    combined_query.extend([case])
                query: str = '\n'.join(combined_query)
                try:
                    out: str = self.benchmark_query(command=command, query=self.prepare_query(query), timeout=timeout,
                                                    benchmark_info=path_to_file, verbose=self.verbose)
                    durations = self.parse_results(out, config['pattern'])
                except BenchmarkTimeoutException as ex:
                    tqdm_print(str(ex))
                    # Add timeout durations
                    for case in cases.keys():
                        execution_times[case] = float(TIMEOUT_PER_CASE * 1000)
                else:
                    if len(durations) != len(cases):
                        raise ConnectorException(f"Expected {len(cases)} measurements but got {len(durations)}.")
                    # Add measured times
                    for case, dur in zip(list(cases.keys()), durations):
                        execution_times[case] = float(dur)
            else:
                timeout = DEFAULT_TIMEOUT + TIMEOUT_PER_CASE
                for case, query in cases.items():
                    # Produce code to load data into tables with scale factor
                    case_imports: list[str] = self.get_setup_statements(suite, path_to_data, yml['data'], case)
                    import_str = '\n'.join(case_imports)

                    query_str: str = import_str + '\n' + query
                    if self.verbose:
                        self.print_command(command, query_str, '    ')
                    try:
                        out: str = self.benchmark_query(command=command, query=self.prepare_query(query_str),
                                                        timeout=timeout, benchmark_info=path_to_file, verbose=self.verbose)
                        durations = self.parse_results(out, config['pattern'])
                    except BenchmarkTimeoutException as ex:
                        tqdm_print(str(ex))
                        execution_times[case] = float(timeout * 1000)
                    else:
                        if len(durations) != 1:
                            raise ConnectorException(f"Expected 1 measurement but got {len(durations)}.")
                        execution_times[case] = float(durations[0])
        except BenchmarkError as ex:
            tqdm_print(str(ex))

        return execution_times


    # =======================================================================================================================
    # Run the repeated case with the parameters specified in 'repeat'
    #
    # @param repeat             the repeated case
    # @param config             the configuration to use for execution
    # @param path_to_file       the path to the YAML file of the current experiment
    # @return                   a map from case name to measurement
    # =======================================================================================================================
    def run_repeated_case(self, repeat: dict[str, Any], config: dict[str, Any], path_to_file: str) -> dict[Case, float]:
        random.seed(42)
        seed_low, seed_high = 0, 2 ** 32 - 1
        pattern: str = config['pattern']
        script: str = repeat['script']

        execution_times: dict[Case, float] = dict()  # Collect results in dict { case: time }
        timeout: int = DEFAULT_TIMEOUT + TIMEOUT_PER_CASE

        parse_n_error: BenchmarkError = BenchmarkError(f"{path_to_file} : 'n' must be either an integer or a list with 1-3 integers.")
        generator: range
        if type(repeat['n']) is int:
            generator = range(repeat['n'])
        elif type(repeat['n']) is list:
            if len(repeat['n']) == 0 or len(repeat['n']) > 3:
                raise parse_n_error

            for x in repeat['n']:
                if type(x) != int:
                    raise parse_n_error

            generator = range(*repeat['n'])
        else:
            raise parse_n_error

        for N in generator:
            # Perform case
            case_seed: int = random.randint(seed_low, seed_high)
            variables: list[str] = [f'{key}={value}' for key, value in config['variables'].items()]
            cmd: list[str] = ['env', f'N={N}', f'RANDOM={case_seed}'] + variables + ['/bin/bash']

            try:
                out: str = self.benchmark_query(command=cmd, query=script, timeout=timeout,
                                                benchmark_info=path_to_file, verbose=self.verbose)
                durations: list[float] = self.parse_results(out, pattern)
            except BenchmarkTimeoutException:
                execution_times[N] = float(timeout * 1000)
            else:
                if len(durations) != 1:
                    raise ConnectorException(f"Expected 1 measurement but got {len(durations)}.")
                execution_times[N] = float(durations[0])

        return execution_times


    ####################################################################################################################
    # Helper functions
    ####################################################################################################################

    in_red   = lambda x: f'{Fore.RED}{x}{Style.RESET_ALL}'
    in_green = lambda x: f'{Fore.GREEN}{x}{Style.RESET_ALL}'
    in_bold  = lambda x: f'{Style.BRIGHT}{x}{Style.RESET_ALL}'

    def get_setup_statements(self, suite: str, path_to_data: str, data: dict[str, dict[str, Any]], case: Case | None) -> list[str]:
        statements: list[str] = list()

        # suite names with a dash ('-') are illegal database names for mutable , e.g. 'plan-enumerators'
        suite = suite.replace("-", "_")
        statements.append(f"CREATE DATABASE {suite};")
        statements.append(f"USE {suite};")

        if not data:
            return statements

        for table_name, table in data.items():
            # Create a CREATE TABLE statement for current table
            columns: str = Connector.parse_attributes(self.MUTABLE_TYPE_PARSER, table['attributes'])
            create: str = f"CREATE TABLE {table_name} {columns};"
            statements.append(create)

            # Create an IMPORT statement for current table
            if 'lines_in_file' not in table:
                continue    # Only import data when lines are given
            lines: int = table['lines_in_file']

            path: str = table.get('file', os.path.join(path_to_data, f'{table_name}.csv'))
            sf: float | int
            if table.get('scale_factors') is not None and case is not None:
                sf = table['scale_factors'][case]
            else:
                sf = 1
            delimiter: str = table.get('delimiter', ',')
            header: int = int(table.get('header', 0))

            rows: int = lines - header
            import_str: str = f'IMPORT INTO {table_name} DSV "{path}" ROWS {int(sf * rows)} DELIMITER "{delimiter}"'
            if header:
                import_str += ' HAS HEADER SKIP HEADER'
            statements.append(import_str + ';')

        return statements


    # Prepare a query for execution
    @staticmethod
    def prepare_query(query: str) -> str:
        return query.strip().replace('\n', ' ') + '\n'


    # Parse `results` for timings
    @staticmethod
    def parse_results(results: str, pattern: str) -> list[float]:
        durations: list[float] = list()
        matcher: re.Pattern = re.compile(pattern)
        for line in results.split('\n'):
            if matcher.match(line):
                for s in line.split():
                    try:
                        dur: float = float(s)
                        durations.append(dur)
                    except ValueError:
                        continue

        return durations


    # Overrides `print_command` from Connector ABC
    @staticmethod
    def print_command(command: list[str], query: str, indent: str = '') -> None:
        if command[-1] != '-':
            command.append('-')
        query_str = query.strip().replace('\n', ' ').replace('"', '\\"')
        command_str = ' '.join(command)
        tqdm_print(f'{indent}$ echo "{query_str}" | {command_str}')
