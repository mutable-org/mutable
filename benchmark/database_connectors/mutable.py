from .connector import *
from benchmark_utils import *

from colorama import Fore, Style
from typeguard import typechecked
from typing import Any
import os
import random
import re


@typechecked
class Mutable(Connector):

    def __init__(self, args: dict[str, Any]) -> None:
        self.mutable_binary: str = args['path_to_binary']     # required
        self.verbose: bool = args.get('verbose', False)       # optional

    MUTABLE_TYPE_PARSER: dict[str, Callable[[list[str]], str]] = {
        'TINYINT':     lambda ty: 'INT(1)',
        'SMALLINT':    lambda ty: 'INT(2)',
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
        'REFERENCES':  lambda ty: f'REFERENCES {ty[ty.index("REFERENCES")+1]}',
    }


    def execute(self, n_runs: int, params: dict[str, Any]) -> ConnectorResult:
        configs: dict[str, Any] = params.get('configurations', dict())
        result: ConnectorResult = dict()

        # Run experiment under different configurations
        for config_name, config in configs.items():
            config_name = f"mutable (single core, {config_name})"
            measurements: ConfigResult = self.run_configuration(n_runs, config_name, config, params)
            result[config_name] = measurements

        return result


#=======================================================================================================================
# Perform an experiment under a particular configuration.
#
# @param experiment     the name of the experiment (YAML file)
# @param config_name    the name of this configuration
# @param yml            the loaded YAML file
# @param config         the configuration of this experiment
# @return               a config result
#=======================================================================================================================
    def run_configuration(self, n_runs: int, config_name: str, config: dict[str, Any], yml: dict[str, Any]) -> ConfigResult:
        # Extract YAML settings
        suite: str = yml['suite']
        benchmark: str = yml['benchmark']
        experiment_name: str = yml['name']
        path_to_file: str = yml['path_to_file']
        path_to_data: str = os.path.join('benchmark', suite, 'data')
        cases: dict[Case, Any] = yml['cases']

        tqdm_print(f'` Perform experiment {suite}/{benchmark}/{experiment_name} with configuration {config_name}.')

        if 'script' in cases:
            return self.run_repeated_case(n_runs, cases, config, path_to_file)

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
        command = command + ['--plan', '-']

        # Variables
        timeout: int
        import_str: str
        measurements: list[list[float]]

        # Extract pattern labels
        pattern_labels: list[str] = [ 'Execution Time' ] if isinstance(config['pattern'], str) else list(config['pattern'].keys())

        # Init config result
        config_result: ConfigResult = dict()
        for label in pattern_labels:
            config_result[label] = dict()
            for case in cases.keys():
                config_result[label][case] = list()

        if not self.check_execute_single_cases(yml):
            # All cases can be executed at once
            # Produce code to load data into tables
            imports: list[str] = self.get_setup_statements(suite, path_to_data, yml['data'], None)
            import_str = '\n'.join(imports)

            combined_query: list[str] = list()
            combined_query.append(import_str)
            for _ in range(n_runs):
                combined_query.extend(list(cases.values()))
            query: str = '\n'.join(combined_query)

            timeout = DEFAULT_TIMEOUT + TIMEOUT_PER_CASE * len(cases) * n_runs
            try:
                out: str = self.benchmark_query(command=command, query=self.prepare_query(query), timeout=timeout,
                                                benchmark_info=path_to_file, verbose=self.verbose)
                measurements = self.parse_results(out, config['pattern'])
            except ExperimentTimeoutExpired:
                # Add timeout durations
                for _ in range(n_runs):
                    for label in pattern_labels:
                        for case in cases.keys():
                            config_result[label][case].append(float(TIMEOUT_PER_CASE * 1000))
            else:
                if len(measurements) != len(pattern_labels):
                    raise ConnectorException(f"Expected {len(pattern_labels)} measured patterns but got {len(measurements)}.")
                for label, durations in zip(pattern_labels, measurements):
                    if len(durations) != n_runs * len(cases):
                        raise ConnectorException(f"Expected {n_runs * len(cases)} measurements per pattern but got {len(durations)} "
                                                 f"for pattern label '{label}'.")
                    # Add measured times
                    for i in range(n_runs):
                        run_durations: list[float] = durations[i * len(cases) : (i+1) * len(cases)]
                        for case, dur in zip(list(cases.keys()), run_durations):
                            config_result[label][case].append(float(dur))
        else:
            # Each case has to be executed singly
            timeout = DEFAULT_TIMEOUT + TIMEOUT_PER_CASE
            for _ in range(n_runs):
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
                        measurements = self.parse_results(out, config['pattern'])
                    except ExperimentTimeoutExpired:
                        # Add timeout duration
                        for label in pattern_labels:
                            config_result[label][case].append(float(timeout * 1000))
                    else:
                        if len(measurements) != len(pattern_labels):
                            raise ConnectorException(f"Expected {len(pattern_labels)} measured patterns but got {len(measurements)}.")
                        for label, durations in zip(pattern_labels, measurements):
                            if len(durations) != 1:
                                raise ConnectorException(f"Expected 1 measurement per pattern but got {len(durations)} "
                                                         f"for pattern label '{label}'.")
                            # Add measured time
                            config_result[label][case].append(durations[0])

        return config_result


    # =======================================================================================================================
    # Run the repeated case with the parameters specified in 'repeat'
    #
    # @param repeat             the repeated case
    # @param config             the configuration to use for execution
    # @param path_to_file       the path to the YAML file of the current experiment
    # @return                   a config result
    # =======================================================================================================================
    def run_repeated_case(self, n_runs: int, repeat: dict[str, Any], config: dict[str, Any], path_to_file: str) -> ConfigResult:
        pattern: str | dict[str, str] = config['pattern']
        script: str = repeat['script']
        timeout: int = DEFAULT_TIMEOUT + TIMEOUT_PER_CASE

        parse_n_error: ConnectorException = ConnectorException(f"{path_to_file} : 'n' must be either an integer or a list with 1-3 integers.")
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

        # Extract pattern labels
        pattern_labels: list[str] = [ 'Execution Time' ] if isinstance(pattern, str) else list(pattern.keys())

        # Collect results in dict
        config_result: ConfigResult = dict()
        for label in pattern_labels:
            config_result[label] = dict()
            for N in generator:
                config_result[label][N] = list()

        for _ in range(n_runs):
            random.seed(42)
            seed_low, seed_high = 0, 2 ** 32 - 1
            for N in generator:
                # Perform case
                case_seed: int = random.randint(seed_low, seed_high)
                variables: list[str] = [f'{key}={value}' for key, value in config['variables'].items()]
                cmd: list[str] = ['env', f'N={N}', f'RANDOM={case_seed}'] + variables + ['/bin/bash']

                try:
                    out: str = self.benchmark_query(command=cmd, query=script, timeout=timeout,
                                                    benchmark_info=path_to_file, verbose=self.verbose)
                    measurements: list[list[float]] = self.parse_results(out, pattern)
                except ExperimentTimeoutExpired:
                    # Add timeout duration
                    for label in pattern_labels:
                        config_result[label][N].append(float(timeout * 1000))
                else:
                    if len(measurements) != len(pattern_labels):
                        raise ConnectorException(f"Expected {len(pattern_labels)} measured patterns but got {len(measurements)}.")
                    for label, durations in zip(pattern_labels, measurements):
                        if len(durations) != 1:
                            raise ConnectorException(f"Expected 1 measurement per pattern but got {len(durations)} "
                                                     f"for pattern label '{label}'.")
                        # Add measured time
                        config_result[label][N].append(durations[0])

        return config_result


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
            delimiter: str = table.get('delimiter', ',')
            header: int = int(table.get('header', 0))

            rows: int = lines - header
            import_str: str = f'IMPORT INTO {table_name} DSV "{path}" ROWS {int(sf * rows)} DELIMITER "{delimiter}"'
            if header:
                import_str += ' HAS HEADER SKIP HEADER'
            statements.append(import_str + ';')

            # Create CREATE INDEX statements for current table
            create_indexes: list[str] = self.generate_create_index_stmts(table_name, table.get('indexes', dict()))
            statements.extend(create_indexes)

        return statements


    # Prepare a query for execution
    @staticmethod
    def prepare_query(query: str) -> str:
        return query.strip().replace('\n', ' ') + '\n'


    # Parse `results` for timings and return list of durations for each pattern
    @staticmethod
    def parse_results(results: str, pattern: str | dict[str, str]) -> list[list[float]]:
        measurements: list[list[float]] = list()
        matchers: list[re.Pattern] = \
            [ re.compile(pattern) ] if isinstance(pattern, str) else [ re.compile(p) for p in pattern.values() ]
        for matcher in matchers:
            durations: list[float] = list()
            for line in results.split('\n'):
                if matcher.match(line):
                    for s in line.split():
                        try:
                            dur: float = float(s)
                            durations.append(dur)
                        except ValueError:
                            continue
            measurements.append(durations)
        return measurements


    # Overrides `generate_create_index_stmts` from Connector ABC
    @staticmethod
    def generate_create_index_stmts(table_name: str, indexes: dict[str, dict[str, Any]]) -> list[str]:
        create_indexes: list[str] = list()
        for index_name, index in indexes.items():
            method: str | None = index.get('method')
            attributes: str | list[str] = index['attributes']
            if isinstance(attributes, list):
                attributes = ', '.join(attributes)
            index_str: str = f'CREATE INDEX {index_name} ON {table_name}'
            if method:
                index_str += f' USING {method}'
            index_str += f' ({attributes});'
            create_indexes.append(index_str)
        return create_indexes


    # Overrides `print_command` from Connector ABC
    def print_command(self, command: str | bytes | Sequence[str | bytes], query: str, indent: str = '') -> None:
        assert isinstance(command, Sequence) and isinstance(command[0], str) and not isinstance(command, str), \
            "mutable connector only uses Sequence[str] as command"
        indent = '    '
        if command[-1] != '-':
            command.append('-')
        query_str = query.strip().replace('\n', ' ').replace('"', '\\"')
        command_str = ' '.join(command)
        tqdm_print(f'{indent}$ echo "{query_str}" | {command_str}')
