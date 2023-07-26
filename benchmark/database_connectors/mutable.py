from .connector import *

from tqdm import tqdm
import os
import pandas
import re
import subprocess
import sys

class BenchmarkError(Exception):
    pass

class BenchmarkTimeoutException(Exception):
    pass


class Mutable(Connector):

    def __init__(self, args = dict()):
        self.mutable_binary = args.get('path_to_binary') # required
        self.verbose = args.get('verbose', False) # optional


    def execute(self, n_runs, params: dict):
        result = dict()
        for run_id in range(n_runs):
            exp = self.perform_experiment(run_id, params)
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
    def perform_experiment(self, run_id, params: dict):
        configs = params.get('configurations')
        experiment_name = params['name']
        suite = params['suite']
        benchmark = params['benchmark']
        experiment = dict()

        # Run benchmark under different configurations
        for config_name, config in configs.items():
            config_name = f"mutable (single core, {config_name})"
            if run_id==0:
                tqdm.write(f'` Perform experiment {suite}/{benchmark}/{experiment_name} with configuration {config_name}.')
                sys.stdout.flush()

            measurements = self.run_configuration(experiment_name, config_name, config, params)
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
    def run_configuration(self, experiment, config_name, config, yml):
        # Extract YAML settings
        suite = yml['suite']
        benchmark = yml['benchmark']
        is_readonly = yml['readonly']
        assert type(is_readonly) == bool
        cases = yml['cases']
        supplementary_args = yml.get('args', None)
        binargs = yml.get('binargs', None)
        path_to_file = yml['path_to_file']

        # Assemble command
        command = [ self.mutable_binary, '--benchmark', '--times']
        if binargs:
            command.extend(binargs.split(' '))
        if supplementary_args:
            command.extend(supplementary_args.split(' '))
        if config:
            command.extend(config['args'].split(' '))


        if is_readonly:
            tables = yml.get('data')
            if tables:
                for table in tables.values():
                    is_readonly = is_readonly and (table.get('scale_factors') is None)

        # Collect results in dict
        execution_times = dict()
        try:
            if is_readonly:
                # Produce code to load data into tables
                path_to_data = os.path.join('benchmark', suite, 'data')
                imports = get_setup_statements(suite, path_to_data, yml['data'], None)

                import_str = '\n'.join(imports)
                timeout = DEFAULT_TIMEOUT + TIMEOUT_PER_CASE * len(cases)
                combined_query = list()
                combined_query.append(import_str)
                for case in cases.values():
                    if self.verbose:
                        print_command(command, import_str + '\n' + case, '    ')
                    combined_query.extend([case])
                query = '\n'.join(combined_query)
                try:
                    durations = self.benchmark_query(command, query, config['pattern'], timeout, path_to_file)
                except BenchmarkTimeoutException as ex:
                    tqdm.write(str(ex))
                    sys.stdout.flush()
                    # Add timeout durations
                    for case in cases.keys():
                        execution_times[case] = TIMEOUT_PER_CASE * 1000
                else:
                    if len(durations) < len(cases):
                        raise ConnectorException(f"Expected {len(cases)} measurements but got {len(durations)}.")
                    # Add measured times
                    for case, dur in zip(list(cases.keys()), durations):
                        execution_times[case] = dur
            else:
                timeout = DEFAULT_TIMEOUT + TIMEOUT_PER_CASE
                for case, query in cases.items():
                    # Produce code to load data into tables with scale factor
                    path_to_data = os.path.join('benchmark', suite, 'data')
                    case_imports = get_setup_statements(suite, path_to_data, yml['data'], case)
                    import_str = '\n'.join(case_imports)

                    query_str = import_str + '\n' + query
                    if self.verbose:
                        print_command(command, query_str, '    ')
                    try:
                        durations = self.benchmark_query(command, query_str, config['pattern'], timeout, path_to_file)
                    except BenchmarkTimeoutException as ex:
                        tqdm.write(str(ex))
                        sys.stdout.flush()
                        execution_times[case] = timeout * 1000
                    else:
                        if len(durations) == 0:
                            raise ConnectorException("Expected 1 measurement but got 0.")
                        execution_times[case] = durations[0]
        except BenchmarkError as ex:
            tqdm.write(str(ex))
            sys.stdout.flush()

        return execution_times



    #=======================================================================================================================
    # Start the shell with `command` and pass `query` to its stdin.  Search the stdout for timings using the given regex
    # `pattern` and return them as a list.
    #=======================================================================================================================
    def benchmark_query(self, command, query, pattern, timeout, path_to_file):
        cmd = command + [ '--quiet', '-' ]
        query = query.strip().replace('\n', ' ') + '\n' # transform to a one-liner and append new line to submit query

        process = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   cwd=os.getcwd())
        try:
            out, err = process.communicate(query.encode('latin-1'), timeout=timeout)
        except subprocess.TimeoutExpired:
            raise BenchmarkTimeoutException(f'Query timed out after {timeout} seconds')
        finally:
            if process.poll() is None: # if process is still alive
                process.terminate() # try to shut down gracefully
                try:
                    process.wait(timeout=1) #give process 1 second to terminate
                except subprocess.TimeoutExpired:
                    process.kill() # kill if process did not terminate in time

        out = out.decode('latin-1')
        err = err.decode('latin-1')

        assert process.returncode is not None
        if process.returncode or len(err):
            outstr = '\n'.join(out.split('\n')[-20:])
            tqdm.write(f'''\
    Unexpected failure during execution of benchmark "{path_to_file}" with return code {process.returncode}:''')
            print_command(cmd, query)
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
        durations = list()
        matcher = re.compile(pattern)
        for line in out.split('\n'):
            if matcher.match(line):
                for s in line.split():
                    try:
                        dur = float(s)
                        durations.append(dur)
                    except ValueError:
                        continue

        return durations



########################################################################################################################
# Helper functions
########################################################################################################################

in_red   = lambda x: f'{Fore.RED}{x}{Style.RESET_ALL}'
in_green = lambda x: f'{Fore.GREEN}{x}{Style.RESET_ALL}'
in_bold  = lambda x: f'{Style.BRIGHT}{x}{Style.RESET_ALL}'


# Parse attributes of one table, return as string ready for a CREATE TABLE query
def parse_attributes(attributes: dict):
    columns = list()
    for column_name, ty in attributes.items():
        not_null = 'NOT NULL' if 'NOT NULL' in ty else ''
        ty = ty.split(' ')
        match (ty[0]):
            case 'INT':
                type = 'INT(4)'
            case 'CHAR':
                type = f'CHAR({ty[1]})'
            case 'DECIMAL':
                type = f'DECIMAL({ty[1]},{ty[2]})'
            case 'DATE':
                type = 'DATE'
            case 'DOUBLE':
                type = 'DOUBLE'
            case 'FLOAT':
                type = 'FLOAT'
            case 'BIGINT':
                type = 'INT(8)'
            case _:
                raise Exception(f"Unknown type given for '{column_name}'")
        columns.append(f'{column_name} {type} {not_null}')
    return '(' + ',\n'.join(columns) + ')'


def get_setup_statements(suite, path_to_data, data, case):
    statements = list()

    # suite names with a dash ('-') are illegal database names for mutable , e.g. 'plan-enumerators'
    suite = suite.replace("-", "_")
    statements.append(f"CREATE DATABASE {suite};")
    statements.append(f"USE {suite};")

    if not data:
        return statements

    for table_name, table in data.items():
        # Create a CREATE TABLE statement for current table
        columns = parse_attributes(table['attributes'])
        create = f"CREATE TABLE {table_name} {columns};"
        statements.append(create)

        # Create an IMPORT statement for current table
        lines = table.get('lines_in_file')
        if not lines:
            continue    # Only import data when lines are given

        path = table.get('file', os.path.join(path_to_data, f'{table_name}.csv'))
        if table.get('scale_factors') is not None and case is not None:
            sf = table['scale_factors'][case]
        else:
            sf = 1
        delimiter = table.get('delimiter', ',')
        header = int(table.get('header', 0))

        rows = lines - header
        import_str = f'IMPORT INTO {table_name} DSV "{path}" ROWS {int(sf * rows)} DELIMITER "{delimiter}"'
        if header:
            import_str += ' HAS HEADER SKIP HEADER'
        statements.append(import_str + ';')

    return statements


def print_command(command :list, query :str, indent = ''):
    if command[-1] != '-':
        command.append('-')
    query_str = query.strip().replace('\n', ' ').replace('"', '\\"')
    command_str = ' '.join(command)
    tqdm.write(f'{indent}$ echo "{query_str}" | {command_str}')
    sys.stdout.flush()
