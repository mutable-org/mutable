from .connector import *

from tqdm import tqdm
import os
import pandas
import subprocess
import re

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
        for _ in range(n_runs):
            exp = self.perform_experiment(params)
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
    def perform_experiment(self, params: dict):
        configs = params.get('configurations')
        experiment_name = params['name']
        experiment = dict()
        if configs:
            # Run benchmark under different configurations
            for config_name, config in configs.items():
                config_name = f"mutable (single core, {config_name})"
                measurements = self.run_configuration(experiment_name, config_name, config, params)
                experiment[config_name] = measurements
        else:
            measurements = self.run_configuration(experiment_name, '', '', params)
            experiment[''] = measurements

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

        if config_name:
            tqdm.write(f'` Perform experiment {suite}/{benchmark}/{experiment} with configuration {config_name}.')
        else:
            tqdm.write(f'` Perform experiment {suite}/{benchmark}/{experiment}.')

        # Get database schema
        schema = os.path.join(os.path.dirname(path_to_file), 'data', 'schema.sql')

        # Assemble command
        command = [ self.mutable_binary, '--benchmark', '--times', schema ]
        if binargs:
            command.extend(binargs.split(' '))
        if supplementary_args:
            command.extend(supplementary_args.split(' '))
        if config:
            command.extend(config.split(' '))


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
                imports = create_import_statements(path_to_data, yml['data'], None)

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
                    durations = self.benchmark_query(command, query, yml['pattern'], timeout, path_to_file)
                except BenchmarkTimeoutException as ex:
                    tqdm.write(str(ex))
                    # Add timeout durations
                    for case in cases.keys():
                        execution_times[case] = TIMEOUT_PER_CASE * 1000
                else:
                    # Add measured times
                    for case_index in range(len(cases.keys())):
                        execution_times[list(cases.keys())[case_index]] = durations[case_index]
            else:
                timeout = DEFAULT_TIMEOUT + TIMEOUT_PER_CASE
                for case, query in cases.items():
                    # Produce code to load data into tables with scale factor
                    path_to_data = os.path.join('benchmark', suite, 'data')
                    case_imports = create_import_statements(path_to_data, yml['data'], case)
                    import_str = '\n'.join(case_imports)

                    query_str = import_str + '\n' + query
                    if self.verbose:
                        print_command(command, query_str, '    ')
                    try:
                        durations = self.benchmark_query(command, query_str, yml['pattern'], timeout, path_to_file)
                    except BenchmarkTimeoutException as ex:
                        tqdm.write(str(ex))
                        execution_times[case] = timeout * 1000
                    else:
                        execution_times[case] = durations[0]
        except BenchmarkError as ex:
            tqdm.write(str(ex))

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
            process.kill()
            raise BenchmarkTimeoutException(f'Query timed out after {timeout} seconds')
        finally:
            if process.poll() is None: # if process is still alive
                process.terminate() # try to shut down gracefully
                try:
                    process.wait(timeout=5) # wait for process to terminate
                except subprocess.TimeoutExpired:
                    process.kill() # kill if process did not terminate in time

        out = out.decode('latin-1')
        err = err.decode('latin-1')

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


def create_import_statements(path_to_data, data, case):
    imports = list()

    if not data:
        return imports

    for table_name, tbl in data.items():
        path = tbl.get('file', os.path.join(path_to_data, f'{table_name}.csv'))
        if tbl.get('scale_factors') is not None and case is not None:
            sf = tbl['scale_factors'][case]
        else:
            sf = 1
        delimiter = tbl.get('delimiter', ',')
        header = int(tbl.get('header', 0))

        rows = tbl['lines_in_file'] - header
        import_str = f'IMPORT INTO {table_name} DSV "{path}" ROWS {int(sf * rows)} DELIMITER "{delimiter}"'
        if header:
            import_str += ' HAS HEADER SKIP HEADER'

        imports.append(import_str + ';')
    return imports


def print_command(command :list, query :str, indent = ''):
    if command[-1] != '-':
        command.append('-')
    query_str = query.strip().replace('\n', ' ').replace('"', '\\"')
    command_str = ' '.join(command)
    tqdm.write(f'{indent}$ echo "{query_str}" | {command_str}')
