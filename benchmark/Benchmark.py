#!/usr/bin/env python3

from colorama import Fore, Back, Style
from git import Repo
from pandas.api.types import is_numeric_dtype
from tqdm import tqdm
from yattag import Doc, indent
import altair
import argparse
import datetime
import glob
import itertools
import json
import math
import MySQLdb
import numpy
import os
import pandas
import re
import subprocess
import sys
import time
import yamale
import yaml


NUM_RUNS        = 5
DEFAULT_TIMEOUT = 30  # seconds
TIMEOUT_PER_CASE = 10 # seconds
MUTABLE_BINARY  = os.path.join('build', 'release', 'bin', 'shell')
YML_SCHEMA      = os.path.join('benchmark', '_schema.yml')


class BenchmarkError(Exception):
    pass

class BenchmarkTimeoutException(Exception):
    pass


########################################################################################################################
# Helper functions
########################################################################################################################

in_red   = lambda x: f'{Fore.RED}{x}{Style.RESET_ALL}'
in_green = lambda x: f'{Fore.GREEN}{x}{Style.RESET_ALL}'
in_bold  = lambda x: f'{Style.BRIGHT}{x}{Style.RESET_ALL}'

def count_lines(filename):
    count = 0
    with open(filename, 'r') as f:
        for l in f:
            count += 1
    return count

def import_tables(path_to_data, tables):
    imports = list()
    for tbl in tables:
        if isinstance(tbl, str):
            imports.append(f'IMPORT INTO {tbl} DSV "{os.path.join(path_to_data, tbl + ".csv")}" HAS HEADER SKIP HEADER;')
        else:
            name = tbl['name']
            path = tbl.get('path', os.path.join(path_to_data, f'{name}.csv'))
            sf = float(tbl.get('sf', 1))
            delimiter = tbl.get('delimiter', ',')
            header = int(tbl.get('header', 0))

            rows = count_lines(path) - header
            import_str = f'IMPORT INTO {name} DSV "{path}" ROWS {int(sf * rows)} DELIMITER "{delimiter}"'
            if header:
                import_str += ' HAS HEADER SKIP HEADER'
            imports.append(import_str + ';')
    return imports


#=======================================================================================================================
# Validate a YAML file given a YAML schema file (with yamale)
#=======================================================================================================================
def validate_schema(path_to_file, path_to_schema) -> bool:
    schema = yamale.make_schema(path_to_schema)
    data = yamale.make_data(path_to_file)
    try:
        yamale.validate(schema, data)
    except ValueError:
        return False
    return True


def print_command(command :list, query :str, indent = ''):
    if command[-1] != '-':
        command.append('-')
    query_str = query.strip().replace('\n', ' ').replace('"', '\\"')
    command_str = ' '.join(command)
    tqdm.write(f'{indent}$ echo "{query_str}" | {command_str}')


#=======================================================================================================================
# Start the shell with `command` and pass `query` to its stdin.  Search the stdout for timings using the given regex
# `pattern` and return them as a list.
#=======================================================================================================================
def benchmark_query(command, query, pattern, timeout):
    cmd = command + [ '--quiet', '-' ]
    query = query.strip().replace('\n', ' ') + '\n' # transform to a one-liner and append new line to submit query

    process = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               cwd=os.getcwd())
    try:
        out, err = process.communicate(query.encode('latin-1'), timeout=timeout)
    except subprocess.TimeoutExpired:
        process.kill()
        raise BenchmarkTimeoutException(f'Benchmark timed out after {timeout} seconds')
    finally:
        if process.poll() is None: # if process is still alive
            process.terminate() # try to shut down gracefully
            try:
                process.wait(timeout=5) # wait for process to terminate
            except TimeoutExpired:
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
            raise BenchmarkError(f'Benchmark failed with return code {process.returncode}.')

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


#=======================================================================================================================
# Perform an experiment under a particular configuration.
#
# @param experiment the name of the experiment (YAML file)
# @param name       the name of this configuration
# @param yml        the loaded YAML file
# @param config     the configuration of this experiment
# @return           a pandas.DataFrame with the measurements
#=======================================================================================================================
def run_configuration(experiment, name, config, yml):
    # Extract YAML settings
    version = yml.get('version', 1)
    suite = yml['suite']
    benchmark = yml['benchmark']
    is_readonly = yml['readonly']
    cases = yml['cases']
    supplementary_args = yml.get('args', None)

    if name:
        tqdm.write(f'` Perform experiment {suite}/{benchmark}/{experiment} with configuration {name}.')
    else:
        tqdm.write(f'` Perform experiment {suite}/{benchmark}/{experiment}.')

    # Get database schema
    schema = os.path.join(os.path.dirname(path_to_file), 'data', 'schema.sql')

    # Assemble command
    command = [ MUTABLE_BINARY, '--benchmark', '--times', schema ]
    if args.binargs:
        command.extend(args.binargs.split(' '))
    if supplementary_args:
        command.extend(supplementary_args.split(' '))
    if config:
        command.extend(config.split(' '))

    # Collect results in data frame
    measurements = pandas.DataFrame(columns=['version', 'suite', 'benchmark', 'experiment', 'name', 'config', 'case', 'time'])

    # Produce code to load data into tables
    path_to_data = os.path.join('benchmark', yml['suite'], 'data')
    imports = import_tables(path_to_data, yml['tables'])

    if is_readonly:
        for case in cases.values():
            is_readonly = is_readonly and isinstance(case, str)

    try:
        if is_readonly:
            import_str = '\n'.join(imports)
            timeout = DEFAULT_TIMEOUT + NUM_RUNS * TIMEOUT_PER_CASE * len(cases)
            combined_query = list()
            combined_query.append(import_str)
            for case in cases.values():
                if args.verbose:
                    print_command(command, import_str + '\n' + case, '    ')
                combined_query.extend([case] * NUM_RUNS)
            query = '\n'.join(combined_query)
            try:
                durations = benchmark_query(command, query, yml['pattern'], timeout)
            except BenchmarkTimeoutException as ex:
                tqdm.write(str(ex))
                # Add timeout durations
                for case in cases.keys():
                    measurements.loc[len(measurements)] = [ version, suite, benchmark, experiment, name, config, case, TIMEOUT_PER_CASE * 1000 ]
            else:
                # Add measured times
                for case in cases.keys():
                    for i in range(NUM_RUNS):
                        measurements.loc[len(measurements)] = [ version, suite, benchmark, experiment, name, config, case, durations[0] ]
                        durations.pop(0)
        else:
            timeout = DEFAULT_TIMEOUT + NUM_RUNS * TIMEOUT_PER_CASE
            for case, query in cases.items():
                case_imports = imports.copy()
                if isinstance(query, str):
                    query_str = query
                else:
                    query_str = query['query']
                    case_imports.extend(import_tables(path_to_data, query['tables']))
                import_str = '\n'.join(case_imports)

                query_str = import_str + '\n' + query_str
                if args.verbose:
                    print_command(command, query_str, '    ')
                try:
                    durations = benchmark_query(command, query_str, yml['pattern'], timeout)
                except BenchmarkTimeoutException as ex:
                    tqdm.write(str(ex))
                    measurements.loc[len(measurements)] = [ version, suite, benchmark, experiment, name, config, case, TIMEOUT_PER_CASE * 1000 ]
                else:
                    for dur in durations:
                        measurements.loc[len(measurements)] = [ version, suite, benchmark, experiment, name, config, case, dur ]
    except BenchmarkError as ex:
        tqdm.write(str(ex))

    return measurements


#=======================================================================================================================
# Perform the experiment specified in a YAML file with all provided configurations
#
# @param experiment_name    the name of the experiment
# @param yml                the loaded YAML experiment file
# @param path_to_file       the path to the experiment YAML file
# @return                   a map from configuration name to pandas.DataFrame
#=======================================================================================================================
def perform_experiment(experiment_name, yml, path_to_file):
    configs = yml.get('configurations', dict())
    experiment = dict()
    if configs:
        # Run benchmark under different configurations
        for config_name, config in configs.items():
            measurements = run_configuration(experiment_name, config_name, config, yml)
            experiment[config_name] = measurements
    else:
        measurements = run_configuration(experiment_name, '', '', yml)
        experiment[''] = measurements

    return experiment


#=======================================================================================================================
# Generate a .pgsql file to load the results into the database
#=======================================================================================================================

def escape(input_str):
    escaped = MySQLdb.escape_string(str(input_str))
    return escaped.decode('utf-8')

def generate_pgsql(commit, results):
    _, nodename, *_ = os.uname()
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')        # Date in ISO 8601

    # Open and truncate output file
    with open('benchmark.pgsql', 'w') as output_sql_file:
        # Define functions used to insert rows (if unique constraints not yet inserted)
        output_sql_file.write(f'''\
CREATE OR REPLACE FUNCTION insert_suite(text)
RETURNS void
LANGUAGE SQL
AS $func$
    INSERT INTO "Suites" (name)
    SELECT $1
    WHERE NOT $1 IN (SELECT name FROM "Suites")
$func$;

CREATE OR REPLACE FUNCTION insert_benchmark(int, text)
RETURNS void
LANGUAGE SQL
AS $func$
    INSERT INTO "Benchmarks" (suite, name)
    SELECT $1, $2
    WHERE NOT ($1, $2) IN (SELECT suite, name FROM "Benchmarks");
$func$;

CREATE OR REPLACE FUNCTION insert_configuration(text, text)
RETURNS void
LANGUAGE SQL
AS $func$
    INSERT INTO "Configurations" (name, parameters)
    SELECT $1, $2
    WHERE NOT ($1, $2) IN (SELECT name, parameters FROM "Configurations");
$func$;

CREATE OR REPLACE FUNCTION insert_timestamp(text, timestamptz, text)
RETURNS void
LANGUAGE SQL
AS $func$
    INSERT INTO "Timestamps" (commit, timestamp, host)
    SELECT $1, $2, $3
    WHERE NOT ($2, $3) IN (SELECT timestamp, host FROM "Timestamps");
$func$;

CREATE OR REPLACE FUNCTION insert_experiment(int, int, text, int, text, bool, int)
RETURNS void
LANGUAGE SQL
AS $func$
    INSERT INTO "Experiments" (benchmark, suite, name, version, description, is_read_only, chart_config)
    SELECT $1, $2, $3, $4, $5, $6, $7
    WHERE NOT ($1, $2, $3, $4) IN (SELECT benchmark, suite, name, version FROM "Experiments");
$func$;

CREATE OR REPLACE FUNCTION insert_chartconfig(text, text, text, text, text, text)
RETURNS void
LANGUAGE SQL
AS $func$
    INSERT INTO "ChartConfig" (scale_x, scale_y, type_x, type_y, label_x, label_y)
    SELECT $1, $2, $3, $4, $5, $6
    WHERE NOT ($1, $2, $3, $4, $5, $6) IN (SELECT scale_x, scale_y, type_x, type_y, label_x, label_y FROM "ChartConfig");
$func$;

DO $$
DECLARE
    timestamp_id integer;
    suite_id integer;
    benchmark_id integer;
    experiment_id integer;
    configuration_id integer;
    chartconfig_id integer;
BEGIN
    -- Get timestamp
    PERFORM insert_timestamp('{escape(commit)}', '{escape(now)}', '{escape(nodename)}');
    SELECT id FROM "Timestamps"
    WHERE commit='{escape(commit)}'
      AND timestamp='{escape(now)}'
      AND host='{escape(nodename)}'
    INTO timestamp_id;
''')

        for suite, benchmarks in results.items():
            # Insert Suite
            output_sql_file.write(f'''
    -- Get suite
    PERFORM insert_suite('{escape(suite)}');
    SELECT id FROM "Suites"
    WHERE name='{escape(suite)}'
    INTO suite_id;
''')

            for benchmark, experiments in benchmarks.items():
                output_sql_file.write(f'''
    -- Get benchmark
    PERFORM insert_benchmark(suite_id, '{escape(benchmark)}');
    SELECT id FROM "Benchmarks"
    WHERE suite=suite_id
      AND name='{escape(benchmark)}'
    INTO benchmark_id;
''')

                for experiment, data in experiments.items():
                    configs, yml = data

                    chart_config = dict()
                    if 'chart' in yml:
                        chart = yml['chart']
                        for dimension, dimension_config in chart.items():
                            for attr, val in dimension_config.items():
                                chart_config[f'{attr}_{dimension}'] = "'{0}'".format(escape(val))

                    chart_scale_x = chart_config.get('scale_x', "'linear'")
                    chart_scale_y = chart_config.get('scale_y', "'linear'")
                    chart_type_x  = chart_config.get('type_x',  "'Q'")
                    chart_type_y  = chart_config.get('type_y',  "'Q'")
                    chart_label_x = chart_config.get('label_x', "'X'")
                    chart_label_y = chart_config.get('label_y', "'Y'")

                    output_sql_file.write(f'''
    -- Get chart configuration
    PERFORM insert_chartconfig({chart_scale_x}, {chart_scale_y}, {chart_type_x}, {chart_type_y}, {chart_label_x}, {chart_label_y});
    SELECT id FROM "ChartConfig"
    WHERE scale_x={chart_scale_x} AND scale_y={chart_scale_y}
      AND type_x={chart_type_x} AND type_y={chart_type_y}
      AND label_x={chart_label_x} AND label_y={chart_label_y}
    INTO chartconfig_id;
''')


                    version = int(yml.get('version', 1))
                    description = str(yml['description'])
                    read_only = 'TRUE' if (str(yml['readonly']) == 'yes') else 'FALSE'
                    experiment_params = None
                    if 'args' in yml and yml['args']:
                        experiment_params = yml['args']

                    output_sql_file.write(f'''
    -- Get experiment
    PERFORM insert_experiment(benchmark_id, suite_id, '{escape(experiment)}', {version}, '{escape(description)}', {read_only}, chartconfig_id);
    SELECT id FROM "Experiments"
    WHERE benchmark=benchmark_id
      AND suite=suite_id
      AND name='{escape(experiment)}'
      AND version={version}
    INTO experiment_id;
''')
                    for config, measurements in configs.items():
                        config_params = measurements['config'].unique()
                        parameters = list()
                        if experiment_params:
                            parameters.append(experiment_params)
                        if len(config_params) > 0:
                            parameters.append(' '.join(config_params))
                        output_sql_file.write(f'''
    -- Get config
    PERFORM insert_configuration('{escape(config)}', '{escape(" ".join(parameters))}');
    SELECT id FROM "Configurations"
    WHERE name='{escape(config)}'
      AND parameters='{escape(" ".join(parameters))}'
    INTO configuration_id;

    -- Write measurements
    --  timestamp:  ('{commit}', '{now}', '{nodename}')
    --  suite:      '{suite}'
    --  benchmark:  '{benchmark}'
    --  experiment: '{experiment}'
    --  config:     '{config}'
    INSERT INTO "Measurements"
    VALUES
''')

                        with_nan = lambda flt: "'NaN'" if math.isnan(flt) else flt
                        insert = lambda case, time: ' '*8 + f'(default, timestamp_id, experiment_id, benchmark_id, suite_id, configuration_id, {case}, {with_nan(time)})'
                        values = [ insert(row[0], row[1]) for row in zip(measurements['case'], measurements['time']) ]
                        output_sql_file.write(',\n'.join(values))
                        output_sql_file.write(';\n')

        output_sql_file.write('END$$;')


#=======================================================================================================================
# main
#=======================================================================================================================
if __name__ == '__main__':
    # Parse args
    parser = argparse.ArgumentParser(description='''Run benchmarks on mutable.
                                                    The build directory is assumed to be './build/release'.''')
    parser.add_argument('suite', nargs='*', help='a benchmark suite to be run')
    parser.add_argument('--no-compare', dest='compare', default=True, action='store_false',
                        help='Skip comparison to other systems')
    parser.add_argument('-o', '--output', dest='output', metavar='FILE.csv', default=None, action='store',
                        help='Specify file to write measurement in CSV format')
    parser.add_argument('--args', dest='binargs', metavar='ARGS', default=None, action='store',
                        help='provide additional arguments to pass through to the binary')
    parser.add_argument('-v', '--verbose', help='verbose output', dest='verbose', default=False, action='store_true')
    parser.add_argument('--pgsql', dest='pgsql', default=False, action='store_true',
                        help='create a .pgsql file with instructions to insert measurement results into a PostgreSQL '
                             'database')
    args = parser.parse_args()

    # Check whether we are interactive
    is_interactive = True if os.environ.get('TERM', False) else False

    # Get benchmark files
    if not args.suite:
        benchmark_files = sorted(glob.glob(os.path.join('benchmark', '**', '[!_]*.yml'), recursive=True))
    else:
        benchmark_files = []
        for suite in sorted(set(args.suite)):
            benchmark_files.extend(sorted(glob.glob(os.path.join('benchmark', suite, '**', '[!_]*.yml'), recursive=True)))

    # Set up counters
    num_benchmarks_total = len(benchmark_files)
    num_benchmarks_passed = 0

    # Get date
    date = datetime.date.today().isoformat()

    # Write measurements to CSV file
    output_csv_file = args.output
    if output_csv_file:
        if not os.path.isfile(output_csv_file): # file does not yet exist
            tqdm.write(f'Writing measurements to \'{output_csv_file}\'.')
            with open(output_csv_file, 'w') as csv:
                csv.write('commit,date,version,suite,benchmark,experiment,name,config,case,time\n')
        else:
            tqdm.write(f'Adding measurements to \'{output_csv_file}\'.')

    # A central object to collect all measurements of all experiments.  Has the following structure:
    #
    # results
    # └── suites
    #     └── benchmarks
    #         └── experiments
    #             └── configurations
    #                 └── measurements (pandas.DataFrame, YAML object)
    #
    # results:      suite name      --> suite
    # suite:        benchmark name  --> benchmark
    # benchmark:    experiment name --> experiment
    # experiment:   configuration name --> measurements (pandas.DataFrame, YAML object)
    results = dict()

    repo = Repo('.')
    commit = repo.head.commit

    # Set up event log
    log = tqdm(total=0, position=1, ncols=80, leave=False, bar_format='{desc}', disable=not is_interactive)

    # Process experiment files and collect measurements
    for path_to_file in tqdm(benchmark_files, position=0, ncols=80, leave=False,
                             bar_format='|{bar}| {n}/{total}', disable=not is_interactive):
        # Log current file
        log.set_description_str(f'Running benchmark "{path_to_file}"'.ljust(80))

        # Validate schema
        if not validate_schema(path_to_file, YML_SCHEMA):
            tqdm.write(f'Benchmark file "{path_to_file}" violates schema.')
            continue

        with open(path_to_file, 'r') as yml_file:
            yml = yaml.safe_load(yml_file)

            # Get experiment name
            experiment_name = yml.get('name', path_to_file)
            tqdm.write(f'Perform benchmarks in \'{path_to_file}\'.')
            sys.stdout.flush()

            # Perform experiment
            experiment = perform_experiment(experiment_name, yml, path_to_file)

            for config_name, measurements in experiment.items():
                # Add commit SHA and date columns
                measurements.insert(0, 'commit', pandas.Series(str(commit), measurements.index))
                measurements.insert(1, 'date',   pandas.Series(date, measurements.index))

                # Write to CSV file
                if output_csv_file:
                    measurements.to_csv(output_csv_file, index=False, header=False, mode='a')

                # Add to benchmark results
                suite = results.get(yml['suite'], dict())
                benchmark = suite.get(yml['benchmark'], dict())
                experiment, _ = benchmark.get(experiment_name, (dict(), None))
                experiment[config_name] = measurements
                benchmark[experiment_name] = (experiment, yml)
                suite[yml['benchmark']] = benchmark
                results[yml['suite']] = suite

            num_benchmarks_passed += 1

            # Compare to other systems
            if args.compare:
                for name, script in yml.get('compare_to', dict()).items():
                    tqdm.write(f'` Perform experiment {yml["suite"]}/{yml["benchmark"]}/{experiment_name} in system {name}.')
                    sys.stdout.flush()
                    if not os.path.isfile(script) or not os.access(script, os.X_OK):
                        tqdm.write(f'Error: File "{script}" is not executable.')
                        continue
                    measurements = pandas.DataFrame(columns=['commit', 'date', 'version', 'suite', 'benchmark', 'experiment', 'name', 'config', 'case', 'time'])
                    stream = os.popen(f'taskset -c 0 {script}')

                    for idx, line in enumerate(stream):
                        time = float(line) # in milliseconds
                        measurements.loc[len(measurements)] = [
                            str(commit),
                            date,
                            yml.get('version', 1),
                            yml['suite'],
                            yml['benchmark'],
                            experiment_name,
                            name,
                            name,
                            list(yml['cases'].keys())[idx],
                            time
                        ]

                    # Write to CSV file
                    if output_csv_file:
                        measurements.to_csv(output_csv_file, index=False, header=False, mode='a')

                    # Add to benchmark results
                    suite = results.get(yml['suite'], dict())
                    benchmark = suite.get(yml['benchmark'], dict())
                    experiment, _ = benchmark.get(experiment_name, (dict(), None))
                    experiment[name] = measurements
                    benchmark[experiment_name] = (experiment, yml)
                    suite[yml['benchmark']] = benchmark
                    results[yml['suite']] = suite

                    stream.close()

    # Create .pgsql file
    if args.pgsql:
        generate_pgsql(commit, results)

    # Close event log
    log.clear()
    log.close()

    exit(num_benchmarks_passed != num_benchmarks_total)
