#!/usr/bin/env python3

from database_connectors import mutable, postgresql, hyper, duckdb, connector

from colorama import Fore, Back, Style
from git import Repo
from pandas.api.types import is_numeric_dtype
from sqlobject.converters import sqlrepr
from tqdm import tqdm
from types import SimpleNamespace
import argparse
import datetime
import glob
import itertools
import json
import math
import numpy
import os
import pandas
import re
import subprocess
import sys
import time
import yamale
import yaml

YML_SCHEMA      = os.path.join('benchmark', '_schema.yml')

BENCHMARK_SYSTEMS = ['mutable', 'PostgreSQL', 'DuckDB', 'HyPer']

N_RUNS = 5


class BenchmarkError(Exception):
    pass


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



#=======================================================================================================================
# Generate a .pgsql file to load the results into the database
#=======================================================================================================================

# Converts an input string to a string in a SQL statement.  Performs quoting and escaping.
def dbstr(input_str):
    return sqlrepr(input_str, 'postgres')

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
    INSERT INTO "Timestamps" ("commit", "timestamp", "host")
    SELECT $1, $2, $3
    WHERE NOT ($2, $3) IN (SELECT timestamp, host FROM "Timestamps");
$func$;

CREATE OR REPLACE FUNCTION insert_experiment(int, text, int, text, bool, int)
RETURNS void
LANGUAGE SQL
AS $func$
    INSERT INTO "Experiments" (benchmark, name, version, description, is_read_only, chart_config)
    SELECT $1, $2, $3, $4, $5, $6
    WHERE NOT ($1, $2, $3) IN (SELECT benchmark, name, version FROM "Experiments");
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
    PERFORM insert_timestamp({dbstr(commit.hexsha)}, {dbstr(now)}, {dbstr(nodename)});
    SELECT id FROM "Timestamps"
    WHERE "commit"={dbstr(commit.hexsha)}
      AND "timestamp"={dbstr(now)}
      AND "host"={dbstr(nodename)}
    INTO timestamp_id;
''')

        for suite, benchmarks in results.items():
            # Insert Suite
            output_sql_file.write(f'''
    -- Get suite
    PERFORM insert_suite({dbstr(suite)});
    SELECT id FROM "Suites"
    WHERE name={dbstr(suite)}
    INTO suite_id;
''')

            for benchmark, experiments in benchmarks.items():
                output_sql_file.write(f'''
    -- Get benchmark
    PERFORM insert_benchmark(suite_id, {dbstr(benchmark)});
    SELECT id FROM "Benchmarks"
    WHERE suite=suite_id
      AND name={dbstr(benchmark)}
    INTO benchmark_id;
''')

                for experiment, data in experiments.items():
                    configs, yml = data

                    chart_config = dict()
                    if 'chart' in yml:
                        chart = yml['chart']
                        for dimension, dimension_config in chart.items():
                            for attr, val in dimension_config.items():
                                chart_config[f'{attr}_{dimension}'] = dbstr(val)

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
                    read_only = 'TRUE' if (bool(yml['readonly'])) else 'FALSE'
                    experiment_params = None
                    if 'args' in yml and yml['args']:
                        experiment_params = yml['args']

                    output_sql_file.write(f'''
    -- Get experiment
    PERFORM insert_experiment(benchmark_id, {dbstr(experiment)}, {version}, {dbstr(description)}, {read_only}, chartconfig_id);
    SELECT id FROM "Experiments"
    WHERE benchmark=benchmark_id
      AND name={dbstr(experiment)}
      AND version={version}
    INTO experiment_id;
''')
                    for config, measurements in configs.items():
                        if len(measurements['case']) == 0:
                            continue # no results gathered, skip this section
                        config_params = measurements['config'].unique()
                        parameters = list()
                        if experiment_params:
                            parameters.append(experiment_params)
                        if len(config_params) > 0:
                            parameters.append(' '.join(config_params))
                        output_sql_file.write(f'''
    -- Get config
    PERFORM insert_configuration({dbstr(config)}, {dbstr(" ".join(parameters))});
    SELECT id FROM "Configurations"
    WHERE name={dbstr(config)}
      AND parameters={dbstr(" ".join(parameters))}
    INTO configuration_id;

    -- Write measurements
    --  timestamp:  ('{commit.hexsha}', '{now}', '{nodename}')
    --  suite:      '{suite}'
    --  benchmark:  '{benchmark}'
    --  experiment: '{experiment}'
    --  config:     '{config}'
    INSERT INTO "Measurements" ("timestamp", "experiment", "config", "case", "value", "run_id")
    VALUES
''')

                        with_nan = lambda flt: "'NaN'" if math.isnan(flt) else flt
                        combine = lambda case, time, run_id: ' '*8 + f'(timestamp_id, experiment_id, configuration_id, {case}, {with_nan(float(time))}, {run_id})'
                        values = [ combine(case, time, run_id) for case, time, run_id in zip(measurements['case'], measurements['time'], measurements['run_id']) ]
                        output_sql_file.write(',\n'.join(values))
                        output_sql_file.write(';\n')


        output_sql_file.write('END$$;')

def perform_experiment(yml, conn, info, results) -> list():
    # Experiment parameters
    systems = yml.get('systems')
    params = dict(systems[system])
    params['description']  = yml.get('description')
    params['suite']        = info.suite_name
    params['benchmark']    = info.benchmark_name
    params['name']         = info.experiment_name
    params['readonly']     = yml.get('readonly')
    params['chart']        = yml.get('chart')
    params['data']         = info.experiment_data
    params['path_to_file'] = info.path_to_file

    # Perform benchmark
    try:
        sys.stdout.flush()
        experiment_times = conn.execute(N_RUNS, params)
    except connector.ConnectorException as ex:
        tqdm.write(f"\nAn error occurred for {system} while executing {path_to_file}: {str(ex)}\n")
        raise BenchmarkError()

    # Add measurements to result
    for config_name, execution_times in experiment_times.items():
        # Skip empty measurements
        if (not execution_times):
            return list()

        columns = ['commit', 'date', 'version', 'suite', 'benchmark', 'experiment', 'name', 'config', 'case', 'time', 'run_id']
        config = config_name
        configurations = systems[system].get('configurations')
        if configurations:
            if configurations.get(config_name):
                config = configurations.get(config_name)

        measurements = list()
        for case, times in execution_times.items():
            for run in range(len(times)):
                measurements.append([str(commit), info.date, info.version, info.suite_name, info.benchmark_name, info.experiment_name, config_name, config, case, times[run], run])

        # Create dataframe
        measurements = pandas.DataFrame(measurements, columns=columns)

        # Write to CSV file
        if output_csv_file:
            measurements.to_csv(output_csv_file, index=False, header=False, mode='a')

        # Add to benchmark results
        suite = results.get(yml['suite'], dict())
        benchmark = suite.get(yml['benchmark'], dict())
        experiment, _ = benchmark.get(info.experiment_name, (dict(), None))
        experiment[config_name] = measurements
        benchmark[info.experiment_name] = (experiment, yml)
        suite[info.benchmark_name] = benchmark
        results[info.suite_name] = suite


#=======================================================================================================================
# main
#=======================================================================================================================
if __name__ == '__main__':
    # Parse args
    parser = argparse.ArgumentParser(description='''Run benchmarks on mutable.
                                                    The build directory is assumed to be './build/release'.''')
    parser.add_argument('path', nargs='*', help='directory path of a benchmark suite or path to single experiment to be run')
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
    if not args.path:
        benchmark_files = sorted(glob.glob(os.path.join('benchmark', '**', '[!_]*.yml'), recursive=True))
    else:
        benchmark_files = []
        for path in sorted(set(args.path)):
            if os.path.isfile(path):        # path is an experiment file
                benchmark_files.append(path)
            else:                           # path is a directory containing multiple experiment files
                benchmark_files.extend(glob.glob(os.path.join('benchmark', path, '**', '[!_]*.yml'), recursive=True))

    benchmark_files = sorted(list(set(benchmark_files)))


    # Set up counters
    num_experiments_total = 0
    num_experiments_passed = 0

    # Get date
    date = datetime.date.today().isoformat()

    # Get systems
    test_systems = set()
    test_systems.add('mutable')
    if args.compare:
        test_systems.update(BENCHMARK_SYSTEMS)

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

            # Get information about experiment
            info = SimpleNamespace()
            info.path_to_file = path_to_file
            info.date = date
            info.version = yml.get('version', 1)
            info.suite_name = yml.get('suite')
            info.benchmark_name = yml.get('benchmark')
            info.experiment_name = yml.get('name', path_to_file)

            # Count the lines in each table file and add it to the table entry
            info.experiment_data = yml.get('data')
            if info.experiment_data:
                for table_name, table in info.experiment_data.items():
                    p = os.path.join(table['file'])
                    info.experiment_data[table_name]['lines_in_file'] = int(os.popen(f"wc -l < {p}").read())


            tqdm.write('\n\n===========================================')
            tqdm.write(f'Perform benchmarks in \'{path_to_file}\'.')
            tqdm.write('===========================================')

            # Perform experiment for each system
            for system in yml.get('systems').keys():
                if system not in test_systems:
                    continue

                connectors = list()

                match system:
                    case 'mutable':
                        connectors.append(mutable.Mutable(dict(
                            path_to_binary = 'build/release/bin/shell',
                            verbose = args.verbose,
                        )))
                    case 'PostgreSQL':
                        connectors.append(postgresql.PostgreSQL(dict(
                            verbose = args.verbose
                        )))
                    case 'DuckDB':
                        connectors.append(duckdb.DuckDB(dict(
                            path_to_binary = 'benchmark/database_connectors/duckdb',
                            verbose = args.verbose
                        )))
                    case 'HyPer':
                        connectors.append(hyper.HyPer(dict(
                            verbose = args.verbose
                        )))

                for conn in connectors:
                    if system == 'mutable':
                        num_experiments_total += 1
                    try:
                        perform_experiment(yml, conn, info, results)
                    except BenchmarkError:
                        pass # nothing to be done
                    else:
                        # Count number of benchmarks passed on mutable
                        if system == 'mutable':
                            num_experiments_passed += 1

    # Create .pgsql file
    if args.pgsql:
        generate_pgsql(commit, results)

    # Close event log
    log.clear()
    log.close()

    print(f'Performed {num_experiments_total} experiments on mutable')
    exit(num_experiments_passed != num_experiments_total)
