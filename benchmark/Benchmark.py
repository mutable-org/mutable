#!/usr/bin/env python3
from database_connectors import mutable, postgresql, hyper, duckdb, connector
from database_connectors.connector import *
from benchmark_utils import *

from git import Repo
from sqlobject.converters import sqlrepr
from tqdm import tqdm
from typeguard import typechecked
from types import SimpleNamespace
from typing import Callable, TypeAlias, Any
import argparse
import datetime
import git.objects.commit
import glob
import numpy
import math
import os
import pandas
import sys
import yamale
import yaml


YML_SCHEMA: str = os.path.join('benchmark', '_schema.yml')                      # The validation schema

BENCHMARK_SYSTEMS: list[str] = ['mutable', 'PostgreSQL', 'DuckDB', 'HyPer']     # List of systems

N_RUNS: int = 5


class BenchmarkError(Exception):
    pass


# Type definitions used to represent benchmark data
Measurements: TypeAlias = pandas.DataFrame                     # pandas.DataFrame
Experiment: TypeAlias = dict[str, Measurements]                # configuration name --> measurements
Benchmark: TypeAlias = dict[str, tuple[Experiment, dict]]      # experiment name    --> (experiment YAML object)
Suite: TypeAlias = dict[str, Benchmark]                        # benchmark name     --> benchmark
Result: TypeAlias = dict[str, Suite]                           # suite name         --> suite


#=======================================================================================================================
# Validate a YAML file given a YAML schema file (with yamale)
#=======================================================================================================================
@typechecked
def validate_schema(path_to_file: str, path_to_schema: str) -> bool:
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
@typechecked
# Converts an input string to a string in a SQL statement.  Performs quoting and escaping.
def dbstr(input_str: str) -> str:
    return sqlrepr(input_str, 'postgres')

@typechecked
def generate_pgsql(commit: git.objects.commit.Commit, results: Result) -> None:
    _, nodename, *_ = os.uname()
    now: str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')        # Date in ISO 8601

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

                    chart_config: dict[str, str] = dict()
                    if 'chart' in yml:
                        chart: dict[str, dict[str, str]] = yml['chart']
                        for dimension, dimension_config in chart.items():
                            for attr, val in dimension_config.items():
                                chart_config[f'{attr}_{dimension}'] = dbstr(val)

                    chart_scale_x: str = chart_config.get('scale_x', "'linear'")
                    chart_scale_y: str = chart_config.get('scale_y', "'linear'")
                    chart_type_x : str = chart_config.get('type_x',  "'Q'")
                    chart_type_y : str = chart_config.get('type_y',  "'Q'")
                    chart_label_x: str = chart_config.get('label_x', "'X'")
                    chart_label_y: str = chart_config.get('label_y', "'Y'")

                    output_sql_file.write(f'''
    -- Get chart configuration
    PERFORM insert_chartconfig({chart_scale_x}, {chart_scale_y}, {chart_type_x}, {chart_type_y}, {chart_label_x}, {chart_label_y});
    SELECT id FROM "ChartConfig"
    WHERE scale_x={chart_scale_x} AND scale_y={chart_scale_y}
      AND type_x={chart_type_x} AND type_y={chart_type_y}
      AND label_x={chart_label_x} AND label_y={chart_label_y}
    INTO chartconfig_id;
''')

                    version: int = int(yml.get('version', 1))
                    description: str = str(yml['description'])
                    read_only: str = 'TRUE' if (bool(yml['readonly'])) else 'FALSE'
                    experiment_params: str | None = None
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

    -- Update chartconfig of this experiment (if a new one was inserted)
    UPDATE "Experiments"
    SET chart_config=chartconfig_id
    WHERE id=experiment_id;
''')

                    for config, measurements in configs.items():
                        if len(measurements['case']) == 0:
                            continue # no results gathered, skip this section
                        config_params: numpy.ndarray = measurements['config'].unique()
                        parameters: list[str] = list()
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

                        with_nan: Callable[[float], float | str] = lambda flt: "'NaN'" if math.isnan(flt) else flt
                        combine: Callable[[str | float | int, float, int], str] = lambda case, time, run_id: \
                            ' '*8 + f'(timestamp_id, experiment_id, configuration_id, {case}, {with_nan(float(time))}, {run_id})'
                        values: list[str] = [ combine(case, time, run_id) for case, time, run_id in zip(measurements['case'], measurements['time'], measurements['run_id']) ]
                        output_sql_file.write(',\n'.join(values))
                        output_sql_file.write(';\n')


        output_sql_file.write('END$$;')


#=======================================================================================================================
# Perform the experiment specified in `yml` and `info` on the connector `conn` and add the measurements to `results`
#=======================================================================================================================
@typechecked
def perform_experiment(
    yml: dict[str, Any],
    conn: connector.Connector,
    system: str,
    info: SimpleNamespace,
    results: Result,
    output_csv_file: str | None
) -> None:
    # Experiment parameters
    systems: dict[str, Any] = yml.get('systems', dict())
    params: dict[str, Any] = dict(systems[system])
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
        connector_result: ConnectorResult = conn.execute(N_RUNS, params)
    except connector.ConnectorException as ex:
        tqdm_print(f"\nAn error occurred for {system} while executing {info.path_to_file}: {str(ex)}\n")
        raise BenchmarkError()

    # Add measurements to result
    for config_name, config_result in connector_result.items():
        # Skip empty measurements
        if not config_result:
            continue

        columns: list[str] = ['commit', 'date', 'version', 'suite', 'benchmark', 'experiment', 'name', 'config', 'case', 'time', 'run_id']
        config: str = config_name
        configurations: dict[str, Any] | None = systems[system].get('configurations')
        if configurations:
            for name, conf in configurations.items():
                if name in config_name:
                    if 'args' in conf:
                        config = conf['args']
                    elif 'variables' in conf:
                        config = ' '.join([f'{key}={value}' for key, value in conf['variables'].items()])
                    break

        measurements_list: list[list[str | int | float]] = list()
        for case, times in config_result.items():
            for run in range(len(times)):
                measurements_list.append([str(info.commit), info.date, info.version, info.suite_name, info.benchmark_name, info.experiment_name, config_name, config, case, times[run], run])

        # Create dataframe
        measurements: Measurements = pandas.DataFrame(measurements_list, columns=columns)

        # Write to CSV file
        if output_csv_file:
            measurements.to_csv(output_csv_file, index=False, header=False, mode='a')

        # Add to benchmark results
        suite: Suite = results.get(yml['suite'], dict())
        benchmark: Benchmark = suite.get(yml['benchmark'], dict())
        experiment, _ = benchmark.get(info.experiment_name, (dict(), None))
        experiment[config_name] = measurements
        benchmark[info.experiment_name] = (experiment, yml)
        suite[info.benchmark_name] = benchmark
        results[info.suite_name] = suite


#=======================================================================================================================
# Runs all benchmarks using the parsed args
#=======================================================================================================================
@typechecked
def run_benchmarks(args: argparse.Namespace) -> None:
    # Check whether we are interactive
    is_interactive: bool = True if os.environ.get('TERM', False) else False

    # Get benchmark files
    benchmark_files: list[str]
    if not args.path:
        benchmark_files = sorted(glob.glob(os.path.join('benchmark', '**', '[!_]*.yml'), recursive=True))
    else:
        benchmark_files = []
        for path in sorted(set(args.path)):
            if os.path.isfile(path):  # path is an experiment file
                benchmark_files.append(path)
            else:  # path is a directory containing multiple experiment files
                benchmark_files.extend(glob.glob(os.path.join('benchmark', path, '**', '[!_]*.yml'), recursive=True))

    benchmark_files: list[str] = sorted(list(set(benchmark_files)))

    # Set up counters
    num_experiments_total: int = 0
    num_experiments_passed: int = 0

    # Get date
    date: str = datetime.date.today().isoformat()

    # Get systems
    test_systems: set[str] = set()
    test_systems.add('mutable')
    if args.compare:
        test_systems.update(BENCHMARK_SYSTEMS)

    # Write measurements to CSV file
    output_csv_file: str | None = args.output
    if output_csv_file:
        if not os.path.isfile(output_csv_file):  # file does not yet exist
            tqdm_print(f'Writing measurements to \'{output_csv_file}\'.')
            with open(output_csv_file, 'w') as csv:
                csv.write('commit,date,version,suite,benchmark,experiment,name,config,case,time,runid\n')
        else:
            tqdm_print(f'Adding measurements to \'{output_csv_file}\'.')

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
    results: Result = dict()

    repo = Repo('.')
    commit: git.objects.commit.Commit = repo.head.commit
    repo.__del__()

    # Set up event log
    log = tqdm(total=0, position=1, ncols=80, leave=False, bar_format='{desc}', disable=not is_interactive)

    # Process experiment files and collect measurements
    for path_to_file in tqdm(benchmark_files, position=0, ncols=80, leave=False,
                             bar_format='|{bar}| {n}/{total}', disable=not is_interactive):
        # Log current file
        log.set_description_str(f'Running benchmark "{path_to_file}"'.ljust(80))

        # Validate schema
        if not validate_schema(path_to_file, YML_SCHEMA):
            tqdm_print(f'Benchmark file "{path_to_file}" violates schema.')
            continue

        with open(path_to_file, 'r') as yml_file:
            yml: dict[str, Any] = yaml.safe_load(yml_file)

            # Get information about experiment
            info: SimpleNamespace = SimpleNamespace()
            info.path_to_file = path_to_file
            info.date = date
            info.commit = commit
            info.version = yml.get('version', 1)
            info.suite_name = yml.get('suite')
            info.benchmark_name = yml.get('benchmark')
            info.experiment_name = yml.get('name', path_to_file)

            # Count the lines in each table file and add it to the table entry
            info.experiment_data = yml.get('data')
            if info.experiment_data:
                table_access_error: bool = False
                for table_name, table in info.experiment_data.items():
                    if 'file' not in table:
                        continue  # Skip counting files when table does not have a file with data
                    p: str = os.path.join(table['file'])  # Path to file
                    if not os.path.isfile(p):
                        tqdm_print(f'Table file \'{p}\' not found.  Skipping benchmark.\n')
                        table_access_error = True
                    else:
                        info.experiment_data[table_name]['lines_in_file'] = int(os.popen(f"wc -l < {p}").read())
                if table_access_error:
                    continue  # At least one table file could not be opened.  Skip benchmark.

            tqdm_print('\n\n==========================================================')
            tqdm_print(f'Perform benchmarks in \'{path_to_file}\'.')
            tqdm_print('==========================================================')

            # Perform experiment for each system
            for system in yml.get('systems', dict()).keys():
                if system not in test_systems:
                    continue

                connectors: list[connector.Connector] = list()

                match system:
                    case 'mutable':
                        connectors.append(mutable.Mutable(dict(
                            path_to_binary='build/release/bin/shell',
                            verbose=args.verbose,
                        )))
                    case 'PostgreSQL':
                        connectors.append(postgresql.PostgreSQL(dict(
                            verbose=args.verbose
                        )))
                    case 'DuckDB':
                        connectors.append(duckdb.DuckDB(dict(
                            path_to_binary='benchmark/database_connectors/duckdb',
                            verbose=args.verbose,
                            multithreaded=False
                        )))
                        connectors.append(duckdb.DuckDB(dict(
                            path_to_binary='benchmark/database_connectors/duckdb',
                            verbose=args.verbose,
                            multithreaded=True
                        )))
                    case 'HyPer':
                        connectors.append(hyper.HyPer(dict(
                            verbose=args.verbose,
                            multithreaded=False
                        )))
                        connectors.append(hyper.HyPer(dict(
                            verbose=args.verbose,
                            multithreaded=True
                        )))

                for conn in connectors:
                    if system == 'mutable':
                        num_experiments_total += 1
                    try:
                        perform_experiment(yml, conn, system, info, results, output_csv_file)
                    except BenchmarkError:
                        pass  # nothing to be done
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
    run_benchmarks(args)
