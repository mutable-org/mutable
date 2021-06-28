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
            sf = tbl['sf']
            path_to_file = os.path.join(path_to_data, name + '.csv')
            rows = count_lines(path_to_file) - 1
            imports.append(f'IMPORT INTO {name} DSV "{path_to_file}" ROWS {int(sf * rows)} HAS HEADER SKIP HEADER;')
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


def print_command(command, query, indent = ''):
    query_str = query.strip().replace('\n', ' ').replace('"', '\\"')
    command_str = ' '.join(command)
    tqdm.write(f'{indent}$ echo "{query_str}" | {command_str} -')


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
Unexpected failure during execution of benchmark "{path_to_file}" with return code {process.returncode}:
$ echo -e {repr(query)} | {' '.join(cmd)}
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
# @param experiment_name the name of this experiment
# @param path_to_file    the path to the experiment YAML file
# @return                a tuple of a YAML object and a map from configuration name to pandas.DataFrame
#=======================================================================================================================
def perform_experiment(experiment_name, path_to_file):
    tqdm.write(f'Perform benchmarks in \'{path_to_file}\'.')
    sys.stdout.flush()

    # Open file
    with open(path_to_file, 'r') as f:
        yml = yaml.safe_load(f)

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

    return (yml, experiment)


#=======================================================================================================================
# Generate static HTML report
#=======================================================================================================================
def generate_html(commit, results):
    prev_commit_sha = os.environ.get('PREV_COMMIT_SHA', None)
    current_suite = None
    current_benchmark = None

    doc, tag, text = Doc().tagtext()
    doc.asis('<!DOCTYPE html>')
    with tag('html'):
        with tag('head'):
            with tag('script', src='https://cdn.jsdelivr.net/npm/vega@5'):
                pass
            with tag('script', src='https://cdn.jsdelivr.net/npm/vega-lite@3'):
                pass
            with tag('script', src='https://cdn.jsdelivr.net/npm/vega-embed@4'):
                pass
            doc.stag('link', rel='stylesheet', type='text/css',
                             href='https://stackpath.bootstrapcdn.com/bootstrap/4.4.1/css/bootstrap.min.css',
                             integrity='sha384-Vkoo8x4CGsO3+Hhxv8T/Q5PaXtkKtu6ug5TOeNV6gBiFeWPGFN9MuhOf23Q9Ifjh',
                             crossorigin='anonymous')
            doc.stag('link', rel='stylesheet', type='text/css', href='style.css')
            doc.stag('link', rel='stylesheet',
                             href='https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.11.2/css/all.min.css',
                             integrity='sha256-+N4/V/SbAFiW1MPBCXnfnP9QSN3+Keu+NlB+0ev/YKQ=',
                             crossorigin='anonymous')
        with tag('body'):
            # Generate top nav bar
            with tag('nav', klass='navbar navbar-expand-md navbar-dark bg-dark navbar-shadow sticky-top'):
                with tag('a', klass='navbar-brand', href='#'):
                    doc.asis('<i class="fas fa-cubes ico-rotate"></i> mu<i class="text-mutable">t</i>able')
                    # TODO link CSV file with measurements
                with tag('div', klass='collapse navbar-collapse'):
                    with tag('ul', klass='navbar-nav mr-auto'):
                        with tag('li', klass='nav-item active'):
                            doc.line('a', 'Benchmark Results', klass='nav-link', href='#')

            with tag('div', klass='container-fluid'):
                with tag('div', klass='row'):
                    # Generate nav sidebar
                    with tag('nav', klass='col-4 col-md-3 col-lg-2 px-1 d-md-block bg-light sidebar sidebar-sticky position-fixed'):
                        with tag('h6', klass='sidebar-heading px-3 mt-4 mb-1 text-muted'):
                            text('General')
                        with tag('ul', klass='nav flex-column'):
                            with tag('li', klass='nav-item'):
                                doc.line('a', 'Information', klass='nav-link', href=f'#information')
                            if prev_commit_sha:
                                with tag('li', klass='nav-item'):
                                    with tag('a', klass='nav-link', href=f'./{prev_commit_sha}.html'):
                                        text('View previous Benchmark')
                                with tag('li', klass='nav-item'):
                                    with tag('a', klass='nav-link', rel='noopener', href=f'https://gitlab.cs.uni-saarland.de/bigdata/mutable/mutable/-/compare/{prev_commit_sha}...{commit}'):
                                        text('Inspect Diff to previous Version ')
                                        doc.line('i', '',  klass='fas fa-external-link-alt')
                            with tag('li', klass='nav-item'):
                                with tag('a', klass='nav-link', href=f'./{commit}.csv'):
                                    text('Download Measurement Data ')
                                    doc.line('i', '', klass='fas fa-download')
                        with tag('h6', klass='sidebar-heading px-3 mt-4 mb-1 text-muted'):
                            text('Benchmark Suites')
                        with tag('ul', klass='nav flex-column'):
                            for suite, benchmarks in results.items():
                                with tag('li', klass='nav-item'):
                                    with tag('a', klass='nav-link', href=f'#{suite}'):
                                        doc.line('i', '', klass='far fa-arrow-alt-circle-right')
                                        text(suite)
                                    with tag('ul', klass='nav flex-column'):
                                        for benchmark in benchmarks.keys():
                                            with tag('li', klass='nav-item'):
                                                with tag('a', klass='nav-link', href=f'#{suite}_{benchmark}'):
                                                    text(benchmark)

                    # Generate main content
                    with tag('main', role='main', klass='col-8 col-md-9 col-lg-10 ml-sm-auto col-lg-10 px-4'):
                        with tag('div', id='information'):
                            doc.line('h1', 'Information')
                            with tag('p'):
                                text('This benchmark was performed on commit ')
                                with tag('a', rel='noopener',
                                         href=f'https://gitlab.cs.uni-saarland.de/bigdata/mutable/mutable/-/commit/{commit}'):
                                    doc.attr(
                                        ( 'data-toggle', 'tooltip' ),
                                        ( 'title', f'Commit {commit} by {commit.author.name} on \
                                                     {commit.committed_datetime}.' )
                                    )
                                    with tag('span', klass='commitish'):
                                        text(str(commit))
                                sysname, nodename, release, version, machine = os.uname()
                                if nodename:
                                    text(f' on machine {nodename}.')
                                else:
                                    text(' on an unknown machine.')
                                text(f' The operating system is {sysname} {release}.')
                            if prev_commit_sha:
                                with tag('p'):
                                    text('To inspect the diff to the previously benchmarked version locally, execute the \
                                          following command:')
                                    with tag('pre'):
                                        with tag('code'):
                                            text(f'$ git difftool {prev_commit_sha} {commit}')
                            if args.binargs:
                                with tag('p'):
                                    doc.line('b', 'Supplementary Command Line Arguments: ')
                                    doc.line('code', args.binargs)
                        for suite, benchmarks in results.items():
                            with tag('div', id=suite, klass='suite'):
                                doc.line('h2', suite)
                                for benchmark, experiments in benchmarks.items():
                                    with tag('div', id=f'{suite}_{benchmark}', klass='benchmark'):
                                        doc.line('h3', benchmark)
                                        with tag('div', klass='charts'):
                                            # Emit cards with the charts and further information
                                            for experiment, data in experiments.items():
                                                configs, yml = data
                                                with tag('div', klass='card', style='width: auto;'):
                                                    with tag('div', klass='card-header'):
                                                        text(experiment)
                                                    with tag('div', klass='card-img-top'):
                                                        with tag('div', id=f'chart_{suite}_{benchmark}_{experiment}'):
                                                            pass
                                                    with tag('div', klass='card-body'):
                                                        doc.line('h5', yml['description'], klass='card-title')
                                                        with tag('div', klass='info', style='display: none;'):
                                                            with tag('p'):
                                                                text('ver. ')
                                                                text(str(yml.get('version', 1)))
                                                            cmd_args = list()
                                                            if 'args' in yml and yml['args']:
                                                                cmd_args.append(str(yml['args']))
                                                            #  if config and yml['configurations'].get(config, None):
                                                            #      cmd_args.append(str(yml['configurations'][config]))
                                                            with tag('p'):
                                                                if cmd_args:
                                                                    cmd_args = map(lambda x: x.strip(), cmd_args)
                                                                    with tag('code'):
                                                                        text(' '.join(cmd_args))
                                                                else:
                                                                    text('no supplementary command line arguments')
                                            if len(experiments) > 1:
                                                with tag('div', klass='card', style='width: auto;'):
                                                    doc.line('div', f'{suite} / {benchmark}', klass='card-header')
                                                    with tag('div', klass='card-img-top'):
                                                        with tag('div', id=f'chart_{suite}_{benchmark}', klass='chart-interactive'):
                                                            pass
                                                    with tag('div', klass='card-body'):
                                                        doc.line('h5', f'Combined chart for benchmark {suite} / {benchmark}.', klass='card-title')

            with tag('script', src='https://code.jquery.com/jquery-3.4.1.slim.min.js',
                               integrity='sha384-J6qa4849blE2+poT4WnyKhv5vZF5SrPo0iEjwBvKU7imGFAV0wwj1yYfoRSJoZ+n',
                               crossorigin='anonymous'):
                pass
            with tag('script', src='https://cdn.jsdelivr.net/npm/popper.js@1.16.0/dist/umd/popper.min.js',
                               integrity='sha384-Q6E9RHvbIyZFJoft+2mJbHaEWldlvI9IOYy5n3zV9zzTtmI3UksdQRVvoxMfooAo',
                               crossorigin='anonymous'):
                pass
            with tag('script', src='https://stackpath.bootstrapcdn.com/bootstrap/4.4.1/js/bootstrap.bundle.min.js',
                               integrity='sha384-6khuMg9gaYr5AxOqhkVIODVIvm9ynTT5J4V1cfthmT+emCG6yVmEZsRHdxlotUnm',
                               crossorigin='anonymous'):
                pass

            with tag('script'):
#                  text('''\
#  $(function() {
#      var cards = $('.charts .card');
#  });
#  ''')
                pass

            for suite, benchmarks in results.items():
                for benchmark, experiments in benchmarks.items():
                    measurements_benchmark = None
                    labels_benchmark = set()
                    for experiment, data in experiments.items():
                        configs, yml = data
                        measurements_experiment = None
                        labels_experiment = set()
                        for config, measurements in configs.items():
                            measurements_experiment = pandas.concat([measurements_experiment, measurements],
                                                                    ignore_index=True, sort=False)

                        chart_x_label = yml.get('label', 'Cases')
                        measurements_benchmark = pandas.concat([measurements_benchmark, measurements_experiment],
                                                               ignore_index=True, sort=False)
                        labels_benchmark.add(chart_x_label)

                        # Produce combined chart for all configurations of the same experiment
                        is_case_numeric = is_numeric_dtype(measurements_experiment['case'])
                        num_cases = len(measurements_experiment['case'].unique())
                        chart_width = 30 * num_cases

                        selection = altair.selection_multi(fields=['name'], bind='legend')
                        base = altair.Chart(measurements_experiment, width=chart_width).encode(
                            x = altair.X('case:Q' if is_case_numeric else 'case:N', title=' | '.join(sorted(labels_benchmark))),
                            color = altair.Color('name:N', title=None, legend=None)
                        )
                        line = base.mark_line().encode(
                            y = altair.Y('mean(time)', title='Time (ms)'),
                            opacity = altair.condition(selection, altair.value(1), altair.value(.3))
                        )
                        band = base.mark_errorband(extent='ci').encode(
                            y = altair.Y('time', title=None),
                            opacity = altair.condition(selection, altair.value(.4), altair.value(.1))
                        )
                        point = base.mark_point().encode(
                            y = altair.Y('mean(time)', title=None),
                            color = altair.Color('name:N', title=None),
                            shape = altair.Shape('name:N', title=None),
                            opacity = altair.condition(selection, altair.value(1), altair.value(.3))
                        )
                        chart = (line + band + point).resolve_scale(
                            color='independent',
                            shape='independent'
                        ).add_selection(selection).interactive(bind_x=False)

                        with tag('script', type='text/javascript'):
                            text(f'var spec = {chart.to_json()};')
                            text('''var opt = {
                                "renderer": "svg",
                                "actions": { "export": true, "source": false, "editor": false, "compiled": false },
                                "downloadFileName": "''' +  f'{suite}-{benchmark}-{experiment}' + '" };')
                            text(f'vegaEmbed("#chart_{suite}_{benchmark}_{experiment}" , spec, opt);')

                    # Produce combined chart for entire benchmark
                    if len(experiments) > 1:
                        if len(labels_benchmark) == 0:
                            labels_benchmark.add('Cases')
                        measurements_benchmark['ident'] = measurements_benchmark['experiment'] + ' ' + measurements_benchmark['name']
                        num_cases = len(measurements_benchmark['case'].unique())
                        chart_width = 50 * num_cases

                        selection = altair.selection_multi(fields=['ident'], bind='legend')
                        base = altair.Chart(measurements_benchmark, width=chart_width).encode(
                            x = altair.X('case:N', title=' | '.join(sorted(labels_benchmark))),
                            color = altair.Color('ident:N', title=None, legend=None)
                        )
                        line = base.mark_line().encode(
                            y = altair.Y('mean(time)', title='Time (ms)'),
                            opacity = altair.condition(selection, altair.value(1), altair.value(.3))
                        )
                        band = base.mark_errorband(extent='ci').encode(
                            y = altair.Y('time', title=None),
                            opacity = altair.condition(selection, altair.value(.4), altair.value(.1))
                        )
                        point = base.mark_point().encode(
                            y = altair.Y('mean(time)', title=None),
                            color = altair.Color('ident:N', title=None),
                            shape = altair.Shape('ident:N', title='Experiments'),
                            opacity = altair.condition(selection, altair.value(1), altair.value(.3))
                        )
                        chart = (line + band + point).resolve_scale(
                            color='independent',
                            shape='independent'
                        ).add_selection(selection).interactive(bind_x=False)

                        with tag('script', type='text/javascript'):
                            text(f'var spec = {chart.to_json()};')
                            text('''var opt = {
                                "renderer": "svg",
                                "actions": { "export": true, "source": false, "editor": false, "compiled": false },
                                "downloadFileName": "''' +  f'{suite}-{benchmark}-combined' + '" };')
                            text(f'vegaEmbed("#chart_{suite}_{benchmark}" , spec, opt);')

            with tag('script', type='text/javascript'):
                doc.asis('''\
$(function () {
  $('[data-toggle="tooltip"]').tooltip()
});
''')

            with tag('script', type='text/javascript'):
                doc.asis('''\
$(function () {
    var suites = $('.suite');

    var headlines = suites.children('h2');
    var chevron_down = $('<i class="fas fa-chevron-down"></i>');
    var chevron_up   = $('<i class="fas fa-chevron-up"></i>');
    headlines.append(chevron_down);

    var toggle_suite = function (_suite) {
        var suite = $(_suite);
        var experiments = suite.find('.benchmark');
        var headline = suite.children('h2');
        var chevron = headline.find('i.fas');
        if (chevron.hasClass('fa-chevron-down')) {
            experiments.hide();
            chevron.replaceWith(chevron_up.clone());
        } else {
            experiments.show();
            chevron.replaceWith(chevron_down.clone());
        }
    };

    if (headlines.length > 1) {
        headlines.click(function () { toggle_suite($(this).closest('.suite')); });

        var nav_links = $('.nav-link[href^="#"]');
        nav_links.click(function () {
            var target = $(document).find($(this).attr('href'));
            if (!target.is(':visible'))
                toggle_suite(target.closest('.suite'));
        });

        headlines.click();
    }
});
''')

    with open('benchmark.html', 'w') as html:
        html.write(indent(doc.getvalue()))
        html.write('\n')


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

CREATE OR REPLACE FUNCTION insert_experiment(int, int, text, int, text, bool, text)
RETURNS void
LANGUAGE SQL
AS $func$
    INSERT INTO "Experiments" (benchmark, suite, name, version, description, is_read_only, label)
    SELECT $1, $2, $3, $4, $5, $6, $7
    WHERE NOT ($3, $4) IN (SELECT name, version FROM "Experiments");
$func$;

DO $$
DECLARE
    timestamp_id integer;
    suite_id integer;
    benchmark_id integer;
    experiment_id integer;
    configuration_id integer;
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
                    version = int(yml.get('version', 1))
                    description = str(yml['description'])
                    read_only = 'TRUE' if (str(yml['readonly']) == 'yes') else 'FALSE'
                    label = str(yml['label'])
                    experiment_params = None
                    if 'args' in yml and yml['args']:
                        experiment_params = yml['args']

                    output_sql_file.write(f'''
    -- Get experiment
    PERFORM insert_experiment(benchmark_id, suite_id, '{escape(experiment)}', {version}, '{escape(description)}', {read_only}, '{escape(label)}');
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
    parser.add_argument('--html', help='Generate static HTML report', dest='html', default=False, action='store_true')
    parser.add_argument('--no-compare', help='Skip comparison to other systems', dest='compare', default=True, action='store_false')
    parser.add_argument('-o', '--output', dest='output', metavar='FILE.csv', default=None, action='store',
                        help='Specify file to write measurement in CSV format (defaults to \'benchmark.csv\')')
    parser.add_argument('--args', help='provide additional arguments to pass through to the binary', dest='binargs',
                                  metavar='ARGS', default=None, action='store')
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
    output_csv_file = args.output or 'benchmark.csv'
    if not args.output or not os.path.isfile(output_csv_file): # no output file specified or file does not exist
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

        # Process the experiment file
        experiment_name = os.path.splitext(os.path.basename(path_to_file))[0]
        yml, experiment = perform_experiment(experiment_name, path_to_file)

        for config_name, measurements in experiment.items():
            # Add commit SHA and date columns
            measurements.insert(0, 'commit', pandas.Series(str(commit), measurements.index))
            measurements.insert(1, 'date',   pandas.Series(date, measurements.index))

            # Write to CSV file
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

    if args.html:
        generate_html(commit, results)

    # Create .pgsql file
    if args.pgsql:
        generate_pgsql(commit, results)

    # Close event log
    log.clear()
    log.close()

    exit(num_benchmarks_passed != num_benchmarks_total)
