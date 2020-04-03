#!/usr/bin/env python3

from colorama import Fore, Back, Style
from git import Repo
from tqdm import tqdm
from yattag import Doc, indent
import altair
import argparse
import datetime
import glob
import itertools
import numpy
import os
import pandas
import re
import subprocess
import time
import yamale
import yaml


NUM_RUNS        = 5
DEFAULT_TIMEOUT = 300 # 5 minutes
MUTABLE_BINARY  = os.path.join('build', 'release', 'bin', 'shell')
YML_SCHEMA      = os.path.join('benchmark', '_schema.yml')


class BenchmarkError(Exception):
    pass


########################################################################################################################
# Helper functions
########################################################################################################################

in_red   = lambda x: f'{Fore.RED}{x}{Style.RESET_ALL}'
in_green = lambda x: f'{Fore.GREEN}{x}{Style.RESET_ALL}'
in_bold  = lambda x: f'{Style.BRIGHT}{x}{Style.RESET_ALL}'


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
# Start the shell with `command` and pass `query` to its stdin.  Search the stdout for timings using the given regex
# `pattern` and return them as a list.
#=======================================================================================================================
def benchmark_query(command, query, pattern, timeout = DEFAULT_TIMEOUT):
    cmd = command + [ '--quiet', '-' ]
    query = query.strip().replace('\n', ' ') + '\n' # transform to a one-liner and append new line to submit query

    process = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               cwd=os.getcwd())
    try:
        out, err = process.communicate(query.encode('latin-1'), timeout=timeout)
    except subprocess.TimeoutExpired:
        process.kill()
        raise BenchmarkError(f'Benchmark timed out after {timeout} seconds')
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
        query = query.encode('unicode_escape').decode()
        out = '\n'.join(out.split('\n')[-20:])
        tqdm.write(f'''\
Unexpected failure during execution of benchmark "{path_to_file}" with return code {process.returncode}:
$ echo -e "{query}" | {' '.join(cmd)}
===== stdout =====
{out}
===== stderr =====
{err}
==================
''')
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
    measurements = pandas.DataFrame(columns=['suite', 'benchmark', 'experiment', 'name', 'config', 'case', 'time'])

    try:
        if is_readonly:
            timeout = DEFAULT_TIMEOUT + 30 * len(cases)
            combined_query = list()
            for case in cases.values():
                combined_query.extend([case] * NUM_RUNS)
            query = '\n'.join(combined_query)
            durations = benchmark_query(command, query, yml['pattern'])
            for case in cases.keys():
                for i in range(NUM_RUNS):
                    measurements.loc[len(measurements)] = [ suite, benchmark, experiment, name, config, case, durations[0] ]
                    durations.pop(0)
        else:
            for case, query_str in cases.items():
                query = [query_str] * NUM_RUNS
                durations = benchmark_query(command, query_str, yml['pattern'])
                for dur in durations:
                    measurements.loc[len(measurements)] = [ suite, benchmark, experiment, name, config, case, dur ]
    except BenchmarkError as ex:
        tqdm.write(f'Benchmark {suite}/{benchmark} failed: {str(ex)}')

    return measurements


#=======================================================================================================================
# Perform the experiment specified in a YAML file with all provided configurations
#
# @param experiment_name the name of this experiment
# @param path_to_file    the path to the experiment YAML file
# @return                a map from configuration name to (pandas.DataFrame, YAML object)
#=======================================================================================================================
def perform_experiment(experiment_name, path_to_file):
    tqdm.write(f'Perform benchmarks in \'{path_to_file}\'.')

    # Open file
    with open(path_to_file, 'r') as f:
        yml = yaml.safe_load(f)

    configs = yml.get('configurations', dict())

    experiment = dict()
    if configs:
        # Run benchmark under different configurations
        for config_name, config in configs.items():
            measurements = run_configuration(experiment_name, config_name, config, yml)
            experiment[config_name] = (measurements, yml)
    else:
        measurements = run_configuration(experiment_name, '', '', yml)
        experiment[''] = (measurements, yml)

    return experiment


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
                                doc.line('a', 'Information', klass='nav-link', href=f'#info')
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
                        with tag('div', id='info'):
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
                                            for experiment, configs in experiments.items():
                                                for config in configs.keys():
                                                    with tag('div', klass='card', style='width: auto;'):
                                                        if config:
                                                            doc.line('div', f'{experiment} ({config})', klass='card-header')
                                                        else:
                                                            doc.line('div', f'{experiment}', klass='card-header')
                                                        with tag('div', klass='card-img-top'):
                                                            with tag('div', id=f'chart_{suite}_{benchmark}_{experiment}_{config}'):
                                                                pass
                                                        with tag('div', klass='card-body'):
                                                            with tag('h5', klass='card-title'):
                                                                if config:
                                                                    text(f'Experiment {experiment} with configuration {config}.')
                                                                else:
                                                                    text(f'Experiment {experiment}.')
                                            with tag('div', klass='card', style='width: auto;'):
                                                doc.line('div', f'{suite} / {benchmark}', klass='card-header')
                                                with tag('div', klass='card-img-top'):
                                                    with tag('div', id=f'chart_{suite}_{benchmark}'):
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

            for suite, benchmarks in results.items():
                for benchmark, experiments in benchmarks.items():
                    combined_measurements = None
                    combined_labels = set()
                    for experiment, configs in experiments.items():
                        for config, data in configs.items():
                            measurements, yml = data

                            # Produce chart
                            num_cases = len(measurements['case'].unique())
                            chart_width = 30 * num_cases
                            chart_title = yml['description']
                            chart_x_label = yml.get('label', 'Cases')
                            base = altair.Chart(measurements, title=chart_title, width=chart_width).encode(
                                x = altair.X('case:N', title=chart_x_label)
                            )
                            box = base.mark_boxplot().encode(
                                y = altair.Y('time:Q', title='Time (ms)')
                            )
                            line = base.mark_line(color='red').encode(y='mean(time)')
                            chart = box + line

                            combined_measurements = pandas.concat([combined_measurements, measurements],
                                                                  ignore_index=True, sort=False)
                            combined_labels.add(chart_x_label)

                            with tag('script', type='text/javascript'):
                                text(f'var spec = {chart.to_json()};')
                                text('var opt = {"renderer": "canvas", "actions": false};')
                                text(f'vegaEmbed("#chart_{suite}_{benchmark}_{experiment}_{config}" , spec, opt);')

                    # Produce combined chart
                    if len(combined_labels) == 0:
                        combined_labels.add('Cases')
                    num_cases = len(combined_measurements['case'].unique())
                    chart_width = 50 * num_cases
                    #  chart_title = f'Combined chart for {suite} / {benchmark}.'
                    sel = altair.selection_multi(fields=['col'], bind='legend')
                    base = altair.Chart(combined_measurements, width=chart_width
                        ).transform_calculate(
                            col = "datum.experiment + ' ' + datum.name"
                        ).encode(
                            x = altair.X('case:N', title=' | '.join(sorted(combined_labels))),
                            color = altair.Color('col:N', title='Experiments')
                        )
                    line = base.mark_line().encode(
                        y = altair.Y('mean(time)', title='Time (ms)')
                    )
                    band = base.mark_errorband(extent='ci').encode(
                        y = altair.Y('time', title=None)
                    )
                    chart = (line + band).add_selection(sel).interactive()
                    with tag('script', type='text/javascript'):
                        text(f'var spec = {chart.to_json()};')
                        text('var opt = {"renderer": "canvas", "actions": false};')
                        text(f'vegaEmbed("#chart_{suite}_{benchmark}" , spec, opt);')

            with tag('script', type='text/javascript'):
                text('''\
$(function () {
  $('[data-toggle="tooltip"]').tooltip()
})
''')

    with open('benchmark.html', 'w') as html:
        html.write(indent(doc.getvalue()))
        html.write('\n')


#=======================================================================================================================
# main
#=======================================================================================================================
if __name__ == '__main__':
    # Parse args
    parser = argparse.ArgumentParser(description='''Run benchmarks on mutable.
                                                    The build directory is assumed to be './build/release'.''')
    parser.add_argument('suite', nargs='*', help='a benchmark suite to be run')
    parser.add_argument('--html', help='Generate static HTML report', dest='html', default=False, action='store_true')
    parser.add_argument('-o', '--output',
                        help='Specify file to write measurement in CSV format (defaults to \'benchmark.csv\')',
                        dest='output', metavar='FILE.csv', default=None, action='store')
    parser.add_argument('--args', help='provide additional arguments to pass through to the binary', dest='binargs',
                                  metavar='ARGS', default=None, action='store')
    parser.add_argument('-v', '--verbose', help='verbose output', dest='verbose', default=False, action='store_true')
    args = parser.parse_args()

    # Check whether we are interactive
    is_interactive = True if 'TERM' in os.environ else False

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
            csv.write('commit,date,suite,benchmark,experiment,name,config,case,time\n')
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
        experiment = perform_experiment(experiment_name, path_to_file)

        for config_name, data in experiment.items():
            measurements, yml = data

            # Add commit SHA and date columns
            measurements.insert(0, 'commit', pandas.Series(str(commit), measurements.index))
            measurements.insert(1, 'date',   pandas.Series(date, measurements.index))

            # Write to CSV file
            measurements.to_csv(output_csv_file, index=False, header=False, mode='a')

            # Add to benchmark results
            suite = results.get(yml['suite'], dict())
            benchmark = suite.get(yml['benchmark'], dict())
            experiment = benchmark.get(experiment_name, dict())
            experiment[config_name] = data
            benchmark[experiment_name] = experiment
            suite[yml['benchmark']] = benchmark
            results[yml['suite']] = suite

        num_benchmarks_passed += 1

    if args.html:
        generate_html(commit, results)

    # Close event log
    log.clear()
    log.close()

    exit(num_benchmarks_passed != num_benchmarks_total)
