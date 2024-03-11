#!/usr/bin/env python3

import argparse
import difflib
import glob
import itertools
import math
import multiprocessing
import os
import re
import subprocess
import yamale
import yaml

from colorama import Fore, Back, Style
from tqdm import tqdm
from typing import Tuple


# number of CLI args to discard from the front for reporting
NUM_CLI_ARGS_DISCARD = 5

# Timeout per test case in seconds
TIMEOUT_SECONDS_PER_TEST_CASE = 60

#-----------------------------------------------------------------------------------------------------------------------
# HELPERS
#-----------------------------------------------------------------------------------------------------------------------

# Create dummy test case and test result class
class TestCase: pass

class TestResult: pass

yml_schema = os.path.join(os.getcwd(), 'test', '_schema.yml')

def schema_valid(test_case) -> bool:
    schema = yamale.make_schema(yml_schema)
    data = yamale.make_data(test_case.filename)
    try:
        yamale.validate(schema, data)
    except ValueError:
        tqdm.write('\n'.join(report_warning('YAML schema invalid', 'yaml_check', test_case)))
        return False
    return True


def location_valid(test_case) -> bool:
    subdir = os.path.basename(os.path.dirname(test_case.filename))
    if subdir != test_case.db:
        tqdm.write('\n'.join(report_warning('File is placed in incorrect directory', 'location_check', test_case)))
        return False
    return True


class TestException(Exception):
    def __init__(self, str):
        Exception(str)


def colordiff(actual, expected):
    output = []
    matcher = difflib.SequenceMatcher(None, actual, expected)
    for opcode, a0, a1, e0, e1 in matcher.get_opcodes():
        if opcode == "equal":
            output.append(actual[a0:a1])
        elif opcode == "insert":
            output.append(Fore.GREEN + expected[e0:e1] + Fore.RESET)
        elif opcode == "delete":
            output.append(Fore.RED + actual[a0:a1] + Fore.RESET)
        elif opcode == "replace":
            output.append(Fore.GREEN + expected[e0:e1] + Fore.RESET)
            output.append(Fore.RED + actual[a0:a1] + Fore.RESET)
    return "".join(output)


def get_feature_options(feature):
    shell = BINARIES['shell']
    stream = os.popen(f'{shell} --list-{feature}')
    stream.readline() # skip headline
    options = list()
    for line in stream:
        options.append(line.split()[0])
    return options

def enumerate_feature_options(feature, feature_plural=None, optional=False):
    if feature_plural is None:
        feature_plural = f'{feature}s'
    options = get_feature_options(feature_plural)
    flags = list()
    if optional:
        flags.append([])
    for opt in options:
        flags.append([ f'--{feature}', opt ])
    return flags


#-----------------------------------------------------------------------------------------------------------------------
# TEST EXECUTION
#-----------------------------------------------------------------------------------------------------------------------

def run_command(command, query):
    try:
        process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   cwd=os.getcwd())
        out, err = process.communicate(query.encode('latin-1'), timeout=TIMEOUT_SECONDS_PER_TEST_CASE)
        return process.returncode, out, err
    except subprocess.TimeoutExpired as ex:
        raise TestException(f'Timeout expired')
    finally:
        # Make sure the process is being terminated
        process.terminate()
        try:
            ret = process.wait(0.1) # wait 100 ms
        except subprocess.TimeoutExpired:
            process.kill()


def run_stage(args, test_case, stage_name, command) -> Tuple[bool, list[str]]:
    stage = test_case.stages[stage_name]
    try:
        returncode, out, err = run_command(command, test_case.query)
        out = out.decode('UTF-8')
        err = err.decode('UTF-8')
        num_err = err.count('error')
        is_sorted = 'ORDER BY' in test_case.query
        consider_rounding_errors = stage_name == 'end2end'

        if 'returncode' in stage:
            check_returncode(stage['returncode'], returncode)
        if 'num_err' in stage:
            check_numerr(stage['num_err'], num_err)
        if 'err' in stage:
            check_stderr(stage['err'], err)
        # in case there is no expected output, skip this check for all stages except the `end2end` stage
        if 'out' in stage and (stage['out'] != None or stage_name == 'end2end'):
            check_stdout(stage['out'], out, args.verbose, is_sorted, consider_rounding_errors)
    except TestException as ex:
        return False, report_failure(str(ex), stage_name, test_case, args.debug, command)
    cli_args = command[NUM_CLI_ARGS_DISCARD:]
    argsstr = f"[…, {', '.join(cli_args)}]" if cli_args else ''
    return True, report_success(argsstr, stage_name, test_case, args.verbose)


def run_test_case(test_case, args, binaries):
    # On MacOS, the behavior of global variables for spawned subprocesses is different than on
    # linux: the value of a global variable "may not be the same as the value in the parent process".
    # Therefore, we pass the binaries as parameter to this method. We then set the global BINARIES
    # variable again so that we do not have to pass it on to the other methods.
    # For more detailed information, see
    # https://stackoverflow.com/questions/75066117/global-list-variable-in-multiprocessing-pool-is-returning-as-empty-in-python
    global BINARIES
    BINARIES = dict(binaries)

    res = TestResult()
    res.required_counter = 0
    res.required_pass_counter = 0

    stage_counter = dict()
    stage_pass_counter = dict()
    combined_reports = list()

    success = True
    for stage in test_case.stages:
        # Set optionally provided additional CLI args
        test_case.cli_args = test_case.stages[stage].get('cli_args', None)

        # Execute test
        if success:
            cmds = COMMAND[stage](test_case)
            for cmd in cmds:
                stage_success, reports = run_stage(args, test_case, stage, cmd)
                success = success and stage_success
                combined_reports.extend(reports)
        else:
            combined_reports.extend(report_failure("earlier stage failed", stage, test_case, args.debug))

        # Store results
        stage_counter[stage] = stage_counter.get(stage, 0) + 1
        stage_pass_counter[stage] = stage_pass_counter.get(stage, 0) + success
        if test_case.required:
            res.required_counter += 1
            res.required_pass_counter += success

    res.stage_counter = stage_counter
    res.stage_pass_counter = stage_pass_counter
    res.combined_reports = combined_reports
    return res


#-----------------------------------------------------------------------------------------------------------------------
# RESULT CHECKING
#-----------------------------------------------------------------------------------------------------------------------

def check_returncode(expected, actual):
    if expected != None and expected != actual:
        raise TestException(f'Expected return code {expected}, received {actual}')
    return


def check_numerr(num_err_expected, num_err_actual):
    if num_err_expected != None and num_err_expected != num_err_actual:
        raise TestException(f'Expected {num_err_expected} errors, received {num_err_actual}')
    return


def check_stderr(expected, actual):
    if expected != None and expected != actual:
        raise TestException(f'Expected err\n{expected}\nreceived\n{actual}')
    return


def check_result_set(expected, actual, verbose, is_sorted, consider_rounding_errors):
    sort = lambda l: l if is_sorted else sorted(l)
    expected_sorted, actual_sorted = sort(expected.split('\n')), sort(actual.split('\n'))

    def equal(expected, actual):
        if len(expected) != len(actual):
            return False

        if not consider_rounding_errors:
            return expected == actual

        # either the expected or actual result set was empty -> no need to consider rounding error
        if len(expected) == 1 and (not expected[0] or not actual[0]):
            return expected == actual

        for i in range(len(expected)):
            expected_tuple = expected[i].split(',')
            actual_tuple = actual[i].split(',')
            if len(expected_tuple) != len(actual_tuple):
                return False
            for j in range(len(expected_tuple)):
                expected_value = expected_tuple[j]
                actual_value = actual_tuple[j]
                if expected_value[0] != '"' and '.' in expected_value: # floating point -> consider rounding errors
                    if not math.isclose(float(expected_value), float(actual_value)):
                        return False
                else: # all other types -> use exact string comparison
                    if expected_value != actual_value:
                        return False
        return True

    if not equal(expected_sorted, actual_sorted):
        diff = ""
        if verbose:
            diff = '\n==>' + colordiff(actual, expected).rstrip('\n').replace('\n', '\n   ')
        raise TestException(f'Expected output differs.{diff}')

def check_stdout(expected, actual, verbose, is_sorted, consider_rounding_errors):
    # split into indiviudal result sets and filter out the line with the number of rows of the actual output
    def split(s) -> list():
        result_sets = list()
        res = ''
        for line in s.splitlines():
            if line.startswith("Result set for"):
                if res != '':
                    result_sets.append(res)
                res = line + '\n'
            elif re.match(r'^\d+ rows', line): # throw away the line with the number of rows "x rows"
                continue
            else:
                res += line + '\n'
        if res != '':
            result_sets.append(res)
        return result_sets
    # sort result sets including substring "Result set for <table>:"
    # in case the expected output is `None`, we have to check that the actual output is also empty
    actual_split = sorted(split(actual))
    expected_split = sorted(split(expected)) if expected != None else [''] * len(actual_split)

    if len(expected_split) != len(actual_split):
        raise TestException(f'Expected output differs in number of result sets.')
    for expected_result_set, actual_result_set in zip(expected_split, actual_split):
        # remove substring "Result set for <table>:\n" and trailing newline
        expected_result_set = re.sub(r'^Result set for .*:\n', '', expected_result_set).rstrip('\n')
        actual_result_set = re.sub(r'^Result set for .*:\n', '', actual_result_set).rstrip('\n')
        check_result_set(expected_result_set, actual_result_set, verbose, is_sorted, consider_rounding_errors)


#-----------------------------------------------------------------------------------------------------------------------
# REPORTING AND EXCEPTION HANDLING
#-----------------------------------------------------------------------------------------------------------------------

def report(message, stage_name, test_case, symbol) -> list[str]:
    res = list()
    if not test_case.file_reported:
        res.append(f'{test_case.filename}')
        test_case.file_reported = True
    res.append(f'└─ {stage_name} {symbol} {message}')
    return res


def print_debug_command(test_case, stage_name, command) -> str:
    query = test_case.query.replace('\\', '\\\\').replace('"', '\\"').replace('\n', ' ').strip()
    return f'     $ echo "{query}" > debug.sql; {" ".join(command)} debug.sql'


def report_failure(message, stage_name, test_case, debug, command=None) -> list[str]:
    symbol = Fore.RED + '✘' + Style.RESET_ALL
    res = report(message, stage_name, test_case, symbol)
    if debug and command:
        res.append(print_debug_command(test_case, stage_name, command))
    return res


def report_warning(message, stage_name, test_case) -> list[str]:
    symbol = Fore.YELLOW + '!' + Style.RESET_ALL
    return report(message, stage_name, test_case, symbol)


def report_success(message, stage_name, test_case, verbose) -> list[str]:
    if verbose or 'TERM' not in os.environ:
        symbol = Fore.GREEN + '✓' + Style.RESET_ALL
        return report(message, stage_name, test_case, symbol)
    else:
        return list()


def report_summary(stage_counter, stage_pass_counter, required_counter, required_pass_counter, bad_files_counter):
    # Define helper functions
    in_red   = lambda x: f'{Fore.RED}{x}{Style.RESET_ALL}'
    in_green = lambda x: f'{Fore.GREEN}{x}{Style.RESET_ALL}'
    in_bold  = lambda x: f'{Style.BRIGHT}{x}{Style.RESET_ALL}'
    keys = list(stage_counter.keys()) + ['Required', 'Total', 'Bad files']
    max_len = len(max(keys, key=len))
    results = [80 * '-']

    # Bad files
    if bad_files_counter > 0:
        results += [f'{"Bad files".ljust(max_len)}: {bad_files_counter}']
        results += [80 * '-']

    # Assemble stage info
    for stage in stage_counter.keys():
        passed = stage_pass_counter[stage]
        total = stage_counter[stage]
        passed = in_red(passed) if total > passed else in_green(passed)
        results += [f'{stage.ljust(max_len)}: {passed}/{total}']
    results += [80 * '-']

    # Assemble overall info
    passed = required_pass_counter
    required = required_counter
    passed = in_red(passed) if required > passed else in_green(passed)
    results += [f'{in_bold("Required".ljust(max_len))}: {passed}/{required}']
    passed = sum(stage_pass_counter.values())
    total = sum(stage_counter.values())
    passed = in_red(passed) if total > passed else in_green(passed)
    results += [f'{in_bold("Total".ljust(max_len))}: {passed}/{total}']

    # Print results
    print('\n'.join(results))


#-----------------------------------------------------------------------------------------------------------------------
# STAGE AND SETUP DEFINITIONS
#-----------------------------------------------------------------------------------------------------------------------

def lexer_command(test_case):
    binary = BINARIES['lex']
    command = [binary, '-']
    if test_case.cli_args:
        command.extend(test_case.cli_args.split())
    return [ command ]


def parser_command(test_case):
    binary = BINARIES['parse']
    command = [binary, '-']
    if test_case.cli_args:
        command.extend(test_case.cli_args.split())
    return [ command ]


def sema_command(test_case):
    binary = BINARIES['check']
    setup = os.path.join(os.path.dirname(test_case.filename), 'data', 'schema.sql')
    command = [binary, '--quiet', setup, '-']
    if test_case.cli_args:
        command.extend(test_case.cli_args.split())
    return [ command ]


def end2end_command(test_case):
    binary = BINARIES['shell']
    setup = os.path.join(os.path.dirname(test_case.filename), 'data', 'schema.sql')
    command = [binary, '--quiet', '--noprompt', setup, '-']
    if test_case.cli_args:
        command.extend(test_case.cli_args.split())
    configurations = list()
    data_layout_options = [[]] if '--data-layout' in command else enumerate_feature_options('data-layout')
    backend_options = [[]] if '--backend' in command else enumerate_feature_options('backend')
    table_property_options = (
        [[]]
        if '--table-properties' in command
        else enumerate_feature_options('table-properties', 'table-properties', True))
    for combination in itertools.product(data_layout_options, backend_options, table_property_options):
        configurations.append(list(itertools.chain.from_iterable(combination)))
    if 'join' in test_case.filename:
        configurations.extend(enumerate_feature_options('plan-enumerator'))
    commands = list()
    for cfg in configurations:
        commands.append(command + cfg)
    return commands


COMMAND = {
    'lexer': lexer_command,
    'parser': parser_command,
    'sema': sema_command,
    'end2end': end2end_command,
}

BINARIES = dict()


#-----------------------------------------------------------------------------------------------------------------------
# MAIN ROUTINE
#-----------------------------------------------------------------------------------------------------------------------

if __name__ == '__main__':
    # Pars args
    parser = argparse.ArgumentParser(description="""Run integration tests on mutable. Note that the
                                                    build direcory is assumed to be build/debug.""")
    parser.add_argument('path', nargs='*', help='Path to a directory containing tests or path to single test to be run.'
                                                'If not specified, all tests in the \'test\' directory will be considered.')
    parser.add_argument('-a', '--all', help='require optional tests to pass', action='store_true')
    parser.add_argument('-r', '--required-only', help='run only required tests', action='store_true')
    parser.add_argument('-v', '--verbose', help='increase output verbosity', action='store_true')
    parser.add_argument('-d', '--debug', help='print debug commands for failed test cases', action='store_true')
    parser.add_argument('-j', '--jobs', help='Run N jobs in parallel. Defaults to number of CPU threads - 1.',
                        default=(os.cpu_count() - 1), metavar='N', type=int)
    parser.add_argument('-b', '--builddir', help='Path to the build directory.  Defaults to \'build/debug\'.',
                        default=os.path.join('build', 'debug'), type=str, metavar='PATH')
    args = parser.parse_args()

    # Check if interactive terminal
    is_interactive = True if 'TERM' in os.environ else False

    # Locate build directory and binaries
    binaries_dir = os.path.join(args.builddir, 'bin')
    BINARIES['lex']   = os.path.join(binaries_dir, 'lex')
    BINARIES['parse'] = os.path.join(binaries_dir, 'parse')
    BINARIES['check'] = os.path.join(binaries_dir, 'check')
    BINARIES['shell'] = os.path.join(binaries_dir, 'shell')

    # Set up counters
    stage_counter = dict()
    stage_pass_counter = dict()
    required_counter = 0
    required_pass_counter = 0
    bad_files_counter = 0

    # Get test files
    test_files: list[str]
    if not args.path:
        test_files = sorted(glob.glob(os.path.join('test', '**', '[!_]*.yml'), recursive=True))
    else:
        test_files = []
        for path in sorted(set(args.path)):
            if os.path.isfile(path):  # path is an experiment file
                test_files.append(path)
            else:  # path is a directory containing multiple experiment files
                test_files.extend(glob.glob(os.path.join(path, '**', '[!_]*.yml'), recursive=True))

    test_files = sorted(list(set(test_files)))

    progress_bar = tqdm(total=len(test_files), position=0, ncols=80, leave=False, bar_format='|{bar}| {n}/{total}', disable=(not is_interactive))

    # Read all test files and create test data
    all_test_data = list()
    for test_file in test_files:
        test_case = TestCase()

        # Set up file report
        test_case.filename = test_file
        test_case.file_reported = False

        # Validate schema
        if not schema_valid(test_case):
            bad_files_counter += 1
            progress_bar.update(1)
            continue

        # Open test file
        with open(test_file, 'r') as f:
            yml_test_case = yaml.safe_load(f)

        # Import test case into global namespace
        test_case.description = yml_test_case['description']
        test_case.db = yml_test_case['db']
        test_case.query = yml_test_case['query']
        test_case.required = yml_test_case['required']
        test_case.stages = yml_test_case['stages']

        if args.required_only and not test_case.required:
            progress_bar.update(1)
            continue

        # Validate dataset location
        if not location_valid(test_case):
            bad_files_counter += 1
            progress_bar.update(1)
            continue

        all_test_data.append([test_case, args, BINARIES])

    # Start the process pool to run all test cases
    n_tests_left = len(all_test_data)
    process_pool = multiprocessing.Pool(args.jobs)
    jobs = process_pool.starmap_async(run_test_case, all_test_data)
    process_pool.close()

    # Every 5 seconds, look at how many jobs are left. Update `progress_bar` accordingly
    while True:
        if not jobs.ready():
            jobs_left = jobs._number_left
            if jobs_left < n_tests_left:
                progress_bar.update(n_tests_left - jobs_left)
                n_tests_left = jobs_left
            jobs.wait(5)
        else:
            break

    # When all jobs are finished, process their results
    for result in jobs.get():
        for stage, count in result.stage_counter.items():
            stage_counter[stage] = stage_counter.get(stage, 0) + count
        for stage, count in result.stage_pass_counter.items():
            stage_pass_counter[stage] = stage_pass_counter.get(stage, 0) + count
        required_counter += result.required_counter
        required_pass_counter += result.required_pass_counter
        if result.combined_reports:
            tqdm.write('\n'.join(result.combined_reports))

    # Close event log
    progress_bar.clear()
    progress_bar.close()

    # Log test results
    report_summary(stage_counter, stage_pass_counter, required_counter, required_pass_counter, bad_files_counter)

    if args.all:
        # All tests successful
        exit(sum(stage_counter.values()) > sum(stage_pass_counter.values()))
    else:
        # All required tests successful
        exit(required_counter > required_pass_counter)
