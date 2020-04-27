#!/usr/bin/env python3

import subprocess
import os
import glob
import argparse
import difflib

import yaml
import yamale
from tqdm import tqdm
from colorama import Fore, Back, Style


#-----------------------------------------------------------------------------------------------------------------------
# HELPERS
#-----------------------------------------------------------------------------------------------------------------------

yml_schema = os.path.join(os.getcwd(), 'test', '_schema.yml')

def schema_valid(test_case) -> bool:
    schema = yamale.make_schema(yml_schema)
    data = yamale.make_data(test_case.filename)
    try:
        yamale.validate(schema, data)
    except ValueError:
        report_warning('YAML schema invalid', 'yaml_check', test_case)
        return False
    return True


def location_valid(test_case) -> bool:
    subdir = os.path.basename(os.path.dirname(test_case.filename))
    if subdir != test_case.db:
        report_warning('File is placed in incorrect directory', 'location_check', test_case)
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


#-----------------------------------------------------------------------------------------------------------------------
# TEST EXECUTION
#-----------------------------------------------------------------------------------------------------------------------

def run_command(command, query):
    try:
        process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   cwd=os.getcwd())
        out, err = process.communicate(query.encode('latin-1'), timeout=5)
        return process.returncode, out, err
    except subprocess.TimeoutExpired as ex:
        raise TestException(f'Timeout expired')


def run_stage(args, test_case, stage_name, command):
    stage = test_case.stages[stage_name]
    try:
        returncode, out, err = run_command(command, test_case.query)
        out = out.decode('UTF-8')
        err = err.decode('UTF-8')
        num_err = err.count('error')

        if 'returncode' in stage:
            check_returncode(stage['returncode'], returncode)
        if 'num_err' in stage:
            check_numerr(stage['num_err'], num_err)
        if 'err' in stage:
            check_stderr(stage['err'], err)
        if 'out' in stage:
            check_stdout(stage['out'], out, args.verbose)
    except TestException as ex:
        report_failure(str(ex), stage_name, test_case, args.debug)
        return False
    report_success(stage_name, test_case, args.verbose)
    return True


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


def check_stdout(expected, actual, verbose):
    if expected != None:
        expected_sorted = sorted(expected.split('\n'))
        actual_sorted = sorted(actual.split('\n'))
        if expected_sorted != actual_sorted:
            diff = ""
            if verbose:
                diff = '\n==>' + colordiff(actual, expected).rstrip('\n').replace('\n', '\n   ')
            raise TestException(f'Expected output differs.{diff}')
    return


#-----------------------------------------------------------------------------------------------------------------------
# REPORTING AND EXCEPTION HANDLING
#-----------------------------------------------------------------------------------------------------------------------

def report(message, stage_name, test_case, symbol):
    if not test_case.file_reported:
        tqdm.write(f'{test_case.filename}')
        test_case.file_reported = True
    tqdm.write(f'└─ {stage_name} {symbol} {message}')


def print_debug_command(test_case, stage_name):
    query = test_case.query.replace('"', '\\"').strip()
    tqdm.write(f'   echo "{query}" | {" ".join(COMMAND[stage_name](test_case))}')


def report_failure(message, stage_name, test_case, debug):
    symbol = Fore.RED + '✘' + Style.RESET_ALL
    report(message, stage_name, test_case, symbol)
    if debug:
        print_debug_command(test_case, stage_name)


def report_warning(message, stage_name, test_case):
    symbol = Fore.YELLOW + '!' + Style.RESET_ALL
    report(message, stage_name, test_case, symbol)


def report_success(stage_name, test_case, verbose):
    if verbose:
        symbol = Fore.GREEN + '✓' + Style.RESET_ALL
        report('', stage_name, test_case, symbol)


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
    return command


def parser_command(test_case):
    binary = BINARIES['parse']
    command = [binary, '-']
    return command


def sema_command(test_case):
    binary = BINARIES['check']
    setup = os.path.join(os.path.dirname(test_case.filename), 'data', 'schema.sql')
    command = [binary, '--quiet', setup, '-']
    return command


def end2end_command(test_case):
    binary = BINARIES['shell']
    setup = os.path.join(os.path.dirname(test_case.filename), 'data', 'schema.sql')
    command = [binary, '--quiet', '--noprompt', setup, '-']
    return command


COMMAND = {
    'lexer': lexer_command,
    'parser': parser_command,
    'sema': sema_command,
    'end2end': end2end_command,
}

BINARIES_DIR = os.path.join(os.getcwd(), 'build', 'debug', 'bin')
BINARIES = {
    'lex': os.path.join(BINARIES_DIR, 'lex'),
    'parse': os.path.join(BINARIES_DIR, 'parse'),
    'check': os.path.join(BINARIES_DIR, 'check'),
    'shell': os.path.join(BINARIES_DIR, 'shell'),
}


#-----------------------------------------------------------------------------------------------------------------------
# MAIN ROUTINE
#-----------------------------------------------------------------------------------------------------------------------

if __name__ == '__main__':
    # Pars args
    parser = argparse.ArgumentParser(description="""Run integration tests on mutable. Note that the
                                                    build direcory is assumed to be build/debug.""")
    parser.add_argument('-a', '--all', help='require optional tests to pass', action='store_true')
    parser.add_argument('-v', '--verbose', help='increase output verbosity', action='store_true')
    parser.add_argument('-d', '--debug', help='print debug commands for failed test cases', action='store_true')
    args = parser.parse_args()

    # Check if interactive terminal
    is_interactive = True if 'TERM' in os.environ else False

    # Set up counters
    stage_counter = dict()
    stage_pass_counter = dict()
    required_counter = 0
    required_pass_counter = 0
    bad_files_counter = 0

    # Glob test files
    TEST_GLOB = os.path.join('test', '**', '[!_]*.yml')
    test_files = sorted(glob.glob(TEST_GLOB, recursive=True))

    # Create dummy test case class
    class TestCase: pass
    test_case = TestCase()

    # Set up event log
    log = tqdm(total=0, position=1, ncols=80, leave=False, bar_format='{desc}', disable=(not is_interactive))

    for test_file in tqdm(test_files, position=0, ncols=80, leave=False,
            bar_format='|{bar}| {n}/{total}', disable=(not is_interactive)):
        # Log current test file
        log.set_description_str(f'Current file: {test_file}'.ljust(80))

        # Set up file report
        test_case.filename = test_file
        test_case.file_reported = False

        # Validate schema
        if not schema_valid(test_case):
            bad_files_counter += 1
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

        # Validate dataset location
        if not location_valid(test_case):
            bad_files_counter += 1
            continue

        # Execute test stages
        success = True
        for stage in test_case.stages:
            # Execute test
            if success:
                success = run_stage(args, test_case, stage, COMMAND[stage](test_case))
            else:
                report_failure("earlier stage failed", stage, test_case, args.debug)
            # Store results
            stage_counter[stage] = stage_counter.get(stage, 0) + 1
            stage_pass_counter[stage] = stage_pass_counter.get(stage, 0) + success
            if test_case.required:
                required_counter += 1
                required_pass_counter += success

    # Close event log
    log.clear()
    log.close()

    # Log test results
    report_summary(stage_counter, stage_pass_counter, required_counter, required_pass_counter, bad_files_counter)

    if args.all:
        # All tests successful
        exit(sum(stage_counter.values()) > sum(stage_pass_counter.values()))
    else:
        # All required tests successful
        exit(required_counter > required_pass_counter)
