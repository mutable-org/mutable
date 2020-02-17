#!/usr/bin/env python3

import argparse
import glob
import os
import re
import subprocess
import sys
import termcolor
import shutil

from testexception import *
from testutil import *
from tqdm import tqdm

#-----------------------------------------------------------------------------------------------------------------------
# Helpers
#-----------------------------------------------------------------------------------------------------------------------
CWD = os.getcwd()

component_keywords = ['lexer', 'parser', 'sema', 'end2end']

# bar_format = '{desc}: {n}/{total} ({percentage:3.0f}%)|{bar}|'
bar_format = '    |{bar:16}| {n}/{total}'

verbose = quiet = False

def list_to_str(lst):
    return ', '.join(lst)

def print_testcase(test_case):
    if not quiet:
        print(f'{test_case}:')

def print_summary(component, n_tests, n_passed):
    if not quiet:
        print(f'{termcolor.tc(component, termcolor.TermColor.BOLD)}: ', end='')
        if n_passed < n_tests:
            print(f'{termcolor.err(n_passed)}/{termcolor.tc(n_tests, termcolor.TermColor.BOLD)}')
        else:
            print(f'{termcolor.ok(n_passed)}/{termcolor.tc(n_tests, termcolor.TermColor.BOLD)}')
        # terminal_cols, _ = shutil.get_terminal_size()
        terminal_cols = 60
        print(f'{terminal_cols*"-"}')
        # else:
        #     print(f'Passed: {termcolor.ok(n_passed)}/{termcolor.tc(n_tests, termcolor.TermColor.BOLD)}', end=' ')
        # n_failed = n_tests - n_passed
        # if n_failed > 0:
        #     print(f'Failed: {termcolor.err(n_failed)}/{termcolor.tc(n_tests, termcolor.TermColor.BOLD)}')
        # else:
        #     print(f'Failed: {n_failed}/{termcolor.tc(n_tests, termcolor.TermColor.BOLD)}')


def print_results(results):
    if not quiet:
        for success, err in results:
            if not success or verbose:
                print(err)


#-----------------------------------------------------------------------------------------------------------------------
# End to End Tests
#-----------------------------------------------------------------------------------------------------------------------
CWD               = os.getcwd()
SHELL_BIN         = os.path.join(CWD, 'build', 'debug', 'bin', 'shell')
E2E_TEST_DIR      = os.path.join('test', 'end2end')
E2E_SETUP         = os.path.join(E2E_TEST_DIR, 'setup_ours_mutable.sql')
E2E_POSITIVE_DIR  = os.path.join(E2E_TEST_DIR, 'positive', 'ours')
E2E_GLOB_POSITIVE = os.path.join(E2E_POSITIVE_DIR, '**', '*.sql')

def end2end_case(sql_filename, csv_filename):
    stmt = None
    with open(sql_filename, 'r') as sql_file:
        stmt = sql_file.read()

    try:
        # Parse the input statement and pretty print it.
        process = subprocess.Popen([SHELL_BIN, '--quiet', '--noprompt', '--wasm', E2E_SETUP, '-'],
                                   stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=CWD)

        out, err = process.communicate(stmt.encode('latin-1'), timeout=10) # wait 10 seconds

        if err:
            raise TestException(f'failed with error {str(err, "utf-8")}')

        expected = None
        if not os.path.isfile(csv_filename):
            raise TestException(f'result file {csv_filename} does not exist')
        with open(csv_filename, 'r') as csv_file:
            expected = sorted(csv_file.read().strip().splitlines())

        actual = sorted(str(out, 'latin-1').splitlines())

        if actual != expected:
            print(f'\nDiff for \'{sql_filename}\':')
            for line in difflib.unified_diff(actual, expected, fromfile='actual', tofile='expected', lineterm='', n=0):
                print(line)
            print()
            raise TestException(f'failed due to different results')

        return True, f'`{sql_filename} {termcolor.ok("✓")}'

    except TestException as e:
        return False, f'`{sql_filename} {termcolor.err("✘")}\n {str(e).strip()}'
        # print('{}'.format(colordiff_simple(actual,expected)))

    except subprocess.TimeoutExpired as ex:
        return False, f'`{sql_filename} {termcolor.err("✘")}\n  {ex}'


def end2end_test():
    n_tests = n_passed = 0
    results = list()

    test_files = sorted(glob.glob(E2E_GLOB_POSITIVE, recursive=True))

    print_testcase('End to end')
    for sql_filename in tqdm(test_files, desc='End to end', disable=quiet, bar_format=bar_format):
        csv_filename = os.path.splitext(sql_filename)[0] + '.csv'

        success, err = end2end_case(sql_filename, csv_filename)
        n_tests += 1
        n_passed += 1 if success else 0
        results.append((success, err))

    return n_tests, n_passed, results


#-----------------------------------------------------------------------------------------------------------------------
# Main
#-----------------------------------------------------------------------------------------------------------------------
if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="""Run integration tests on mutable. Note that the
                                                    build direcory is assumed to be build/debug.""")
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-v', '--verbose', help='increase output verbosity', action='store_true')
    group.add_argument('-q', '--quiet',   help='disable output, failure indicated by returncode', action='store_true')
    args = parser.parse_args()

    verbose = args.verbose
    quiet = args.quiet
    failed = False

    total_tests = total_passed = 0

    # End2End tests
    n_tests, n_passed, results = end2end_test()
    failed = failed or n_tests > n_passed

    total_tests += n_tests
    total_passed += n_passed

    print_results(results)
    print_summary('End to end (WASM)', n_tests, n_passed)

    exit(0 if not failed else 1)
