#!env python3

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

bar_format = '{desc}: {n}/{total} ({percentage:3.0f}%)|{bar}|'

check_lexer = check_parser = check_sema = check_end2end = False
verbose = quiet = False

def list_to_str(lst):
    return ', '.join(lst)

def parse_components(components):
    global check_lexer
    global check_parser
    global check_sema
    global check_end2end

    if len(components)==0:
        check_lexer = check_parser = check_sema = check_end2end = True
    else:
        if 'lexer' in components:
            check_lexer = True
            components.remove('lexer')
        if 'parser' in components:
            check_parser = True
            components.remove('parser')
        if 'sema' in components:
            check_sema = True
            components.remove('sema')
        if 'end2end' in components:
            check_end2end = True
            components.remove('end2end')
        if len(components) > 0:
            raise Exception(f'Cannot parse command line argument(s): {components}')


def print_summary(component, n_tests, n_passed):
    if not quiet:
        component = component.ljust(30)
        print(f'{termcolor.tc(component, termcolor.TermColor.BOLD)}' + ':', end=' ')
        if n_passed < n_tests:
            print(f'Passed: {termcolor.err(n_passed)}/{termcolor.tc(n_tests, termcolor.TermColor.BOLD)}')
        else:
            print(f'Passed: {termcolor.ok(n_passed)}/{termcolor.tc(n_tests, termcolor.TermColor.BOLD)}')
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
# Lexer Tests
#-----------------------------------------------------------------------------------------------------------------------
LEXER_BIN           = os.path.join(CWD, 'build', 'debug', 'bin', 'lex')
LEXER_TEST_DIR      = os.path.join('test', 'lex')
LEXER_POSITIVE_DIR  = os.path.join(LEXER_TEST_DIR, 'positive')
LEXER_GLOB_POSITIVE = os.path.join(LEXER_POSITIVE_DIR, '**', '*.sql')
LEXER_SANITY_FILE   = os.path.join(LEXER_TEST_DIR, 'sanity.sql')

def lexer_case(sql_filename, is_positive):
    stmt = None
    if is_positive:
        with open(sql_filename, 'r') as sql_file:
            stmt = sql_file.read()
    else:
        stmt = sql_filename  # sanity test input is a string instead of a filename

    try:
        process = subprocess.Popen([LEXER_BIN, '-'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE , cwd=CWD)

        out, err = process.communicate(stmt.encode(), timeout=5)

        if is_positive:
            if err:
                raise TestException(f'test failed with error:\n {str(err, "utf-8")}')

            tok_filename = os.path.splitext(sql_filename)[0] + '.tok'
            with open(tok_filename, 'r') as tok_file:
                out = str(out, 'utf-8').splitlines()
                tokens = tok_file.read().splitlines()

                if len(out) < len(tokens):
                    raise TestException(f'actual output is shorter than expected: received: {len(out)}, expected: {len(tokens)}')
                elif len(out) > len(tokens):
                    raise TestException(f'actual output is longer than expected: received: {len(out)}, expected: {len(tokens)}')

                for actual, expected in zip(out, tokens):
                    if actual != expected:
                        c_actual, c_expected = colordiff(actual, expected)
                        raise TestException(f'token streams differ:\nreceived:\n{c_actual}\nexpected\n{c_expected}')
        else:
            if process.returncode != 1:
                raise TestException(f'unexpected return code {process.returncode}')
            if not err:
                raise TestException(f'expected an error message')

        return True, f'`{sql_filename} {termcolor.ok("✓")}'

    except subprocess.TimeoutExpired as ex:
        return False, f'`{sql_filename} {termcolor.err("✘")}\n {ex}'

    except TestException as e:
        return False, f'`{sql_filename} {termcolor.err("✘")}\n {str(e).strip()}'


def lexer_test():
    n_tests = n_passed = 0
    results = list()

    test_files = sorted(glob.glob(LEXER_GLOB_POSITIVE, recursive=True))

    for sql_filename in tqdm(test_files, desc='Lexer (positive)'.ljust(30), disable=quiet, bar_format=bar_format):

        success, err = lexer_case(sql_filename, True)
        n_tests += 1
        n_passed += 1 if success else 0
        results.append((success, err))

    with open(LEXER_SANITY_FILE, 'r') as sanity_file:
        num_lines = sum(1 for line in open(LEXER_SANITY_FILE, 'r'))
        for test_case in tqdm(sanity_file, total=num_lines, desc='Lexer (sanity)'.ljust(30), disable=quiet, bar_format=bar_format):

            success, err = lexer_case(test_case, False)
            n_tests += 1
            n_passed += 1 if success else 0
            results.append((success, err))

    return n_tests, n_passed, results


#-----------------------------------------------------------------------------------------------------------------------
# Parser Tests
#-----------------------------------------------------------------------------------------------------------------------
PARSER_BIN           = os.path.join(CWD, 'build', 'debug', 'bin', 'parse')
PARSER_TEST_DIR      = os.path.join('test', 'parse')
PARSER_POSITIVE_DIR  = os.path.join(PARSER_TEST_DIR, 'positive')
PARSER_GLOB_POSITIVE = os.path.join(PARSER_POSITIVE_DIR, '**', '*.sql')
PARSER_SANITY_DIR    = os.path.join(PARSER_TEST_DIR, 'sanity')
PARSER_GLOB_SANITY   = os.path.join(PARSER_SANITY_DIR, '**', '*.sql')

def parser_case(sql_filename, is_positive):
    stmts = None
    with open(sql_filename, 'r') as sql_file:
        stmts = re.split(r'\n\s*\n+', sql_file.read())

    n_cases = len(stmts)

    try:
        for stmt in stmts:

            if is_positive:
                # Parse the input statement and pretty print it.
                process = subprocess.Popen([PARSER_BIN, '-'], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                                stderr=subprocess.PIPE, cwd=CWD)

                pretty, err = process.communicate(stmt.encode('latin-1'), timeout=5) # wait 5 seconds
                if err:
                    raise TestException(f'test failed with error:\n {str(err, "utf-8")}')

                # Parse the input statement and print its AST.
                process = subprocess.Popen([PARSER_BIN, '--ast', '-'], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                           stderr=subprocess.PIPE, cwd=CWD)

                ast_original, err = process.communicate(stmt.encode('latin-1'), timeout=5) # wait 5 seconds
                if err:
                    raise TestException(f'test failed with error:\n{str(err, "utf-8")}')

                # Parse the pretty-printed statement and print its AST.
                process = subprocess.Popen([PARSER_BIN, '--ast', '-'], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                           stderr=subprocess.PIPE, cwd=CWD)

                ast_pretty, err = process.communicate(pretty, timeout=5) # wait 5 seconds
                if err:
                    raise TestException(f'test failed with error:\n{str(err, "utf-8")}')

                without_comments = re.sub(r'^--.*', '', stmt)
                in_original = re.sub(r'\s+', '', without_comments)
                in_pretty = re.sub(r'\s+', '', str(pretty, 'latin-1'))
                ast_original = re.sub(r' *\(.*\)', '', str(ast_original, 'latin-1'))
                ast_pretty =  re.sub(r' *\(.*\)', '', str(ast_pretty, 'latin-1'))

                if in_original != in_pretty:
                    actual, expected = colordiff(in_pretty, in_original)
                    raise TestException(f'statements differ:\nreceived:\n{actual}\nexpected:\n{expected}')

                if (ast_original != ast_pretty):
                    actual, expected = colordiff(ast_pretty, ast_original)
                    raise TestException(f'ASTs differ:\noriginal:\n{actual}\npretty:\n{expected}')

            else:
                # Parse the input statement.
                process = subprocess.Popen([PARSER_BIN, '-'], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                               stderr=subprocess.PIPE, cwd=CWD)

                out, err = process.communicate(stmt.encode('latin-1'), timeout=5) # wait 5 seconds

                if process.returncode != 1:
                    raise TestException(f'unexpected return code {process.returncode}')
                if not err:
                    raise TestException(f'expected an error message')

        return True, f'`{sql_filename} {termcolor.ok("✓")}'


    except subprocess.TimeoutExpired as ex:
        return False, f'`{sql_filename} ({n_cases} statements) {termcolor.err("✘")}\n {ex}'

    except TestException as e:
        return False, f'`{sql_filename} ({n_cases} statements) {termcolor.err("✘")}\n {str(e).strip()}'


def parser_test():
    n_tests = n_passed = 0
    results = list()

    test_files = sorted(glob.glob(PARSER_GLOB_POSITIVE, recursive=True))

    for sql_filename in tqdm(test_files, desc='Parser (positive)'.ljust(30), disable=quiet, bar_format=bar_format):

        success, err = parser_case(sql_filename, True)
        n_tests += 1
        n_passed += 1 if success else 0
        results.append((success, err))

    test_files = sorted(glob.glob(PARSER_GLOB_SANITY, recursive=True))

    for sql_filename in tqdm(test_files, desc='Parser (sanity)'.ljust(30), disable=quiet, bar_format=bar_format):

        success, err = parser_case(sql_filename, False)
        n_tests += 1
        n_passed += 1 if success else 0
        results.append((success, err))

    return n_tests, n_passed, results


#-----------------------------------------------------------------------------------------------------------------------
# Semantic Analysis Tests
#-----------------------------------------------------------------------------------------------------------------------
SEMA_BIN           = os.path.join(CWD, 'build', 'debug', 'bin', 'check')
SEMA_TEST_DIR      = os.path.join('test', 'sema')
SEMA_SETUP         = os.path.join(SEMA_TEST_DIR, 'setup.sql')
SEMA_POSITIVE_DIR  = os.path.join(SEMA_TEST_DIR, 'positive')
SEMA_GLOB_POSITIVE = os.path.join(SEMA_POSITIVE_DIR, '**', '*.sql')
SEMA_SANITY_DIR    = os.path.join(SEMA_TEST_DIR, 'sanity')
SEMA_GLOB_SANITY   = os.path.join(SEMA_SANITY_DIR, '**', '*.sql')

def sema_case(sql_filename, is_positive, sql_setup=None, verbose=False):
    stmt = None
    with open(sql_filename, 'r') as sql_file:
        stmt = sql_file.read()
    if sql_setup:
        stmt = sql_setup + stmt

    try:
        # Run the semantic analysis on the entire input file
        process = subprocess.Popen([SEMA_BIN, '-'], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE, cwd=CWD)

        out, err = process.communicate(stmt.encode('latin-1'), timeout=5) # wait 5 seconds

        if is_positive:
            if err:
                raise TestException(f'failed with unexpected error\n{str(err, "utf-8")}')
            if process.returncode != 0:
                raise TestException(f'failed with unexpected error code {process.returncode}')
        else:
            if not err:
                raise TestException('expected error message')
            if err and process.returncode != 1:
                raise TestException(f'failed wit unexpected error\n{str(err, "utf-8")}')
            if process.returncode != 1:
                raise TestException(f'failed with unexpected returncode {process.returncode}')

        return True, f'`{sql_filename} {termcolor.ok("✓")}'

    except subprocess.TimeoutExpired as ex:
        return False, f'`{sql_filename} {termcolor.err("✘")}\n {ex}'

    except TestException as e:
        return False, f'`{sql_filename} {termcolor.err("✘")}\n {str(e).strip()}'


def sema_test():
    n_tests = n_passed = 0
    results = list()

    with open(SEMA_SETUP, 'r') as setup_file:
        sql_setup = setup_file.read()

    test_files = sorted(glob.glob(SEMA_GLOB_POSITIVE, recursive=True))

    for sql_filename in tqdm(test_files, desc='Semantic analysis (positive)'.ljust(30), disable=quiet, bar_format=bar_format):

        success, err = sema_case(sql_filename, True, sql_setup)
        n_tests += 1
        n_passed += 1 if success else 0
        results.append((success, err))

    test_files = sorted(glob.glob(SEMA_GLOB_SANITY, recursive=True))

    for sql_filename in tqdm(test_files, desc='Semantic analysis (sanity)'.ljust(30), disable=quiet, bar_format=bar_format):

        success, err = sema_case(sql_filename, False, sql_setup)
        n_tests += 1
        n_passed += 1 if success else 0
        results.append((success, err))

    return n_tests, n_passed, results


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
        process = subprocess.Popen([SHELL_BIN, '--noprompt', E2E_SETUP, '-'],
                                   stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=CWD)

        out, err = process.communicate(stmt.encode('latin-1'), timeout=10) # wait 10 seconds

        if err:
            raise TestException(f'failed with error {str(err, "utf-8")}')

        expected = None
        if not os.path.isfile(csv_filename):
            raise TestException(f'result file {csv_filename} does not exist')
        with open(csv_filename, 'r') as csv_file:
            expected = csv_file.read().strip()

        actual = '\n'.join(str(out, 'latin-1').splitlines()[9:])

        if actual != expected:
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

    for sql_filename in tqdm(test_files, desc='End to end'.ljust(30), disable=quiet, bar_format=bar_format):
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
    # parser.add_argument('lexer',   type=bool, help='run lexer tests')
    # parser.add_argument('parser',  type=bool, help='run parser tests')
    # parser.add_argument('sema',    type=bool, help='run semantic analysis tests')
    # parser.add_argument('end2end', type=bool, help='run end to end tests')
    parser.add_argument('component', nargs='*', help=f'a component to be tested, available options: {list_to_str(component_keywords)}.')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-v', '--verbose', help='increase output verbosity', action='store_true')
    group.add_argument('-q', '--quiet',   help='disable output, failure indicated by returncode', action='store_true')
    args = parser.parse_args()

    parse_components(args.component)

    verbose = args.verbose
    quiet = args.quiet
    failed = False

    total_tests = total_passed = 0

    # Lexer tests
    if (check_lexer):
        n_tests, n_passed, results = lexer_test()
        failed = failed or n_tests > n_passed

        total_tests += n_tests
        total_passed += n_passed

        print_results(results)
        print_summary('Lexer', n_tests, n_passed)

    # Parser tests
    if (check_parser):
        n_tests, n_passed, results = parser_test()
        failed = failed or n_tests > n_passed

        total_tests += n_tests
        total_passed += n_passed

        print_results(results)
        print_summary('Parser', n_tests, n_passed)

    # Sema tests
    if (check_sema):
        n_tests, n_passed, results = sema_test()
        failed = failed or n_tests > n_passed

        total_tests += n_tests
        total_passed += n_passed

        print_results(results)
        print_summary('Semantic analysis', n_tests, n_passed)

    # End2End tests
    if (check_end2end):
        n_tests, n_passed, results = end2end_test()
        failed = failed or n_tests > n_passed

        total_tests += n_tests
        total_passed += n_passed

        print_results(results)
        print_summary('End to end', n_tests, n_passed)

    # Summary
    terminal_cols, _ = shutil.get_terminal_size()
    print_summary(f'{terminal_cols*"-"}\nTotal', total_tests, total_passed)

    exit(0 if not failed else 1)
