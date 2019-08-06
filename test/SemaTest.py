#!env python3


from termcolor import TermColor
from testexception import *
from testutil import *
import glob
import os
import re
import subprocess
import sys
import termcolor


CWD                 = os.getcwd()
SEMA_BIN            = os.path.join(CWD, 'build_debug', 'bin', 'check')
TEST_DIR            = os.path.join('test', 'sema')
SEMA_SETUP          = os.path.join(TEST_DIR, 'setup.sql')
POSITIVE_TEST_DIR   = os.path.join(TEST_DIR, 'positive')
GLOB_POSITIVE       = os.path.join(POSITIVE_TEST_DIR, '**', '*.sql')
SANITY_TEST_DIR     = os.path.join(TEST_DIR, 'sanity')
GLOB_SANITY         = os.path.join(SANITY_TEST_DIR, '**', '*.sql')


def run(filename, is_positive, setup=None):
    # Read input file
    with open(filename, 'r') as f:
        sql = f.read()

    # Check there is input
    if not sql or type(sql) != str:
        print(' {}  -->  no valid input'.format(termcolor.err('✘')))
        return False # empty file

    # If we have setup script, prepend it
    if setup:
        sql = str(setup) + sql

    # Run the semantic analysis on the entire input file
    process = subprocess.Popen([SEMA_BIN, '-'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=CWD)
    try:
        pretty, err = process.communicate(input=sql.encode('latin-1'), timeout=5) # wait 5 seconds
    except subprocess.TimeoutExpired:
        print(' {}  -->  timed out'.format(termcolor.err('✘')))
        return False
    else:
        if is_positive:
            if err:
                print(' {}  -->  unexpected error:\n{}'.format(termcolor.err('✘'), str(err, 'utf-8')))
                return False
            if process.returncode != 0:
                print(' {}  -->  unexpected return code {}'.format(termcolor.err('✘'), process.returncode))
                return False
            print(termcolor.ok(' ✓'))
            return True
        else:
            if not err:
                print(' {}  -->  expected error message'.format(termcolor.err('✘')))
                return False
            if err and process.returncode != 1:
                print(' {}  -->  unexpected error:\n{}'.format(termcolor.err('✘'), str(err, 'utf-8')))
                return False
            if process.returncode != 1:
                print(' {}  -->  unexpected return code {}'.format(termcolor.err('✘'), process.returncode))
                return False
            print(termcolor.ok(' ✓'))
            return True

if __name__ == '__main__':
    n_tests = n_passed = 0

    with open(SEMA_SETUP, 'r') as s:
        setup = s.read()

    # Positive tests
    print('Run positive tests')
    for sql_filename in sorted(glob.glob(GLOB_POSITIVE, recursive=True)):
        print('` {}'.format(sql_filename), end='')
        n_tests += 1
        if run(sql_filename, True, setup):
            n_passed += 1

    # Sanity tests
    print('\nRun sanity tests')
    for sql_filename in sorted(glob.glob(GLOB_SANITY, recursive=True)):
        print('` {}'.format(sql_filename), end='')
        n_tests += 1
        if run(sql_filename, False, setup):
            n_passed += 1

    # Show summary
    print('\nPassed {} out of {} tests ({} tests failed).'.format(
        termcolor.ok(n_passed),
        termcolor.tc(n_tests, TermColor.BOLD),
        termcolor.err(n_tests - n_passed)))

    exit(0 if n_passed == n_tests else 1)
