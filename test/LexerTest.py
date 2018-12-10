#!env python3


from termcolor import TermColor
from testexception import *
import glob
import os
import re
import subprocess
import sys
import termcolor


CWD                 = os.getcwd()
LEXER_BIN           = os.path.join(CWD, 'build_debug', 'bin', 'lex')
TEST_DIR            = os.path.join('test', 'lex')
POSITIVE_TEST_DIR   = os.path.join(TEST_DIR, 'positive')
SANITY_TEST_DIR     = os.path.join(TEST_DIR, 'sanity')
GLOB_POSITIVE       = os.path.join(POSITIVE_TEST_DIR, '**', '*.sql')
GLOB_SANITY         = os.path.join(SANITY_TEST_DIR, '**', '*.sql')


if __name__ == '__main__':
    n_tests = n_passed = 0

    # Positive tests
    print('Run positive tests')
    for sql_filename in sorted(glob.glob(GLOB_POSITIVE, recursive=True)):
        tok_filename = os.path.splitext(sql_filename)[0] + '.tok'
        print('` {}'.format(sql_filename), end='')

        n_tests += 1

        if not os.path.isfile(tok_filename):
            raise TestException('no token file for test case "{}"'.format(sql_filename))

        with open(sql_filename, 'r') as sql_file:
            process = subprocess.Popen([LEXER_BIN, '-'], stdin=sql_file, stdout=subprocess.PIPE, cwd=CWD)
            try:
                out, err = process.communicate(timeout=5) # wait 5 seconds
            except subprocess.TimeoutExpired:
                raise TestException('test case "{}" timed out'.format(sql_filename))

        if err:
            err = str(err, 'utf-8')
            raise TestException('test failed with errors:\n{}'.format(err))

        with open(tok_filename, 'r') as tok_file:
            out = str(out, 'utf-8').splitlines()
            tokens = tok_file.read().splitlines()

            def fail():
                if not fail.flag:
                    print(termcolor.err(' ✘'))
                    fail.flag = True
            fail.flag = False

            if len(out) < len(tokens):
                fail()
                print('    {}: actual output is shorter than expected'.format(termcolor.FAILURE))
            elif len(out) > len(tokens):
                fail()
                print('    {}: actual output is longer than expected'.format(termcolor.FAILURE))

            for actual, expected in zip(out, tokens):
                if actual != expected:
                    fail()
                    print('    {}: got {}, expected {}'.format(
                        termcolor.FAILURE,
                        termcolor.tc(actual, TermColor.BOLD, TermColor.BG_RED, TermColor.FG_WHITE),
                        termcolor.tc(expected, TermColor.BOLD, TermColor.BG_WHITE, TermColor.FG_BLACK)),
                        file=sys.stderr)

            if not fail.flag:
                print(termcolor.ok(' ✓'))

        n_passed += 1

    # Sanity tests
    print('\nRun sanity tests')
    for sql_file in sorted(glob.glob(GLOB_SANITY, recursive=True)):
        print('` {}'.format(sql_file))

    # Show summary
    print('\nPassed {} out of {} tests ({} tests failed).'.format(
        termcolor.tc(n_passed, TermColor.BOLD, TermColor.FG_GREEN),
        termcolor.tc(n_tests, TermColor.BOLD),
        termcolor.tc(n_tests - n_passed, TermColor.BOLD, TermColor.FG_RED)))
