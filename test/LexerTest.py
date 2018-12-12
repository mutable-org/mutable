#!env python3


from termcolor import TermColor
from testexception import *
import difflib
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
GLOB_POSITIVE       = os.path.join(POSITIVE_TEST_DIR, '**', '*.sql')
SANITY_FILENAME     = os.path.join(TEST_DIR, 'sanity.sql')


def colordiff(actual, expected):
    c_actual = c_expected = ''
    for delta in difflib.ndiff(actual, expected):
        if delta[0] == '+':
                c_expected += termcolor.tc(delta[-1], TermColor.BOLD, TermColor.BG_BLUE, TermColor.FG_WHITE)
        elif delta[0] == '-':
                c_actual += termcolor.tc(delta[-1], TermColor.BOLD, TermColor.BG_RED, TermColor.FG_WHITE)
        else:
            char = termcolor.tc(delta[-1], TermColor.BOLD, TermColor.BG_WHITE, TermColor.FG_BLACK)
            c_actual += char
            c_expected += char
    return c_actual, c_expected

if __name__ == '__main__':
    n_tests = n_passed = 0

    # Positive tests
    print('Run positive tests')
    for sql_filename in sorted(glob.glob(GLOB_POSITIVE, recursive=True)):
        try:
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
                        c_actual, c_expected = colordiff(actual, expected)
                        print('    {}: got {}, expected {}'.format(
                            termcolor.FAILURE,
                            c_actual,
                            c_expected),
                            file=sys.stderr)

                if not fail.flag:
                    n_passed += 1
                    print(termcolor.ok(' ✓'))
        except TestException as e:
            print(' {}  -->  {}'.format(termcolor.err('✘'), e))

    # Sanity tests
    print('\nRun sanity tests')
    with open(SANITY_FILENAME, 'r') as sanity_file:
        for line in sanity_file:
            n_tests += 1
            try:
                print('` input "{}"'.format(line.strip()), end='')

                process = subprocess.Popen([LEXER_BIN, '-'], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE , cwd=CWD)
                try:
                    out, err = process.communicate(line.encode(), timeout=5) # wait 5 seconds
                except subprocess.TimeoutExpired:
                    raise TestException('sanity test timed out with input "{}"'.format(line))

                if process.returncode != 1:
                    raise TestException('unexpected return code {}'.format(process.returncode))

                if not err:
                    raise TestException('expected an error message')
            except TestException as e:
                print(' {}  -->  {}'.format(termcolor.err('✘'), e))
            else:
                n_passed += 1
                print(termcolor.ok(' ✓'))



    # Show summary
    print('\nPassed {} out of {} tests ({} tests failed).'.format(
        termcolor.ok(n_passed),
        termcolor.tc(n_tests, TermColor.BOLD),
        termcolor.err(n_tests - n_passed)))

    exit(0 if n_passed == n_tests else 1)
