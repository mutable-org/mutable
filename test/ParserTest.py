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
PARSER_BIN          = os.path.join(CWD, 'build', 'debug', 'bin', 'parse')
TEST_DIR            = os.path.join('test', 'parse')
POSITIVE_TEST_DIR   = os.path.join(TEST_DIR, 'positive')
GLOB_POSITIVE       = os.path.join(POSITIVE_TEST_DIR, '**', '*.sql')
SANITY_TEST_DIR     = os.path.join(TEST_DIR, 'sanity')
GLOB_SANITY         = os.path.join(SANITY_TEST_DIR, '**', '*.sql')


if __name__ == '__main__':
    n_tests = n_passed = 0

    # Positive tests
    print('Run positive tests')
    for sql_filename in sorted(glob.glob(GLOB_POSITIVE, recursive=True)):
        print('` {}'.format(sql_filename), end='')
        with open(sql_filename, 'r') as sql_file:
            statements = re.split(r'\n\s*\n+', sql_file.read())
        n_tests += len(statements)
        print(' ({} statements)'.format(len(statements)), end='')
        def fail():
            if not fail.flag:
                print(termcolor.err(' ✘'))
                fail.flag = True
        fail.flag = False

        try:
            for s in statements:
                # Parse the input statement and pretty print it.
                process = subprocess.Popen([PARSER_BIN, '-'], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                           stderr=subprocess.PIPE, cwd=CWD)
                try:
                    pretty, err = process.communicate(s.encode('latin-1'), timeout=5) # wait 5 seconds
                except subprocess.TimeoutExpired:
                    raise TestException('test case timed out:\n{}'.format(s))
                if err:
                    raise TestException('test failed with error:\n{}'.format(str(err, 'utf-8')))

                # Parse the input statement and print its AST.
                process = subprocess.Popen([PARSER_BIN, '--ast', '-'], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                           stderr=subprocess.PIPE, cwd=CWD)
                try:
                    ast_original, err = process.communicate(s.encode('latin-1'), timeout=5) # wait 5 seconds
                except subprocess.TimeoutExpired:
                    raise TestException('test case timed out:\n{}'.format(s))
                if err:
                    raise TestException('test failed with error:\n{}'.format(str(err, 'utf-8')))

                # Parse the pretty-printed statement and print its AST.
                process = subprocess.Popen([PARSER_BIN, '--ast', '-'], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                           stderr=subprocess.PIPE, cwd=CWD)
                try:
                    ast_pretty, err = process.communicate(pretty, timeout=5) # wait 5 seconds
                except subprocess.TimeoutExpired:
                    raise TestException('test case timed out:\n{}'.format(s))
                if err:
                    raise TestException('test failed with error:\n{}'.format(str(err, 'utf-8')))

                without_comments = re.sub(r'^--.*', '', s)
                in_original = re.sub(r'\s+', '', without_comments)
                in_pretty = re.sub(r'\s+', '', str(pretty, 'latin-1'))
                ast_original = re.sub(r' *\(.*\)', '', str(ast_original, 'latin-1'))
                ast_pretty =  re.sub(r' *\(.*\)', '', str(ast_pretty, 'latin-1'))

                if in_original != in_pretty:
                    fail()
                    actual, expected = colordiff(in_pretty, in_original)
                    print('    {}: statements differ: got {}, expected {}'.format(termcolor.FAILURE, actual, expected))

                if (ast_original != ast_pretty):
                    fail()
                    actual, expected = colordiff(ast_pretty, ast_original)
                    print('    {}: ASTs differ\nOriginal:\n{}\nPretty:{}'.format(termcolor.FAILURE, actual, expected))
                else:
                    n_passed += 1
        except TestException as e:
            print(' {}  -->  {}'.format(termcolor.err('✘'), e))
        else:
            if not fail.flag:
                print(termcolor.ok(' ✓'))

    # Sanity tests
    print('\nRun sanity tests')
    for sql_filename in sorted(glob.glob(GLOB_SANITY, recursive=True)):
        print('` {}'.format(sql_filename), end='')
        with open(sql_filename, 'r') as sql_file:
            statements = re.split(r'\n\n+', sql_file.read())
        n_tests += len(statements)
        print(' ({} statements)'.format(len(statements)), end='')

        try:
            for s in statements:
                # Parse the input statement.
                process = subprocess.Popen([PARSER_BIN, '-'], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                           stderr=subprocess.PIPE, cwd=CWD)
                try:
                    out, err = process.communicate(s.encode('latin-1'), timeout=5) # wait 5 seconds
                except subprocess.TimeoutExpired:
                    raise TestException('test case timed out:\n{}'.format(s))

                if process.returncode != 1:
                    raise TestException('unexpected return code {}'.format(process.returncode))
                if not err:
                    raise TestException('expected an error message')
                n_passed += 1
        except TestException as e:
            print(' {}\n    "{}"  -->  {}'.format(termcolor.err('✘'), s.strip(), e), end='')
        else:
            print(termcolor.ok(' ✓'))

    # Show summary
    print('\nPassed {} out of {} tests ({} tests failed).'.format(
        termcolor.ok(n_passed),
        termcolor.tc(n_tests, TermColor.BOLD),
        termcolor.err(n_tests - n_passed)))

    exit(0 if n_passed == n_tests else 1)
