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
SHELL_BIN           = os.path.join(CWD, 'build', 'debug', 'bin', 'shell')
TEST_DIR            = os.path.join('test', 'end2end')
SETUP_SQL           = os.path.join(TEST_DIR, 'setup_ours_mutable.sql')
POSITIVE_TEST_DIR   = os.path.join(TEST_DIR, 'positive', 'ours')
GLOB_POSITIVE       = os.path.join(POSITIVE_TEST_DIR, '**', '*.sql')


if __name__ == '__main__':
    n_tests = n_passed = 0

    # Positive tests
    print('Run positive tests')
    for sql_filename in sorted(glob.glob(GLOB_POSITIVE, recursive=True)):
        print('` {}'.format(sql_filename), end='')
        csv_filename = os.path.splitext(sql_filename)[0] + '.csv'
        stmt = None
        expected = None
        actual = None
        with open(sql_filename, 'r') as sql_file:
            stmt = sql_file.read()
        with open(csv_filename, 'r') as csv_file:
            expected = csv_file.read().strip()
        n_tests += 1
        def fail():
            if not fail.flag:
                print(termcolor.err(' ✘'))
                fail.flag = True
        fail.flag = False

        try:
            # Parse the input statement and pretty print it.
            process = subprocess.Popen([SHELL_BIN, '--noprompt', SETUP_SQL, '-'],
                                       stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=CWD)
            try:
                out, err = process.communicate(stmt.encode('latin-1'), timeout=10) # wait 10 seconds
                # process.stdin.close()
            except subprocess.TimeoutExpired as ex:
                raise TestException('test case timed out: {}'.format(ex))
            if err:
                raise TestException('test failed with error:\n{}'.format(str(err, 'utf-8')))

            actual = '\n'.join(str(out, 'latin-1').splitlines()[8:])

            if actual != expected:
                raise TestException('test failed due to different results')
            else:
                n_passed += 1
        except TestException as e:
            print(' {}\n  -->  {}'.format(termcolor.err('✘'), e))
            # print('{}'.format(colordiff_simple(actual,expected)))
        else:
            if not fail.flag:
                print(termcolor.ok(' ✓'))

    # Show summary
    print('\nPassed {} out of {} tests ({} tests failed).'.format(
        termcolor.ok(n_passed),
        termcolor.tc(n_tests, TermColor.BOLD),
        termcolor.err(n_tests - n_passed)))

    exit(0 if n_passed == n_tests else 1)
