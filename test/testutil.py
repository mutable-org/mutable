from termcolor import TermColor
import difflib
import termcolor

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

def colordiff_simple(actual, expected):
    diff = ''
    for delta in difflib.ndiff(actual, expected):
        if delta.startswith('+'):
            diff += termcolor.tc(delta[-1], TermColor.FG_GREEN)
        elif delta.startswith('-'):
            diff += termcolor.tc(delta[-1], TermColor.FG_RED)
        elif delta.startswith('^'):
            diff += termcolor.tc(delta[-1], TermColor.FG_BLUE)
        else:
            diff += termcolor.tc(delta[-1])
    return diff
