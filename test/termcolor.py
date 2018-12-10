#!env python3
# ANSII Terminal Color module

import enum


class TermColorException(Exception):
    def __init__(str):
        Exception(str)

class TermColor(enum.Enum):
    def __str__(self):
        return str(self.value)

    NONE            = 0
    BOLD            = 1
    FAINT           = 2
    UNDERLINE       = 4

    FG_BLACK        = 30
    FG_RED          = 31
    FG_GREEN        = 32
    FG_YELLOW       = 33
    FG_BLUE         = 34
    FG_MAGENTA      = 35
    FG_CYAN         = 36
    FG_WHITE        = 37

    BG_BLACK        = 40
    BG_RED          = 41
    BG_GREEN        = 42
    BG_YELLOW       = 43
    BG_BLUE         = 44
    BG_MAGENTA      = 45
    BG_CYAN         = 46
    BG_WHITE        = 47

    @staticmethod
    def show():
        for e in TermColor:
            print('{} ({})'.format(tc(e.name, e), e.value))


def tc(obj, *args):
    opts = list()
    for arg in args:
        if not type(arg) is TermColor:
            raise TermColorException("'" + repr(arg) + "' is not a TermColor")
        opts.append(str(arg))
    return '\033[{}m{}\033[0m'.format(';'.join(opts), str(obj))

def bold(obj):
    return tc(obj, TermColor.BOLD)

def err(obj):
    return tc(obj, TermColor.FG_RED, TermColor.BOLD)

def ok(obj):
    return tc(obj, TermColor.BOLD, TermColor.FG_GREEN)

FAILURE = err('FAILURE')
ERROR   = err('ERROR')


if __name__ == '__main__':
    TermColor.show()
