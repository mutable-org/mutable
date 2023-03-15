#!env python3

import os
import subprocess
import sys

LEX_BINARY = os.path.join(os.path.dirname(sys.argv[0]), '../build/release-static/bin/lex')
FOLLOW_SET_TABLE = os.path.join(os.path.dirname(sys.argv[0]), '../src/tables/FollowSet.tbl')


def codeGenerator(file):
    in_block = False
    for l in file:
        if l.startswith('```'):
            in_block = not in_block
        elif in_block:
            yield l
    return


def tokenGenerator(code):
    for l in code:
        for tk in l.split():
            yield tk
    return


def parse(stream):
    tk = ''
    rules = dict()
    lhs = ''
    rhs = []

    def nextTK():
        nonlocal tk
        try:
            tk = next(stream)
        except StopIteration as e:
            raise e

    def expectTK(c: str):
        if tk == c:
            try:
                nextTK()
            except StopIteration as e:
                raise e
        else:
            print(f"Warning: expected token \"{c}\" but received token \"{tk}\".")

    def parseDerivation():
        expr = []
        inner_expr = []  # used to handle `|`-Operator
        while True:
            if tk == '[':
                nextTK()
                inner_expr.append(('OPTIONAL', parseDerivation()))
                expectTK(']')
            elif tk == '{':
                nextTK()
                inner_expr.append(('REPETITION', parseDerivation()))
                expectTK('}')
            elif tk == '(':
                nextTK()
                inner_expr.append(('GROUPING', parseDerivation()))
                expectTK(')')
            elif tk == '(*':
                while not tk == '*)':
                    nextTK()
                nextTK()
            elif tk == '|':
                expr.append(inner_expr)
                inner_expr = []
                nextTK()
            elif tk[0] == "'":
                # terminal
                if tk[-1] != "'" or len(tk) == 1:
                    # terminal contains whitespaces
                    concat = tk
                    while True:
                        nextTK()
                        concat += ' ' + tk
                        if tk[-1] == "'":
                            break
                    inner_expr.append(concat)
                else:
                    inner_expr.append(tk)
                nextTK()
            elif tk.isupper():
                # terminal constant
                inner_expr.append(tk)
                nextTK()
            elif tk.islower():
                # non-terminal
                inner_expr.append(tk)
                nextTK()
            else:
                if len(expr) > 0:
                    expr.append(inner_expr)
                else:
                    expr = inner_expr
                return expr

    nextTK()
    while True:
        # expect non-terminal
        if tk.islower():
            lhs = tk
            nextTK()
        else:
            print(f"ERROR: expected non-terminal but received {tk}.")
        expectTK('::=')
        rhs = parseDerivation()
        rules[lhs] = rhs
        try:
            expectTK(';')
        except StopIteration:
            return rules


def readGrammar(filename=None):
    if filename is None:
        filename = "../doc/syntax-grammar.md"
    with open(filename, 'rt') as f:
        return parse(tokenGenerator(codeGenerator(f)))


def firstSet(G: dict):
    Fi = {}
    for nt in G.keys():
        Fi[nt] = set()

    def first(x: list):
        M = set()
        for rhs in x:
            if type(rhs) is tuple:
                if rhs[0] == 'OPTIONAL' or rhs[0] == 'REPETITION':
                    M = M.union(first(rhs[1]))
                elif rhs[0] == 'GROUPING':
                    M = M.union(first(rhs[1]))
                    break
                else:
                    print("Unexpected Operator" + rhs)
                    exit()
            elif type(rhs) is list:
                M = M.union(first(rhs))
            elif rhs[0] == "'" and rhs[-1] == "'":
                M.add(rhs[1:-1])
                break
            elif rhs[0].isupper():
                M.add(rhs)
                break
            else:
                M = M.union(Fi[rhs])
                break
        return M

    while True:
        Fi_ = Fi.copy()
        for nt in G.keys():
            Fi_[nt] = first(G[nt])
        if Fi_ == Fi:
            return Fi
        Fi = Fi_


def followSet(G: dict, Fi: dict):
    Fo = {}
    for nt in G.keys():
        Fo[nt] = {'EOF'}    # EOF can always follow

    def rule2(pred, succ):
        # handle predecessor
        if type(pred) is tuple:
            rule2(pred[1], succ)
        elif type(pred) is list:
            if type(pred[0]) is list:  # expressions seperated by OR-Operator
                for e in pred:
                    assert type(e) is list
                    follow(e + [succ])
            else:
                follow(pred + [succ])
        elif (pred[0] == "'" and pred[-1] == "'") or pred.isupper():
            # predecessor is a terminal -> skip
            return
        else:
            # handle successor
            if type(succ) is tuple:
                rule2(pred, succ[1])
            elif type(succ) is list:
                if type(succ[0]) is list:  # expressions seperated by OR-Operator
                    for e in succ:
                        assert type(e) is list
                        follow([pred] + e)
                else:
                    follow([pred] + succ)
            elif succ[0] == "'" and succ[-1] == "'":
                # successor is a terminal -> add to follow set of predecessor
                Fo_[pred] = Fo_[pred].union({succ[1:-1]})
            elif succ.isupper():
                # successor is a terminal -> add to follow set of predecessor
                Fo_[pred] = Fo_[pred].union({succ})
            else:
                # successor is a non-terminal -> add first set of successor to the follow set of predecessor
                Fo_[pred] = Fo_[pred].union(Fi[succ])

    def rule3(lhs_, rhs_):
        if type(rhs_) is tuple:
            follow(rhs_[1], lhs_)
        elif type(rhs_) is list:
            if type(rhs_[0]) is list:  # expressions seperated by OR-Operator
                for e in rhs_:
                    assert type(e) is list
                    follow(e, lhs_)
            else:
                follow(rhs_, lhs_)
        elif (rhs_[0] == "'" and rhs_[-1] == "'") or rhs_.isupper():
            # is terminal -> skip
            return
        else:
            Fo_[rhs_] = Fo_[rhs_].union(Fo_[lhs_])

    def follow(rhs: list, lhs=None):
        i = 0
        offset = 1
        while i < len(rhs):
            # REPETITION can fellow itself
            if rhs[i][0] == 'REPETITION':
                rule2(rhs[i][1], rhs[i][1])
            if i + offset < len(rhs):
                if type(rhs[i]) is list and type(rhs[i + offset]):  # expressions seperated by OR-Operator
                    follow(rhs[i], lhs)
                    follow(rhs[i + offset], lhs)
                else:
                    rule2(rhs[i], rhs[i + offset])
                    if type(rhs[i + offset]) is tuple \
                            and (rhs[i + offset][0] == 'OPTIONAL' or rhs[i + offset][0] == 'REPETITION'):
                        offset += 1
                        continue
            elif lhs is not None:
                rule3(lhs, rhs[i])
            i += 1
            offset = 1

    while True:
        Fo_ = Fo.copy()
        for nt in G.keys():
            follow(G[nt], nt)
        if Fo_ == Fo:
            return Fo
        Fo = Fo_


def genTableFile(Fo: dict):
    tk_cache = dict()
    special_tokens = {
        'EOF': {'TK_EOF'},
        'IDENTIFIER': {'TK_IDENTIFIER'},
        'STRING-LITERAL': {'TK_STRING_LITERAL'},
        'INTEGER-CONSTANT': {'TK_DEC_INT', 'TK_OCT_INT', 'TK_HEX_INT'},
        'DECIMAL-CONSTANT': {'TK_DEC_INT'},
        'CONSTANT': {'TK_STRING_LITERAL', 'TK_DEC_INT', 'TK_OCT_INT', 'TK_HEX_INT', 'TK_DEC_FLOAT', 'TK_HEX_FLOAT',
                     'TK_True', 'TK_False', 'TK_DATE', 'TK_DATE_TIME'}
    }

    def getTokenTypes(s: str):
        nonlocal special_tokens
        try:
            return special_tokens[s]
        except KeyError:
            pass
        nonlocal tk_cache
        if s not in tk_cache:
            p = subprocess.run([LEX_BINARY, '-'],
                               input=s, text=True, capture_output=True, timeout=5, check=True)
            tk_cache[s] = p.stdout.split()[2]
        return {tk_cache[s]}

    # create FollowSet.tbl
    with open(FOLLOW_SET_TABLE, 'wt') as f:
        # write header
        f.write('/* vim: set filetype=cpp: */\n\n')
        # convert terminals into `TokenType`
        for key, val in Fo.items():
            f.write(f'M_FOLLOW( {key.upper().replace("-", "_")}, ({{ ')

            token_set = set()
            for e in val:
                token_set = token_set.union(getTokenTypes(e))
            f.write(', '.join(token_set))

            f.write(' }))\n')


# check arguments for filename
if len(sys.argv) > 1 and os.path.isfile(sys.argv[1]):
    grammar = readGrammar(sys.argv[1])
else:
    grammar = readGrammar()
first_set = firstSet(grammar)
follow_set = followSet(grammar, first_set)
genTableFile(follow_set)
